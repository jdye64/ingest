from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server.mcpserver import MCPServer
from sqlalchemy import func, select

from ingest.config import Settings, get_settings
from ingest.db.models import Document, DocumentStatus, IndexConfig, IndexRun, WatchSource
from ingest.db.session import init_db, session_scope
from ingest.pipeline.embedders import build_embedder
from ingest.services.bootstrap import ensure_default_index_config, ensure_watch_sources
from ingest.services.queue import IngestQueue
from ingest.vectors.lancedb_store import LanceStore

logger = logging.getLogger(__name__)


def create_mcp_server(
    settings: Settings | None = None,
    *,
    lance: LanceStore | None = None,
    queue: IngestQueue | None = None,
) -> MCPServer:
    settings = settings or get_settings()
    settings.ensure_dirs()
    lance = lance or LanceStore(settings.lancedb_path, settings.lance_table, settings.embedder_dimension)
    queue = queue or IngestQueue(maxsize=settings.queue_maxsize)

    mcp = MCPServer("ingest")

    @mcp.tool()
    async def list_documents(status: str | None = None, limit: int = 50) -> str:
        """List ingested documents with optional status filter."""
        async with session_scope() as session:
            stmt = select(Document).order_by(Document.updated_at.desc()).limit(min(limit, 200))
            if status:
                stmt = stmt.where(Document.status == DocumentStatus(status))
            docs = list((await session.execute(stmt)).scalars().all())
            payload = [
                {
                    "id": d.id,
                    "path": d.path,
                    "status": d.status.value,
                    "content_sha256": d.content_sha256,
                    "indexed_at": d.indexed_at.isoformat() if d.indexed_at else None,
                }
                for d in docs
            ]
            return json.dumps(payload, indent=2)

    @mcp.tool()
    async def get_document(document_id: str) -> str:
        """Get document provenance including latest index run and config."""
        async with session_scope() as session:
            document = await session.get(Document, document_id)
            if document is None:
                return json.dumps({"error": "not_found"})
            run = (
                await session.execute(
                    select(IndexRun).where(IndexRun.document_id == document_id).order_by(IndexRun.started_at.desc()).limit(1)
                )
            ).scalars().first()
            config = await session.get(IndexConfig, run.config_id) if run else None
            return json.dumps(
                {
                    "document": {
                        "id": document.id,
                        "path": document.path,
                        "status": document.status.value,
                        "content_sha256": document.content_sha256,
                        "error_message": document.error_message,
                        "indexed_at": document.indexed_at.isoformat() if document.indexed_at else None,
                    },
                    "latest_run": None
                    if run is None
                    else {
                        "id": run.id,
                        "status": run.status.value,
                        "chunk_count": run.chunk_count,
                        "content_sha256": run.content_sha256,
                        "started_at": run.started_at.isoformat(),
                        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                        "notes": run.notes,
                    },
                    "index_config": None
                    if config is None
                    else {
                        "id": config.id,
                        "name": config.name,
                        "version": config.version,
                        "config_json": config.config_json,
                    },
                },
                indent=2,
            )

    @mcp.tool()
    async def search(query: str, limit: int = 10) -> str:
        """Vector search over indexed document chunks."""
        embedder = build_embedder(settings)
        vector = embedder.embed([query])[0]
        hits = lance.search(vector, limit=min(limit, 50))
        return json.dumps({"query": query, "hits": hits}, indent=2)

    @mcp.tool()
    async def list_sources() -> str:
        """List watched directories."""
        async with session_scope() as session:
            sources = list((await session.execute(select(WatchSource).order_by(WatchSource.created_at.desc()))).scalars().all())
            payload = [
                {
                    "id": s.id,
                    "path": s.path,
                    "enabled": s.enabled,
                    "recursive": s.recursive,
                    "include_globs": s.include_globs,
                    "exclude_globs": s.exclude_globs,
                }
                for s in sources
            ]
            return json.dumps(payload, indent=2)

    @mcp.tool()
    async def reindex_document(document_id: str) -> str:
        """Queue a document for forced reindexing."""
        async with session_scope() as session:
            document = await session.get(Document, document_id)
            if document is None:
                return json.dumps({"error": "not_found"})
            document.status = DocumentStatus.pending
            await session.flush()
        queued = await queue.enqueue(document_id, force=True)
        return json.dumps({"status": "queued" if queued else "already_pending", "document_id": document_id})

    @mcp.tool()
    async def status_summary() -> str:
        """Return document status counts."""
        async with session_scope() as session:
            result = await session.execute(select(Document.status, func.count()).group_by(Document.status))
            counts = { (s.value if hasattr(s, "value") else str(s)): c for s, c in result.all() }
        return json.dumps({"counts": counts, "queue_depth": queue.depth}, indent=2)

    return mcp


async def run_mcp_stdio() -> None:
    settings = get_settings()
    settings.ensure_dirs()
    await init_db(settings)
    async with session_scope() as session:
        await ensure_default_index_config(session, settings)
        await ensure_watch_sources(session, settings)
    mcp = create_mcp_server(settings)
    await mcp.run_stdio_async()
