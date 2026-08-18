from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.config import Settings
from ingest.db.models import (
    Document,
    DocumentChunk,
    DocumentStatus,
    IndexConfig,
    IndexRun,
    RunStatus,
    WatchSource,
    utcnow,
)
from ingest.pipeline.chunkers import chunk_text, estimate_tokens
from ingest.pipeline.embedders import build_embedder
from ingest.pipeline.parsers import UnsupportedDocumentError, parse_file
from ingest.vectors.lancedb_store import ChunkRecord, LanceStore, new_chunk_id
from ingest.watcher.hasher import sha256_file


class PipelineRunner:
    def __init__(self, settings: Settings, lance: LanceStore) -> None:
        self.settings = settings
        self.lance = lance

    async def get_default_config(self, session: AsyncSession) -> IndexConfig:
        result = await session.execute(
            select(IndexConfig).where(IndexConfig.is_default.is_(True)).order_by(IndexConfig.created_at.desc())
        )
        config = result.scalars().first()
        if config is None:
            raise RuntimeError("No default index config found")
        return config

    async def index_document(
        self,
        session: AsyncSession,
        document: Document,
        *,
        force: bool = False,
        config: IndexConfig | None = None,
    ) -> IndexRun:
        path = Path(document.path)
        if not path.exists() or not path.is_file():
            document.status = DocumentStatus.deleted
            document.error_message = "File missing"
            document.updated_at = utcnow()
            self.lance.delete_document(document.id)
            await session.flush()
            raise FileNotFoundError(document.path)

        content_sha = sha256_file(path)
        stat = path.stat()
        document.size_bytes = stat.st_size
        document.mtime = stat.st_mtime
        document.content_sha256 = content_sha
        document.updated_at = utcnow()

        if (
            not force
            and document.status == DocumentStatus.ready
            and document.content_sha256 == content_sha
        ):
            # Already current; still allow re-run if forced
            pass

        config = config or await self.get_default_config(session)
        cfg = config.config_json or {}
        chunk_size = int(cfg.get("chunk_size", self.settings.chunk_size))
        overlap = int(cfg.get("chunk_overlap", self.settings.chunk_overlap))
        embedder_cfg = cfg.get("embedder", {})

        document.status = DocumentStatus.indexing
        document.error_message = None
        run = IndexRun(
            document_id=document.id,
            config_id=config.id,
            content_sha256=content_sha,
            status=RunStatus.running,
            lance_table=self.settings.lance_table,
        )
        session.add(run)
        await session.flush()

        try:
            text = parse_file(path)
            chunks = chunk_text(text, chunk_size=chunk_size, overlap=overlap)
            if not chunks:
                raise ValueError("Document produced no text chunks")

            embedder = build_embedder(self.settings, embedder_cfg)
            vectors = embedder.embed(chunks)
            records: list[ChunkRecord] = []
            meta_rows: list[DocumentChunk] = []
            for idx, (chunk, vector) in enumerate(zip(chunks, vectors, strict=True)):
                chunk_id = new_chunk_id()
                records.append(
                    ChunkRecord(
                        chunk_id=chunk_id,
                        document_id=document.id,
                        run_id=run.id,
                        source_id=document.source_id,
                        path=document.path,
                        chunk_index=idx,
                        text=chunk,
                        vector=vector,
                        content_sha256=content_sha,
                        metadata={"config_id": config.id, "config_name": config.name},
                    )
                )
                meta_rows.append(
                    DocumentChunk(
                        document_id=document.id,
                        run_id=run.id,
                        chunk_index=idx,
                        chunk_id=chunk_id,
                        token_estimate=estimate_tokens(chunk),
                    )
                )

            self.lance.upsert_chunks(records)
            for row in meta_rows:
                session.add(row)

            run.status = RunStatus.success
            run.chunk_count = len(records)
            run.finished_at = utcnow()
            document.status = DocumentStatus.ready
            document.indexed_at = utcnow()
            document.error_message = None
            await session.flush()
            return run
        except Exception as exc:
            run.status = RunStatus.error
            run.notes = str(exc)
            run.finished_at = utcnow()
            document.status = DocumentStatus.error
            document.error_message = str(exc)
            await session.flush()
            raise
