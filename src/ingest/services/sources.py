from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.db.models import (
    Document,
    DocumentChunk,
    IndexRun,
    SourceAction,
    SourceAuditEvent,
    WatchSource,
    utcnow,
)
from ingest.vectors.lancedb_store import LanceStore

logger = logging.getLogger(__name__)


async def record_source_audit(
    session: AsyncSession,
    *,
    source_id: str,
    path: str,
    action: SourceAction,
    ingestor_id: str | None = None,
    actor: str = "portal",
    details: dict[str, Any] | None = None,
) -> SourceAuditEvent:
    event = SourceAuditEvent(
        source_id=source_id,
        path=path,
        ingestor_id=ingestor_id,
        action=action,
        actor=actor,
        details=details or {},
    )
    session.add(event)
    await session.flush()
    return event


async def set_source_enabled(
    session: AsyncSession,
    source: WatchSource,
    *,
    enabled: bool,
    actor: str = "portal",
) -> WatchSource:
    if source.enabled == enabled:
        return source
    source.enabled = enabled
    source.updated_at = utcnow()
    await session.flush()
    await record_source_audit(
        session,
        source_id=source.id,
        path=source.path,
        ingestor_id=source.ingestor_id,
        action=SourceAction.enabled if enabled else SourceAction.disabled,
        actor=actor,
        details={"enabled": enabled},
    )
    return source


async def delete_watch_source(
    session: AsyncSession,
    source: WatchSource,
    lance: LanceStore,
    *,
    actor: str = "portal",
) -> dict[str, Any]:
    """Permanently remove a source and purge its documents from DB + LanceDB."""
    source_id = source.id
    path = source.path
    ingestor_id = source.ingestor_id

    doc_ids = list(
        (
            await session.execute(select(Document.id).where(Document.source_id == source_id))
        )
        .scalars()
        .all()
    )
    document_count = len(doc_ids)

    # Remove vectors first (by source_id when possible, else per document).
    try:
        lance.delete_source(source_id)
    except Exception:
        logger.exception("Lance bulk delete by source_id failed; falling back per document")
        for document_id in doc_ids:
            try:
                lance.delete_document(document_id)
            except Exception:
                logger.exception("Failed deleting Lance rows for document %s", document_id)

    chunk_count = 0
    run_count = 0
    if doc_ids:
        chunk_count = int(
            (
                await session.execute(
                    select(func.count()).select_from(DocumentChunk).where(DocumentChunk.document_id.in_(doc_ids))
                )
            ).scalar_one()
        )
        run_count = int(
            (
                await session.execute(
                    select(func.count()).select_from(IndexRun).where(IndexRun.document_id.in_(doc_ids))
                )
            ).scalar_one()
        )
        await session.execute(delete(DocumentChunk).where(DocumentChunk.document_id.in_(doc_ids)))
        await session.execute(delete(IndexRun).where(IndexRun.document_id.in_(doc_ids)))
        await session.execute(delete(Document).where(Document.source_id == source_id))

    details = {
        "documents_removed": document_count,
        "chunks_removed": chunk_count,
        "runs_removed": run_count,
        "recursive": source.recursive,
        "include_globs": source.include_globs,
        "exclude_globs": source.exclude_globs,
        "was_enabled": source.enabled,
    }
    await record_source_audit(
        session,
        source_id=source_id,
        path=path,
        ingestor_id=ingestor_id,
        action=SourceAction.deleted,
        actor=actor,
        details=details,
    )
    await session.delete(source)
    await session.flush()
    logger.info(
        "Deleted source %s (%s); removed %s documents from DB/VDB",
        source_id,
        path,
        document_count,
    )
    return {"source_id": source_id, "path": path, **details}


async def list_source_audit_events(
    session: AsyncSession,
    *,
    limit: int = 100,
    source_id: str | None = None,
) -> list[SourceAuditEvent]:
    stmt = select(SourceAuditEvent).order_by(SourceAuditEvent.created_at.desc()).limit(limit)
    if source_id:
        stmt = stmt.where(SourceAuditEvent.source_id == source_id)
    result = await session.execute(stmt)
    return list(result.scalars().all())
