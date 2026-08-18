from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.db.models import Document, DocumentStatus, WatchSource, utcnow
from ingest.services.metadata import original_filename_from_path


def claim_is_active(document: Document, timeout_seconds: int) -> bool:
    if document.status != DocumentStatus.indexing:
        return False
    if not document.claimed_by_ingestor_id or document.claimed_at is None:
        return False
    age = (utcnow() - document.claimed_at).total_seconds()
    return age <= timeout_seconds


async def find_document_for_source_path(
    session: AsyncSession,
    source_id: str,
    path: str,
    *,
    for_update: bool = False,
) -> Document | None:
    stmt = select(Document).where(Document.source_id == source_id, Document.path == path)
    if for_update:
        stmt = stmt.with_for_update()
    result = await session.execute(stmt)
    return result.scalars().first()


async def find_blocking_document_for_path(
    session: AsyncSession,
    path: str,
    *,
    content_sha256: str | None,
    timeout_seconds: int,
    exclude_document_id: str | None = None,
) -> Document | None:
    """Find another document for the same absolute path that already owns the work."""
    stmt = select(Document).where(Document.path == path, Document.status != DocumentStatus.deleted)
    if exclude_document_id:
        stmt = stmt.where(Document.id != exclude_document_id)
    stmt = stmt.with_for_update()
    result = await session.execute(stmt)
    candidates = list(result.scalars().all())
    for doc in candidates:
        if (
            document_is_already_indexed(doc, content_sha256)
            or claim_is_active(doc, timeout_seconds)
        ):
            return doc
    return None


def document_is_already_indexed(document: Document, content_sha256: str | None) -> bool:
    return (
        document.status == DocumentStatus.ready
        and bool(document.content_sha256)
        and bool(content_sha256)
        and document.content_sha256 == content_sha256
    )


def apply_claim(document: Document, ingestor_id: str, *, content_sha256: str | None) -> None:
    document.ingestor_id = ingestor_id
    document.claimed_by_ingestor_id = ingestor_id
    document.claimed_at = utcnow()
    document.status = DocumentStatus.indexing
    document.error_message = None
    document.updated_at = utcnow()
    if content_sha256:
        document.content_sha256 = content_sha256


def release_claim(document: Document) -> None:
    document.claimed_by_ingestor_id = None
    document.claimed_at = None
    document.updated_at = utcnow()


async def claim_or_skip_document(
    session: AsyncSession,
    *,
    source: WatchSource,
    ingestor_id: str,
    path: str,
    content_sha256: str | None,
    size_bytes: int | None,
    mtime: float | None,
    timeout_seconds: int,
) -> tuple[Document, bool, str]:
    """Atomically decide whether this ingestor should index the path.

    Returns (document, needs_index, reason).
    Reasons: claimed | already_ready | claimed_by_other | already_indexing_self
    """
    document = await find_document_for_source_path(session, source.id, path, for_update=True)

    blocking = await find_blocking_document_for_path(
        session,
        path,
        content_sha256=content_sha256,
        timeout_seconds=timeout_seconds,
        exclude_document_id=document.id if document else None,
    )
    if blocking is not None:
        if document_is_already_indexed(blocking, content_sha256):
            return blocking, False, "already_ready"
        if claim_is_active(blocking, timeout_seconds):
            if blocking.claimed_by_ingestor_id == ingestor_id:
                return blocking, False, "already_indexing_self"
            return blocking, False, "claimed_by_other"

    if document is None:
        document = Document(
            source_id=source.id,
            ingestor_id=ingestor_id,
            path=path,
            original_filename=original_filename_from_path(path),
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            mtime=mtime,
            status=DocumentStatus.pending,
            model_invocations=[],
        )
        session.add(document)
        await session.flush()
    else:
        if document_is_already_indexed(document, content_sha256):
            document.size_bytes = size_bytes
            document.mtime = mtime
            if not document.original_filename:
                document.original_filename = original_filename_from_path(path)
            document.updated_at = utcnow()
            await session.flush()
            return document, False, "already_ready"

        if claim_is_active(document, timeout_seconds):
            document.size_bytes = size_bytes
            document.mtime = mtime
            if not document.original_filename:
                document.original_filename = original_filename_from_path(path)
            document.updated_at = utcnow()
            await session.flush()
            if document.claimed_by_ingestor_id == ingestor_id:
                return document, False, "already_indexing_self"
            return document, False, "claimed_by_other"

        document.size_bytes = size_bytes
        document.mtime = mtime
        if not document.original_filename:
            document.original_filename = original_filename_from_path(path)

    apply_claim(document, ingestor_id, content_sha256=content_sha256)
    await session.flush()
    return document, True, "claimed"
