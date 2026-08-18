from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.api.deps import ingestor_is_online, count_sources_for_ingestor, get_authenticated_ingestor
from ingest.api.schemas import (
    IngestorCreate,
    IngestorCreatedOut,
    IngestorDocumentCheckOut,
    IngestorDocumentUpsertIn,
    IngestorDocumentUpsertOut,
    IngestorFailIn,
    IngestorHeartbeatIn,
    IngestorIndexIn,
    IngestorIndexOut,
    IngestorOut,
    WatchSourceOut,
)
from ingest.db.models import (
    IngestorStatus,
    Document,
    DocumentChunk,
    DocumentStatus,
    IndexConfig,
    IndexRun,
    Ingestor,
    RunStatus,
    WatchSource,
    utcnow,
)
from ingest.db.session import get_session
from ingest.services.auth import generate_api_key, hash_api_key
from ingest.services.claims import (
    claim_is_active,
    claim_or_skip_document,
    document_is_already_indexed,
    find_blocking_document_for_path,
    find_document_for_source_path,
    release_claim,
)
from ingest.services.events import get_event_hub
from ingest.services.metadata import embedder_model_invocation, normalize_model_invocations, original_filename_from_path
from ingest.services.throughput import get_throughput_meter
from ingest.vectors.lancedb_store import ChunkRecord

router = APIRouter(prefix="/ingestors", tags=["ingestors"])


def _settings(request: Request):
    return request.app.state.settings


def _timeout(request: Request) -> int:
    return int(getattr(request.app.state.settings, "ingestor_heartbeat_timeout_seconds", 15))


async def _to_ingestor_out(session: AsyncSession, ingestor: Ingestor, timeout: int) -> IngestorOut:
    online = ingestor_is_online(ingestor, timeout)
    effective = IngestorStatus.online if online else (
        IngestorStatus.disabled if ingestor.status == IngestorStatus.disabled else IngestorStatus.offline
    )
    return IngestorOut(
        id=ingestor.id,
        name=ingestor.name,
        hostname=ingestor.hostname,
        status=effective,
        last_heartbeat_at=ingestor.last_heartbeat_at,
        last_seen_ip=ingestor.last_seen_ip,
        current_activity=ingestor.current_activity or {},
        created_at=ingestor.created_at,
        updated_at=ingestor.updated_at,
        online=online,
        source_count=await count_sources_for_ingestor(session, ingestor.id),
    )


@router.post("", response_model=IngestorCreatedOut)
async def create_ingestor(
    payload: IngestorCreate,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> IngestorCreatedOut:
    existing = await session.get(Ingestor, payload.id)
    if existing is not None:
        raise HTTPException(status_code=409, detail="Ingestor id already exists")
    api_key = generate_api_key()
    ingestor = Ingestor(
        id=payload.id,
        name=payload.name,
        api_key_hash=hash_api_key(api_key),
        status=IngestorStatus.offline,
        current_activity={},
    )
    session.add(ingestor)
    await session.flush()
    out = await _to_ingestor_out(session, ingestor, _timeout(request))
    await get_event_hub().publish("ingestor", {"action": "created", "ingestor_id": ingestor.id})
    return IngestorCreatedOut(**out.model_dump(), api_key=api_key)


@router.get("", response_model=list[IngestorOut])
async def list_ingestors(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> list[IngestorOut]:
    result = await session.execute(select(Ingestor).order_by(Ingestor.created_at.desc()))
    ingestors = list(result.scalars().all())
    timeout = _timeout(request)
    return [await _to_ingestor_out(session, a, timeout) for a in ingestors]


@router.post("/me/heartbeat", response_model=IngestorOut)
async def ingestor_heartbeat(
    payload: IngestorHeartbeatIn,
    request: Request,
    ingestor: Ingestor = Depends(get_authenticated_ingestor),
    session: AsyncSession = Depends(get_session),
) -> IngestorOut:
    ingestor.last_heartbeat_at = utcnow()
    ingestor.status = IngestorStatus.online
    ingestor.updated_at = utcnow()
    if payload.hostname:
        ingestor.hostname = payload.hostname
    ingestor.current_activity = payload.current_activity or {}
    ip = getattr(request.state, "ingestor_client_ip", None)
    if ip:
        ingestor.last_seen_ip = ip
    await session.flush()
    await get_event_hub().publish(
        "ingestor",
        {
            "action": "heartbeat",
            "ingestor_id": ingestor.id,
            "activity": ingestor.current_activity,
            "hostname": ingestor.hostname,
        },
    )
    return await _to_ingestor_out(session, ingestor, _timeout(request))


@router.get("/me/sources", response_model=list[WatchSourceOut])
async def ingestor_list_sources(
    ingestor: Ingestor = Depends(get_authenticated_ingestor),
    session: AsyncSession = Depends(get_session),
) -> list[WatchSource]:
    result = await session.execute(
        select(WatchSource)
        .where(WatchSource.ingestor_id == ingestor.id, WatchSource.enabled.is_(True))
        .order_by(WatchSource.created_at.desc())
    )
    return list(result.scalars().all())


@router.get("/me/documents/check", response_model=IngestorDocumentCheckOut)
async def ingestor_check_document(
    path: str,
    request: Request,
    content_sha256: str | None = None,
    source_id: str | None = None,
    ingestor: Ingestor = Depends(get_authenticated_ingestor),
    session: AsyncSession = Depends(get_session),
) -> IngestorDocumentCheckOut:
    """Pre-flight check: is this path already indexed or actively claimed?"""
    timeout = int(request.app.state.settings.ingestor_claim_timeout_seconds)
    document = None
    if source_id:
        source = await session.get(WatchSource, source_id)
        if source is None or source.ingestor_id != ingestor.id:
            raise HTTPException(status_code=404, detail="Source not found for this ingestor")
        document = await find_document_for_source_path(session, source_id, path)

    blocking = await find_blocking_document_for_path(
        session,
        path,
        content_sha256=content_sha256,
        timeout_seconds=timeout,
        exclude_document_id=None,
    )
    target = blocking or document
    already_indexed = bool(target and document_is_already_indexed(target, content_sha256))
    indexing = bool(target and claim_is_active(target, timeout))
    can_claim = not already_indexed and not indexing
    return IngestorDocumentCheckOut(
        path=path,
        content_sha256=content_sha256,
        already_indexed=already_indexed,
        indexing_in_progress=indexing,
        claimed_by_ingestor_id=target.claimed_by_ingestor_id if target else None,
        document_id=target.id if target else None,
        status=target.status if target else None,
        can_claim=can_claim,
    )


@router.post("/me/documents/upsert", response_model=IngestorDocumentUpsertOut)
async def ingestor_upsert_document(
    payload: IngestorDocumentUpsertIn,
    request: Request,
    ingestor: Ingestor = Depends(get_authenticated_ingestor),
    session: AsyncSession = Depends(get_session),
) -> IngestorDocumentUpsertOut:
    source = await session.get(WatchSource, payload.source_id)
    if source is None or source.ingestor_id != ingestor.id:
        raise HTTPException(status_code=404, detail="Source not found for this ingestor")

    lance = request.app.state.lance

    if payload.deleted:
        document = await find_document_for_source_path(session, source.id, payload.path, for_update=True)
        if document is None:
            raise HTTPException(status_code=404, detail="Document not found")
        document.status = DocumentStatus.deleted
        document.error_message = None
        document.ingestor_id = ingestor.id
        release_claim(document)
        lance.delete_document(document.id)
        await session.flush()
        await get_event_hub().publish(
            "document",
            {"action": "deleted", "document_id": document.id, "ingestor_id": ingestor.id, "status": "deleted"},
        )
        return IngestorDocumentUpsertOut(
            document_id=document.id,
            status=document.status,
            needs_index=False,
            claimed=False,
            reason="deleted",
            claimed_by_ingestor_id=None,
        )

    timeout = int(request.app.state.settings.ingestor_claim_timeout_seconds)
    document, needs_index, reason = await claim_or_skip_document(
        session,
        source=source,
        ingestor_id=ingestor.id,
        path=payload.path,
        content_sha256=payload.content_sha256,
        size_bytes=payload.size_bytes,
        mtime=payload.mtime,
        timeout_seconds=timeout,
    )

    await get_event_hub().publish(
        "document",
        {
            "action": "claim" if needs_index else "skip",
            "document_id": document.id,
            "ingestor_id": ingestor.id,
            "status": document.status.value,
            "needs_index": needs_index,
            "reason": reason,
            "claimed_by_ingestor_id": document.claimed_by_ingestor_id,
        },
    )
    return IngestorDocumentUpsertOut(
        document_id=document.id,
        status=document.status,
        needs_index=needs_index,
        claimed=needs_index and reason == "claimed",
        reason=reason,
        claimed_by_ingestor_id=document.claimed_by_ingestor_id,
    )


@router.post("/me/documents/{document_id}/index", response_model=IngestorIndexOut)
async def ingestor_index_document(
    document_id: str,
    payload: IngestorIndexIn,
    request: Request,
    ingestor: Ingestor = Depends(get_authenticated_ingestor),
    session: AsyncSession = Depends(get_session),
) -> IngestorIndexOut:
    timeout = int(request.app.state.settings.ingestor_claim_timeout_seconds)
    result = await session.execute(select(Document).where(Document.id == document_id).with_for_update())
    document = result.scalars().first()
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found for this ingestor")

    # Only the ingestor that currently holds the claim may write vectors.
    owns_claim = (
        document.claimed_by_ingestor_id == ingestor.id
        and document.status == DocumentStatus.indexing
        and claim_is_active(document, timeout)
    )
    if not owns_claim:
        raise HTTPException(
            status_code=409,
            detail=(
                "Document is not claimed by this ingestor for indexing "
                f"(status={document.status.value}, claimed_by={document.claimed_by_ingestor_id})"
            ),
        )

    config = await session.get(IndexConfig, payload.config_id)
    if config is None:
        raise HTTPException(status_code=400, detail="Unknown index config")

    settings = _settings(request)
    expected_dim = int((config.config_json or {}).get("embedder", {}).get("dimension", settings.embedder_dimension))
    if not payload.chunks:
        raise HTTPException(status_code=400, detail="No chunks provided")
    for chunk in payload.chunks:
        if len(chunk.vector) != expected_dim:
            raise HTTPException(
                status_code=400,
                detail=f"Vector dimension mismatch: got {len(chunk.vector)}, expected {expected_dim}",
            )

    document.content_sha256 = payload.content_sha256
    document.ingestor_id = ingestor.id
    document.updated_at = utcnow()
    document.error_message = None
    if payload.original_filename:
        document.original_filename = payload.original_filename
    elif not document.original_filename:
        document.original_filename = original_filename_from_path(document.path)
    if payload.size_bytes is not None:
        document.size_bytes = payload.size_bytes

    invocations = normalize_model_invocations(payload.model_invocations)
    if not invocations:
        embedder_cfg = (config.config_json or {}).get("embedder") or {}
        invocations = [
            embedder_model_invocation(
                provider=str(embedder_cfg.get("provider") or settings.embedder_provider),
                model=str(embedder_cfg.get("model") or settings.embedder_model),
                chunk_count=len(payload.chunks),
            )
        ]

    run = IndexRun(
        document_id=document.id,
        config_id=config.id,
        ingestor_id=ingestor.id,
        content_sha256=payload.content_sha256,
        status=RunStatus.running,
        lance_table=settings.lance_table,
        started_at=payload.started_at or utcnow(),
        page_count=payload.page_count,
        model_invocations=invocations,
    )
    session.add(run)
    await session.flush()

    lance = request.app.state.lance
    records: list[ChunkRecord] = []
    for chunk in payload.chunks:
        records.append(
            ChunkRecord(
                chunk_id=chunk.chunk_id,
                document_id=document.id,
                run_id=run.id,
                source_id=document.source_id,
                path=document.path,
                chunk_index=chunk.chunk_index,
                text=chunk.text,
                vector=chunk.vector,
                content_sha256=payload.content_sha256,
                metadata={**chunk.metadata, "config_id": config.id, "ingestor_id": ingestor.id},
            )
        )
        session.add(
            DocumentChunk(
                document_id=document.id,
                run_id=run.id,
                chunk_index=chunk.chunk_index,
                chunk_id=chunk.chunk_id,
                token_estimate=chunk.token_estimate,
            )
        )

    try:
        lance.upsert_chunks(records)
        run.status = RunStatus.success
        run.chunk_count = len(records)
        run.page_count = payload.page_count
        run.model_invocations = invocations
        run.finished_at = utcnow()
        document.status = DocumentStatus.ready
        document.page_count = payload.page_count
        document.model_invocations = invocations
        document.indexed_at = utcnow()
        document.error_message = None
        release_claim(document)
        # Keep ingestor_id as the ingestor that completed indexing.
        document.ingestor_id = ingestor.id
        await session.flush()
        get_throughput_meter().record(payload.page_count, ingestor_id=ingestor.id)
    except Exception as exc:
        run.status = RunStatus.error
        run.notes = str(exc)
        run.finished_at = utcnow()
        document.status = DocumentStatus.error
        document.error_message = str(exc)
        release_claim(document)
        await session.flush()
        raise HTTPException(status_code=500, detail=f"Failed to write vectors: {exc}") from exc

    await get_event_hub().publish(
        "document",
        {
            "action": "indexed",
            "document_id": document.id,
            "ingestor_id": ingestor.id,
            "status": "ready",
            "chunk_count": len(records),
            "page_count": payload.page_count,
            "model_invocations": invocations,
        },
    )
    return IngestorIndexOut(
        document_id=document.id,
        run_id=run.id,
        status=run.status.value,
        chunk_count=run.chunk_count,
    )


@router.post("/me/documents/{document_id}/fail", response_model=IngestorDocumentUpsertOut)
async def ingestor_fail_document(
    document_id: str,
    payload: IngestorFailIn,
    ingestor: Ingestor = Depends(get_authenticated_ingestor),
    session: AsyncSession = Depends(get_session),
) -> IngestorDocumentUpsertOut:
    result = await session.execute(select(Document).where(Document.id == document_id).with_for_update())
    document = result.scalars().first()
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found for this ingestor")
    if document.claimed_by_ingestor_id and document.claimed_by_ingestor_id != ingestor.id:
        raise HTTPException(status_code=409, detail="Document claim is held by another ingestor")

    document.status = DocumentStatus.error
    document.error_message = payload.error_message
    document.ingestor_id = ingestor.id
    if payload.content_sha256:
        document.content_sha256 = payload.content_sha256
    release_claim(document)

    if payload.config_id:
        run = IndexRun(
            document_id=document.id,
            config_id=payload.config_id,
            ingestor_id=ingestor.id,
            content_sha256=payload.content_sha256,
            status=RunStatus.error,
            notes=payload.error_message,
            finished_at=utcnow(),
        )
        session.add(run)

    await session.flush()
    await get_event_hub().publish(
        "document",
        {"action": "failed", "document_id": document.id, "ingestor_id": ingestor.id, "status": "error"},
    )
    return IngestorDocumentUpsertOut(
        document_id=document.id,
        status=document.status,
        needs_index=False,
        claimed=False,
        reason="failed",
        claimed_by_ingestor_id=None,
    )
@router.get("/{ingestor_id}", response_model=IngestorOut)
async def get_ingestor(
    ingestor_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> IngestorOut:
    ingestor = await session.get(Ingestor, ingestor_id)
    if ingestor is None:
        raise HTTPException(status_code=404, detail="Ingestor not found")
    return await _to_ingestor_out(session, ingestor, _timeout(request))


@router.post("/{ingestor_id}/rotate-key", response_model=IngestorCreatedOut)
async def rotate_ingestor_key(
    ingestor_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> IngestorCreatedOut:
    ingestor = await session.get(Ingestor, ingestor_id)
    if ingestor is None:
        raise HTTPException(status_code=404, detail="Ingestor not found")
    api_key = generate_api_key()
    ingestor.api_key_hash = hash_api_key(api_key)
    ingestor.updated_at = utcnow()
    await session.flush()
    out = await _to_ingestor_out(session, ingestor, _timeout(request))
    await get_event_hub().publish("ingestor", {"action": "rotated_key", "ingestor_id": ingestor.id})
    return IngestorCreatedOut(**out.model_dump(), api_key=api_key)


@router.post("/{ingestor_id}/disable", response_model=IngestorOut)
async def disable_ingestor(
    ingestor_id: str,
    request: Request,
    disabled: bool = True,
    session: AsyncSession = Depends(get_session),
) -> IngestorOut:
    ingestor = await session.get(Ingestor, ingestor_id)
    if ingestor is None:
        raise HTTPException(status_code=404, detail="Ingestor not found")
    ingestor.status = IngestorStatus.disabled if disabled else IngestorStatus.offline
    ingestor.updated_at = utcnow()
    await session.flush()
    await get_event_hub().publish("ingestor", {"action": "disabled" if disabled else "enabled", "ingestor_id": ingestor.id})
    return await _to_ingestor_out(session, ingestor, _timeout(request))


