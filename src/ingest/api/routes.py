from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.api.ingestors import router as ingestors_router
from ingest.api.deps import ingestor_is_online
from ingest.api.schemas import (
    DocumentDetailOut,
    DocumentListOut,
    DocumentOut,
    HealthResponse,
    IndexConfigOut,
    IndexRunOut,
    SearchHit,
    SearchResponse,
    SourceAuditEventOut,
    SourceDeleteOut,
    StatusCounts,
    ThroughputOut,
    WatchSourceCreate,
    WatchSourceOut,
)
from ingest.db.models import Document, DocumentStatus, IndexConfig, IndexRun, Ingestor, WatchSource, utcnow
from ingest.db.session import get_session
from ingest.pipeline.embedders import build_embedder
from ingest.services.bootstrap import create_watch_source, document_status_counts
from ingest.services.events import get_event_hub
from ingest.services.sources import delete_watch_source, list_source_audit_events, set_source_enabled
from ingest.services.throughput import get_throughput_meter

router = APIRouter(prefix="/api/v1")
router.include_router(ingestors_router)


def _app_state(request: Request):
    return request.app.state


@router.get("/health", response_model=HealthResponse)
async def health(request: Request, session: AsyncSession = Depends(get_session)) -> HealthResponse:
    state = _app_state(request)
    await session.execute(select(1))
    return HealthResponse(
        status="ok",
        database="ok",
        lancedb=state.lance.health(),
        queue_depth=state.queue.depth,
    )


@router.get("/status", response_model=StatusCounts)
async def status(request: Request, session: AsyncSession = Depends(get_session)) -> StatusCounts:
    state = _app_state(request)
    counts = await document_status_counts(session)
    timeout = state.settings.ingestor_heartbeat_timeout_seconds
    ingestors = list((await session.execute(select(Ingestor))).scalars().all())
    online = sum(1 for a in ingestors if ingestor_is_online(a, timeout))
    return StatusCounts(
        counts=counts,
        queue_depth=state.queue.depth,
        ingestors_online=online,
        ingestors_total=len(ingestors),
    )


@router.get("/throughput", response_model=ThroughputOut)
async def throughput(window_seconds: float = Query(10.0, ge=1.0, le=120.0)) -> ThroughputOut:
    snap = get_throughput_meter().snapshot(window_seconds)
    return ThroughputOut(
        pps=snap.pps,
        window_seconds=snap.window_seconds,
        pages_in_window=snap.pages_in_window,
        documents_in_window=snap.documents_in_window,
        by_ingestor=[
            {
                "ingestor_id": row.ingestor_id,
                "pps": row.pps,
                "pages": row.pages,
                "documents": row.documents,
            }
            for row in snap.by_ingestor
        ],
    )


@router.get("/index-config/default", response_model=IndexConfigOut)
async def get_default_index_config(session: AsyncSession = Depends(get_session)) -> IndexConfig:
    result = await session.execute(
        select(IndexConfig).where(IndexConfig.is_default.is_(True)).order_by(IndexConfig.created_at.desc())
    )
    config = result.scalars().first()
    if config is None:
        raise HTTPException(status_code=404, detail="No default index config")
    return config


@router.get("/sources", response_model=list[WatchSourceOut])
async def list_sources(session: AsyncSession = Depends(get_session)) -> list[WatchSource]:
    result = await session.execute(select(WatchSource).order_by(WatchSource.created_at.desc()))
    return list(result.scalars().all())


@router.post("/sources", response_model=WatchSourceOut)
async def add_source(
    payload: WatchSourceCreate,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> WatchSource:
    if payload.ingestor_id:
        ingestor = await session.get(Ingestor, payload.ingestor_id)
        if ingestor is None:
            raise HTTPException(status_code=400, detail="Unknown ingestor_id")
    source = await create_watch_source(
        session,
        payload.path,
        enabled=payload.enabled,
        recursive=payload.recursive,
        include_globs=payload.include_globs,
        exclude_globs=payload.exclude_globs,
        ingestor_id=payload.ingestor_id,
        actor="api",
    )
    await session.flush()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None and source.ingestor_id is None:
        await watcher.reload_watches()
    await get_event_hub().publish("source", {"action": "created", "source_id": source.id, "ingestor_id": source.ingestor_id})
    return source


@router.post("/sources/{source_id}/enable", response_model=WatchSourceOut)
async def enable_source(
    source_id: str,
    request: Request,
    enabled: bool = True,
    session: AsyncSession = Depends(get_session),
) -> WatchSource:
    source = await session.get(WatchSource, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    await set_source_enabled(session, source, enabled=enabled, actor="api")
    await session.flush()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None:
        await watcher.reload_watches()
    await get_event_hub().publish(
        "source",
        {"action": "enabled" if enabled else "disabled", "source_id": source.id, "ingestor_id": source.ingestor_id},
    )
    return source


@router.delete("/sources/{source_id}", response_model=SourceDeleteOut)
async def delete_source(
    source_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> SourceDeleteOut:
    source = await session.get(WatchSource, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    result = await delete_watch_source(session, source, request.app.state.lance, actor="api")
    await session.commit()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None:
        await watcher.reload_watches()
    await get_event_hub().publish(
        "source",
        {
            "action": "deleted",
            "source_id": result["source_id"],
            "path": result["path"],
            "documents_removed": result["documents_removed"],
        },
    )
    return SourceDeleteOut(**{k: result[k] for k in ("source_id", "path", "documents_removed", "chunks_removed", "runs_removed")})


@router.get("/sources/audit", response_model=list[SourceAuditEventOut])
async def source_audit(
    limit: int = Query(100, ge=1, le=500),
    source_id: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> list[SourceAuditEventOut]:
    events = await list_source_audit_events(session, limit=limit, source_id=source_id)
    return [SourceAuditEventOut.model_validate(e) for e in events]


@router.get("/documents", response_model=DocumentListOut)
async def list_documents(
    status: DocumentStatus | None = None,
    source_id: str | None = None,
    ingestor_id: str | None = None,
    filename: str | None = None,
    path: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    size_min: int | None = Query(None, ge=0),
    size_max: int | None = Query(None, ge=0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> DocumentListOut:
    from ingest.services.documents import document_query_from_params, query_documents

    query = document_query_from_params(
        status=status.value if status else None,
        ingestor_id=ingestor_id,
        filename=filename,
        path=path,
        date_from=date_from,
        date_to=date_to,
        size_min=size_min,
        size_max=size_max,
        page=1,
        page_size=limit,
    )
    items, total = await query_documents(
        session,
        query,
        source_id=source_id,
        limit=limit,
        offset=offset,
    )
    return DocumentListOut(items=items, total=total, limit=limit, offset=offset)


@router.get("/documents/{document_id}", response_model=DocumentDetailOut)
async def get_document(document_id: str, session: AsyncSession = Depends(get_session)) -> DocumentDetailOut:
    document = await session.get(Document, document_id)
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")

    run_result = await session.execute(
        select(IndexRun).where(IndexRun.document_id == document_id).order_by(IndexRun.started_at.desc()).limit(1)
    )
    latest_run = run_result.scalars().first()
    config = None
    if latest_run is not None:
        config = await session.get(IndexConfig, latest_run.config_id)

    return DocumentDetailOut(
        **DocumentOut.model_validate(document).model_dump(),
        latest_run=IndexRunOut.model_validate(latest_run) if latest_run else None,
        index_config=IndexConfigOut.model_validate(config) if config else None,
    )


@router.post("/documents/{document_id}/reindex")
async def reindex_document(
    document_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    document = await session.get(Document, document_id)
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")
    if document.ingestor_id:
        document.status = DocumentStatus.pending
        document.updated_at = utcnow()
        await session.flush()
        await get_event_hub().publish(
            "document",
            {"action": "reindex_requested", "document_id": document_id, "ingestor_id": document.ingestor_id},
        )
        return {"status": "pending_ingestor", "document_id": document_id}
    document.status = DocumentStatus.pending
    await session.flush()
    await request.app.state.queue.enqueue(document_id, force=True)
    return {"status": "queued", "document_id": document_id}


@router.get("/search", response_model=SearchResponse)
async def search(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=100),
    source_id: str | None = None,
    path_contains: str | None = None,
) -> SearchResponse:
    state = _app_state(request)
    embedder = build_embedder(state.settings)
    vector = embedder.embed([q])[0]
    rows = state.lance.search(vector, limit=limit, source_id=source_id, path_contains=path_contains)
    hits = [SearchHit(**row) for row in rows]
    return SearchResponse(query=q, hits=hits)


@router.get("/events")
async def events_stream(request: Request) -> StreamingResponse:
    hub = get_event_hub()
    sid, queue = await hub.subscribe()

    async def generator():
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                if event is None:
                    break
                yield event.to_sse()
        finally:
            await hub.unsubscribe(sid)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
