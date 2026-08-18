from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.api.schemas import (
    DocumentDetailOut,
    DocumentListOut,
    DocumentOut,
    HealthResponse,
    IndexConfigOut,
    IndexRunOut,
    SearchHit,
    SearchResponse,
    StatusCounts,
    WatchSourceCreate,
    WatchSourceOut,
)
from ingest.db.models import Document, DocumentStatus, IndexConfig, IndexRun, WatchSource
from ingest.db.session import get_session
from ingest.pipeline.embedders import build_embedder
from ingest.services.bootstrap import create_watch_source, document_status_counts

router = APIRouter(prefix="/api/v1")


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
    return StatusCounts(counts=counts, queue_depth=state.queue.depth)


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
    source = await create_watch_source(
        session,
        payload.path,
        enabled=payload.enabled,
        recursive=payload.recursive,
        include_globs=payload.include_globs,
        exclude_globs=payload.exclude_globs,
    )
    await session.commit()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None:
        await watcher.reload_watches()
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
    source.enabled = enabled
    await session.commit()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None:
        await watcher.reload_watches()
    return source


@router.get("/documents", response_model=DocumentListOut)
async def list_documents(
    status: DocumentStatus | None = None,
    source_id: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> DocumentListOut:
    filters = []
    if status is not None:
        filters.append(Document.status == status)
    if source_id is not None:
        filters.append(Document.source_id == source_id)

    count_stmt = select(func.count()).select_from(Document)
    stmt = select(Document).order_by(Document.updated_at.desc()).limit(limit).offset(offset)
    if filters:
        count_stmt = count_stmt.where(*filters)
        stmt = stmt.where(*filters)

    total = (await session.execute(count_stmt)).scalar_one()
    items = list((await session.execute(stmt)).scalars().all())
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
    document.status = DocumentStatus.pending
    await session.commit()
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
