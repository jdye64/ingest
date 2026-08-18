from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.db.models import Document, DocumentStatus, WatchSource
from ingest.db.session import get_session
from ingest.pipeline.embedders import build_embedder
from ingest.services.bootstrap import create_watch_source, document_status_counts

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    counts = await document_status_counts(session)
    errors = list(
        (
            await session.execute(
                select(Document)
                .where(Document.status == DocumentStatus.error)
                .order_by(Document.updated_at.desc())
                .limit(10)
            )
        )
        .scalars()
        .all()
    )
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "counts": counts,
            "queue_depth": request.app.state.queue.depth,
            "errors": errors,
        },
    )


@router.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    sources = list((await session.execute(select(WatchSource).order_by(WatchSource.created_at.desc()))).scalars().all())
    return templates.TemplateResponse(request, "sources.html", {"sources": sources})


@router.post("/sources")
async def sources_create(
    request: Request,
    path: str = Form(...),
    recursive: bool = Form(True),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    await create_watch_source(session, path, recursive=recursive)
    await session.commit()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None:
        await watcher.reload_watches()
    return RedirectResponse(url="/sources", status_code=303)


@router.post("/sources/{source_id}/toggle")
async def sources_toggle(
    source_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    source = await session.get(WatchSource, source_id)
    if source is not None:
        source.enabled = not source.enabled
        await session.commit()
        watcher = getattr(request.app.state, "watcher", None)
        if watcher is not None:
            await watcher.reload_watches()
    return RedirectResponse(url="/sources", status_code=303)


@router.get("/documents", response_class=HTMLResponse)
async def documents_page(
    request: Request,
    status: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    stmt = select(Document).order_by(Document.updated_at.desc()).limit(200)
    if status:
        try:
            stmt = stmt.where(Document.status == DocumentStatus(status))
        except ValueError:
            pass
    docs = list((await session.execute(stmt)).scalars().all())
    return templates.TemplateResponse(
        request,
        "documents.html",
        {"documents": docs, "status": status, "statuses": [s.value for s in DocumentStatus]},
    )


@router.get("/documents/{document_id}", response_class=HTMLResponse)
async def document_detail(
    document_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    from ingest.db.models import IndexConfig, IndexRun

    document = await session.get(Document, document_id)
    if document is None:
        return templates.TemplateResponse(request, "not_found.html", {"message": "Document not found"}, status_code=404)
    runs = list(
        (
            await session.execute(
                select(IndexRun).where(IndexRun.document_id == document_id).order_by(IndexRun.started_at.desc())
            )
        )
        .scalars()
        .all()
    )
    config = None
    if runs:
        config = await session.get(IndexConfig, runs[0].config_id)
    return templates.TemplateResponse(
        request,
        "document_detail.html",
        {"document": document, "runs": runs, "config": config},
    )


@router.post("/documents/{document_id}/reindex")
async def document_reindex(
    document_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    document = await session.get(Document, document_id)
    if document is not None:
        document.status = DocumentStatus.pending
        await session.commit()
        await request.app.state.queue.enqueue(document_id, force=True)
    return RedirectResponse(url=f"/documents/{document_id}", status_code=303)


@router.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, q: str | None = None) -> HTMLResponse:
    hits = []
    if q:
        embedder = build_embedder(request.app.state.settings)
        vector = embedder.embed([q])[0]
        hits = request.app.state.lance.search(vector, limit=20)
    return templates.TemplateResponse(request, "search.html", {"q": q or "", "hits": hits})
