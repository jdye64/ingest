from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.api.deps import ingestor_is_online
from ingest.db.models import IngestorStatus, Document, DocumentStatus, Ingestor, WatchSource, utcnow
from ingest.db.session import get_session
from ingest.pipeline.embedders import build_embedder
from ingest.services.auth import generate_api_key, hash_api_key
from ingest.services.bootstrap import create_watch_source, document_status_counts
from ingest.services.documents import (
    basename,
    document_query_from_params,
    format_bytes,
    list_ingestor_ids,
    query_documents,
    query_qs,
)
from ingest.services.events import get_event_hub
from ingest.services.sources import delete_watch_source, list_source_audit_events, set_source_enabled
from ingest.services.throughput import get_throughput_meter

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["format_bytes"] = format_bytes
templates.env.filters["basename"] = basename

router = APIRouter()


def _timeout(request: Request) -> int:
    return int(request.app.state.settings.ingestor_heartbeat_timeout_seconds)


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
    ingestors = list((await session.execute(select(Ingestor).order_by(Ingestor.created_at.desc()))).scalars().all())
    timeout = _timeout(request)
    ingestor_rows = [
        {
            "ingestor": a,
            "online": ingestor_is_online(a, timeout),
        }
        for a in ingestors
    ]
    online = sum(1 for row in ingestor_rows if row["online"])
    throughput = get_throughput_meter().snapshot(10.0)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "counts": counts,
            "queue_depth": request.app.state.queue.depth,
            "errors": errors,
            "ingestors": ingestor_rows,
            "ingestors_online": online,
            "ingestors_total": len(ingestors),
            "throughput": throughput,
        },
    )


@router.get("/partials/dashboard-stats", response_class=HTMLResponse)
async def dashboard_stats_partial(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    counts = await document_status_counts(session)
    ingestors = list((await session.execute(select(Ingestor))).scalars().all())
    timeout = _timeout(request)
    online = sum(1 for a in ingestors if ingestor_is_online(a, timeout))
    throughput = get_throughput_meter().snapshot(10.0)
    return templates.TemplateResponse(
        request,
        "partials/dashboard_stats.html",
        {
            "counts": counts,
            "queue_depth": request.app.state.queue.depth,
            "ingestors_online": online,
            "ingestors_total": len(ingestors),
            "throughput": throughput,
        },
    )


@router.get("/partials/ingestors-table", response_class=HTMLResponse)
async def ingestors_table_partial(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    ingestors = list((await session.execute(select(Ingestor).order_by(Ingestor.created_at.desc()))).scalars().all())
    timeout = _timeout(request)
    from ingest.api.deps import count_sources_for_ingestor

    rows = []
    for a in ingestors:
        rows.append(
            {
                "ingestor": a,
                "online": ingestor_is_online(a, timeout),
                "source_count": await count_sources_for_ingestor(session, a.id),
            }
        )
    return templates.TemplateResponse(request, "partials/ingestors_table.html", {"rows": rows})


@router.get("/ingestors", response_class=HTMLResponse)
async def ingestors_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    ingestors = list((await session.execute(select(Ingestor).order_by(Ingestor.created_at.desc()))).scalars().all())
    timeout = _timeout(request)
    from ingest.api.deps import count_sources_for_ingestor

    rows = []
    for a in ingestors:
        rows.append(
            {
                "ingestor": a,
                "online": ingestor_is_online(a, timeout),
                "source_count": await count_sources_for_ingestor(session, a.id),
            }
        )
    created_key = request.query_params.get("api_key")
    created_id = request.query_params.get("created")
    return templates.TemplateResponse(
        request,
        "ingestors.html",
        {
            "rows": rows,
            "created_key": created_key,
            "created_id": created_id,
        },
    )


@router.post("/ingestors")
async def ingestors_create(
    request: Request,
    ingestor_id: str = Form(...),
    name: str = Form(...),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    ingestor_id = ingestor_id.strip()
    name = name.strip()
    existing = await session.get(Ingestor, ingestor_id)
    if existing is not None:
        return RedirectResponse(url="/ingestors?error=exists", status_code=303)
    api_key = generate_api_key()
    ingestor = Ingestor(
        id=ingestor_id,
        name=name,
        api_key_hash=hash_api_key(api_key),
        status=IngestorStatus.offline,
        current_activity={},
    )
    session.add(ingestor)
    await session.flush()
    await get_event_hub().publish("ingestor", {"action": "created", "ingestor_id": ingestor.id})
    return RedirectResponse(url=f"/ingestors?created={ingestor_id}&api_key={api_key}", status_code=303)


@router.post("/ingestors/{ingestor_id}/rotate-key")
async def ingestors_rotate_key(
    ingestor_id: str,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    ingestor = await session.get(Ingestor, ingestor_id)
    if ingestor is None:
        return RedirectResponse(url="/ingestors", status_code=303)
    api_key = generate_api_key()
    ingestor.api_key_hash = hash_api_key(api_key)
    ingestor.updated_at = utcnow()
    await session.flush()
    await get_event_hub().publish("ingestor", {"action": "rotated_key", "ingestor_id": ingestor.id})
    return RedirectResponse(url=f"/ingestors?created={ingestor_id}&api_key={api_key}", status_code=303)


@router.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request, session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    sources = list((await session.execute(select(WatchSource).order_by(WatchSource.created_at.desc()))).scalars().all())
    ingestors = list((await session.execute(select(Ingestor).order_by(Ingestor.name))).scalars().all())
    audit_events = await list_source_audit_events(session, limit=50)
    return templates.TemplateResponse(
        request,
        "sources.html",
        {
            "sources": sources,
            "ingestors": ingestors,
            "audit_events": audit_events,
            "deleted_message": request.query_params.get("deleted"),
            "deleted_docs": request.query_params.get("docs"),
        },
    )


@router.post("/sources")
async def sources_create(
    request: Request,
    path: str = Form(...),
    recursive: bool = Form(False),
    ingestor_id: str = Form(""),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    aid = ingestor_id.strip() or None
    await create_watch_source(session, path, recursive=recursive, ingestor_id=aid, actor="portal")
    await session.flush()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None and aid is None:
        await watcher.reload_watches()
    await get_event_hub().publish("source", {"action": "created"})
    return RedirectResponse(url="/sources", status_code=303)


@router.post("/sources/{source_id}/toggle")
async def sources_toggle(
    source_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    source = await session.get(WatchSource, source_id)
    if source is not None:
        await set_source_enabled(session, source, enabled=not source.enabled, actor="portal")
        await session.flush()
        watcher = getattr(request.app.state, "watcher", None)
        if watcher is not None:
            await watcher.reload_watches()
        await get_event_hub().publish(
            "source",
            {"action": "enabled" if source.enabled else "disabled", "source_id": source.id},
        )
    return RedirectResponse(url="/sources", status_code=303)


@router.post("/sources/{source_id}/delete")
async def sources_delete(
    source_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    source = await session.get(WatchSource, source_id)
    if source is None:
        return RedirectResponse(url="/sources", status_code=303)
    result = await delete_watch_source(session, source, request.app.state.lance, actor="portal")
    await session.commit()
    watcher = getattr(request.app.state, "watcher", None)
    if watcher is not None:
        await watcher.reload_watches()
    await get_event_hub().publish(
        "source",
        {
            "action": "deleted",
            "source_id": result["source_id"],
            "documents_removed": result["documents_removed"],
        },
    )
    return RedirectResponse(
        url=f"/sources?deleted={result['path']}&docs={result['documents_removed']}",
        status_code=303,
    )


@router.get("/documents", response_class=HTMLResponse)
async def documents_page(
    request: Request,
    status: str | None = None,
    ingestor_id: str | None = None,
    filename: str | None = None,
    path: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    size_min: str | None = None,
    size_max: str | None = None,
    page: int = 1,
    page_size: int = 50,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    query = document_query_from_params(
        status=status,
        ingestor_id=ingestor_id,
        filename=filename,
        path=path,
        date_from=date_from,
        date_to=date_to,
        size_min=size_min,
        size_max=size_max,
        page=page,
        page_size=page_size,
    )
    docs, total = await query_documents(session, query)
    total_pages = max(1, (total + query.page_size - 1) // query.page_size)
    showing_from = 0 if total == 0 else ((query.page - 1) * query.page_size) + 1
    showing_to = min(query.page * query.page_size, total)
    return templates.TemplateResponse(
        request,
        "documents.html",
        {
            "documents": docs,
            "query": query,
            "total": total,
            "total_pages": total_pages,
            "showing_from": showing_from,
            "showing_to": showing_to,
            "statuses": [s.value for s in DocumentStatus],
            "ingestor_ids": await list_ingestor_ids(session),
            "qs": query_qs(query),
            "prev_qs": query_qs(query, page=max(1, query.page - 1)),
            "next_qs": query_qs(query, page=min(total_pages, query.page + 1)),
        },
    )


@router.get("/partials/documents-table", response_class=HTMLResponse)
async def documents_table_partial(
    request: Request,
    status: str | None = None,
    ingestor_id: str | None = None,
    filename: str | None = None,
    path: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    size_min: str | None = None,
    size_max: str | None = None,
    page: int = 1,
    page_size: int = 50,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    query = document_query_from_params(
        status=status,
        ingestor_id=ingestor_id,
        filename=filename,
        path=path,
        date_from=date_from,
        date_to=date_to,
        size_min=size_min,
        size_max=size_max,
        page=page,
        page_size=page_size,
    )
    docs, total = await query_documents(session, query)
    total_pages = max(1, (total + query.page_size - 1) // query.page_size)
    showing_from = 0 if total == 0 else ((query.page - 1) * query.page_size) + 1
    showing_to = min(query.page * query.page_size, total)
    return templates.TemplateResponse(
        request,
        "partials/documents_table.html",
        {
            "documents": docs,
            "query": query,
            "total": total,
            "total_pages": total_pages,
            "showing_from": showing_from,
            "showing_to": showing_to,
            "qs": query_qs(query),
            "prev_qs": query_qs(query, page=max(1, query.page - 1)),
            "next_qs": query_qs(query, page=min(total_pages, query.page + 1)),
        },
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
        document.updated_at = utcnow()
        await session.flush()
        if document.ingestor_id:
            await get_event_hub().publish(
                "document",
                {"action": "reindex_requested", "document_id": document_id, "ingestor_id": document.ingestor_id},
            )
        else:
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
