from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.db.models import Document, DocumentStatus, Ingestor


@dataclass(frozen=True)
class DocumentQuery:
    status: str | None = None
    ingestor_id: str | None = None
    filename: str | None = None
    path: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    size_min: int | None = None
    size_max: int | None = None
    page: int = 1
    page_size: int = 50

    @property
    def offset(self) -> int:
        return max(0, (max(1, self.page) - 1) * self.page_size)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value or not value.strip():
        return None
    raw = value.strip()
    # HTML date input: YYYY-MM-DD
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        return datetime.fromisoformat(raw)
    # datetime-local: YYYY-MM-DDTHH:MM
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _parse_int(value: str | int | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def document_query_from_params(
    *,
    status: str | None = None,
    ingestor_id: str | None = None,
    filename: str | None = None,
    path: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    size_min: str | int | None = None,
    size_max: str | int | None = None,
    page: int = 1,
    page_size: int = 50,
) -> DocumentQuery:
    page = max(1, int(page or 1))
    page_size = min(200, max(1, int(page_size or 50)))
    return DocumentQuery(
        status=(status or None),
        ingestor_id=(ingestor_id or None),
        filename=(filename.strip() if filename and filename.strip() else None),
        path=(path.strip() if path and path.strip() else None),
        date_from=_parse_datetime(date_from),
        date_to=_parse_datetime(date_to),
        size_min=_parse_int(size_min),
        size_max=_parse_int(size_max),
        page=page,
        page_size=page_size,
    )


def apply_document_filters(
    stmt: Select[Any],
    query: DocumentQuery,
    *,
    source_id: str | None = None,
) -> Select[Any]:
    if query.status:
        try:
            stmt = stmt.where(Document.status == DocumentStatus(query.status))
        except ValueError:
            pass
    if query.ingestor_id == "local":
        stmt = stmt.where(Document.ingestor_id.is_(None))
    elif query.ingestor_id:
        stmt = stmt.where(Document.ingestor_id == query.ingestor_id)
    if source_id:
        stmt = stmt.where(Document.source_id == source_id)
    if query.filename:
        pattern = f"%{query.filename}%"
        stmt = stmt.where(
            or_(
                Document.original_filename.ilike(pattern),
                Document.path.ilike(pattern),
            )
        )
    if query.path:
        stmt = stmt.where(Document.path.ilike(f"%{query.path}%"))
    if query.date_from is not None:
        stmt = stmt.where(Document.updated_at >= query.date_from)
    if query.date_to is not None:
        end = query.date_to
        if end.hour == 0 and end.minute == 0 and end.second == 0 and end.microsecond == 0:
            end = end.replace(hour=23, minute=59, second=59)
        stmt = stmt.where(Document.updated_at <= end)
    if query.size_min is not None:
        stmt = stmt.where(Document.size_bytes >= query.size_min)
    if query.size_max is not None:
        stmt = stmt.where(Document.size_bytes <= query.size_max)
    return stmt


async def query_documents(
    session: AsyncSession,
    query: DocumentQuery,
    *,
    source_id: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[Document], int]:
    count_stmt = apply_document_filters(select(func.count()).select_from(Document), query, source_id=source_id)
    use_limit = query.page_size if limit is None else limit
    use_offset = query.offset if offset is None else offset
    stmt = apply_document_filters(
        select(Document).order_by(Document.updated_at.desc()),
        query,
        source_id=source_id,
    ).limit(use_limit).offset(use_offset)

    total = int((await session.execute(count_stmt)).scalar_one())
    items = list((await session.execute(stmt)).scalars().all())
    return items, total


def query_to_params(query: DocumentQuery, *, page: int | None = None) -> dict[str, str]:
    params: dict[str, str] = {}
    if query.status:
        params["status"] = query.status
    if query.ingestor_id:
        params["ingestor_id"] = query.ingestor_id
    if query.filename:
        params["filename"] = query.filename
    if query.path:
        params["path"] = query.path
    if query.date_from:
        params["date_from"] = query.date_from.date().isoformat()
    if query.date_to:
        params["date_to"] = query.date_to.date().isoformat()
    if query.size_min is not None:
        params["size_min"] = str(query.size_min)
    if query.size_max is not None:
        params["size_max"] = str(query.size_max)
    params["page"] = str(page if page is not None else query.page)
    params["page_size"] = str(query.page_size)
    return params


def query_qs(query: DocumentQuery, *, page: int | None = None) -> str:
    return urlencode(query_to_params(query, page=page))


def format_bytes(size: int | None) -> str:
    if size is None:
        return "—"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def basename(path: str) -> str:
    if "\\" in path and path.count("\\") >= path.count("/"):
        return PureWindowsPath(path).name
    return PurePosixPath(path).name


async def list_ingestor_ids(session: AsyncSession) -> list[str]:
    result = await session.execute(select(Ingestor.id).order_by(Ingestor.id))
    return list(result.scalars().all())
