from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.config import Settings
from ingest.db.models import Document, DocumentStatus, IndexConfig, WatchSource, utcnow
from ingest.pipeline.registry import default_index_config_payload

logger = logging.getLogger(__name__)


async def ensure_default_index_config(session: AsyncSession, settings: Settings) -> IndexConfig:
    result = await session.execute(select(IndexConfig).where(IndexConfig.is_default.is_(True)))
    existing = result.scalars().first()
    if existing is not None:
        return existing
    config = IndexConfig(
        name="default",
        version=1,
        is_default=True,
        config_json=default_index_config_payload(settings),
    )
    session.add(config)
    await session.flush()
    logger.info("Created default index config %s", config.id)
    return config


async def ensure_watch_sources(session: AsyncSession, settings: Settings) -> list[WatchSource]:
    created: list[WatchSource] = []
    for path in settings.watch_path_list:
        path.mkdir(parents=True, exist_ok=True)
        resolved = str(path.resolve())
        result = await session.execute(select(WatchSource).where(WatchSource.path == resolved))
        source = result.scalars().first()
        if source is None:
            source = WatchSource(path=resolved, enabled=True, recursive=True)
            session.add(source)
            created.append(source)
            logger.info("Bootstrapped watch source %s", resolved)
    await session.flush()
    return created


async def document_status_counts(session: AsyncSession) -> dict[str, int]:
    result = await session.execute(select(Document.status, func.count()).group_by(Document.status))
    counts = {status.value if hasattr(status, "value") else str(status): count for status, count in result.all()}
    for status in DocumentStatus:
        counts.setdefault(status.value, 0)
    return counts


async def create_watch_source(
    session: AsyncSession,
    path: str,
    *,
    enabled: bool = True,
    recursive: bool = True,
    include_globs: str = "*",
    exclude_globs: str | None = None,
) -> WatchSource:
    resolved = str(Path(path).expanduser().resolve())
    Path(resolved).mkdir(parents=True, exist_ok=True)
    result = await session.execute(select(WatchSource).where(WatchSource.path == resolved))
    existing = result.scalars().first()
    if existing is not None:
        existing.enabled = enabled
        existing.recursive = recursive
        existing.include_globs = include_globs
        existing.exclude_globs = exclude_globs
        existing.updated_at = utcnow()
        await session.flush()
        return existing
    source = WatchSource(
        path=resolved,
        enabled=enabled,
        recursive=recursive,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
    )
    session.add(source)
    await session.flush()
    return source
