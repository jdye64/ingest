from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.config import Settings
from ingest.db.models import Document, DocumentStatus, IndexConfig, SourceAction, WatchSource, utcnow
from ingest.pipeline.registry import default_index_config_payload
from ingest.services.sources import record_source_audit

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
        result = await session.execute(
            select(WatchSource).where(WatchSource.path == resolved, WatchSource.ingestor_id.is_(None))
        )
        source = result.scalars().first()
        if source is None:
            source = WatchSource(path=resolved, enabled=True, recursive=True, ingestor_id=None)
            session.add(source)
            await session.flush()
            await record_source_audit(
                session,
                source_id=source.id,
                path=source.path,
                ingestor_id=None,
                action=SourceAction.created,
                actor="system",
                details={"bootstrap": True, "recursive": True},
            )
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
    ingestor_id: str | None = None,
    actor: str = "portal",
) -> WatchSource:
    raw = path.strip()
    if ingestor_id:
        # Path lives on the remote ingestor host — do not resolve/mkdir locally.
        stored_path = str(Path(raw).expanduser())
    else:
        stored_path = str(Path(raw).expanduser().resolve())
        Path(stored_path).mkdir(parents=True, exist_ok=True)

    stmt = select(WatchSource).where(WatchSource.path == stored_path)
    if ingestor_id is None:
        stmt = stmt.where(WatchSource.ingestor_id.is_(None))
    else:
        stmt = stmt.where(WatchSource.ingestor_id == ingestor_id)
    result = await session.execute(stmt)
    existing = result.scalars().first()
    if existing is not None:
        changed = (
            existing.enabled != enabled
            or existing.recursive != recursive
            or existing.include_globs != include_globs
            or existing.exclude_globs != exclude_globs
            or existing.ingestor_id != ingestor_id
        )
        existing.enabled = enabled
        existing.recursive = recursive
        existing.include_globs = include_globs
        existing.exclude_globs = exclude_globs
        existing.ingestor_id = ingestor_id
        existing.updated_at = utcnow()
        await session.flush()
        if changed:
            await record_source_audit(
                session,
                source_id=existing.id,
                path=existing.path,
                ingestor_id=existing.ingestor_id,
                action=SourceAction.updated,
                actor=actor,
                details={
                    "enabled": enabled,
                    "recursive": recursive,
                    "include_globs": include_globs,
                    "exclude_globs": exclude_globs,
                },
            )
        return existing
    source = WatchSource(
        path=stored_path,
        enabled=enabled,
        recursive=recursive,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        ingestor_id=ingestor_id,
    )
    session.add(source)
    await session.flush()
    await record_source_audit(
        session,
        source_id=source.id,
        path=source.path,
        ingestor_id=source.ingestor_id,
        action=SourceAction.created,
        actor=actor,
        details={
            "enabled": enabled,
            "recursive": recursive,
            "include_globs": include_globs,
            "exclude_globs": exclude_globs,
        },
    )
    return source
