from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import SQLModel

from ingest.config import Settings, get_settings

logger = logging.getLogger(__name__)

# create_all() only creates missing tables, so databases created before the
# multi-ingestor feature need these columns backfilled in place.
_INGESTOR_COLUMN_TABLES = ("watch_sources", "documents", "index_runs", "source_audit_events")

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine(settings: Settings | None = None) -> AsyncEngine:
    global _engine, _session_factory
    if _engine is None:
        settings = settings or get_settings()
        settings.ensure_dirs()
        connect_args = {}
        if settings.database_url.startswith("sqlite"):
            connect_args = {"check_same_thread": False}
        _engine = create_async_engine(
            settings.database_url,
            echo=False,
            future=True,
            connect_args=connect_args,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        get_engine()
    assert _session_factory is not None
    return _session_factory


def _drop_column_if_exists(conn, table: str, column: str) -> None:
    columns = {col["name"] for col in inspect(conn).get_columns(table)}
    if column not in columns:
        return
    # SQLite refuses DROP COLUMN while an index still references the column.
    for idx in inspect(conn).get_indexes(table):
        if column in (idx.get("column_names") or []):
            conn.execute(text(f'DROP INDEX IF EXISTS "{idx["name"]}"'))
            logger.info("Dropped index %s before removing %s.%s", idx["name"], table, column)
    try:
        conn.execute(text(f'ALTER TABLE {table} DROP COLUMN "{column}"'))
        logger.info("Dropped stale column %s.%s", table, column)
    except Exception:
        logger.exception("Could not drop stale column %s.%s", table, column)


def _backfill_original_filenames(conn) -> None:
    rows = conn.execute(
        text(
            "SELECT id, path FROM documents "
            "WHERE original_filename IS NULL OR original_filename = ''"
        )
    ).fetchall()
    if not rows:
        return
    for doc_id, path in rows:
        conn.execute(
            text("UPDATE documents SET original_filename = :name WHERE id = :id"),
            {"name": Path(path).name, "id": doc_id},
        )
    logger.info("Backfilled original_filename for %s documents", len(rows))


def _rename_column_if_needed(conn, table: str, old: str, new: str) -> None:
    columns = {col["name"] for col in inspect(conn).get_columns(table)}
    if old in columns and new not in columns:
        conn.execute(text(f'ALTER TABLE {table} RENAME COLUMN "{old}" TO "{new}"'))
        logger.info("Renamed %s.%s -> %s", table, old, new)
    elif old in columns and new in columns:
        # Prefer the new column; drop the stale one after copying nulls if needed.
        conn.execute(
            text(f'UPDATE {table} SET "{new}" = "{old}" WHERE "{new}" IS NULL AND "{old}" IS NOT NULL')
        )
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{table}_{new} ON {table} ({new})"))
        logger.info("Merged stale column %s.%s into %s", table, old, new)
        _drop_column_if_exists(conn, table, old)


def _migrate_agents_to_ingestors(conn) -> None:
    """Rename legacy agent table/columns to ingestor names (SQLite / Postgres)."""
    inspector = inspect(conn)
    tables = set(inspector.get_table_names())

    if "ingest_agents" in tables and "ingestors" not in tables:
        conn.execute(text("ALTER TABLE ingest_agents RENAME TO ingestors"))
        logger.info("Renamed table ingest_agents -> ingestors")
        tables.discard("ingest_agents")
        tables.add("ingestors")

    for table in _INGESTOR_COLUMN_TABLES:
        if table not in tables:
            continue
        _rename_column_if_needed(conn, table, "agent_id", "ingestor_id")
        if table == "documents":
            # Legacy claim column name from the pre-rename schema.
            _rename_column_if_needed(conn, table, "claimed_by_agent_id", "claimed_by_ingestor_id")


def _sync_ingestor_columns(conn) -> None:
    _migrate_agents_to_ingestors(conn)

    inspector = inspect(conn)
    tables = set(inspector.get_table_names())
    for table in _INGESTOR_COLUMN_TABLES:
        if table not in tables:
            continue
        columns = {col["name"] for col in inspector.get_columns(table)}
        if "ingestor_id" not in columns:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN ingestor_id VARCHAR"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{table}_ingestor_id ON {table} (ingestor_id)"))
            logger.info("Added ingestor_id column to %s", table)

    if "documents" in tables:
        columns = {col["name"] for col in inspector.get_columns("documents")}
        if "claimed_by_ingestor_id" not in columns:
            conn.execute(text("ALTER TABLE documents ADD COLUMN claimed_by_ingestor_id VARCHAR"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_documents_claimed_by_ingestor_id "
                    "ON documents (claimed_by_ingestor_id)"
                )
            )
            logger.info("Added claimed_by_ingestor_id column to documents")
        if "claimed_at" not in columns:
            conn.execute(text("ALTER TABLE documents ADD COLUMN claimed_at DATETIME"))
            logger.info("Added claimed_at column to documents")
        if "original_filename" not in columns:
            conn.execute(text("ALTER TABLE documents ADD COLUMN original_filename VARCHAR"))
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_documents_original_filename ON documents (original_filename)")
            )
            logger.info("Added original_filename column to documents")
        if "page_count" not in columns:
            conn.execute(text("ALTER TABLE documents ADD COLUMN page_count INTEGER"))
            logger.info("Added page_count column to documents")
        if "model_invocations" not in columns:
            conn.execute(text("ALTER TABLE documents ADD COLUMN model_invocations JSON DEFAULT '[]'"))
            # Backfill nulls for dialects that ignore DEFAULT on existing rows.
            conn.execute(text("UPDATE documents SET model_invocations = '[]' WHERE model_invocations IS NULL"))
            logger.info("Added model_invocations column to documents")
        _backfill_original_filenames(conn)

    if "index_runs" in tables:
        run_columns = {col["name"] for col in inspect(conn).get_columns("index_runs")}
        if "page_count" not in run_columns:
            conn.execute(text("ALTER TABLE index_runs ADD COLUMN page_count INTEGER"))
            logger.info("Added page_count column to index_runs")
        if "model_invocations" not in run_columns:
            conn.execute(text("ALTER TABLE index_runs ADD COLUMN model_invocations JSON DEFAULT '[]'"))
            conn.execute(text("UPDATE index_runs SET model_invocations = '[]' WHERE model_invocations IS NULL"))
            logger.info("Added model_invocations column to index_runs")

    if "watch_sources" not in tables:
        return
    # Uniqueness moved from path alone to (ingestor_id, path) so the same path can
    # be owned by the local server and by ingestors on different hosts.
    stale_unique = any(
        idx["name"] == "ix_watch_sources_path" and idx.get("unique")
        for idx in inspector.get_indexes("watch_sources")
    )
    if stale_unique:
        conn.execute(text("DROP INDEX ix_watch_sources_path"))
        conn.execute(text("CREATE INDEX ix_watch_sources_path ON watch_sources (path)"))
        logger.info("Relaxed unique index on watch_sources.path")
    # Drop legacy unique index name if present
    conn.execute(text("DROP INDEX IF EXISTS uq_watch_source_agent_path"))
    conn.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_watch_source_ingestor_path "
            "ON watch_sources (ingestor_id, path)"
        )
    )

    # Orphaned index names from the pre-rename schema (SQLite keeps names across RENAME TABLE/COLUMN).
    for stale in (
        "ix_ingest_agents_api_key_hash",
        "ix_ingest_agents_status",
        "ix_ingest_agents_name",
        "ix_watch_sources_agent_id",
        "ix_documents_agent_id",
        "ix_index_runs_agent_id",
        "ix_source_audit_events_agent_id",
        "ix_documents_claimed_by_agent_id",
    ):
        conn.execute(text(f'DROP INDEX IF EXISTS "{stale}"'))
    if "ingestors" in tables:
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ingestors_name ON ingestors (name)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ingestors_api_key_hash ON ingestors (api_key_hash)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ingestors_status ON ingestors (status)"))
    for table in ("watch_sources", "documents", "index_runs", "source_audit_events"):
        if table in tables:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{table}_ingestor_id ON {table} (ingestor_id)"))
    if "documents" in tables:
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_documents_claimed_by_ingestor_id "
                "ON documents (claimed_by_ingestor_id)"
            )
        )


async def init_db(settings: Settings | None = None) -> None:
    engine = get_engine(settings)
    async with engine.begin() as conn:
        await conn.run_sync(_sync_ingestor_columns)
        await conn.run_sync(SQLModel.metadata.create_all)


async def dispose_db() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()
