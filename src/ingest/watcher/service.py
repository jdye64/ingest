from __future__ import annotations

import asyncio
import fnmatch
import logging
from pathlib import Path

from sqlalchemy import select
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from ingest.config import Settings
from ingest.db.models import Document, DocumentStatus, WatchSource
from ingest.db.session import session_scope
from ingest.services.queue import IngestQueue, mark_document_deleted, upsert_document_for_path
from ingest.vectors.lancedb_store import LanceStore

logger = logging.getLogger(__name__)


def _match_globs(path: Path, include: str | None, exclude: str | None) -> bool:
    name = path.name
    includes = [g.strip() for g in (include or "*").split(",") if g.strip()]
    excludes = [g.strip() for g in (exclude or "").split(",") if g.strip()]
    if includes and not any(fnmatch.fnmatch(name, g) for g in includes):
        return False
    if any(fnmatch.fnmatch(name, g) for g in excludes):
        return False
    return True


class _Handler(FileSystemEventHandler):
    def __init__(self, service: "WatcherService", source_id: str) -> None:
        super().__init__()
        self.service = service
        self.source_id = source_id

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self.service.schedule_path(self.source_id, Path(str(event.src_path)))

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self.service.schedule_path(self.source_id, Path(str(event.src_path)))

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self.service.schedule_deleted(self.source_id, Path(str(event.src_path)))
        dest = getattr(event, "dest_path", None)
        if dest:
            self.service.schedule_path(self.source_id, Path(str(dest)))

    def on_deleted(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self.service.schedule_deleted(self.source_id, Path(str(event.src_path)))


class WatcherService:
    def __init__(
        self,
        settings: Settings,
        queue: IngestQueue,
        lance: LanceStore,
    ) -> None:
        self.settings = settings
        self.queue = queue
        self.lance = lance
        self._observer = Observer()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reconcile_task: asyncio.Task | None = None
        self._started = False

    def schedule_path(self, source_id: str, path: Path) -> None:
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._handle_path(source_id, path), self._loop)

    def schedule_deleted(self, source_id: str, path: Path) -> None:
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._handle_deleted(source_id, path), self._loop)

    async def start(self) -> None:
        if self._started:
            return
        self._loop = asyncio.get_running_loop()
        await self.reload_watches()
        self._reconcile_task = asyncio.create_task(self._reconcile_loop(), name="ingest-reconcile")
        self._started = True
        logger.info("Watcher service started")

    async def stop(self) -> None:
        if self._reconcile_task:
            self._reconcile_task.cancel()
            try:
                await self._reconcile_task
            except asyncio.CancelledError:
                pass
            self._reconcile_task = None
        if self._observer.is_alive():
            self._observer.stop()
            self._observer.join(timeout=5)
        self._observer = Observer()
        self._started = False
        logger.info("Watcher service stopped")

    async def reload_watches(self) -> None:
        if self._observer.is_alive():
            self._observer.unschedule_all()
        else:
            # Stopped observers cannot be restarted; always use a fresh instance.
            self._observer = Observer()

        async with session_scope() as session:
            result = await session.execute(
                select(WatchSource).where(WatchSource.enabled.is_(True), WatchSource.ingestor_id.is_(None))
            )
            sources = list(result.scalars().all())

        for source in sources:
            path = Path(source.path)
            if not path.exists():
                logger.warning("Watch source missing on disk: %s", source.path)
                continue
            handler = _Handler(self, source.id)
            self._observer.schedule(handler, str(path), recursive=source.recursive)
            logger.info("Watching %s (recursive=%s)", path, source.recursive)

        if not self._observer.is_alive():
            self._observer.start()

        await self.reconcile_once()
    async def _handle_path(self, source_id: str, path: Path) -> None:
        try:
            async with session_scope() as session:
                source = await session.get(WatchSource, source_id)
                if source is None or not source.enabled:
                    return
                if not _match_globs(path, source.include_globs, source.exclude_globs):
                    return
                document = await upsert_document_for_path(
                    session,
                    source,
                    path,
                    supported_extensions=self.settings.supported_extensions,
                )
                if document is not None:
                    await self.queue.enqueue(document.id)
        except Exception:
            logger.exception("Failed handling path %s", path)

    async def _handle_deleted(self, source_id: str, path: Path) -> None:
        try:
            resolved = str(path.resolve()) if path.exists() else str(Path(path).absolute())
            async with session_scope() as session:
                result = await session.execute(
                    select(Document).where(Document.source_id == source_id, Document.path == resolved)
                )
                document = result.scalars().first()
                if document is None:
                    # Try non-resolved absolute
                    result = await session.execute(
                        select(Document).where(Document.source_id == source_id, Document.path == str(path))
                    )
                    document = result.scalars().first()
                if document is None:
                    return
                await mark_document_deleted(session, document, self.lance)
        except Exception:
            logger.exception("Failed handling delete for %s", path)

    async def _reconcile_loop(self) -> None:
        interval = max(5, self.settings.reconcile_interval_seconds)
        while True:
            await asyncio.sleep(interval)
            try:
                await self.reconcile_once()
            except Exception:
                logger.exception("Reconcile failed")

    async def reconcile_once(self) -> None:
        async with session_scope() as session:
            result = await session.execute(
                select(WatchSource).where(WatchSource.enabled.is_(True), WatchSource.ingestor_id.is_(None))
            )
            sources = list(result.scalars().all())

            for source in sources:
                root = Path(source.path)
                if not root.exists():
                    continue
                seen_paths: set[str] = set()
                paths = root.rglob("*") if source.recursive else root.glob("*")
                for path in paths:
                    if not path.is_file():
                        continue
                    if not _match_globs(path, source.include_globs, source.exclude_globs):
                        continue
                    if path.suffix.lower() not in self.settings.supported_extensions:
                        continue
                    resolved = str(path.resolve())
                    seen_paths.add(resolved)
                    document = await upsert_document_for_path(
                        session,
                        source,
                        path,
                        supported_extensions=self.settings.supported_extensions,
                    )
                    if document is not None:
                        await self.queue.enqueue(document.id)

                # Mark vanished files deleted
                docs = await session.execute(
                    select(Document).where(
                        Document.source_id == source.id,
                        Document.status != DocumentStatus.deleted,
                    )
                )
                for document in docs.scalars().all():
                    if document.path not in seen_paths and not Path(document.path).exists():
                        await mark_document_deleted(session, document, self.lance)
