from __future__ import annotations

import asyncio
import fnmatch
import logging
import socket
from pathlib import Path
from typing import Any

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from ingest.ingestor.client import IngestorClient
from ingest.ingestor.local_index import index_file_locally, settings_from_index_config
from ingest.config import Settings, get_settings
from ingest.watcher.hasher import sha256_file

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
    def __init__(self, runner: "IngestorRunner", source_id: str) -> None:
        super().__init__()
        self.runner = runner
        self.source_id = source_id

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.runner.schedule_path(self.source_id, Path(str(event.src_path)))

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.runner.schedule_path(self.source_id, Path(str(event.src_path)))

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self.runner.schedule_deleted(self.source_id, Path(str(event.src_path)))
        dest = getattr(event, "dest_path", None)
        if dest:
            self.runner.schedule_path(self.source_id, Path(str(dest)))

    def on_deleted(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.runner.schedule_deleted(self.source_id, Path(str(event.src_path)))


class IngestorRunner:
    def __init__(
        self,
        client: IngestorClient,
        *,
        settings: Settings | None = None,
        supported_extensions: tuple[str, ...] | None = None,
    ) -> None:
        self.client = client
        self.settings = settings or get_settings()
        self.supported_extensions = supported_extensions or self.settings.supported_extensions
        self.hostname = socket.gethostname()
        self._observer = Observer()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._sources: dict[str, dict[str, Any]] = {}
        self._activity: dict[str, Any] = {"stage": "idle"}
        self._pending: set[str] = set()
        self._lock = asyncio.Lock()
        self._index_config: dict[str, Any] | None = None
        self._local_settings: Settings = self.settings
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    def schedule_path(self, source_id: str, path: Path) -> None:
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._handle_path(source_id, path), self._loop)

    def schedule_deleted(self, source_id: str, path: Path) -> None:
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._handle_deleted(source_id, path), self._loop)

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._tasks = [
            asyncio.create_task(self._heartbeat_loop(), name="ingestor-heartbeat"),
            asyncio.create_task(self._source_sync_loop(), name="ingestor-source-sync"),
            asyncio.create_task(self._reconcile_loop(), name="ingestor-reconcile"),
        ]
        await self._refresh_index_config()
        await self._sync_sources()
        try:
            await self._stop.wait()
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            if self._observer.is_alive():
                self._observer.stop()
                self._observer.join(timeout=5)
            await self.client.aclose()

    def request_stop(self) -> None:
        self._stop.set()

    async def _heartbeat_loop(self) -> None:
        interval = max(2, self.settings.ingestor_heartbeat_interval_seconds)
        while not self._stop.is_set():
            try:
                await self.client.heartbeat(hostname=self.hostname, current_activity=self._activity)
            except Exception:
                logger.exception("Heartbeat failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def _source_sync_loop(self) -> None:
        interval = max(5, self.settings.ingestor_source_sync_seconds)
        while not self._stop.is_set():
            try:
                await self._refresh_index_config()
                await self._sync_sources()
            except Exception:
                logger.exception("Source sync failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def _reconcile_loop(self) -> None:
        interval = max(5, self.settings.reconcile_interval_seconds)
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            try:
                await self._reconcile_once()
            except Exception:
                logger.exception("Ingestor reconcile failed")

    async def _refresh_index_config(self) -> None:
        config = await self.client.get_default_index_config()
        self._index_config = config
        self._local_settings = settings_from_index_config(config, self.settings)
        parsers = (config.get("config_json") or {}).get("parsers") or {}
        exts = parsers.get("supported_extensions")
        if exts:
            self.supported_extensions = tuple(exts)

    async def _sync_sources(self) -> None:
        sources = await self.client.list_sources()
        new_map = {s["id"]: s for s in sources}
        if set(new_map) == set(self._sources) and all(
            new_map[i].get("path") == self._sources[i].get("path")
            and new_map[i].get("recursive") == self._sources[i].get("recursive")
            and new_map[i].get("include_globs") == self._sources[i].get("include_globs")
            and new_map[i].get("exclude_globs") == self._sources[i].get("exclude_globs")
            for i in new_map
        ):
            return
        self._sources = new_map
        if self._observer.is_alive():
            self._observer.unschedule_all()
        else:
            self._observer = Observer()
        for source in self._sources.values():
            path = Path(source["path"])
            if not path.exists():
                logger.warning("Assigned source missing on disk: %s", path)
                continue
            handler = _Handler(self, source["id"])
            self._observer.schedule(handler, str(path), recursive=bool(source.get("recursive", True)))
            logger.info("Ingestor watching %s", path)
        if self._sources and not self._observer.is_alive():
            self._observer.start()
        await self._reconcile_once()

    async def _reconcile_once(self) -> None:
        for source in list(self._sources.values()):
            root = Path(source["path"])
            if not root.exists():
                continue
            paths = root.rglob("*") if source.get("recursive", True) else root.glob("*")
            for path in paths:
                if not path.is_file():
                    continue
                if not _match_globs(path, source.get("include_globs"), source.get("exclude_globs")):
                    continue
                if path.suffix.lower() not in self.supported_extensions:
                    continue
                await self._handle_path(source["id"], path)

    async def _handle_path(self, source_id: str, path: Path) -> None:
        key = f"{source_id}:{path}"
        async with self._lock:
            if key in self._pending:
                return
            self._pending.add(key)
        try:
            source = self._sources.get(source_id)
            if source is None:
                return
            if not path.is_file():
                return
            if not _match_globs(path, source.get("include_globs"), source.get("exclude_globs")):
                return
            if path.suffix.lower() not in self.supported_extensions:
                return

            resolved = str(path.resolve())
            content_sha = sha256_file(path)
            stat = path.stat()
            self._activity = {"stage": "check", "path": resolved, "source_id": source_id}
            check = await self.client.check_document(
                resolved,
                content_sha256=content_sha,
                source_id=source_id,
            )
            if not check.get("can_claim"):
                reason = (
                    "already_indexed"
                    if check.get("already_indexed")
                    else "indexing_in_progress"
                )
                logger.info(
                    "Skipping %s (%s; claimed_by=%s)",
                    resolved,
                    reason,
                    check.get("claimed_by_ingestor_id"),
                )
                self._activity = {
                    "stage": "skipped",
                    "path": resolved,
                    "reason": reason,
                    "claimed_by_ingestor_id": check.get("claimed_by_ingestor_id"),
                }
                return

            self._activity = {"stage": "upsert", "path": resolved, "source_id": source_id}
            upsert = await self.client.upsert_document(
                {
                    "source_id": source_id,
                    "path": resolved,
                    "content_sha256": content_sha,
                    "size_bytes": stat.st_size,
                    "mtime": stat.st_mtime,
                    "deleted": False,
                }
            )
            if not upsert.get("needs_index"):
                logger.info(
                    "Server denied index claim for %s (reason=%s, claimed_by=%s)",
                    resolved,
                    upsert.get("reason"),
                    upsert.get("claimed_by_ingestor_id"),
                )
                self._activity = {
                    "stage": "skipped",
                    "path": resolved,
                    "reason": upsert.get("reason"),
                    "claimed_by_ingestor_id": upsert.get("claimed_by_ingestor_id"),
                }
                return

            document_id = upsert["document_id"]
            if self._index_config is None:
                await self._refresh_index_config()
            assert self._index_config is not None

            self._activity = {
                "stage": "indexing",
                "path": resolved,
                "document_id": document_id,
                "source_id": source_id,
            }
            try:
                result = await asyncio.to_thread(
                    index_file_locally,
                    path,
                    config=self._index_config,
                    settings=self._local_settings,
                )
                await self.client.index_document(
                    document_id,
                    {
                        "config_id": result.config_id,
                        "content_sha256": result.content_sha256,
                        "chunks": result.chunks,
                        "started_at": result.started_at,
                        "page_count": result.page_count,
                        "original_filename": result.original_filename,
                        "size_bytes": result.size_bytes,
                        "model_invocations": result.model_invocations,
                    },
                )
                self._activity = {
                    "stage": "ready",
                    "path": resolved,
                    "document_id": document_id,
                    "chunk_count": len(result.chunks),
                    "page_count": result.page_count,
                }
            except Exception as exc:
                logger.exception("Failed indexing %s", path)
                await self.client.fail_document(
                    document_id,
                    {
                        "error_message": str(exc),
                        "content_sha256": content_sha,
                        "config_id": self._index_config["id"],
                    },
                )
                self._activity = {"stage": "error", "path": resolved, "document_id": document_id, "error": str(exc)}
        except Exception:
            logger.exception("Failed handling path %s", path)
            self._activity = {"stage": "error", "path": str(path)}
        finally:
            async with self._lock:
                self._pending.discard(key)

    async def _handle_deleted(self, source_id: str, path: Path) -> None:
        try:
            resolved = str(path.resolve()) if path.exists() else str(Path(path).absolute())
            self._activity = {"stage": "delete", "path": resolved, "source_id": source_id}
            await self.client.upsert_document(
                {
                    "source_id": source_id,
                    "path": resolved,
                    "deleted": True,
                }
            )
            self._activity = {"stage": "idle"}
        except Exception:
            logger.exception("Failed handling delete for %s", path)
