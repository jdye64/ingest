from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.config import Settings
from ingest.db.models import Document, DocumentStatus, WatchSource, utcnow
from ingest.db.session import session_scope
from ingest.pipeline.runner import PipelineRunner
from ingest.services.metadata import original_filename_from_path
from ingest.watcher.hasher import sha256_file

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestJob:
    document_id: str
    force: bool = False


class IngestQueue:
    def __init__(self, maxsize: int = 1000) -> None:
        self._queue: asyncio.Queue[IngestJob | None] = asyncio.Queue(maxsize=maxsize)
        self._pending: set[str] = set()
        self._lock = asyncio.Lock()

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    async def enqueue(self, document_id: str, *, force: bool = False) -> bool:
        async with self._lock:
            if document_id in self._pending and not force:
                return False
            self._pending.add(document_id)
        try:
            self._queue.put_nowait(IngestJob(document_id=document_id, force=force))
            return True
        except asyncio.QueueFull:
            async with self._lock:
                self._pending.discard(document_id)
            logger.warning("Ingest queue full; dropping job for %s", document_id)
            return False

    async def get(self) -> IngestJob | None:
        return await self._queue.get()

    def task_done(self, document_id: str) -> None:
        self._pending.discard(document_id)
        self._queue.task_done()

    async def stop(self, workers: int) -> None:
        for _ in range(workers):
            await self._queue.put(None)


class WorkerPool:
    def __init__(
        self,
        settings: Settings,
        queue: IngestQueue,
        runner: PipelineRunner,
        concurrency: int | None = None,
    ) -> None:
        self.settings = settings
        self.queue = queue
        self.runner = runner
        self.concurrency = concurrency or settings.worker_concurrency
        self._tasks: list[asyncio.Task] = []

    def start(self) -> None:
        for i in range(self.concurrency):
            self._tasks.append(asyncio.create_task(self._worker_loop(i), name=f"ingest-worker-{i}"))

    async def stop(self) -> None:
        await self.queue.stop(self.concurrency)
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _worker_loop(self, worker_id: int) -> None:
        logger.info("Worker %s started", worker_id)
        while True:
            job = await self.queue.get()
            if job is None:
                self.queue.task_done("")
                break
            try:
                await self._process(job)
                from ingest.services.events import get_event_hub

                await get_event_hub().publish(
                    "document",
                    {"action": "indexed_local", "document_id": job.document_id},
                )
            except Exception:
                logger.exception("Worker %s failed job %s", worker_id, job.document_id)
                from ingest.services.events import get_event_hub

                await get_event_hub().publish(
                    "document",
                    {"action": "failed_local", "document_id": job.document_id},
                )
            finally:
                self.queue.task_done(job.document_id)
        logger.info("Worker %s stopped", worker_id)

    async def _process(self, job: IngestJob) -> None:
        async with session_scope() as session:
            document = await session.get(Document, job.document_id)
            if document is None:
                return
            if document.status == DocumentStatus.deleted and not job.force:
                return
            run = await self.runner.index_document(session, document, force=job.force)
            from ingest.services.throughput import get_throughput_meter

            pages = max(1, int(document.page_count or run.page_count or 1))
            get_throughput_meter().record(pages, ingestor_id=document.ingestor_id or "local")


async def upsert_document_for_path(
    session: AsyncSession,
    source: WatchSource,
    path: Path,
    *,
    supported_extensions: tuple[str, ...],
) -> Document | None:
    if not path.is_file():
        return None
    if path.suffix.lower() not in supported_extensions:
        return None

    resolved = str(path.resolve())
    result = await session.execute(
        select(Document).where(Document.source_id == source.id, Document.path == resolved)
    )
    document = result.scalars().first()
    content_sha = sha256_file(path)
    stat = path.stat()

    if document is None:
        document = Document(
            source_id=source.id,
            path=resolved,
            original_filename=original_filename_from_path(resolved),
            content_sha256=content_sha,
            size_bytes=stat.st_size,
            mtime=stat.st_mtime,
            status=DocumentStatus.pending,
            model_invocations=[],
        )
        session.add(document)
        await session.flush()
        return document

    unchanged = (
        document.status == DocumentStatus.ready
        and document.content_sha256 == content_sha
    )
    document.size_bytes = stat.st_size
    document.mtime = stat.st_mtime
    if not document.original_filename:
        document.original_filename = original_filename_from_path(resolved)
    document.updated_at = utcnow()
    if document.status == DocumentStatus.deleted:
        document.status = DocumentStatus.pending
        document.content_sha256 = content_sha
        document.error_message = None
        await session.flush()
        return document
    if unchanged:
        await session.flush()
        return None

    document.content_sha256 = content_sha
    document.status = DocumentStatus.pending
    document.error_message = None
    await session.flush()
    return document


async def mark_document_deleted(session: AsyncSession, document: Document, lance) -> None:
    document.status = DocumentStatus.deleted
    document.updated_at = utcnow()
    document.error_message = None
    lance.delete_document(document.id)
    await session.flush()
