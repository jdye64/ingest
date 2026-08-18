from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from ingest.api.routes import router as api_router
from ingest.config import Settings, get_settings
from ingest.db.session import dispose_db, init_db, session_scope
from ingest.mcp.server import create_mcp_server
from ingest.pipeline.runner import PipelineRunner
from ingest.services.bootstrap import ensure_default_index_config, ensure_watch_sources
from ingest.services.queue import IngestQueue, WorkerPool
from ingest.vectors.lancedb_store import LanceStore
from ingest.watcher.service import WatcherService
from ingest.web.routes import router as web_router

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    settings.ensure_dirs()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        await init_db(settings)
        lance = LanceStore(settings.lancedb_path, settings.lance_table, settings.embedder_dimension)
        queue = IngestQueue(maxsize=settings.queue_maxsize)
        runner = PipelineRunner(settings, lance)
        workers = WorkerPool(settings, queue, runner)
        watcher = WatcherService(settings, queue, lance)

        async with session_scope() as session:
            await ensure_default_index_config(session, settings)
            await ensure_watch_sources(session, settings)

        app.state.settings = settings
        app.state.lance = lance
        app.state.queue = queue
        app.state.runner = runner
        app.state.workers = workers
        app.state.watcher = watcher

        workers.start()
        await watcher.start()

        # Mount MCP streamable HTTP / SSE under /mcp
        mcp = create_mcp_server(settings, lance=lance, queue=queue)
        app.state.mcp = mcp
        try:
            app.mount("/mcp", mcp.streamable_http_app())
        except Exception:
            logger.exception("Failed to mount MCP streamable HTTP app; SSE-only fallback may be unavailable")

        logger.info("Ingest app started on %s:%s", settings.host, settings.port)
        try:
            yield
        finally:
            await watcher.stop()
            await workers.stop()
            await dispose_db()
            logger.info("Ingest app stopped")

    app = FastAPI(title="Ingest", version="0.1.0", lifespan=lifespan)
    app.include_router(api_router)
    app.include_router(web_router)

    @app.get("/docs-ui")
    async def docs_redirect() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    return app


app = create_app()
