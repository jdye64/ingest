from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from ingest.app import create_app
from ingest.config import Settings
from ingest.db.models import Document, DocumentStatus, WatchSource
from ingest.db.session import dispose_db, init_db, session_scope
from ingest.pipeline.chunkers import chunk_text
from ingest.pipeline.embedders import DeterministicEmbedder, clear_embedder_cache
from ingest.pipeline.parsers import parse_file
from ingest.pipeline.runner import PipelineRunner
from ingest.services.bootstrap import ensure_default_index_config, ensure_watch_sources
from ingest.services.queue import upsert_document_for_path
from ingest.vectors.lancedb_store import LanceStore
from ingest.watcher.hasher import sha256_file


@pytest.fixture
def tmp_settings(tmp_path: Path) -> Settings:
    clear_embedder_cache()
    watch = tmp_path / "watch"
    watch.mkdir()
    settings = Settings(
        data_dir=tmp_path / "data",
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'data' / 'test.db'}",
        lancedb_path=tmp_path / "data" / "lancedb",
        watch_paths=str(watch),
        embedder_provider="deterministic",
        embedder_model="hash-384",
        embedder_dimension=384,
        worker_concurrency=1,
        reconcile_interval_seconds=3600,
    )
    settings.ensure_dirs()
    return settings


@pytest.mark.asyncio
async def test_chunk_and_hash(tmp_path: Path) -> None:
    text = "abcdefghijklmnopqrstuvwxyz" * 40
    chunks = chunk_text(text, chunk_size=50, overlap=10)
    assert len(chunks) > 1
    assert chunks[0]
    f = tmp_path / "a.txt"
    f.write_text("hello", encoding="utf-8")
    assert len(sha256_file(f)) == 64


@pytest.mark.asyncio
async def test_parse_markdown(tmp_path: Path) -> None:
    f = tmp_path / "doc.md"
    f.write_text("# Title\n\nHello world", encoding="utf-8")
    assert "Hello world" in parse_file(f)


@pytest.mark.asyncio
async def test_pipeline_indexes_and_searches(tmp_settings: Settings, tmp_path: Path) -> None:
    await dispose_db()
    await init_db(tmp_settings)
    lance = LanceStore(tmp_settings.lancedb_path, tmp_settings.lance_table, tmp_settings.embedder_dimension)
    runner = PipelineRunner(tmp_settings, lance)

    watch = Path(tmp_settings.watch_path_list[0])
    doc_path = watch / "notes.txt"
    doc_path.write_text("Cats are wonderful companions. Cats purr when happy.", encoding="utf-8")

    async with session_scope() as session:
        await ensure_default_index_config(session, tmp_settings)
        await ensure_watch_sources(session, tmp_settings)
        source = (await session.execute(select(WatchSource))).scalars().first()
        assert source is not None
        document = await upsert_document_for_path(
            session,
            source,
            doc_path,
            supported_extensions=tmp_settings.supported_extensions,
        )
        assert document is not None
        run = await runner.index_document(session, document)
        assert run.chunk_count >= 1
        assert document.status == DocumentStatus.ready

    embedder = DeterministicEmbedder(dimension=384)
    hits = lance.search(embedder.embed(["wonderful companions"])[0], limit=5)
    assert hits
    assert "Cats" in hits[0]["text"] or "cats" in hits[0]["text"].lower()
    await dispose_db()


@pytest.mark.asyncio
async def test_api_health_and_documents(tmp_settings: Settings, tmp_path: Path) -> None:
    await dispose_db()
    # Avoid module-level settings cache affecting create_app
    from ingest.config import get_settings

    get_settings.cache_clear()

    app = create_app(tmp_settings)
    watch = Path(tmp_settings.watch_path_list[0])
    (watch / "hello.md").write_text("# Hello\n\nIndexed content about rockets.", encoding="utf-8")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # lifespan start
        async with app.router.lifespan_context(app):
            # wait briefly for watcher/reconcile to enqueue and worker to process
            for _ in range(40):
                resp = await client.get("/api/v1/documents")
                assert resp.status_code == 200
                data = resp.json()
                if data["total"] >= 1 and any(item["status"] == "ready" for item in data["items"]):
                    break
                await asyncio.sleep(0.25)
            else:
                pytest.fail(f"Document never became ready: {data}")

            health = await client.get("/api/v1/health")
            assert health.status_code == 200
            assert health.json()["status"] == "ok"

            search = await client.get("/api/v1/search", params={"q": "rockets"})
            assert search.status_code == 200
            assert "hits" in search.json()

            portal = await client.get("/")
            assert portal.status_code == 200
            assert "Dashboard" in portal.text

    await dispose_db()
    get_settings.cache_clear()
