from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from ingest.app import create_app
from ingest.config import Settings, get_settings
from ingest.db.models import Document, DocumentStatus, WatchSource
from ingest.db.session import dispose_db, session_scope
from ingest.pipeline.embedders import clear_embedder_cache
from ingest.services.documents import document_query_from_params, query_documents


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
async def test_documents_pagination_and_filters(tmp_settings: Settings) -> None:
    await dispose_db()
    get_settings.cache_clear()
    clear_embedder_cache()

    app = create_app(tmp_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            created = await client.post("/api/v1/ingestors", json={"id": "filter-ingestor", "name": "Filter"})
            assert created.status_code == 200

            now = datetime.utcnow().replace(microsecond=0)
            async with session_scope() as session:
                source = WatchSource(path="/tmp/docs", enabled=True, ingestor_id="filter-ingestor")
                session.add(source)
                await session.flush()
                rows = [
                    Document(
                        source_id=source.id,
                        ingestor_id="filter-ingestor",
                        path=f"/data/reports/report-{i}.pdf",
                        content_sha256=f"sha{i}",
                        size_bytes=1000 * (i + 1),
                        status=DocumentStatus.ready,
                        updated_at=now - timedelta(days=i),
                        indexed_at=now - timedelta(days=i),
                    )
                    for i in range(5)
                ]
                rows.append(
                    Document(
                        source_id=source.id,
                        ingestor_id=None,
                        path="/data/notes/local-only.txt",
                        content_sha256="localsha",
                        size_bytes=50,
                        status=DocumentStatus.ready,
                        updated_at=now,
                    )
                )
                session.add_all(rows)

            page1 = await client.get("/api/v1/documents", params={"page_size": 2, "limit": 2, "offset": 0})
            assert page1.status_code == 200
            assert page1.json()["total"] == 6
            assert len(page1.json()["items"]) == 2

            by_name = await client.get("/api/v1/documents", params={"filename": "report-1.pdf"})
            assert by_name.status_code == 200
            assert by_name.json()["total"] == 1
            assert by_name.json()["items"][0]["path"].endswith("report-1.pdf")

            by_path = await client.get("/api/v1/documents", params={"path": "/data/notes"})
            assert by_path.json()["total"] == 1

            by_ingestor = await client.get("/api/v1/documents", params={"ingestor_id": "filter-ingestor"})
            assert by_ingestor.json()["total"] == 5

            by_local = await client.get("/api/v1/documents", params={"ingestor_id": "local"})
            assert by_local.json()["total"] == 1

            by_size = await client.get("/api/v1/documents", params={"size_min": 3000, "size_max": 5000})
            assert by_size.json()["total"] == 3

            portal = await client.get(
                "/documents",
                params={"filename": "report", "page": 1, "page_size": 2},
            )
            assert portal.status_code == 200
            assert "report-0.pdf" in portal.text or "report-1.pdf" in portal.text
            assert "page 1" in portal.text
            assert "of 5" in portal.text

            query = document_query_from_params(filename="report", page=2, page_size=2)
            async with session_scope() as session:
                items, total = await query_documents(session, query)
            assert total == 5
            assert len(items) == 2

    await dispose_db()
    get_settings.cache_clear()
