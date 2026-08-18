from __future__ import annotations

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select

from ingest.app import create_app
from ingest.config import Settings, get_settings
from ingest.db.models import Document, DocumentStatus, IndexRun, SourceAction, SourceAuditEvent, WatchSource
from ingest.db.session import dispose_db, session_scope
from ingest.pipeline.embedders import DeterministicEmbedder, clear_embedder_cache
from ingest.pipeline.runner import PipelineRunner
from ingest.services.bootstrap import ensure_default_index_config
from ingest.services.queue import upsert_document_for_path


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
async def test_delete_source_purges_documents_and_vectors(tmp_settings: Settings, tmp_path: Path) -> None:
    await dispose_db()
    get_settings.cache_clear()
    clear_embedder_cache()

    source_dir = tmp_path / "sensitive"
    source_dir.mkdir()
    doc_path = source_dir / "secret.txt"
    doc_path.write_text("Sensitive content that must be purged.", encoding="utf-8")

    app = create_app(tmp_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            created = await client.post(
                "/api/v1/sources",
                json={"path": str(source_dir), "recursive": True},
            )
            assert created.status_code == 200, created.text
            source_id = created.json()["id"]

            # Index a document under this source
            lance = app.state.lance
            runner = PipelineRunner(tmp_settings, lance)
            async with session_scope() as session:
                await ensure_default_index_config(session, tmp_settings)
                source = await session.get(WatchSource, source_id)
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
                document_id = document.id

            embedder = DeterministicEmbedder(dimension=384)
            assert lance.search(embedder.embed(["Sensitive"])[0], limit=5)

            deleted = await client.delete(f"/api/v1/sources/{source_id}")
            assert deleted.status_code == 200, deleted.text
            body = deleted.json()
            assert body["documents_removed"] == 1
            assert body["source_id"] == source_id

            # Source gone
            sources = await client.get("/api/v1/sources")
            assert all(s["id"] != source_id for s in sources.json())

            async with session_scope() as session:
                assert await session.get(WatchSource, source_id) is None
                assert await session.get(Document, document_id) is None
                runs = list(
                    (
                        await session.execute(select(IndexRun).where(IndexRun.document_id == document_id))
                    )
                    .scalars()
                    .all()
                )
                assert runs == []

                audit = list(
                    (
                        await session.execute(
                            select(SourceAuditEvent)
                            .where(SourceAuditEvent.source_id == source_id)
                            .order_by(SourceAuditEvent.created_at.asc())
                        )
                    )
                    .scalars()
                    .all()
                )
                actions = [e.action for e in audit]
                assert SourceAction.created in actions
                assert SourceAction.deleted in actions
                deleted_event = [e for e in audit if e.action == SourceAction.deleted][0]
                assert deleted_event.details.get("documents_removed") == 1

            hits = lance.search(embedder.embed(["Sensitive"])[0], limit=10, source_id=source_id)
            assert hits == []

            audit_api = await client.get("/api/v1/sources/audit", params={"source_id": source_id})
            assert audit_api.status_code == 200
            assert any(e["action"] == "deleted" for e in audit_api.json())

            portal = await client.get("/sources")
            assert portal.status_code == 200
            assert "Source audit log" in portal.text

    await dispose_db()
    get_settings.cache_clear()
