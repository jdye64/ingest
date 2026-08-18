from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from ingest.app import create_app
from ingest.config import Settings, get_settings
from ingest.db.session import dispose_db
from ingest.pipeline.embedders import clear_embedder_cache
from ingest.ingestor.local_index import index_file_locally


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
        ingestor_heartbeat_timeout_seconds=15,
    )
    settings.ensure_dirs()
    return settings


@pytest.mark.asyncio
async def test_ingestor_round_trip_indexes_into_central_vdb(tmp_settings: Settings, tmp_path: Path) -> None:
    await dispose_db()
    get_settings.cache_clear()
    clear_embedder_cache()

    ingestor_watch = tmp_path / "ingestor_watch"
    ingestor_watch.mkdir()
    doc_path = ingestor_watch / "remote.txt"
    doc_path.write_text("Remote ingestor indexed content about nebulae and stars.", encoding="utf-8")

    app = create_app(tmp_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            created = await client.post("/api/v1/ingestors", json={"id": "edge-1", "name": "Edge One"})
            assert created.status_code == 200, created.text
            body = created.json()
            api_key = body["api_key"]
            headers = {"X-Ingestor-Id": "edge-1", "X-Ingestor-Key": api_key}

            source = await client.post(
                "/api/v1/sources",
                json={"path": str(ingestor_watch), "ingestor_id": "edge-1", "recursive": True},
            )
            assert source.status_code == 200, source.text
            source_id = source.json()["id"]

            hb = await client.post(
                "/api/v1/ingestors/me/heartbeat",
                headers=headers,
                json={"hostname": "testhost", "current_activity": {"stage": "idle"}},
            )
            assert hb.status_code == 200
            assert hb.json()["online"] is True

            sources = await client.get("/api/v1/ingestors/me/sources", headers=headers)
            assert sources.status_code == 200
            assert len(sources.json()) == 1
            assert sources.json()[0]["id"] == source_id

            # Local sources still present for server watcher
            all_sources = await client.get("/api/v1/sources")
            assert any(s["ingestor_id"] is None for s in all_sources.json())
            assert any(s["ingestor_id"] == "edge-1" for s in all_sources.json())

            config = await client.get("/api/v1/index-config/default")
            assert config.status_code == 200
            config_json = config.json()

            upsert = await client.post(
                "/api/v1/ingestors/me/documents/upsert",
                headers=headers,
                json={
                    "source_id": source_id,
                    "path": str(doc_path.resolve()),
                    "content_sha256": "abc",
                    "size_bytes": doc_path.stat().st_size,
                    "mtime": doc_path.stat().st_mtime,
                },
            )
            assert upsert.status_code == 200, upsert.text
            assert upsert.json()["needs_index"] is True
            assert upsert.json()["claimed"] is True
            assert upsert.json()["reason"] == "claimed"
            assert upsert.json()["status"] == "indexing"
            assert upsert.json()["claimed_by_ingestor_id"] == "edge-1"
            document_id = upsert.json()["document_id"]

            check = await client.get(
                "/api/v1/ingestors/me/documents/check",
                headers=headers,
                params={"path": str(doc_path.resolve()), "content_sha256": "abc", "source_id": source_id},
            )
            assert check.status_code == 200
            assert check.json()["indexing_in_progress"] is True
            assert check.json()["can_claim"] is False
            assert check.json()["claimed_by_ingestor_id"] == "edge-1"

            result = index_file_locally(doc_path, config=config_json, settings=tmp_settings)

            # Dimension mismatch rejected while claim is held
            bad_chunks = [
                {
                    **result.chunks[0],
                    "vector": [0.1, 0.2],
                }
            ]
            bad = await client.post(
                f"/api/v1/ingestors/me/documents/{document_id}/index",
                headers=headers,
                json={
                    "config_id": result.config_id,
                    "content_sha256": result.content_sha256,
                    "chunks": bad_chunks,
                },
            )
            assert bad.status_code == 400
            assert "dimension" in bad.json()["detail"].lower()

            indexed = await client.post(
                f"/api/v1/ingestors/me/documents/{document_id}/index",
                headers=headers,
                json={
                    "config_id": result.config_id,
                    "content_sha256": result.content_sha256,
                    "chunks": result.chunks,
                    "started_at": result.started_at,
                    "page_count": result.page_count,
                    "original_filename": result.original_filename,
                    "size_bytes": result.size_bytes,
                    "model_invocations": [
                        {"model": "page_elements_v3", "detection_count": 3, "counts_by_label": {"table": 1, "text": 2}},
                        *result.model_invocations,
                    ],
                },
            )
            assert indexed.status_code == 200, indexed.text
            assert indexed.json()["chunk_count"] >= 1

            docs = await client.get("/api/v1/documents", params={"ingestor_id": "edge-1"})
            assert docs.status_code == 200
            items = docs.json()["items"]
            assert len(items) == 1
            assert items[0]["status"] == "ready"
            assert items[0]["ingestor_id"] == "edge-1"
            assert items[0]["claimed_by_ingestor_id"] is None
            assert items[0]["original_filename"] == "remote.txt"
            assert items[0]["size_bytes"] == result.size_bytes
            assert items[0]["page_count"] == result.page_count
            assert items[0]["model_invocations"][0]["model"] == "page_elements_v3"
            assert items[0]["model_invocations"][0]["detection_count"] == 3
            assert any(inv["model"].startswith("embedder:") for inv in items[0]["model_invocations"])

            detail = await client.get(f"/api/v1/documents/{document_id}")
            assert detail.status_code == 200
            assert detail.json()["page_count"] == result.page_count
            assert detail.json()["latest_run"]["page_count"] == result.page_count
            assert detail.json()["latest_run"]["model_invocations"][0]["detection_count"] == 3

            search = await client.get("/api/v1/search", params={"q": "nebulae"})
            assert search.status_code == 200
            assert search.json()["hits"]

            portal = await client.get("/ingestors")
            assert portal.status_code == 200
            assert "edge-1" in portal.text

            status = await client.get("/api/v1/status")
            assert status.json()["ingestors_total"] >= 1
            assert status.json()["ingestors_online"] >= 1

    await dispose_db()
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_ingestor_offline_after_heartbeat_timeout(tmp_settings: Settings) -> None:
    await dispose_db()
    get_settings.cache_clear()
    tmp_settings.ingestor_heartbeat_timeout_seconds = 1
    app = create_app(tmp_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            created = await client.post("/api/v1/ingestors", json={"id": "brief", "name": "Brief"})
            api_key = created.json()["api_key"]
            headers = {"X-Ingestor-Id": "brief", "X-Ingestor-Key": api_key}
            await client.post("/api/v1/ingestors/me/heartbeat", headers=headers, json={"current_activity": {}})
            online = await client.get("/api/v1/ingestors/brief")
            assert online.json()["online"] is True
            await asyncio.sleep(1.2)
            offline = await client.get("/api/v1/ingestors/brief")
            assert offline.json()["online"] is False

    await dispose_db()
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_local_source_still_ingested_when_ingestor_sources_exist(tmp_settings: Settings, tmp_path: Path) -> None:
    await dispose_db()
    get_settings.cache_clear()
    clear_embedder_cache()

    app = create_app(tmp_settings)
    watch = Path(tmp_settings.watch_path_list[0])
    (watch / "local.md").write_text("# Local\n\nContent about rockets.", encoding="utf-8")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            created = await client.post("/api/v1/ingestors", json={"id": "other", "name": "Other"})
            api_key = created.json()["api_key"]
            remote = tmp_path / "remote_only"
            remote.mkdir()
            await client.post(
                "/api/v1/sources",
                json={"path": str(remote), "ingestor_id": "other"},
                headers={"X-Ingestor-Id": "other", "X-Ingestor-Key": api_key},
            )

            for _ in range(40):
                resp = await client.get("/api/v1/documents")
                data = resp.json()
                if data["total"] >= 1 and any(
                    item["status"] == "ready" and item.get("ingestor_id") is None for item in data["items"]
                ):
                    break
                await asyncio.sleep(0.25)
            else:
                pytest.fail(f"Local document never became ready: {data}")

    await dispose_db()
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_second_ingestor_cannot_claim_same_path(tmp_settings: Settings, tmp_path: Path) -> None:
    await dispose_db()
    get_settings.cache_clear()
    clear_embedder_cache()

    shared = tmp_path / "shared"
    shared.mkdir()
    doc_path = shared / "dup.txt"
    doc_path.write_text("Shared content for race guard.", encoding="utf-8")
    resolved = str(doc_path.resolve())

    app = create_app(tmp_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            a1 = await client.post("/api/v1/ingestors", json={"id": "a1", "name": "A1"})
            a2 = await client.post("/api/v1/ingestors", json={"id": "a2", "name": "A2"})
            key1, key2 = a1.json()["api_key"], a2.json()["api_key"]
            h1 = {"X-Ingestor-Id": "a1", "X-Ingestor-Key": key1}
            h2 = {"X-Ingestor-Id": "a2", "X-Ingestor-Key": key2}

            s1 = await client.post("/api/v1/sources", json={"path": str(shared), "ingestor_id": "a1"})
            s2 = await client.post("/api/v1/sources", json={"path": str(shared), "ingestor_id": "a2"})
            source1, source2 = s1.json()["id"], s2.json()["id"]

            first = await client.post(
                "/api/v1/ingestors/me/documents/upsert",
                headers=h1,
                json={
                    "source_id": source1,
                    "path": resolved,
                    "content_sha256": "sha-shared",
                    "size_bytes": 10,
                    "mtime": 1.0,
                },
            )
            assert first.status_code == 200
            assert first.json()["needs_index"] is True
            assert first.json()["claimed_by_ingestor_id"] == "a1"
            doc_id = first.json()["document_id"]

            second = await client.post(
                "/api/v1/ingestors/me/documents/upsert",
                headers=h2,
                json={
                    "source_id": source2,
                    "path": resolved,
                    "content_sha256": "sha-shared",
                    "size_bytes": 10,
                    "mtime": 1.0,
                },
            )
            assert second.status_code == 200
            assert second.json()["needs_index"] is False
            assert second.json()["reason"] == "claimed_by_other"
            assert second.json()["claimed_by_ingestor_id"] == "a1"
            assert second.json()["document_id"] == doc_id

            # Second ingestor cannot submit an index while the claim is held elsewhere
            reject = await client.post(
                f"/api/v1/ingestors/me/documents/{doc_id}/index",
                headers=h2,
                json={"config_id": "nope", "content_sha256": "sha-shared", "chunks": []},
            )
            assert reject.status_code == 409

            # After first ingestor finishes, second sees already_ready
            config = (await client.get("/api/v1/index-config/default")).json()
            result = index_file_locally(doc_path, config=config, settings=tmp_settings)
            done = await client.post(
                f"/api/v1/ingestors/me/documents/{doc_id}/index",
                headers=h1,
                json={
                    "config_id": result.config_id,
                    "content_sha256": result.content_sha256,
                    "chunks": result.chunks,
                    "started_at": result.started_at,
                },
            )
            assert done.status_code == 200, done.text

            again = await client.post(
                "/api/v1/ingestors/me/documents/upsert",
                headers=h2,
                json={
                    "source_id": source2,
                    "path": resolved,
                    "content_sha256": result.content_sha256,
                    "size_bytes": 10,
                    "mtime": 1.0,
                },
            )
            assert again.status_code == 200
            assert again.json()["needs_index"] is False
            assert again.json()["reason"] == "already_ready"

            docs = await client.get("/api/v1/documents")
            ready = [d for d in docs.json()["items"] if d["path"] == resolved and d["status"] == "ready"]
            assert len(ready) == 1
            assert ready[0]["ingestor_id"] == "a1"
            assert ready[0]["claimed_by_ingestor_id"] is None

    await dispose_db()
    get_settings.cache_clear()
