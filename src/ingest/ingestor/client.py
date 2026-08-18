from __future__ import annotations

from typing import Any

import httpx


class IngestorClient:
    def __init__(self, server_url: str, ingestor_id: str, api_key: str, *, timeout: float = 60.0) -> None:
        self.server_url = server_url.rstrip("/")
        self.ingestor_id = ingestor_id
        self.api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=self.server_url,
            timeout=timeout,
            headers={
                "X-Ingestor-Id": ingestor_id,
                "X-Ingestor-Key": api_key,
                "Authorization": f"Bearer {api_key}",
            },
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def heartbeat(self, *, hostname: str | None = None, current_activity: dict[str, Any] | None = None) -> dict:
        resp = await self._client.post(
            "/api/v1/ingestors/me/heartbeat",
            json={"hostname": hostname, "current_activity": current_activity or {}},
        )
        resp.raise_for_status()
        return resp.json()

    async def list_sources(self) -> list[dict]:
        resp = await self._client.get("/api/v1/ingestors/me/sources")
        resp.raise_for_status()
        return resp.json()

    async def get_default_index_config(self) -> dict:
        resp = await self._client.get("/api/v1/index-config/default")
        resp.raise_for_status()
        return resp.json()

    async def check_document(
        self,
        path: str,
        *,
        content_sha256: str | None = None,
        source_id: str | None = None,
    ) -> dict:
        params: dict[str, str] = {"path": path}
        if content_sha256:
            params["content_sha256"] = content_sha256
        if source_id:
            params["source_id"] = source_id
        resp = await self._client.get("/api/v1/ingestors/me/documents/check", params=params)
        resp.raise_for_status()
        return resp.json()

    async def upsert_document(self, payload: dict[str, Any]) -> dict:
        resp = await self._client.post("/api/v1/ingestors/me/documents/upsert", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def index_document(self, document_id: str, payload: dict[str, Any]) -> dict:
        resp = await self._client.post(f"/api/v1/ingestors/me/documents/{document_id}/index", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def fail_document(self, document_id: str, payload: dict[str, Any]) -> dict:
        resp = await self._client.post(f"/api/v1/ingestors/me/documents/{document_id}/fail", json=payload)
        resp.raise_for_status()
        return resp.json()
