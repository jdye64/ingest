from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingest.config import Settings
from ingest.pipeline.chunkers import chunk_text, estimate_tokens
from ingest.pipeline.embedders import build_embedder
from ingest.pipeline.parsers import count_pages, parse_file
from ingest.services.metadata import embedder_model_invocation, original_filename_from_path
from ingest.vectors.lancedb_store import new_chunk_id
from ingest.watcher.hasher import sha256_file


@dataclass
class LocalIndexResult:
    content_sha256: str
    config_id: str
    chunks: list[dict[str, Any]]
    started_at: str
    page_count: int = 1
    original_filename: str = ""
    size_bytes: int = 0
    model_invocations: list[dict[str, Any]] = field(default_factory=list)


def settings_from_index_config(config: dict[str, Any], base: Settings | None = None) -> Settings:
    """Build embedder settings from server index config JSON."""
    cfg = config.get("config_json") or config
    embedder = cfg.get("embedder") or {}
    data = {
        "embedder_provider": embedder.get("provider", "deterministic"),
        "embedder_model": embedder.get("model", "hash-384"),
        "embedder_dimension": int(embedder.get("dimension", 384)),
        "openai_base_url": embedder.get("openai_base_url", "https://api.openai.com/v1"),
        "openai_model": embedder.get("openai_model", "text-embedding-3-small"),
        "chunk_size": int(cfg.get("chunk_size", 800)),
        "chunk_overlap": int(cfg.get("chunk_overlap", 100)),
        "lance_table": cfg.get("lance_table", "chunks"),
    }
    if base is not None:
        data["openai_api_key"] = base.openai_api_key
    return Settings(**data)


def index_file_locally(
    path: Path,
    *,
    config: dict[str, Any],
    settings: Settings,
    extra_model_invocations: list[dict[str, Any]] | None = None,
) -> LocalIndexResult:
    config_id = config["id"]
    cfg = config.get("config_json") or {}
    chunk_size = int(cfg.get("chunk_size", settings.chunk_size))
    overlap = int(cfg.get("chunk_overlap", settings.chunk_overlap))
    embedder_cfg = cfg.get("embedder") or {}

    started = datetime.now(timezone.utc).replace(tzinfo=None)
    content_sha = sha256_file(path)
    size_bytes = path.stat().st_size
    text = parse_file(path)
    chunks = chunk_text(text, chunk_size=chunk_size, overlap=overlap)
    if not chunks:
        raise ValueError("Document produced no text chunks")

    embedder = build_embedder(settings, embedder_cfg)
    vectors = embedder.embed(chunks)
    payload_chunks: list[dict[str, Any]] = []
    for idx, (chunk, vector) in enumerate(zip(chunks, vectors, strict=True)):
        payload_chunks.append(
            {
                "chunk_index": idx,
                "chunk_id": new_chunk_id(),
                "text": chunk,
                "vector": list(vector),
                "token_estimate": estimate_tokens(chunk),
                "metadata": {"config_id": config_id},
            }
        )
    invocations = list(extra_model_invocations or [])
    invocations.append(
        embedder_model_invocation(
            provider=str(embedder_cfg.get("provider") or settings.embedder_provider),
            model=str(embedder_cfg.get("model") or settings.embedder_model),
            chunk_count=len(payload_chunks),
        )
    )
    return LocalIndexResult(
        content_sha256=content_sha,
        config_id=config_id,
        chunks=payload_chunks,
        started_at=started.isoformat(),
        page_count=count_pages(path),
        original_filename=original_filename_from_path(path),
        size_bytes=size_bytes,
        model_invocations=invocations,
    )
