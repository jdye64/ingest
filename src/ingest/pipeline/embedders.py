from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from typing import Any

import httpx
import numpy as np

from ingest.config import Settings


class Embedder(ABC):
    provider: str
    model: str
    dimension: int

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError

    def describe(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "dimension": self.dimension,
        }


class DeterministicEmbedder(Embedder):
    """Fast, offline embedder for tests and lightweight local use."""

    provider = "deterministic"

    def __init__(self, model: str = "hash-384", dimension: int = 384) -> None:
        self.model = model
        self.dimension = dimension

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._one(text) for text in texts]

    def _one(self, text: str) -> list[float]:
        seed = hashlib.sha256(text.encode("utf-8")).digest()
        rng = np.random.default_rng(int.from_bytes(seed[:8], "little"))
        vec = rng.standard_normal(self.dimension).astype(np.float32)
        norm = float(np.linalg.norm(vec)) or 1.0
        return (vec / norm).tolist()


class LocalSentenceTransformerEmbedder(Embedder):
    provider = "local"

    def __init__(self, model: str = "sentence-transformers/all-MiniLM-L6-v2", dimension: int | None = None) -> None:
        from sentence_transformers import SentenceTransformer

        self.model = model
        self._model = SentenceTransformer(model)
        probed = int(self._model.get_sentence_embedding_dimension())
        self.dimension = dimension or probed
        if dimension is not None and dimension != probed:
            raise ValueError(f"Model dimension {probed} does not match configured {dimension}")

    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors = self._model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
        return [v.tolist() for v in vectors]


class OpenAICompatibleEmbedder(Embedder):
    provider = "openai_compatible"

    def __init__(
        self,
        *,
        model: str,
        dimension: int,
        base_url: str,
        api_key: str,
    ) -> None:
        self.model = model
        self.dimension = dimension
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def embed(self, texts: list[str]) -> list[list[float]]:
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        payload = {"model": self.model, "input": texts}
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(f"{self.base_url}/embeddings", json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()["data"]
            data = sorted(data, key=lambda row: row["index"])
            vectors = [row["embedding"] for row in data]
        for vec in vectors:
            if len(vec) != self.dimension:
                raise ValueError(f"Expected dimension {self.dimension}, got {len(vec)}")
        return vectors


_embedder_cache: dict[str, Embedder] = {}


def build_embedder(settings: Settings, config: dict[str, Any] | None = None) -> Embedder:
    cfg = config or {}
    provider = cfg.get("provider", settings.embedder_provider)
    model = cfg.get("model", settings.embedder_model)
    dimension = int(cfg.get("dimension", settings.embedder_dimension))
    cache_key = f"{provider}:{model}:{dimension}"
    if cache_key in _embedder_cache:
        return _embedder_cache[cache_key]

    if provider == "deterministic":
        embedder: Embedder = DeterministicEmbedder(model=model, dimension=dimension)
    elif provider == "local":
        embedder = LocalSentenceTransformerEmbedder(model=model, dimension=dimension)
    elif provider == "openai_compatible":
        embedder = OpenAICompatibleEmbedder(
            model=cfg.get("model", settings.openai_model),
            dimension=dimension,
            base_url=cfg.get("base_url", settings.openai_base_url),
            api_key=cfg.get("api_key", settings.openai_api_key),
        )
    else:
        raise ValueError(f"Unknown embedder provider: {provider}")

    _embedder_cache[cache_key] = embedder
    return embedder


def clear_embedder_cache() -> None:
    _embedder_cache.clear()
