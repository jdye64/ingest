from __future__ import annotations

from typing import Any

from ingest.config import Settings


def default_index_config_payload(settings: Settings) -> dict[str, Any]:
    return {
        "chunk_size": settings.chunk_size,
        "chunk_overlap": settings.chunk_overlap,
        "embedder": {
            "provider": settings.embedder_provider,
            "model": settings.embedder_model,
            "dimension": settings.embedder_dimension,
            "openai_base_url": settings.openai_base_url,
            "openai_model": settings.openai_model,
        },
        "parsers": {
            "supported_extensions": list(settings.supported_extensions),
        },
        "lance_table": settings.lance_table,
    }
