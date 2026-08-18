from __future__ import annotations

from pathlib import Path
from typing import Any


def original_filename_from_path(path: str | Path) -> str:
    return Path(path).name


def normalize_model_invocations(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Normalize model invocation records to a stable shape.

    Each entry records one model call and how many detections it produced.
    Extra keys (counts_by_label, timing, etc.) are preserved.
    """
    out: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        model = str(item.get("model") or item.get("name") or "").strip()
        if not model:
            continue
        try:
            detection_count = int(item.get("detection_count", 0) or 0)
        except (TypeError, ValueError):
            detection_count = 0
        entry = {**item, "model": model, "detection_count": max(0, detection_count)}
        out.append(entry)
    return out


def embedder_model_invocation(*, provider: str, model: str, chunk_count: int) -> dict[str, Any]:
    """Synthesize an invocation record for the embedder step (no detections)."""
    label = f"embedder:{provider}:{model}" if provider else f"embedder:{model}"
    return {
        "model": label,
        "detection_count": 0,
        "chunk_count": int(chunk_count),
    }
