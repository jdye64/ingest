from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Any


def generate_api_key() -> str:
    return secrets.token_urlsafe(32)


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def verify_api_key(api_key: str, api_key_hash: str) -> bool:
    return hmac.compare_digest(hash_api_key(api_key), api_key_hash)


def extract_api_key(authorization: str | None, x_ingestor_key: str | None) -> str | None:
    if x_ingestor_key and x_ingestor_key.strip():
        return x_ingestor_key.strip()
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        return token or None
    return None


def activity_summary(activity: dict[str, Any] | None) -> str:
    if not activity:
        return ""
    stage = activity.get("stage")
    path = activity.get("path")
    if stage and path:
        return f"{stage}: {path}"
    if stage:
        return str(stage)
    return ""
