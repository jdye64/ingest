from __future__ import annotations

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingest.db.models import IngestorStatus, Ingestor, WatchSource, utcnow
from ingest.db.session import get_session
from ingest.services.auth import extract_api_key, verify_api_key


async def get_authenticated_ingestor(
    request: Request,
    session: AsyncSession = Depends(get_session),
    authorization: str | None = Header(default=None),
    x_ingestor_key: str | None = Header(default=None, alias="X-Ingestor-Key"),
    x_ingestor_id: str | None = Header(default=None, alias="X-Ingestor-Id"),
) -> Ingestor:
    api_key = extract_api_key(authorization, x_ingestor_key)
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing ingestor API key")
    if not x_ingestor_id:
        raise HTTPException(status_code=401, detail="Missing X-Ingestor-Id header")

    ingestor = await session.get(Ingestor, x_ingestor_id)
    if ingestor is None or ingestor.status == IngestorStatus.disabled:
        raise HTTPException(status_code=401, detail="Unknown or disabled ingestor")
    if not verify_api_key(api_key, ingestor.api_key_hash):
        raise HTTPException(status_code=401, detail="Invalid ingestor API key")

    request.state.ingestor_client_ip = request.client.host if request.client else None
    return ingestor


def ingestor_is_online(ingestor: Ingestor, timeout_seconds: int) -> bool:
    if ingestor.status == IngestorStatus.disabled:
        return False
    if ingestor.last_heartbeat_at is None:
        return False
    age = (utcnow() - ingestor.last_heartbeat_at).total_seconds()
    return age <= timeout_seconds


async def count_sources_for_ingestor(session: AsyncSession, ingestor_id: str) -> int:
    result = await session.execute(
        select(func.count()).select_from(WatchSource).where(WatchSource.ingestor_id == ingestor_id)
    )
    return int(result.scalar_one())
