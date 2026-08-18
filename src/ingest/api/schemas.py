from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from ingest.db.models import DocumentStatus


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HealthResponse(BaseModel):
    status: str
    database: str
    lancedb: dict[str, Any]
    queue_depth: int


class WatchSourceCreate(BaseModel):
    path: str
    enabled: bool = True
    recursive: bool = True
    include_globs: str = "*"
    exclude_globs: Optional[str] = None


class WatchSourceOut(ORMModel):
    id: str
    path: str
    enabled: bool
    recursive: bool
    include_globs: Optional[str]
    exclude_globs: Optional[str]
    created_at: datetime
    updated_at: datetime


class DocumentOut(ORMModel):
    id: str
    source_id: str
    path: str
    content_sha256: Optional[str]
    size_bytes: Optional[int]
    mtime: Optional[float]
    status: DocumentStatus
    error_message: Optional[str]
    indexed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class IndexConfigOut(ORMModel):
    id: str
    name: str
    version: int
    is_default: bool
    config_json: dict[str, Any]
    created_at: datetime


class IndexRunOut(ORMModel):
    id: str
    document_id: str
    config_id: str
    content_sha256: Optional[str]
    status: str
    chunk_count: int
    lance_table: str
    notes: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]


class DocumentDetailOut(DocumentOut):
    latest_run: Optional[IndexRunOut] = None
    index_config: Optional[IndexConfigOut] = None


class DocumentListOut(BaseModel):
    items: list[DocumentOut]
    total: int
    limit: int
    offset: int


class SearchHit(BaseModel):
    chunk_id: str
    document_id: str
    run_id: str
    source_id: str
    path: str
    chunk_index: int
    text: str
    score: Optional[float] = None
    content_sha256: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    query: str
    hits: list[SearchHit]


class StatusCounts(BaseModel):
    counts: dict[str, int]
    queue_depth: int
