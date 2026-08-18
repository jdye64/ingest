from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ingest.db.models import IngestorStatus, DocumentStatus


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
    ingestor_id: Optional[str] = None


class WatchSourceOut(ORMModel):
    id: str
    path: str
    ingestor_id: Optional[str] = None
    enabled: bool
    recursive: bool
    include_globs: Optional[str]
    exclude_globs: Optional[str]
    created_at: datetime
    updated_at: datetime


class SourceDeleteOut(BaseModel):
    source_id: str
    path: str
    documents_removed: int
    chunks_removed: int
    runs_removed: int


class SourceAuditEventOut(ORMModel):
    id: str
    source_id: str
    path: str
    ingestor_id: Optional[str] = None
    action: str
    actor: str
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class DocumentOut(ORMModel):
    id: str
    source_id: str
    ingestor_id: Optional[str] = None
    path: str
    original_filename: Optional[str] = None
    content_sha256: Optional[str]
    size_bytes: Optional[int]
    page_count: Optional[int] = None
    mtime: Optional[float]
    status: DocumentStatus
    error_message: Optional[str]
    model_invocations: list[dict[str, Any]] = Field(default_factory=list)
    claimed_by_ingestor_id: Optional[str] = None
    claimed_at: Optional[datetime] = None
    indexed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    @field_validator("model_invocations", mode="before")
    @classmethod
    def _coerce_invocations(cls, value: Any) -> list[dict[str, Any]]:
        return value or []


class IndexConfigOut(ORMModel):
    id: str
    name: str
    version: int
    is_default: bool
    config_json: dict[str, Any]
    created_at: datetime


class ModelInvocationOut(BaseModel):
    model: str
    detection_count: int = 0
    model_config = ConfigDict(extra="allow")


class IndexRunOut(ORMModel):
    id: str
    document_id: str
    config_id: str
    ingestor_id: Optional[str] = None
    content_sha256: Optional[str]
    status: str
    chunk_count: int
    page_count: Optional[int] = None
    model_invocations: list[dict[str, Any]] = Field(default_factory=list)
    lance_table: str
    notes: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]

    @field_validator("model_invocations", mode="before")
    @classmethod
    def _coerce_invocations(cls, value: Any) -> list[dict[str, Any]]:
        return value or []


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
    ingestors_online: int = 0
    ingestors_total: int = 0


class IngestorCreate(BaseModel):
    id: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=256)


class IngestorOut(ORMModel):
    id: str
    name: str
    hostname: Optional[str] = None
    status: IngestorStatus
    last_heartbeat_at: Optional[datetime] = None
    last_seen_ip: Optional[str] = None
    current_activity: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    online: bool = False
    source_count: int = 0


class IngestorCreatedOut(IngestorOut):
    api_key: str


class IngestorHeartbeatIn(BaseModel):
    hostname: Optional[str] = None
    current_activity: dict[str, Any] = Field(default_factory=dict)


class IngestorDocumentUpsertIn(BaseModel):
    source_id: str
    path: str
    content_sha256: Optional[str] = None
    size_bytes: Optional[int] = None
    mtime: Optional[float] = None
    deleted: bool = False


class IngestorDocumentUpsertOut(BaseModel):
    document_id: str
    status: DocumentStatus
    needs_index: bool
    claimed: bool = False
    reason: str = ""
    claimed_by_ingestor_id: Optional[str] = None


class IngestorDocumentCheckOut(BaseModel):
    path: str
    content_sha256: Optional[str] = None
    already_indexed: bool
    indexing_in_progress: bool
    claimed_by_ingestor_id: Optional[str] = None
    document_id: Optional[str] = None
    status: Optional[DocumentStatus] = None
    can_claim: bool


class IngestorChunkIn(BaseModel):
    chunk_index: int
    chunk_id: str
    text: str
    vector: list[float]
    token_estimate: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestorIndexIn(BaseModel):
    config_id: str
    content_sha256: str
    chunks: list[IngestorChunkIn]
    started_at: Optional[datetime] = None
    page_count: int = Field(default=1, ge=1)
    original_filename: Optional[str] = None
    size_bytes: Optional[int] = Field(default=None, ge=0)
    # One entry per model call: {model, detection_count, ...}
    model_invocations: list[dict[str, Any]] = Field(default_factory=list)


class IngestorIndexOut(BaseModel):
    document_id: str
    run_id: str
    status: str
    chunk_count: int


class IngestorFailIn(BaseModel):
    error_message: str
    content_sha256: Optional[str] = None
    config_id: Optional[str] = None


class IngestorThroughputOut(BaseModel):
    ingestor_id: str
    pps: float
    pages: int
    documents: int


class ThroughputOut(BaseModel):
    pps: float
    window_seconds: float
    pages_in_window: int
    documents_in_window: int
    by_ingestor: list[IngestorThroughputOut] = Field(default_factory=list)
