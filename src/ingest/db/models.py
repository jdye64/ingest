from __future__ import annotations

import enum
import uuid
from datetime import UTC, datetime
from typing import Any, Optional

from sqlalchemy import JSON, Column, Text, UniqueConstraint
from sqlmodel import Field, SQLModel


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)

def new_id() -> str:
    return str(uuid.uuid4())


class DocumentStatus(str, enum.Enum):
    pending = "pending"
    indexing = "indexing"
    ready = "ready"
    error = "error"
    deleted = "deleted"


class RunStatus(str, enum.Enum):
    running = "running"
    success = "success"
    error = "error"


class WatchSource(SQLModel, table=True):
    __tablename__ = "watch_sources"

    id: str = Field(default_factory=new_id, primary_key=True)
    path: str = Field(index=True, unique=True)
    enabled: bool = Field(default=True)
    recursive: bool = Field(default=True)
    include_globs: Optional[str] = Field(default="*", description="Comma-separated globs")
    exclude_globs: Optional[str] = Field(default=None, description="Comma-separated globs")
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class IndexConfig(SQLModel, table=True):
    __tablename__ = "index_configs"

    id: str = Field(default_factory=new_id, primary_key=True)
    name: str = Field(index=True)
    version: int = Field(default=1)
    is_default: bool = Field(default=False, index=True)
    # Immutable snapshot of indexing parameters
    config_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    created_at: datetime = Field(default_factory=utcnow)


class Document(SQLModel, table=True):
    __tablename__ = "documents"
    __table_args__ = (UniqueConstraint("source_id", "path", name="uq_document_source_path"),)

    id: str = Field(default_factory=new_id, primary_key=True)
    source_id: str = Field(foreign_key="watch_sources.id", index=True)
    path: str = Field(index=True)
    content_sha256: Optional[str] = Field(default=None, index=True)
    size_bytes: Optional[int] = Field(default=None)
    mtime: Optional[float] = Field(default=None)
    status: DocumentStatus = Field(default=DocumentStatus.pending, index=True)
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    indexed_at: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class IndexRun(SQLModel, table=True):
    __tablename__ = "index_runs"

    id: str = Field(default_factory=new_id, primary_key=True)
    document_id: str = Field(foreign_key="documents.id", index=True)
    config_id: str = Field(foreign_key="index_configs.id", index=True)
    content_sha256: Optional[str] = Field(default=None)
    status: RunStatus = Field(default=RunStatus.running, index=True)
    chunk_count: int = Field(default=0)
    lance_table: str = Field(default="chunks")
    notes: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    started_at: datetime = Field(default_factory=utcnow)
    finished_at: Optional[datetime] = Field(default=None)


class DocumentChunk(SQLModel, table=True):
    __tablename__ = "document_chunks"

    id: str = Field(default_factory=new_id, primary_key=True)
    document_id: str = Field(foreign_key="documents.id", index=True)
    run_id: str = Field(foreign_key="index_runs.id", index=True)
    chunk_index: int = Field(default=0)
    chunk_id: str = Field(index=True)
    token_estimate: int = Field(default=0)
    created_at: datetime = Field(default_factory=utcnow)
