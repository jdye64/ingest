"""Initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-08-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "watch_sources",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("recursive", sa.Boolean(), nullable=False),
        sa.Column("include_globs", sa.String(), nullable=True),
        sa.Column("exclude_globs", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_watch_sources_path", "watch_sources", ["path"], unique=True)

    op.create_table(
        "index_configs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_index_configs_name", "index_configs", ["name"])
    op.create_index("ix_index_configs_is_default", "index_configs", ["is_default"])

    op.create_table(
        "documents",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("source_id", sa.String(), sa.ForeignKey("watch_sources.id"), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("content_sha256", sa.String(), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("mtime", sa.Float(), nullable=True),
        sa.Column("status", sa.Enum("pending", "indexing", "ready", "error", "deleted", name="documentstatus"), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("indexed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("source_id", "path", name="uq_document_source_path"),
    )
    op.create_index("ix_documents_source_id", "documents", ["source_id"])
    op.create_index("ix_documents_path", "documents", ["path"])
    op.create_index("ix_documents_content_sha256", "documents", ["content_sha256"])
    op.create_index("ix_documents_status", "documents", ["status"])

    op.create_table(
        "index_runs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("document_id", sa.String(), sa.ForeignKey("documents.id"), nullable=False),
        sa.Column("config_id", sa.String(), sa.ForeignKey("index_configs.id"), nullable=False),
        sa.Column("content_sha256", sa.String(), nullable=True),
        sa.Column("status", sa.Enum("running", "success", "error", name="runstatus"), nullable=False),
        sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("lance_table", sa.String(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_index_runs_document_id", "index_runs", ["document_id"])
    op.create_index("ix_index_runs_config_id", "index_runs", ["config_id"])
    op.create_index("ix_index_runs_status", "index_runs", ["status"])

    op.create_table(
        "document_chunks",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("document_id", sa.String(), sa.ForeignKey("documents.id"), nullable=False),
        sa.Column("run_id", sa.String(), sa.ForeignKey("index_runs.id"), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("chunk_id", sa.String(), nullable=False),
        sa.Column("token_estimate", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_document_chunks_document_id", "document_chunks", ["document_id"])
    op.create_index("ix_document_chunks_run_id", "document_chunks", ["run_id"])
    op.create_index("ix_document_chunks_chunk_id", "document_chunks", ["chunk_id"])


def downgrade() -> None:
    op.drop_table("document_chunks")
    op.drop_table("index_runs")
    op.drop_table("documents")
    op.drop_table("index_configs")
    op.drop_table("watch_sources")
