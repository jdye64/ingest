"""Add document filename, page_count, and model_invocations

Revision ID: 0005_document_metadata
Revises: 0004_source_audit
Create Date: 2026-08-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005_document_metadata"
down_revision = "0004_source_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("documents") as batch:
        batch.add_column(sa.Column("original_filename", sa.String(), nullable=True))
        batch.add_column(sa.Column("page_count", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("model_invocations", sa.JSON(), nullable=False, server_default=sa.text("'[]'")))
        batch.create_index("ix_documents_original_filename", ["original_filename"])

    with op.batch_alter_table("index_runs") as batch:
        batch.add_column(sa.Column("page_count", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("model_invocations", sa.JSON(), nullable=False, server_default=sa.text("'[]'")))


def downgrade() -> None:
    with op.batch_alter_table("index_runs") as batch:
        batch.drop_column("model_invocations")
        batch.drop_column("page_count")

    with op.batch_alter_table("documents") as batch:
        batch.drop_index("ix_documents_original_filename")
        batch.drop_column("model_invocations")
        batch.drop_column("page_count")
        batch.drop_column("original_filename")
