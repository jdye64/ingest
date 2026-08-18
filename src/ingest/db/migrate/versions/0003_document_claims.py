"""Add document claim columns for ingestor race guards

Revision ID: 0003_document_claims
Revises: 0002_agents
Create Date: 2026-08-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003_document_claims"
down_revision = "0002_agents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("documents") as batch:
        batch.add_column(sa.Column("claimed_by_ingestor_id", sa.String(), nullable=True))
        batch.add_column(sa.Column("claimed_at", sa.DateTime(), nullable=True))
        batch.create_foreign_key(
            "fk_documents_claimed_by_ingestor_id",
            "ingestors",
            ["claimed_by_ingestor_id"],
            ["id"],
        )
        batch.create_index("ix_documents_claimed_by_ingestor_id", ["claimed_by_ingestor_id"])


def downgrade() -> None:
    with op.batch_alter_table("documents") as batch:
        batch.drop_index("ix_documents_claimed_by_ingestor_id")
        batch.drop_constraint("fk_documents_claimed_by_ingestor_id", type_="foreignkey")
        batch.drop_column("claimed_at")
        batch.drop_column("claimed_by_ingestor_id")
