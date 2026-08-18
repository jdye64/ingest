"""Add source audit events table

Revision ID: 0004_source_audit
Revises: 0003_document_claims
Create Date: 2026-08-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_source_audit"
down_revision = "0003_document_claims"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "source_audit_events",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("source_id", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("ingestor_id", sa.String(), nullable=True),
        sa.Column(
            "action",
            sa.Enum("created", "updated", "enabled", "disabled", "deleted", name="sourceaction"),
            nullable=False,
        ),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_source_audit_events_source_id", "source_audit_events", ["source_id"])
    op.create_index("ix_source_audit_events_path", "source_audit_events", ["path"])
    op.create_index("ix_source_audit_events_ingestor_id", "source_audit_events", ["ingestor_id"])
    op.create_index("ix_source_audit_events_action", "source_audit_events", ["action"])
    op.create_index("ix_source_audit_events_created_at", "source_audit_events", ["created_at"])


def downgrade() -> None:
    op.drop_table("source_audit_events")
