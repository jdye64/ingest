"""Add ingest ingestors and ingestor_id FKs

Revision ID: 0002_agents
Revises: 0001_initial
Create Date: 2026-08-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_agents"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingestors",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("api_key_hash", sa.String(), nullable=False),
        sa.Column("hostname", sa.String(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("online", "offline", "disabled", name="ingestorstatus"),
            nullable=False,
        ),
        sa.Column("last_heartbeat_at", sa.DateTime(), nullable=True),
        sa.Column("last_seen_ip", sa.String(), nullable=True),
        sa.Column("current_activity", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_ingestors_name", "ingestors", ["name"])
    op.create_index("ix_ingestors_api_key_hash", "ingestors", ["api_key_hash"])
    op.create_index("ix_ingestors_status", "ingestors", ["status"])

    with op.batch_alter_table("watch_sources") as batch:
        batch.add_column(sa.Column("ingestor_id", sa.String(), nullable=True))
        batch.create_foreign_key("fk_watch_sources_ingestor_id", "ingestors", ["ingestor_id"], ["id"])
        batch.create_index("ix_watch_sources_ingestor_id", ["ingestor_id"])
        batch.drop_index("ix_watch_sources_path")
        batch.create_index("ix_watch_sources_path", ["path"], unique=False)
        batch.create_unique_constraint("uq_watch_source_ingestor_path", ["ingestor_id", "path"])

    with op.batch_alter_table("documents") as batch:
        batch.add_column(sa.Column("ingestor_id", sa.String(), nullable=True))
        batch.create_foreign_key("fk_documents_ingestor_id", "ingestors", ["ingestor_id"], ["id"])
        batch.create_index("ix_documents_ingestor_id", ["ingestor_id"])

    with op.batch_alter_table("index_runs") as batch:
        batch.add_column(sa.Column("ingestor_id", sa.String(), nullable=True))
        batch.create_foreign_key("fk_index_runs_ingestor_id", "ingestors", ["ingestor_id"], ["id"])
        batch.create_index("ix_index_runs_ingestor_id", ["ingestor_id"])


def downgrade() -> None:
    with op.batch_alter_table("index_runs") as batch:
        batch.drop_index("ix_index_runs_ingestor_id")
        batch.drop_constraint("fk_index_runs_ingestor_id", type_="foreignkey")
        batch.drop_column("ingestor_id")

    with op.batch_alter_table("documents") as batch:
        batch.drop_index("ix_documents_ingestor_id")
        batch.drop_constraint("fk_documents_ingestor_id", type_="foreignkey")
        batch.drop_column("ingestor_id")

    with op.batch_alter_table("watch_sources") as batch:
        batch.drop_constraint("uq_watch_source_ingestor_path", type_="unique")
        batch.drop_index("ix_watch_sources_ingestor_id")
        batch.drop_constraint("fk_watch_sources_ingestor_id", type_="foreignkey")
        batch.drop_column("ingestor_id")
        batch.drop_index("ix_watch_sources_path")
        batch.create_index("ix_watch_sources_path", ["path"], unique=True)

    op.drop_table("ingestors")
