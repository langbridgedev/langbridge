"""add connector sync state table

Revision ID: 8f3c1b0e4c2d
Revises: 4c8a6b9d12ef
Create Date: 2026-03-06 18:45:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8f3c1b0e4c2d"
down_revision = "4c8a6b9d12ef"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_states",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("connection_id", sa.UUID(), nullable=False),
        sa.Column("connector_type", sa.String(length=64), nullable=False),
        sa.Column("resource_name", sa.String(length=255), nullable=False),
        sa.Column("sync_mode", sa.String(length=32), nullable=False),
        sa.Column("last_cursor", sa.String(length=255), nullable=True),
        sa.Column("last_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("state_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_message", sa.String(length=2048), nullable=True),
        sa.Column("records_synced", sa.BigInteger(), nullable=False),
        sa.Column("bytes_synced", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["workspace_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["connection_id"], ["connectors.id"], ondelete="cascade"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "workspace_id",
            "connection_id",
            "resource_name",
            name="uq_connector_sync_states_workspace_connection_resource",
        ),
    )
    op.create_index(
        op.f("ix_connector_sync_states_workspace_id"),
        "connector_sync_states",
        ["workspace_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_connector_sync_states_connection_id"),
        "connector_sync_states",
        ["connection_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_connector_sync_states_updated_at"),
        "connector_sync_states",
        ["updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_connector_sync_states_workspace_connection_updated",
        "connector_sync_states",
        ["workspace_id", "connection_id", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_connector_sync_states_workspace_resource",
        "connector_sync_states",
        ["workspace_id", "resource_name"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_connector_sync_states_workspace_resource", table_name="connector_sync_states")
    op.drop_index("ix_connector_sync_states_workspace_connection_updated", table_name="connector_sync_states")
    op.drop_index(op.f("ix_connector_sync_states_updated_at"), table_name="connector_sync_states")
    op.drop_index(op.f("ix_connector_sync_states_connection_id"), table_name="connector_sync_states")
    op.drop_index(op.f("ix_connector_sync_states_workspace_id"), table_name="connector_sync_states")
    op.drop_table("connector_sync_states")
