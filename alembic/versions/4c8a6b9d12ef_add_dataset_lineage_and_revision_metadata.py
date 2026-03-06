"""add dataset lineage and revision metadata

Revision ID: 4c8a6b9d12ef
Revises: 7b9f6d6f4c21
Create Date: 2026-03-06 12:30:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4c8a6b9d12ef"
down_revision = "7b9f6d6f4c21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dataset_revisions", sa.Column("revision_hash", sa.String(length=64), nullable=True))
    op.add_column("dataset_revisions", sa.Column("change_summary", sa.String(length=1024), nullable=True))
    op.add_column("dataset_revisions", sa.Column("definition_json", sa.JSON(), nullable=True))
    op.add_column("dataset_revisions", sa.Column("schema_json", sa.JSON(), nullable=True))
    op.add_column("dataset_revisions", sa.Column("policy_json", sa.JSON(), nullable=True))
    op.add_column("dataset_revisions", sa.Column("source_bindings_json", sa.JSON(), nullable=True))
    op.add_column(
        "dataset_revisions",
        sa.Column("execution_characteristics_json", sa.JSON(), nullable=True),
    )
    op.add_column("dataset_revisions", sa.Column("status", sa.String(length=32), nullable=True))
    op.create_index(
        op.f("ix_dataset_revisions_revision_hash"),
        "dataset_revisions",
        ["revision_hash"],
        unique=False,
    )
    op.create_index(
        "ix_dataset_revisions_dataset_created_at",
        "dataset_revisions",
        ["dataset_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_dataset_revisions_workspace_created_at",
        "dataset_revisions",
        ["workspace_id", "created_at"],
        unique=False,
    )

    op.create_table(
        "lineage_edges",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("target_type", sa.String(length=64), nullable=False),
        sa.Column("target_id", sa.String(length=255), nullable=False),
        sa.Column("edge_type", sa.String(length=64), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["workspace_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "workspace_id",
            "source_type",
            "source_id",
            "target_type",
            "target_id",
            "edge_type",
            name="uq_lineage_edges_workspace_source_target_edge",
        ),
    )
    op.create_index(op.f("ix_lineage_edges_workspace_id"), "lineage_edges", ["workspace_id"], unique=False)
    op.create_index(
        "ix_lineage_edges_workspace_source",
        "lineage_edges",
        ["workspace_id", "source_type", "source_id"],
        unique=False,
    )
    op.create_index(
        "ix_lineage_edges_workspace_target",
        "lineage_edges",
        ["workspace_id", "target_type", "target_id"],
        unique=False,
    )
    op.create_index(
        "ix_lineage_edges_workspace_created_at",
        "lineage_edges",
        ["workspace_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_lineage_edges_workspace_created_at", table_name="lineage_edges")
    op.drop_index("ix_lineage_edges_workspace_target", table_name="lineage_edges")
    op.drop_index("ix_lineage_edges_workspace_source", table_name="lineage_edges")
    op.drop_index(op.f("ix_lineage_edges_workspace_id"), table_name="lineage_edges")
    op.drop_table("lineage_edges")

    op.drop_index("ix_dataset_revisions_workspace_created_at", table_name="dataset_revisions")
    op.drop_index("ix_dataset_revisions_dataset_created_at", table_name="dataset_revisions")
    op.drop_index(op.f("ix_dataset_revisions_revision_hash"), table_name="dataset_revisions")
    op.drop_column("dataset_revisions", "status")
    op.drop_column("dataset_revisions", "execution_characteristics_json")
    op.drop_column("dataset_revisions", "source_bindings_json")
    op.drop_column("dataset_revisions", "policy_json")
    op.drop_column("dataset_revisions", "schema_json")
    op.drop_column("dataset_revisions", "definition_json")
    op.drop_column("dataset_revisions", "change_summary")
    op.drop_column("dataset_revisions", "revision_hash")
