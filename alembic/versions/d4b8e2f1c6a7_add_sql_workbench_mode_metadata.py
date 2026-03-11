"""add sql workbench mode metadata

Revision ID: d4b8e2f1c6a7
Revises: b7e1c2a4d9f0
Create Date: 2026-03-11 13:00:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d4b8e2f1c6a7"
down_revision = "b7e1c2a4d9f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sql_job",
        sa.Column("workbench_mode", sa.String(length=32), nullable=False, server_default="dataset"),
    )
    op.add_column(
        "sql_job",
        sa.Column(
            "selected_datasets_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )
    op.create_index(op.f("ix_sql_job_workbench_mode"), "sql_job", ["workbench_mode"], unique=False)

    op.add_column(
        "sql_saved_query",
        sa.Column("workbench_mode", sa.String(length=32), nullable=False, server_default="dataset"),
    )
    op.add_column(
        "sql_saved_query",
        sa.Column(
            "selected_datasets_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )
    op.create_index(
        op.f("ix_sql_saved_query_workbench_mode"),
        "sql_saved_query",
        ["workbench_mode"],
        unique=False,
    )

    op.execute(
        """
        UPDATE sql_job
        SET workbench_mode = CASE
            WHEN COALESCE(is_federated, FALSE) = TRUE OR connection_id IS NULL THEN 'dataset'
            ELSE 'direct_sql'
        END
        """
    )
    op.execute(
        """
        UPDATE sql_saved_query
        SET workbench_mode = CASE
            WHEN connection_id IS NULL THEN 'dataset'
            ELSE 'direct_sql'
        END
        """
    )

    op.alter_column("sql_job", "workbench_mode", server_default=None)
    op.alter_column("sql_job", "selected_datasets_json", server_default=None)
    op.alter_column("sql_saved_query", "workbench_mode", server_default=None)
    op.alter_column("sql_saved_query", "selected_datasets_json", server_default=None)


def downgrade() -> None:
    op.drop_index(op.f("ix_sql_saved_query_workbench_mode"), table_name="sql_saved_query")
    op.drop_column("sql_saved_query", "selected_datasets_json")
    op.drop_column("sql_saved_query", "workbench_mode")

    op.drop_index(op.f("ix_sql_job_workbench_mode"), table_name="sql_job")
    op.drop_column("sql_job", "selected_datasets_json")
    op.drop_column("sql_job", "workbench_mode")
