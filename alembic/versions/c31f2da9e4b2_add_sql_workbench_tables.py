"""add sql workbench tables

Revision ID: c31f2da9e4b2
Revises: 9d5a8a12f983
Create Date: 2026-03-02 09:45:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c31f2da9e4b2"
down_revision = "9d5a8a12f983"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sql_job",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("project_id", sa.UUID(), nullable=True),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("connection_id", sa.UUID(), nullable=True),
        sa.Column("execution_mode", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("query_text", sa.Text(), nullable=False),
        sa.Column("query_hash", sa.String(length=128), nullable=False),
        sa.Column("query_params_json", sa.JSON(), nullable=False),
        sa.Column("requested_limit", sa.Integer(), nullable=True),
        sa.Column("enforced_limit", sa.Integer(), nullable=False),
        sa.Column("requested_timeout_seconds", sa.Integer(), nullable=True),
        sa.Column("enforced_timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("is_explain", sa.Boolean(), nullable=False),
        sa.Column("is_federated", sa.Boolean(), nullable=False),
        sa.Column("correlation_id", sa.String(length=255), nullable=True),
        sa.Column("policy_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("result_columns_json", sa.JSON(), nullable=True),
        sa.Column("result_rows_json", sa.JSON(), nullable=True),
        sa.Column("row_count_preview", sa.Integer(), nullable=False),
        sa.Column("total_rows_estimate", sa.Integer(), nullable=True),
        sa.Column("bytes_scanned", sa.BigInteger(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("result_cursor", sa.String(length=255), nullable=True),
        sa.Column("redaction_applied", sa.Boolean(), nullable=False),
        sa.Column("error_json", sa.JSON(), nullable=True),
        sa.Column("warning_json", sa.JSON(), nullable=True),
        sa.Column("stats_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["connection_id"], ["connectors.id"]),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["workspace_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_sql_job_workspace_id"), "sql_job", ["workspace_id"], unique=False)
    op.create_index(op.f("ix_sql_job_project_id"), "sql_job", ["project_id"], unique=False)
    op.create_index(op.f("ix_sql_job_user_id"), "sql_job", ["user_id"], unique=False)
    op.create_index(op.f("ix_sql_job_connection_id"), "sql_job", ["connection_id"], unique=False)
    op.create_index(op.f("ix_sql_job_status"), "sql_job", ["status"], unique=False)
    op.create_index(op.f("ix_sql_job_query_hash"), "sql_job", ["query_hash"], unique=False)
    op.create_index(op.f("ix_sql_job_created_at"), "sql_job", ["created_at"], unique=False)
    op.create_index(op.f("ix_sql_job_correlation_id"), "sql_job", ["correlation_id"], unique=False)
    op.create_index(
        "ix_sql_job_workspace_created_at",
        "sql_job",
        ["workspace_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_sql_job_workspace_status_created_at",
        "sql_job",
        ["workspace_id", "status", "created_at"],
        unique=False,
    )

    op.create_table(
        "sql_job_result_artifact",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("sql_job_id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("format", sa.String(length=32), nullable=False),
        sa.Column("mime_type", sa.String(length=128), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=True),
        sa.Column("storage_backend", sa.String(length=32), nullable=False),
        sa.Column("storage_reference", sa.String(length=1024), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["sql_job_id"], ["sql_job.id"], ondelete="cascade"),
        sa.ForeignKeyConstraint(["workspace_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_sql_job_result_artifact_sql_job_id"),
        "sql_job_result_artifact",
        ["sql_job_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_sql_job_result_artifact_workspace_id"),
        "sql_job_result_artifact",
        ["workspace_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_sql_job_result_artifact_created_at"),
        "sql_job_result_artifact",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "ix_sql_job_result_artifact_workspace_created_at",
        "sql_job_result_artifact",
        ["workspace_id", "created_at"],
        unique=False,
    )

    op.create_table(
        "sql_saved_query",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("project_id", sa.UUID(), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("updated_by", sa.UUID(), nullable=False),
        sa.Column("connection_id", sa.UUID(), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=True),
        sa.Column("query_text", sa.Text(), nullable=False),
        sa.Column("query_hash", sa.String(length=128), nullable=False),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("default_params_json", sa.JSON(), nullable=False),
        sa.Column("is_shared", sa.Boolean(), nullable=False),
        sa.Column("last_sql_job_id", sa.UUID(), nullable=True),
        sa.Column("last_result_artifact_id", sa.UUID(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["connection_id"], ["connectors.id"]),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["last_result_artifact_id"], ["sql_job_result_artifact.id"]),
        sa.ForeignKeyConstraint(["last_sql_job_id"], ["sql_job.id"]),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"]),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["workspace_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_sql_saved_query_workspace_id"), "sql_saved_query", ["workspace_id"], unique=False)
    op.create_index(op.f("ix_sql_saved_query_project_id"), "sql_saved_query", ["project_id"], unique=False)
    op.create_index(op.f("ix_sql_saved_query_created_by"), "sql_saved_query", ["created_by"], unique=False)
    op.create_index(op.f("ix_sql_saved_query_updated_by"), "sql_saved_query", ["updated_by"], unique=False)
    op.create_index(
        op.f("ix_sql_saved_query_connection_id"),
        "sql_saved_query",
        ["connection_id"],
        unique=False,
    )
    op.create_index(op.f("ix_sql_saved_query_query_hash"), "sql_saved_query", ["query_hash"], unique=False)
    op.create_index(op.f("ix_sql_saved_query_is_shared"), "sql_saved_query", ["is_shared"], unique=False)
    op.create_index(op.f("ix_sql_saved_query_created_at"), "sql_saved_query", ["created_at"], unique=False)
    op.create_index(
        "ix_sql_saved_query_workspace_created_at",
        "sql_saved_query",
        ["workspace_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_sql_saved_query_workspace_updated_at",
        "sql_saved_query",
        ["workspace_id", "updated_at"],
        unique=False,
    )

    op.create_table(
        "sql_workspace_policy",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("max_preview_rows", sa.Integer(), nullable=False),
        sa.Column("max_export_rows", sa.Integer(), nullable=False),
        sa.Column("max_runtime_seconds", sa.Integer(), nullable=False),
        sa.Column("max_concurrency", sa.Integer(), nullable=False),
        sa.Column("allow_dml", sa.Boolean(), nullable=False),
        sa.Column("allow_federation", sa.Boolean(), nullable=False),
        sa.Column("allowed_schemas_json", sa.JSON(), nullable=False),
        sa.Column("allowed_tables_json", sa.JSON(), nullable=False),
        sa.Column("default_datasource_id", sa.UUID(), nullable=True),
        sa.Column("budget_limit_bytes", sa.BigInteger(), nullable=True),
        sa.Column("updated_by_user_id", sa.UUID(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["default_datasource_id"], ["connectors.id"]),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["workspace_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("workspace_id"),
    )
    op.create_index(
        op.f("ix_sql_workspace_policy_workspace_id"),
        "sql_workspace_policy",
        ["workspace_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_sql_workspace_policy_created_at"),
        "sql_workspace_policy",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_sql_workspace_policy_created_at"), table_name="sql_workspace_policy")
    op.drop_index(op.f("ix_sql_workspace_policy_workspace_id"), table_name="sql_workspace_policy")
    op.drop_table("sql_workspace_policy")

    op.drop_index("ix_sql_saved_query_workspace_updated_at", table_name="sql_saved_query")
    op.drop_index("ix_sql_saved_query_workspace_created_at", table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_created_at"), table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_is_shared"), table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_query_hash"), table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_connection_id"), table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_updated_by"), table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_created_by"), table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_project_id"), table_name="sql_saved_query")
    op.drop_index(op.f("ix_sql_saved_query_workspace_id"), table_name="sql_saved_query")
    op.drop_table("sql_saved_query")

    op.drop_index(
        "ix_sql_job_result_artifact_workspace_created_at",
        table_name="sql_job_result_artifact",
    )
    op.drop_index(op.f("ix_sql_job_result_artifact_created_at"), table_name="sql_job_result_artifact")
    op.drop_index(op.f("ix_sql_job_result_artifact_workspace_id"), table_name="sql_job_result_artifact")
    op.drop_index(op.f("ix_sql_job_result_artifact_sql_job_id"), table_name="sql_job_result_artifact")
    op.drop_table("sql_job_result_artifact")

    op.drop_index("ix_sql_job_workspace_status_created_at", table_name="sql_job")
    op.drop_index("ix_sql_job_workspace_created_at", table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_correlation_id"), table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_created_at"), table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_query_hash"), table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_status"), table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_connection_id"), table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_user_id"), table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_project_id"), table_name="sql_job")
    op.drop_index(op.f("ix_sql_job_workspace_id"), table_name="sql_job")
    op.drop_table("sql_job")

