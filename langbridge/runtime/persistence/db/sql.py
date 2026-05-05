
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    Uuid as UUID,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, synonym

from .base import Base


class SqlJobRecord(Base):
    __tablename__ = "sql_job"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workspaces.id"),
        nullable=False,
        index=True,
    )
    actor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    connection_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("connectors.id"),
        nullable=True,
        index=True,
    )
    workbench_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="dataset", index=True)
    selected_datasets_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)
    execution_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="single")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", index=True)

    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    query_hash: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    query_params_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    requested_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    enforced_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)
    requested_timeout_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    enforced_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

    is_explain: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_federated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    correlation_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    result_columns_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    result_rows_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    row_count_preview: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_rows_estimate: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bytes_scanned: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_cursor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    redaction_applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    error_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    warning_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    stats_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    result_artifacts: Mapped[list["SqlJobResultArtifactRecord"]] = relationship(
        "SqlJobResultArtifactRecord",
        back_populates="sql_job",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_sql_job_workspace_created_at", "workspace_id", "created_at"),
        Index("ix_sql_job_workspace_status_created_at", "workspace_id", "status", "created_at"),
    )


class SqlJobResultArtifactRecord(Base):
    __tablename__ = "sql_job_result_artifact"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sql_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("sql_job.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workspaces.id"),
        nullable=False,
        index=True,
    )
    created_by_actor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    format: Mapped[str] = mapped_column(String(32), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    byte_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    storage_backend: Mapped[str] = mapped_column(String(32), nullable=False, default="inline")
    storage_reference: Mapped[str] = mapped_column(String(1024), nullable=False)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )

    sql_job: Mapped[SqlJobRecord] = relationship("SqlJobRecord", back_populates="result_artifacts")

    __table_args__ = (
        Index("ix_sql_job_result_artifact_workspace_created_at", "workspace_id", "created_at"),
    )
    created_by = synonym("created_by_actor_id")
