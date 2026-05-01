import enum
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
    UniqueConstraint,
    Uuid as UUID,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class JobStatus(enum.Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class JobPriority(enum.Enum):
    low = "low"
    normal = "normal"
    high = "high"


class JobEventVisibility(enum.Enum):
    public = "public"
    internal = "internal"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobRecord(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workspaces.id"),
        nullable=False,
        index=True,
    )
    job_type: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=JobStatus.queued.value, index=True)
    priority: Mapped[str] = mapped_column(String(32), nullable=False, default=JobPriority.normal.value, index=True)
    actor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    subject_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    subject_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)

    queue_name: Mapped[str] = mapped_column(String(128), nullable=False, default="default", index=True)
    required_capabilities: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    runtime_pool_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    affinity_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    concurrency_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)

    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    progress: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    status_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    terminal_sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    lock_owner: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        nullable=False,
    )

    job_tasks: Mapped[list["JobTaskRecord"]] = relationship(
        "JobTaskRecord",
        back_populates="job",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    job_events: Mapped[list["JobEventRecord"]] = relationship(
        "JobEventRecord",
        back_populates="job",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    job_artifacts: Mapped[list["JobArtifactRecord"]] = relationship(
        "JobArtifactRecord",
        back_populates="job",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    __table_args__ = (
        Index("ix_jobs_runnable", "status", "scheduled_at", "locked_until", "priority", "created_at"),
        Index("ix_jobs_workspace_type_status", "workspace_id", "job_type", "status", "created_at"),
        Index("ix_jobs_workspace_subject_status", "workspace_id", "subject_type", "subject_id", "status"),
        UniqueConstraint("workspace_id", "idempotency_key", name="uq_jobs_workspace_idempotency_key"),
    )


class JobTaskRecord(Base):
    __tablename__ = "job_tasks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    task_key: Mapped[str] = mapped_column(String(255), nullable=False)
    task_type: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=JobStatus.queued.value, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    resume_policy: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reuse_policy: Mapped[str | None] = mapped_column(String(64), nullable=True)

    input: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    state: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    diagnostics: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    started_sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    terminal_sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        nullable=False,
    )

    job: Mapped[JobRecord] = relationship("JobRecord", back_populates="job_tasks")
    job_events: Mapped[list["JobEventRecord"]] = relationship("JobEventRecord", back_populates="task")
    job_artifacts: Mapped[list["JobArtifactRecord"]] = relationship("JobArtifactRecord", back_populates="task")

    __table_args__ = (
        UniqueConstraint("job_id", "task_key", name="uq_job_tasks_job_task_key"),
        Index("ix_job_tasks_job_status", "job_id", "status", "updated_at"),
    )


class JobEventRecord(Base):
    __tablename__ = "job_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    task_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("job_tasks.id", ondelete="set null"),
        nullable=True,
        index=True,
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    stage: Mapped[str] = mapped_column(String(128), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    visibility: Mapped[str] = mapped_column(String(32), nullable=False, default=JobEventVisibility.internal.value)
    terminal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_event_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now, nullable=False, index=True)

    job: Mapped[JobRecord] = relationship("JobRecord", back_populates="job_events")
    task: Mapped[JobTaskRecord | None] = relationship("JobTaskRecord", back_populates="job_events")

    __table_args__ = (
        UniqueConstraint("job_id", "sequence", name="uq_job_events_job_sequence"),
        Index("ix_job_events_job_sequence", "job_id", "sequence"),
    )


class JobArtifactRecord(Base):
    __tablename__ = "job_artifacts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    task_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("job_tasks.id", ondelete="set null"),
        nullable=True,
        index=True,
    )
    artifact_key: Mapped[str] = mapped_column(String(255), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    storage_kind: Mapped[str] = mapped_column(String(64), nullable=False, default="inline")
    storage_uri: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    data: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    schema: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    formatting: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        nullable=False,
    )

    job: Mapped[JobRecord] = relationship("JobRecord", back_populates="job_artifacts")
    task: Mapped[JobTaskRecord | None] = relationship("JobTaskRecord", back_populates="job_artifacts")

    __table_args__ = (
        UniqueConstraint("job_id", "artifact_key", name="uq_job_artifacts_job_key"),
        Index("ix_job_artifacts_job_type", "job_id", "artifact_type", "created_at"),
    )
