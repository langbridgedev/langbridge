import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Uuid as UUID,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, synonym

from .base import Base


class RuntimeInstanceStatus(enum.Enum):
    active = "active"
    draining = "draining"
    offline = "offline"


class EdgeTaskStatus(enum.Enum):
    queued = "queued"
    leased = "leased"
    acked = "acked"
    failed = "failed"
    dead_letter = "dead_letter"


class RuntimeInstanceRecord(Base):
    __tablename__ = "ep_runtime_instances"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[RuntimeInstanceStatus] = mapped_column(
        SAEnum(RuntimeInstanceStatus, name="runtime_instance_status"),
        nullable=False,
        default=RuntimeInstanceStatus.active,
        index=True,
    )
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    registration_tokens: Mapped[list["RuntimeRegistrationTokenRecord"]] = relationship(
        "RuntimeRegistrationTokenRecord",
        back_populates="runtime",
    )
    edge_tasks: Mapped[list["EdgeTaskRecord"]] = relationship(
        "EdgeTaskRecord",
        back_populates="target_runtime",
    )

    __table_args__ = (
        Index("ix_ep_runtime_instances_workspace_status_seen", "workspace_id", "status", "last_seen_at"),
    )


class RuntimeRegistrationTokenRecord(Base):
    __tablename__ = "ep_runtime_registration_tokens"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    runtime_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ep_runtime_instances.id"),
        nullable=True,
        index=True,
    )
    created_by_actor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    runtime: Mapped[RuntimeInstanceRecord | None] = relationship(
        "RuntimeInstanceRecord",
        back_populates="registration_tokens",
    )


class EdgeTaskRecord(Base):
    __tablename__ = "edge_task_records"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    message_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    message_payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[EdgeTaskStatus] = mapped_column(
        SAEnum(EdgeTaskStatus, name="edge_task_status"),
        nullable=False,
        default=EdgeTaskStatus.queued,
        index=True,
    )
    target_runtime_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ep_runtime_instances.id"),
        nullable=True,
        index=True,
    )
    lease_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    leased_to_runtime_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    last_error: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    acked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    target_runtime: Mapped[RuntimeInstanceRecord | None] = relationship(
        "RuntimeInstanceRecord",
        back_populates="edge_tasks",
    )
    result_receipts: Mapped[list["EdgeResultReceiptRecord"]] = relationship(
        "EdgeResultReceiptRecord",
        back_populates="task",
    )

    __table_args__ = (
        Index("ix_edge_task_records_workspace_status_runtime", "workspace_id", "status", "target_runtime_id"),
    )


class EdgeResultReceiptRecord(Base):
    __tablename__ = "edge_result_receipts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    runtime_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    task_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("edge_task_records.id", ondelete="set null"),
        nullable=True,
        index=True,
    )
    payload_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    task: Mapped[EdgeTaskRecord | None] = relationship(
        "EdgeTaskRecord",
        back_populates="result_receipts",
    )

    __table_args__ = (
        UniqueConstraint("workspace_id", "runtime_id", "request_id", name="uq_edge_result_receipt_workspace_request"),
    )
