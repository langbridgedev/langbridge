import enum
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    DateTime, 
    Enum as SAEnum, 
    JSON,
    ForeignKey,
    Index, 
    Integer, 
    String, 
    UUID, 
    func
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
    
class JobTaskRecord(Base):
    __tablename__ = "job_tasks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="cascade"), index=True)

    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    parameters: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    
class JobEventRecord(Base):
    __tablename__ = "job_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="cascade"), index=True)

    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    details: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    visibility: Mapped[JobEventVisibility] = mapped_column(
        SAEnum(JobEventVisibility, name="job_event_visibility"),
        nullable=False,
        default=JobEventVisibility.public,
    )

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    
class JobRecord(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organisation_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    job_type: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    headers: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus, name="job_status"),
        nullable=False,
        default=JobStatus.queued,
        index=True,
    )

    priority: Mapped[JobPriority] = mapped_column(
        SAEnum(JobPriority, name="job_priority"),
        nullable=False,
        default=JobPriority.normal,
        index=True,
    )
    
    # Tasks
    job_tasks: Mapped[List[JobTaskRecord]] = relationship(
        "JobTaskRecord", backref="job", cascade="all, delete-orphan"
    )
    
    # Events
    job_events: Mapped[List[JobEventRecord]] = relationship(
        "JobEventRecord", backref="job", cascade="all, delete-orphan"
    )

    # Retry / attempts
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)

    # Lease-based claiming (to make at-least-once delivery safe)
    lock_owner: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    locked_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    # Progress + output references (keep outputs small; store big artifacts elsewhere)
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)  # 0..100
    status_message: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)  # e.g., {type, uri, query_id}
    error: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_jobs_runnable",
            "status",
            "locked_until",
            "priority",
            "created_at",
        ),
    )
