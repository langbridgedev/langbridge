
import enum
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import JSON, String, Text, Enum as SAEnum, ForeignKey, DateTime, Integer, UUID, func
from sqlalchemy.orm import Mapped, mapped_column, relationship, synonym

from .base import Base


class ThreadState(enum.Enum):
    awaiting_user_input = "awaiting_user_input"
    processing = "processing"

class Role(enum.Enum):
    system = "system"
    user = "user"
    assistant = "assistant"
    tool = "tool"


class Thread(Base):
    __tablename__ = "threads"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    title: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    state: Mapped[ThreadState] = mapped_column(SAEnum(ThreadState, name="thread_state"), nullable=False, default=ThreadState.awaiting_user_input)

    metadata_json: Mapped[Optional[Dict[str, Any]]] = mapped_column("metadata", JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    created_by_actor_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    
    messages: Mapped[list["ThreadMessage"]] = relationship(
        "ThreadMessage", backref="thread", cascade="all, delete-orphan", lazy="selectin"
    )
    last_message_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_by = synonym("created_by_actor_id")


class ThreadMessage(Base):
    __tablename__ = "thread_messages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    thread_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("threads.id", ondelete="cascade"), index=True)
    parent_message_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)

    role: Mapped[Role] = mapped_column(SAEnum(Role, name="message_role"), nullable=False)
    
    content: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)  # array-of-parts schema
    model_snapshot: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    token_usage: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)  # {prompt, completion, total, costs...}
    error: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MemoryCategory(enum.Enum):
    fact = "fact"
    preference = "preference"
    decision = "decision"
    tool_outcome = "tool_outcome"
    answer = "answer"


class ConversationMemoryItem(Base):
    __tablename__ = "conversation_memory_items"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    thread_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("threads.id", ondelete="cascade"), index=True)
    actor_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    category: Mapped[MemoryCategory] = mapped_column(
        SAEnum(MemoryCategory, name="memory_category"),
        nullable=False,
        default=MemoryCategory.fact,
        index=True,
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[Optional[Dict[str, Any]]] = mapped_column("metadata", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    last_accessed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class RunStatus(enum.Enum):
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    thread_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("threads.id", ondelete="cascade"), index=True
    )
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="cascade"), index=True)
    root_message_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("thread_messages.id", ondelete="cascade")
    )

    graph: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    state_before: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    state_after: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    status: Mapped[RunStatus] = mapped_column(
        SAEnum(RunStatus, name="run_status"), nullable=False
    )

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class ToolCall(Base):
    __tablename__ = "tool_calls"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    message_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("thread_messages.id", ondelete="cascade"), index=True
    )
    tool_name: Mapped[str] = mapped_column(String, nullable=False)
    arguments: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
