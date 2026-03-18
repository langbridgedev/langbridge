from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import Field, field_validator

from langbridge.contracts.base import _Base


class Role(str, Enum):
    system = "system"
    user = "user"
    assistant = "assistant"
    tool = "tool"


class ThreadResponse(_Base):
    id: UUID | None = None
    project_id: UUID | None = None
    title: str | None = None
    status: str = "active"
    metadata_json: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ThreadListResponse(_Base):
    threads: list[ThreadResponse] = Field(default_factory=list)


class ThreadCreateRequest(_Base):
    project_id: UUID | None = None
    title: str | None = None
    metadata_json: dict[str, Any] | None = None


class ThreadUpdateRequest(_Base):
    title: str | None = None
    metadata_json: dict[str, Any] | None = None


class ThreadChatRequest(_Base):
    message: str
    agent_id: UUID


class ThreadMessageResponse(_Base):
    id: UUID | None = None
    thread_id: UUID
    parent_message_id: UUID | None = None
    role: Role
    content: dict[str, Any] = Field(default_factory=dict)
    model_snapshot: dict[str, Any] | None = None
    token_usage: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    created_at: datetime | None = None

    @field_validator("role", mode="before")
    @classmethod
    def _normalize_role(cls, value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        return value


class ThreadHistoryResponse(_Base):
    messages: list[ThreadMessageResponse] = Field(default_factory=list)


class ThreadTabularResult(_Base):
    columns: list[str] = Field(default_factory=list)
    rows: list[Any] = Field(default_factory=list)
    row_count: int | None = None
    elapsed_ms: int | None = None


class ThreadVisualizationSpec(_Base):
    chart_type: str | None = None
    x: str | None = None
    y: list[str] | str | None = None
    group_by: str | None = None
    title: str | None = None
    options: dict[str, Any] | None = None


class ThreadChatResponse(_Base):
    job_id: UUID | None = None
    job_status: str | None = None
    result: ThreadTabularResult | None = None
    visualization: ThreadVisualizationSpec | None = None
    summary: str | None = None


__all__ = [
    "Role",
    "ThreadResponse",
    "ThreadListResponse",
    "ThreadCreateRequest",
    "ThreadUpdateRequest",
    "ThreadChatRequest",
    "ThreadMessageResponse",
    "ThreadHistoryResponse",
    "ThreadTabularResult",
    "ThreadVisualizationSpec",
    "ThreadChatResponse",
]
