from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel
from langbridge.runtime.models.llm import LLMConnectionSecret


class RuntimeAgentDefinition(RuntimeModel):
    id: uuid.UUID
    name: str
    description: str | None = None
    llm_connection_id: uuid.UUID
    definition: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RuntimeThreadState(str, enum.Enum):
    awaiting_user_input = "awaiting_user_input"
    processing = "processing"


class RuntimeMessageRole(str, enum.Enum):
    system = "system"
    user = "user"
    assistant = "assistant"
    tool = "tool"


class RuntimeConversationMemoryCategory(str, enum.Enum):
    fact = "fact"
    preference = "preference"
    decision = "decision"
    tool_outcome = "tool_outcome"
    answer = "answer"


class RuntimeThread(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    title: str | None = None
    state: RuntimeThreadState | str = RuntimeThreadState.awaiting_user_input
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    created_by: uuid.UUID
    last_message_id: uuid.UUID | None = None

    @property
    def metadata_json(self) -> dict[str, Any]:
        return dict(self.metadata)


class RuntimeThreadMessage(RuntimeModel):
    id: uuid.UUID
    thread_id: uuid.UUID
    parent_message_id: uuid.UUID | None = None
    role: RuntimeMessageRole | str
    content: dict[str, Any] = Field(default_factory=dict)
    model_snapshot: dict[str, Any] | None = None
    token_usage: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    created_at: datetime | None = None

    @property
    def model_snapshot_json(self) -> dict[str, Any] | None:
        return None if self.model_snapshot is None else dict(self.model_snapshot)

    @property
    def token_usage_json(self) -> dict[str, Any] | None:
        return None if self.token_usage is None else dict(self.token_usage)


class RuntimeConversationMemoryItem(RuntimeModel):
    id: uuid.UUID
    thread_id: uuid.UUID
    actor_id: uuid.UUID | None = Field(default=None)
    category: RuntimeConversationMemoryCategory | str = RuntimeConversationMemoryCategory.fact
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_accessed_at: datetime | None = None

    @property
    def metadata_json(self) -> dict[str, Any]:
        return dict(self.metadata)


__all__ = [
    "LLMConnectionSecret",
    "RuntimeConversationMemoryCategory",
    "RuntimeConversationMemoryItem",
    "RuntimeAgentDefinition",
    "RuntimeMessageRole",
    "RuntimeThread",
    "RuntimeThreadMessage",
    "RuntimeThreadState",
]
