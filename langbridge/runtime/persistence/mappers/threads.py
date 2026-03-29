
from typing import Any

from langbridge.runtime.models import (
    RuntimeConversationMemoryCategory,
    RuntimeConversationMemoryItem,
    RuntimeMessageRole,
    RuntimeThread,
    RuntimeThreadMessage,
    RuntimeThreadState,
)
from langbridge.runtime.persistence.db.threads import (
    ConversationMemoryItem,
    MemoryCategory,
    Role,
    Thread,
    ThreadMessage,
    ThreadState,
)


def from_thread_record(value: Any | None) -> RuntimeThread | None:
    if value is None:
        return None
    if isinstance(value, RuntimeThread):
        return value
    state = getattr(value, "state", RuntimeThreadState.awaiting_user_input)
    return RuntimeThread(
        id=getattr(value, "id"),
        workspace_id=getattr(value, "workspace_id"),
        title=getattr(value, "title", None),
        state=str(getattr(state, "value", state)),
        metadata=dict(
            getattr(value, "metadata_json", None)
            or getattr(value, "metadata", None)
            or {}
        ),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
        created_by=(
            getattr(value, "created_by_actor_id", None)
            or getattr(value, "created_by")
        ),
        last_message_id=getattr(value, "last_message_id", None),
    )


def to_thread_record(value: RuntimeThread | Thread) -> Thread:
    if isinstance(value, Thread):
        return value
    return Thread(
        id=value.id,
        workspace_id=value.workspace_id,
        title=value.title,
        state=ThreadState(str(getattr(value.state, "value", value.state))),
        metadata_json=value.metadata_json,
        created_at=value.created_at,
        updated_at=value.updated_at,
        created_by_actor_id=value.created_by,
        last_message_id=value.last_message_id,
    )


def from_thread_message_record(value: Any | None) -> RuntimeThreadMessage | None:
    if value is None:
        return None
    if isinstance(value, RuntimeThreadMessage):
        return value
    role = getattr(value, "role", RuntimeMessageRole.user)
    return RuntimeThreadMessage(
        id=getattr(value, "id"),
        thread_id=getattr(value, "thread_id"),
        parent_message_id=getattr(value, "parent_message_id", None),
        role=str(getattr(role, "value", role)),
        content=dict(getattr(value, "content", None) or {}),
        model_snapshot=(
            getattr(value, "model_snapshot", None)
            or getattr(value, "model_snapshot_json", None)
        ),
        token_usage=(
            getattr(value, "token_usage", None)
            or getattr(value, "token_usage_json", None)
        ),
        error=getattr(value, "error", None),
        created_at=getattr(value, "created_at", None),
    )


def to_thread_message_record(
    value: RuntimeThreadMessage | ThreadMessage,
) -> ThreadMessage:
    if isinstance(value, ThreadMessage):
        return value
    return ThreadMessage(
        id=value.id,
        thread_id=value.thread_id,
        parent_message_id=value.parent_message_id,
        role=Role(str(getattr(value.role, "value", value.role))),
        content=dict(value.content or {}),
        model_snapshot=value.model_snapshot_json,
        token_usage=value.token_usage_json,
        error=value.error,
        created_at=value.created_at,
    )


def from_conversation_memory_record(
    value: Any | None,
) -> RuntimeConversationMemoryItem | None:
    if value is None:
        return None
    if isinstance(value, RuntimeConversationMemoryItem):
        return value
    category = getattr(value, "category", RuntimeConversationMemoryCategory.fact)
    return RuntimeConversationMemoryItem(
        id=getattr(value, "id"),
        thread_id=getattr(value, "thread_id"),
        actor_id=getattr(value, "actor_id", None),
        category=str(getattr(category, "value", category)),
        content=str(getattr(value, "content", "") or ""),
        metadata=dict(
            getattr(value, "metadata_json", None)
            or getattr(value, "metadata", None)
            or {}
        ),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
        last_accessed_at=getattr(value, "last_accessed_at", None),
    )


def to_conversation_memory_record(
    value: RuntimeConversationMemoryItem | ConversationMemoryItem,
) -> ConversationMemoryItem:
    if isinstance(value, ConversationMemoryItem):
        return value
    return ConversationMemoryItem(
        id=value.id,
        thread_id=value.thread_id,
        actor_id=value.actor_id,
        category=MemoryCategory(str(getattr(value.category, "value", value.category))),
        content=value.content,
        metadata_json=value.metadata_json,
        created_at=value.created_at,
        updated_at=value.updated_at,
        last_accessed_at=value.last_accessed_at,
    )


__all__ = [
    "from_conversation_memory_record",
    "from_thread_message_record",
    "from_thread_record",
    "to_conversation_memory_record",
    "to_thread_message_record",
    "to_thread_record",
]
