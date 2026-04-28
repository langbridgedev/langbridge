
from typing import Any

from langbridge.runtime.models import LLMConnectionSecret
from langbridge.runtime.persistence.db.agent import LLMConnection


def from_llm_connection_record(value: Any | None) -> LLMConnectionSecret | None:
    if value is None:
        return None
    if isinstance(value, LLMConnectionSecret):
        return value
    return LLMConnectionSecret(
        id=getattr(value, "id"),
        name=str(getattr(value, "name")),
        provider=str(getattr(value, "provider")),
        model=str(getattr(value, "model")),
        configuration=dict(getattr(value, "configuration", None) or {}),
        api_key=str(getattr(value, "api_key")),
        description=getattr(value, "description", None),
        is_active=bool(getattr(value, "is_active", True)),
        default=bool(getattr(value, "default", False)),
        workspace_id=getattr(value, "workspace_id", None),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_llm_connection_record(
    value: LLMConnectionSecret | LLMConnection,
) -> LLMConnection:
    if isinstance(value, LLMConnection):
        return value
    return LLMConnection(
        id=value.id,
        name=value.name,
        description=value.description,
        provider=str(getattr(value.provider, "value", value.provider)),
        api_key=value.api_key,
        model=value.model,
        configuration=dict(value.configuration or {}),
        is_active=value.is_active,
        default=value.default,
        workspace_id=value.workspace_id,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


__all__ = ["from_llm_connection_record", "to_llm_connection_record"]
