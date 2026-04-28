from typing import Any
import uuid


def _coerce_uuid(value: Any, *, field_name: str | None = None) -> uuid.UUID | None:
    if value in {None, ""}:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None
