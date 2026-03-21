from __future__ import annotations

import json
from typing import Any

from langbridge.runtime.models import SemanticModelMetadata


def from_semantic_model_record(value: Any | None) -> SemanticModelMetadata | None:
    if value is None:
        return None
    if isinstance(value, SemanticModelMetadata):
        return value
    content_json = getattr(value, "content_json", None)
    if isinstance(content_json, str):
        try:
            parsed = json.loads(content_json)
        except json.JSONDecodeError:
            parsed = content_json
        content_json = parsed
    return SemanticModelMetadata(
        id=getattr(value, "id"),
        connector_id=getattr(value, "connector_id", None),
        workspace_id=getattr(value, "workspace_id"),
        name=str(getattr(value, "name")),
        description=getattr(value, "description", None),
        content_yaml=str(getattr(value, "content_yaml")),
        content_json=content_json,
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


__all__ = ["from_semantic_model_record"]
