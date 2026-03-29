
import json
from enum import Enum
from typing import Any

from langbridge.runtime.models import (
    SemanticModelMetadata,
    SemanticVectorIndexMetadata,
)
from langbridge.runtime.models.metadata import LifecycleState, ManagementMode
from langbridge.runtime.persistence.db.semantic import SemanticModelEntry
from langbridge.runtime.persistence.db.semantic import SemanticVectorIndexEntry


def _enum_value(value: Any, *, default: str | None = None) -> str | None:
    if value is None:
        return default
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


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
        management_mode=ManagementMode(str(getattr(value, "management_mode", "runtime_managed")).lower()),
        lifecycle_state=LifecycleState(str(getattr(value, "lifecycle_state", "active")).lower())
    )
    
def to_semantic_model_record(
    value: SemanticModelMetadata | Any,
) -> Any:
    if isinstance(value, SemanticModelEntry):
        return value
    if hasattr(value, "content_json") and not isinstance(value.content_json, str):
        content_json = json.dumps(value.content_json)
    else:
        content_json = getattr(value, "content_json", None)
    return SemanticModelEntry(
        id=value.id,
        connector_id=getattr(value, "connector_id", None),
        workspace_id=getattr(value, "workspace_id"),
        name=value.name,
        description=value.description,
        content_yaml=str(getattr(value, "content_yaml")),
        content_json=content_json,
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
        management_mode=str(value.management_mode.value or "runtime_managed"),
        lifecycle_state=str(value.lifecycle_state.value or "active"),
    )


def from_semantic_vector_index_record(
    value: Any | None,
) -> SemanticVectorIndexMetadata | None:
    if value is None:
        return None
    if isinstance(value, SemanticVectorIndexMetadata):
        return value
    return SemanticVectorIndexMetadata(
        id=getattr(value, "id"),
        workspace_id=getattr(value, "workspace_id"),
        semantic_model_id=getattr(value, "semantic_model_id"),
        dataset_key=str(getattr(value, "dataset_key")),
        dimension_name=str(getattr(value, "dimension_name")),
        vector_store_target=_enum_value(getattr(value, "vector_store_target")),
        vector_connector_name=getattr(value, "vector_connector_name", None),
        vector_connector_id=getattr(value, "vector_connector_id", None),
        vector_index_name=str(getattr(value, "vector_index_name")),
        refresh_interval_seconds=getattr(value, "refresh_interval_seconds", None),
        refresh_status=_enum_value(
            getattr(value, "refresh_status", None),
            default="pending",
        ),
        indexed_value_count=getattr(value, "indexed_value_count", None),
        embedding_dimension=getattr(value, "embedding_dimension", None),
        last_refresh_started_at=getattr(value, "last_refresh_started_at", None),
        last_refreshed_at=getattr(value, "last_refreshed_at", None),
        last_refresh_error=getattr(value, "last_refresh_error", None),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_semantic_vector_index_record(
    value: SemanticVectorIndexMetadata | SemanticVectorIndexEntry,
) -> SemanticVectorIndexEntry:
    if isinstance(value, SemanticVectorIndexEntry):
        return value
    return SemanticVectorIndexEntry(
        id=value.id,
        workspace_id=value.workspace_id,
        semantic_model_id=value.semantic_model_id,
        dataset_key=value.dataset_key,
        dimension_name=value.dimension_name,
        vector_store_target=_enum_value(value.vector_store_target) or "managed_faiss",
        vector_connector_name=value.vector_connector_name,
        vector_connector_id=value.vector_connector_id,
        vector_index_name=value.vector_index_name,
        refresh_interval_seconds=value.refresh_interval_seconds,
        refresh_status=_enum_value(value.refresh_status, default="pending") or "pending",
        indexed_value_count=value.indexed_value_count,
        embedding_dimension=value.embedding_dimension,
        last_refresh_started_at=value.last_refresh_started_at,
        last_refreshed_at=value.last_refreshed_at,
        last_refresh_error=value.last_refresh_error,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


__all__ = [
    "from_semantic_model_record",
    "from_semantic_vector_index_record",
    "to_semantic_vector_index_record",
    "to_semantic_model_record",
]
