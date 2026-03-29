
from typing import Any

from langbridge.runtime.models import LineageEdge
from langbridge.runtime.persistence.db.lineage import LineageEdgeRecord


def from_lineage_edge_record(value: Any | None) -> LineageEdge | None:
    if value is None:
        return None
    if isinstance(value, LineageEdge):
        return value
    return LineageEdge(
        id=getattr(value, "id", None),
        workspace_id=getattr(value, "workspace_id"),
        source_type=str(getattr(value, "source_type")),
        source_id=str(getattr(value, "source_id")),
        target_type=str(getattr(value, "target_type")),
        target_id=str(getattr(value, "target_id")),
        edge_type=str(getattr(value, "edge_type")),
        metadata=dict(getattr(value, "metadata_json", None) or {}),
        created_at=getattr(value, "created_at", None),
    )


def to_lineage_edge_record(value: LineageEdge | LineageEdgeRecord) -> LineageEdgeRecord:
    if isinstance(value, LineageEdgeRecord):
        return value
    return LineageEdgeRecord(
        id=value.id,
        workspace_id=value.workspace_id,
        source_type=value.source_type,
        source_id=value.source_id,
        target_type=value.target_type,
        target_id=value.target_id,
        edge_type=value.edge_type,
        metadata_json=value.metadata_json,
        created_at=value.created_at,
    )


__all__ = ["from_lineage_edge_record", "to_lineage_edge_record"]
