from typing import Any
from urllib.parse import urlencode

from langbridge.runtime.models import DatasetMetadata
from langbridge.runtime.models.metadata import DatasetSource, DatasetSourceKind
from langbridge.runtime.utils.lineage import stable_payload_hash


def enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def relation_parts(relation_name: str) -> tuple[str | None, str | None, str]:
    parts = [part.strip() for part in str(relation_name or "").split(".") if part.strip()]
    if not parts:
        raise ValueError("Dataset table source must not be empty.")
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[0], parts[1], parts[2]


class DatasetSyncSourceResolver:
    """Normalizes dataset sync source contracts into stable runtime identifiers."""

    def sync_source(self, dataset: DatasetMetadata) -> DatasetSource:
        source = dataset.source_json
        if isinstance(source, dict) and source:
            return DatasetSource.model_validate(source)
        sync_meta = dict(dataset.sync_json or {})
        legacy_source = sync_meta.get("source")
        if isinstance(legacy_source, dict) and legacy_source:
            return DatasetSource.model_validate(legacy_source)
        raise ValueError(f"Dataset '{dataset.name}' is missing source.")

    def sync_source_key(self, source: DatasetSource) -> str:
        if source.resource:
            return f"resource:{str(source.resource).strip()}"
        if source.request:
            return f"request:{self.request_signature(source)}"
        if source.table:
            return f"table:{str(source.table).strip()}"
        if source.sql:
            return f"sql:{stable_payload_hash(str(source.sql).strip())}"
        if source.storage_uri:
            return f"storage:{str(source.storage_uri).strip()}"
        raise ValueError("Dataset sync source is missing.")

    def sync_source_kind(self, source: DatasetSource) -> DatasetSourceKind:
        if source.resource or source.request:
            return DatasetSourceKind.API
        if source.table or source.sql:
            return DatasetSourceKind.DATABASE
        if source.storage_uri:
            return DatasetSourceKind.FILE
        raise ValueError("Dataset sync source is missing.")

    def sync_source_payload(self, source: DatasetSource) -> dict[str, Any]:
        return source.model_dump(mode="json", exclude_none=True)

    def sync_source_label(self, source: DatasetSource) -> str:
        if source.resource:
            return f"resource path '{str(source.resource).strip()}'"
        if source.request:
            return f"request '{self.request_display_path(source)}'"
        if source.table:
            return f"table '{str(source.table).strip()}'"
        if source.sql:
            return "SQL query"
        if source.storage_uri:
            return f"storage source '{str(source.storage_uri).strip()}'"
        return "sync source"

    def request_display_path(self, source: DatasetSource) -> str:
        request = source.request
        if request is None:
            return ""
        path = str(request.path or "").strip()
        if not path:
            return ""
        params = dict(request.params or {})
        if not params:
            return path
        return f"{path}?{urlencode(sorted((str(key), value) for key, value in params.items()), doseq=True)}"

    def request_signature(self, source: DatasetSource) -> str:
        request = source.request
        if request is None:
            return ""
        payload = request.model_dump(mode="json", exclude_none=True)
        if source.extraction is not None:
            payload["extraction"] = source.extraction.model_dump(mode="json", exclude_none=True)
        return stable_payload_hash(payload)

    def request_resource_path(self, source: DatasetSource) -> str:
        request = source.request
        if request is None:
            raise ValueError("Dataset request source is missing request config.")
        path = str(request.path or "").strip().split("?", 1)[0].strip("/")
        if not path:
            return "request"
        return ".".join(segment for segment in path.split("/") if segment)
