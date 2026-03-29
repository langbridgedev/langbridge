
import re
import uuid
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from langbridge.runtime.models import (
    DatasetExecutionCapabilities,
    DatasetMaterializationMode,
    DatasetRelationIdentity,
    DatasetSourceKind,
    DatasetStorageKind,
)

def resolve_dataset_source_kind(
    *,
    explicit_source_kind: str | None,
    legacy_dataset_type: str | None,
    connector_kind: str | None,
    file_config: Mapping[str, Any] | None,
) -> DatasetSourceKind:
    normalized = _normalize_enum_value(explicit_source_kind)
    if normalized:
        return DatasetSourceKind(normalized)

    legacy_type = _normalize_enum_value(legacy_dataset_type)
    sync_meta = _connector_sync_meta(file_config)
    if legacy_type in {"table", "sql"}:
        return DatasetSourceKind.DATABASE
    if legacy_type == "federated":
        return DatasetSourceKind.VIRTUAL
    if legacy_type == "file":
        if sync_meta and connector_kind:
            return DatasetSourceKind.API
        return DatasetSourceKind.FILE
    return DatasetSourceKind.FILE


def resolve_dataset_connector_kind(
    *,
    explicit_connector_kind: str | None,
    connection_connector_type: str | None,
    file_config: Mapping[str, Any] | None,
    storage_uri: str | None,
    legacy_dataset_type: str | None,
) -> str | None:
    normalized = _normalize_enum_value(explicit_connector_kind)
    if normalized:
        return normalized

    connector_type = _normalize_enum_value(connection_connector_type)
    if connector_type:
        return connector_type

    sync_meta = _connector_sync_meta(file_config)
    sync_connector_kind = _normalize_enum_value(
        sync_meta.get("connector_type") if sync_meta else None
    )
    if sync_connector_kind:
        return sync_connector_kind

    file_format = infer_file_storage_kind(file_config=file_config, storage_uri=storage_uri)
    legacy_type = _normalize_enum_value(legacy_dataset_type)
    if legacy_type == "file":
        if file_format == DatasetStorageKind.CSV:
            return "csv_upload"
        if file_format == DatasetStorageKind.PARQUET:
            return "parquet_lake"
        if file_format == DatasetStorageKind.JSON:
            return "json_upload"
        return "file_upload"
    if legacy_type == "federated":
        return "virtual"
    return None


def resolve_dataset_storage_kind(
    *,
    explicit_storage_kind: str | None,
    legacy_dataset_type: str | None,
    file_config: Mapping[str, Any] | None,
    storage_uri: str | None,
) -> DatasetStorageKind:
    normalized = _normalize_enum_value(explicit_storage_kind)
    if normalized:
        return DatasetStorageKind(normalized)

    legacy_type = _normalize_enum_value(legacy_dataset_type)
    if legacy_type == "table":
        return DatasetStorageKind.TABLE
    if legacy_type == "sql":
        return DatasetStorageKind.VIEW
    if legacy_type == "federated":
        return DatasetStorageKind.VIRTUAL
    if legacy_type == "file":
        return infer_file_storage_kind(file_config=file_config, storage_uri=storage_uri)
    return DatasetStorageKind.VIRTUAL


def infer_file_storage_kind(
    *,
    file_config: Mapping[str, Any] | None,
    storage_uri: str | None,
) -> DatasetStorageKind:
    configured = _normalize_enum_value(
        (file_config or {}).get("format") or (file_config or {}).get("file_format")
    )
    if configured in {"csv", "parquet", "json"}:
        return DatasetStorageKind(configured)

    suffix = Path(str(storage_uri or "")).suffix.strip().lower()
    if suffix == ".parquet":
        return DatasetStorageKind.PARQUET
    if suffix == ".json":
        return DatasetStorageKind.JSON
    return DatasetStorageKind.CSV


def build_dataset_relation_identity(
    *,
    dataset_id: uuid.UUID | str | None,
    connector_id: uuid.UUID | str | None,
    dataset_name: str | None,
    catalog_name: str | None,
    schema_name: str | None,
    table_name: str | None,
    storage_uri: str | None,
    source_kind: DatasetSourceKind,
    storage_kind: DatasetStorageKind,
    existing_payload: Mapping[str, Any] | None = None,
) -> DatasetRelationIdentity:
    if existing_payload:
        try:
            parsed = DatasetRelationIdentity.model_validate(dict(existing_payload))
            if _should_suppress_synthetic_schema(
                source_kind=source_kind,
                storage_kind=storage_kind,
                schema_name=parsed.schema_name,
            ):
                return parsed.model_copy(
                    update={
                        "qualified_name": parsed.table_name or parsed.relation_name,
                        "catalog_name": None,
                        "schema_name": None,
                    }
                )
            return parsed
        except Exception:
            pass

    normalized_schema_name = None if _should_suppress_synthetic_schema(
        source_kind=source_kind,
        storage_kind=storage_kind,
        schema_name=schema_name,
    ) else schema_name
    normalized_table_name = str(table_name or "").strip() or _normalized_relation_name(
        dataset_name=dataset_name,
        storage_uri=storage_uri,
    )
    qualified_name = _qualified_name(
        catalog_name=catalog_name,
        schema_name=normalized_schema_name,
        table_name=table_name or normalized_table_name,
    )
    if dataset_id is not None:
        canonical_reference = f"dataset:{dataset_id}"
    elif storage_uri:
        canonical_reference = f"storage:{storage_uri.strip()}"
    elif qualified_name:
        canonical_reference = f"relation:{qualified_name}"
    else:
        canonical_reference = f"relation:{normalized_table_name}"

    return DatasetRelationIdentity(
        canonical_reference=canonical_reference,
        relation_name=normalized_table_name,
        qualified_name=qualified_name,
        catalog_name=_string_or_none(catalog_name),
        schema_name=_string_or_none(normalized_schema_name),
        table_name=_string_or_none(table_name or normalized_table_name),
        storage_uri=_string_or_none(storage_uri),
        dataset_id=_coerce_uuid(dataset_id),
        connector_id=_coerce_uuid(connector_id),
        source_kind=source_kind,
        storage_kind=storage_kind,
    )


def build_dataset_execution_capabilities(
    *,
    source_kind: DatasetSourceKind,
    storage_kind: DatasetStorageKind,
    existing_payload: Mapping[str, Any] | None = None,
) -> DatasetExecutionCapabilities:
    if existing_payload:
        try:
            return DatasetExecutionCapabilities.model_validate(dict(existing_payload))
        except Exception:
            pass

    if storage_kind in {DatasetStorageKind.TABLE, DatasetStorageKind.VIEW}:
        return DatasetExecutionCapabilities(
            supports_structured_scan=True,
            supports_sql_federation=True,
            supports_filter_pushdown=True,
            supports_projection_pushdown=True,
            supports_aggregation_pushdown=True,
            supports_join_pushdown=False,
            supports_materialization=True,
            supports_semantic_modeling=True,
        )

    if storage_kind in {DatasetStorageKind.CSV, DatasetStorageKind.PARQUET}:
        return DatasetExecutionCapabilities(
            supports_structured_scan=True,
            supports_sql_federation=True,
            supports_filter_pushdown=True,
            supports_projection_pushdown=True,
            supports_aggregation_pushdown=True,
            supports_join_pushdown=False,
            supports_materialization=True,
            supports_semantic_modeling=True,
        )

    if storage_kind == DatasetStorageKind.VIRTUAL:
        return DatasetExecutionCapabilities(
            supports_structured_scan=True,
            supports_sql_federation=True,
            supports_filter_pushdown=True,
            supports_projection_pushdown=True,
            supports_aggregation_pushdown=False,
            supports_join_pushdown=False,
            supports_materialization=True,
            supports_semantic_modeling=True,
        )

    supports_materialization = source_kind in {
        DatasetSourceKind.API,
        DatasetSourceKind.FILE,
        DatasetSourceKind.SAAS,
    }
    return DatasetExecutionCapabilities(
        supports_structured_scan=False,
        supports_sql_federation=False,
        supports_filter_pushdown=False,
        supports_projection_pushdown=False,
        supports_aggregation_pushdown=False,
        supports_join_pushdown=False,
        supports_materialization=supports_materialization,
        supports_semantic_modeling=False,
    )


def resolve_dataset_materialization_mode(
    *,
    explicit_materialization_mode: str | DatasetMaterializationMode | None,
    file_config: Mapping[str, Any] | None,
) -> DatasetMaterializationMode:
    normalized = _normalize_enum_value(explicit_materialization_mode)
    if normalized:
        return DatasetMaterializationMode(normalized)

    sync_meta = _connector_sync_meta(file_config)
    if sync_meta:
        return DatasetMaterializationMode.SYNCED

    config_payload = dict(file_config or {})
    if config_payload.get("managed_dataset") or config_payload.get("source_storage_uri"):
        return DatasetMaterializationMode.SYNCED
    return DatasetMaterializationMode.LIVE


def dataset_supports_structured_federation(
    *,
    source_kind: str | DatasetSourceKind | None,
    storage_kind: str | DatasetStorageKind | None,
    capabilities: Mapping[str, Any] | DatasetExecutionCapabilities | None,
) -> bool:
    capability_payload: DatasetExecutionCapabilities
    if isinstance(capabilities, DatasetExecutionCapabilities):
        capability_payload = capabilities
    else:
        capability_payload = build_dataset_execution_capabilities(
            source_kind=DatasetSourceKind(
                str(source_kind or DatasetSourceKind.FILE.value).lower()
            ),
            storage_kind=DatasetStorageKind(
                str(storage_kind or DatasetStorageKind.CSV.value).lower()
            ),
            existing_payload=capabilities,
        )
    return bool(
        capability_payload.supports_structured_scan
        and capability_payload.supports_sql_federation
    )


def derive_legacy_dataset_type(
    *,
    source_kind: str | DatasetSourceKind,
    storage_kind: str | DatasetStorageKind,
) -> str:
    source_value = str(source_kind).split(".")[-1].lower()
    storage_value = str(storage_kind).split(".")[-1].lower()
    if (
        source_value == DatasetSourceKind.VIRTUAL.value
        or storage_value == DatasetStorageKind.VIRTUAL.value
    ):
        return "FEDERATED"
    if storage_value in {
        DatasetStorageKind.CSV.value,
        DatasetStorageKind.PARQUET.value,
        DatasetStorageKind.JSON.value,
    }:
        return "FILE"
    if storage_value == DatasetStorageKind.VIEW.value:
        return "SQL"
    return "TABLE"


def _connector_sync_meta(file_config: Mapping[str, Any] | None) -> Mapping[str, Any]:
    payload = (file_config or {}).get("connector_sync")
    if isinstance(payload, Mapping):
        return payload
    return {}


def _should_suppress_synthetic_schema(
    *,
    source_kind: DatasetSourceKind,
    storage_kind: DatasetStorageKind,
    schema_name: str | None,
) -> bool:
    return (
        storage_kind
        in {DatasetStorageKind.CSV, DatasetStorageKind.PARQUET, DatasetStorageKind.JSON}
        and str(schema_name or "").strip().lower() == "api_connector"
    )


def _normalize_enum_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Enum):
        raw_value = value.value
    else:
        raw_value = value
    normalized = str(raw_value).strip().lower()
    return normalized or None


def _normalized_relation_name(*, dataset_name: str | None, storage_uri: str | None) -> str:
    name = str(dataset_name or "").strip()
    if not name and storage_uri:
        name = Path(str(storage_uri)).stem
    cleaned = re.sub(r"[^0-9a-zA-Z_]+", "_", name).strip("_").lower()
    return cleaned or "dataset"


def _qualified_name(
    *,
    catalog_name: str | None,
    schema_name: str | None,
    table_name: str | None,
) -> str | None:
    parts = [
        str(part).strip()
        for part in (catalog_name, schema_name, table_name)
        if str(part or "").strip()
    ]
    if not parts:
        return None
    return ".".join(parts)


def _string_or_none(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _coerce_uuid(value: Any) -> uuid.UUID | None:
    if value in {None, ""}:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None
