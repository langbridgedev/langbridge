
from typing import Any

from langbridge.runtime.models import (
    DatasetColumnMetadata,
    DatasetMaterializationConfig,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    DatasetSchemaHint,
    DatasetSource,
    DatasetSyncConfig,
)
from langbridge.runtime.models.metadata import LifecycleState, ManagementMode
from langbridge.runtime.persistence.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)


def from_dataset_column_record(value: Any) -> DatasetColumnMetadata:
    if isinstance(value, DatasetColumnMetadata):
        return value
    return DatasetColumnMetadata(
        id=getattr(value, "id"),
        dataset_id=getattr(value, "dataset_id"),
        workspace_id=getattr(value, "workspace_id", None),
        name=str(getattr(value, "name")),
        data_type=str(getattr(value, "data_type")),
        nullable=bool(getattr(value, "nullable", True)),
        description=getattr(value, "description", None),
        is_allowed=bool(getattr(value, "is_allowed", True)),
        is_computed=bool(getattr(value, "is_computed", False)),
        expression=getattr(value, "expression", None),
        ordinal_position=int(getattr(value, "ordinal_position", 0) or 0),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_dataset_column_record(
    value: DatasetColumnMetadata | DatasetColumnRecord,
) -> DatasetColumnRecord:
    if isinstance(value, DatasetColumnRecord):
        return value
    return DatasetColumnRecord(
        id=value.id,
        dataset_id=value.dataset_id,
        workspace_id=getattr(value, "workspace_id"),
        name=value.name,
        data_type=value.data_type,
        nullable=value.nullable,
        ordinal_position=value.ordinal_position,
        description=value.description,
        is_allowed=value.is_allowed,
        is_computed=value.is_computed,
        expression=value.expression,
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def from_dataset_policy_record(value: Any | None) -> DatasetPolicyMetadata | None:
    if value is None:
        return None
    if isinstance(value, DatasetPolicyMetadata):
        return value
    return DatasetPolicyMetadata(
        id=getattr(value, "id", None),
        dataset_id=getattr(value, "dataset_id", None),
        workspace_id=getattr(value, "workspace_id", None),
        max_rows_preview=int(getattr(value, "max_rows_preview", 1000) or 1000),
        max_export_rows=int(getattr(value, "max_export_rows", 10000) or 10000),
        redaction_rules=dict(
            getattr(value, "redaction_rules", None)
            or getattr(value, "redaction_rules_json", None)
            or {}
        ),
        row_filters=list(
            getattr(value, "row_filters", None)
            or getattr(value, "row_filters_json", None)
            or []
        ),
        allow_dml=bool(getattr(value, "allow_dml", False)),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_dataset_policy_record(
    value: DatasetPolicyMetadata | DatasetPolicyRecord,
) -> DatasetPolicyRecord:
    if isinstance(value, DatasetPolicyRecord):
        return value
    return DatasetPolicyRecord(
        id=value.id,
        dataset_id=value.dataset_id,
        workspace_id=value.workspace_id,
        max_rows_preview=value.max_rows_preview,
        max_export_rows=value.max_export_rows,
        redaction_rules_json=value.redaction_rules_json,
        row_filters_json=value.row_filters_json,
        allow_dml=value.allow_dml,
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def from_dataset_record(value: Any | None) -> DatasetMetadata | None:
    if value is None:
        return None
    if isinstance(value, DatasetMetadata):
        return value
    columns_raw = getattr(value, "columns", None) or []
    materialization_mode = _infer_dataset_record_materialization_mode(value)
    source = _infer_dataset_record_source(value, materialization_mode=materialization_mode)
    schema_hint = _infer_dataset_record_schema_hint(value)
    materialization = _infer_dataset_record_materialization(value, materialization_mode=materialization_mode)
    return DatasetMetadata(
        id=getattr(value, "id"),
        workspace_id=getattr(value, "workspace_id"),
        connection_id=getattr(value, "connection_id", None),
        owner_id=(
            getattr(value, "owner_id", None)
            or getattr(value, "created_by_actor_id", None)
            or getattr(value, "created_by", None)
        ),
        created_by=(
            getattr(value, "created_by_actor_id", None)
            or getattr(value, "created_by", None)
        ),
        updated_by=(
            getattr(value, "updated_by_actor_id", None)
            or getattr(value, "updated_by", None)
        ),
        name=str(getattr(value, "name")),
        label=getattr(value, "label", None),
        sql_alias=str(getattr(value, "sql_alias")),
        description=getattr(value, "description", None),
        tags=list(getattr(value, "tags", None) or getattr(value, "tags_json", None) or []),
        dataset_type=getattr(value, "dataset_type"),
        materialization=materialization,
        source=source,
        schema_hint=schema_hint,
        source_kind=getattr(value, "source_kind", None),
        connector_kind=getattr(value, "connector_kind", None),
        storage_kind=getattr(value, "storage_kind", None),
        dialect=getattr(value, "dialect", None),
        catalog_name=getattr(value, "catalog_name", None),
        schema_name=getattr(value, "schema_name", None),
        table_name=getattr(value, "table_name", None),
        storage_uri=getattr(value, "storage_uri", None),
        sql_text=getattr(value, "sql_text", None),
        relation_identity=(
            getattr(value, "relation_identity", None)
            or getattr(value, "relation_identity_json", None)
        ),
        execution_capabilities=(
            getattr(value, "execution_capabilities", None)
            or getattr(value, "execution_capabilities_json", None)
        ),
        referenced_dataset_ids=list(
            getattr(value, "referenced_dataset_ids", None)
            or getattr(value, "referenced_dataset_ids_json", None)
            or []
        ),
        federated_plan=(
            getattr(value, "federated_plan", None)
            or getattr(value, "federated_plan_json", None)
        ),
        file_config=getattr(value, "file_config", None)
        or getattr(value, "file_config_json", None),
        status=getattr(value, "status", None),
        revision_id=getattr(value, "revision_id", None),
        row_count_estimate=getattr(value, "row_count_estimate", None),
        bytes_estimate=getattr(value, "bytes_estimate", None),
        last_profiled_at=getattr(value, "last_profiled_at", None),
        columns=[from_dataset_column_record(column) for column in columns_raw],
        policy=from_dataset_policy_record(getattr(value, "policy", None)),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
        management_mode=ManagementMode(str(getattr(value, "management_mode", "runtime_managed")).lower()),
        lifecycle_state=LifecycleState(str(getattr(value, "lifecycle_state", "active")).lower())
    )


def _infer_dataset_record_materialization_mode(value: Any) -> Any:
    explicit = getattr(value, "materialization_mode", None)
    if not _is_blank_value(explicit):
        return explicit
    sync = getattr(value, "sync", None) or getattr(value, "sync_json", None)
    if not _is_blank_value(sync):
        return "synced"
    return "live"


def _infer_dataset_record_materialization(
    value: Any,
    *,
    materialization_mode: Any,
) -> DatasetMaterializationConfig:
    materialization_payload = {"mode": materialization_mode}
    sync_payload = getattr(value, "sync", None) or getattr(value, "sync_json", None)
    if not _is_blank_value(sync_payload):
        payload = dict(sync_payload)
        payload.pop("source", None)
        materialization_payload["sync"] = payload
    return DatasetMaterializationConfig.model_validate(materialization_payload)


def _infer_dataset_record_source(
    value: Any,
    *,
    materialization_mode: Any,
) -> Any:
    explicit = getattr(value, "source", None) or getattr(value, "source_json", None)
    if not _is_blank_value(explicit):
        if isinstance(explicit, dict):
            payload = dict(explicit)
            payload.pop("schema_hint", None)
            payload.pop("schema_hint_dynamic", None)
            return payload
        return explicit

    if str(materialization_mode or "").strip().lower() == "synced":
        sync_payload = getattr(value, "sync", None) or getattr(value, "sync_json", None) or {}
        if isinstance(sync_payload, dict):
            source_payload = sync_payload.get("source")
            if isinstance(source_payload, dict):
                payload = dict(source_payload)
                payload.pop("schema_hint", None)
                payload.pop("schema_hint_dynamic", None)
                return payload
        return None

    storage_uri = str(getattr(value, "storage_uri", None) or "").strip()
    if storage_uri:
        source = {"storage_uri": storage_uri}
        file_config = getattr(value, "file_config", None) or getattr(value, "file_config_json", None) or {}
        if isinstance(file_config, dict):
            for key in ("format", "file_format", "header", "delimiter", "quote"):
                if file_config.get(key) is not None:
                    source[key] = file_config[key]
        return source

    sql_text = str(getattr(value, "sql_text", None) or "").strip()
    if sql_text:
        return {"sql": sql_text}

    table_name = str(getattr(value, "table_name", None) or "").strip()
    if table_name:
        return {"table": table_name}
    return None


def _infer_dataset_record_schema_hint(value: Any) -> DatasetSchemaHint | None:
    source_payload = getattr(value, "source", None) or getattr(value, "source_json", None) or {}
    if not isinstance(source_payload, dict):
        sync_payload = getattr(value, "sync", None) or getattr(value, "sync_json", None) or {}
        if isinstance(sync_payload, dict):
            source_payload = sync_payload.get("source") or {}
    if not isinstance(source_payload, dict):
        return None
    columns = source_payload.get("schema_hint")
    dynamic = bool(source_payload.get("schema_hint_dynamic", False))
    if not columns and not dynamic:
        return None
    normalized_columns = []
    for column in columns or []:
        if not isinstance(column, dict):
            continue
        payload = dict(column)
        if payload.get("type") is None and payload.get("data_type") is not None:
            payload["type"] = payload.pop("data_type")
        normalized_columns.append(payload)
    payload = {
        "columns": normalized_columns,
        "dynamic": dynamic,
    }
    return DatasetSchemaHint.model_validate(payload)


def _is_blank_value(value: Any) -> bool:
    if value is None or value == "":
        return True
    if isinstance(value, dict) and not value:
        return True
    return False


def to_dataset_record(value: DatasetMetadata | DatasetRecord) -> DatasetRecord:
    if isinstance(value, DatasetRecord):
        return value
    return DatasetRecord(
        id=value.id,
        workspace_id=value.workspace_id,
        connection_id=value.connection_id,
        created_by_actor_id=value.created_by,
        updated_by_actor_id=value.updated_by,
        name=value.name,
        label=value.label,
        sql_alias=value.sql_alias,
        description=value.description,
        tags_json=list(value.tags),
        dataset_type=value.dataset_type_value,
        materialization_mode=value.materialization_mode_value,
        source_json=(
            None
            if value.source is None
            else (
                _source_json_with_schema_hint(value)
                if isinstance(value.source, DatasetSource)
                else DatasetSource.model_validate(value.source).model_dump(mode="json")
            )
        ),
        sync_json=(
            None
            if value.sync is None
            else (
                value.sync_json
                if isinstance(value.sync, DatasetSyncConfig)
                else DatasetSyncConfig.model_validate(value.sync).model_dump(mode="json")
            )
        ),
        source_kind=value.source_kind_value,
        connector_kind=value.connector_kind,
        storage_kind=value.storage_kind_value,
        dialect=value.dialect,
        catalog_name=value.catalog_name,
        schema_name=value.schema_name,
        table_name=value.table_name,
        storage_uri=value.storage_uri,
        sql_text=value.sql_text,
        relation_identity_json=value.relation_identity_json,
        execution_capabilities_json=value.execution_capabilities_json,
        referenced_dataset_ids_json=[str(item) for item in value.referenced_dataset_ids_json],
        federated_plan_json=value.federated_plan_json,
        file_config_json=value.file_config_json,
        status=value.status_value,
        revision_id=value.revision_id,
        row_count_estimate=value.row_count_estimate,
        bytes_estimate=value.bytes_estimate,
        last_profiled_at=value.last_profiled_at,
        created_at=value.created_at,
        updated_at=value.updated_at,
        management_mode=str(value.management_mode.value or "runtime_managed"),
        lifecycle_state=str(value.lifecycle_state.value or "active"),
    )


def _source_json_with_schema_hint(value: DatasetMetadata) -> dict[str, Any]:
    payload = dict(value.source_json or {})
    if value.schema_hint is None:
        return payload
    payload["schema_hint"] = [
        {
            "name": column.name,
            "data_type": column.type,
            "nullable": bool(column.nullable),
            **({"description": column.description} if column.description is not None else {}),
            **({"path": column.path} if column.path is not None else {}),
            **({"default_value": column.default_value} if column.default_value is not None else {}),
        }
        for column in value.schema_hint.columns
    ]
    if value.schema_hint.dynamic:
        payload["schema_hint_dynamic"] = True
    return payload


def from_dataset_revision_record(value: Any | None) -> DatasetRevision | None:
    if value is None:
        return None
    if isinstance(value, DatasetRevision):
        return value
    return DatasetRevision.model_validate(value)


def to_dataset_revision_record(
    value: DatasetRevision | DatasetRevisionRecord,
) -> DatasetRevisionRecord:
    if isinstance(value, DatasetRevisionRecord):
        return value
    return DatasetRevisionRecord(
        id=value.id,
        dataset_id=value.dataset_id,
        workspace_id=value.workspace_id,
        revision_number=value.revision_number,
        revision_hash=value.revision_hash,
        change_summary=value.change_summary,
        definition_json=value.definition_json,
        schema_json=value.schema_json,
        policy_json=value.policy_json,
        source_bindings_json=value.source_bindings_json,
        execution_characteristics_json=value.execution_characteristics_json,
        status=value.status,
        snapshot_json=value.snapshot_json,
        note=value.note,
        created_by_actor_id=value.created_by,
        created_at=value.created_at,
    )


__all__ = [
    "from_dataset_column_record",
    "from_dataset_policy_record",
    "from_dataset_record",
    "from_dataset_revision_record",
    "to_dataset_column_record",
    "to_dataset_policy_record",
    "to_dataset_record",
    "to_dataset_revision_record",
]
