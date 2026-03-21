from __future__ import annotations

from typing import Any

from langbridge.runtime.models import (
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
)
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
        sql_alias=str(getattr(value, "sql_alias")),
        description=getattr(value, "description", None),
        tags=list(getattr(value, "tags", None) or getattr(value, "tags_json", None) or []),
        dataset_type=str(getattr(value, "dataset_type")),
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
        status=str(getattr(value, "status", "published") or "published"),
        revision_id=getattr(value, "revision_id", None),
        row_count_estimate=getattr(value, "row_count_estimate", None),
        bytes_estimate=getattr(value, "bytes_estimate", None),
        last_profiled_at=getattr(value, "last_profiled_at", None),
        columns=[from_dataset_column_record(column) for column in columns_raw],
        policy=from_dataset_policy_record(getattr(value, "policy", None)),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


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
        sql_alias=value.sql_alias,
        description=value.description,
        tags_json=list(value.tags),
        dataset_type=value.dataset_type,
        source_kind=value.source_kind,
        connector_kind=value.connector_kind,
        storage_kind=value.storage_kind,
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
        status=value.status,
        revision_id=value.revision_id,
        row_count_estimate=value.row_count_estimate,
        bytes_estimate=value.bytes_estimate,
        last_profiled_at=value.last_profiled_at,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


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
