import re
from typing import Any, Mapping

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.runtime.models import DatasetColumnMetadata, DatasetMetadata, DatasetPolicyMetadata
from langbridge.runtime.models.metadata import DatasetSource
from langbridge.runtime.services.dataset_sync.sources import DatasetSyncSourceResolver
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
)

_RESOURCE_SANITIZER = re.compile(r"[^0-9A-Za-z_]+")


class DatasetSyncMetadataBuilder:
    """Builds dataset metadata, tags, descriptors, and revision snapshots."""

    def __init__(self, *, source_resolver: DatasetSyncSourceResolver) -> None:
        self._source_resolver = source_resolver

    def column_snapshot(self, column: DatasetColumnMetadata) -> dict[str, Any]:
        return {
            "id": str(column.id),
            "dataset_id": str(column.dataset_id),
            "name": column.name,
            "data_type": column.data_type,
            "nullable": column.nullable,
            "description": column.description,
            "is_allowed": column.is_allowed,
            "is_computed": column.is_computed,
            "expression": column.expression,
            "ordinal_position": column.ordinal_position,
        }

    def policy_snapshot(self, policy: DatasetPolicyMetadata) -> dict[str, Any]:
        return {
            "max_rows_preview": policy.max_rows_preview,
            "max_export_rows": policy.max_export_rows,
            "redaction_rules": dict(policy.redaction_rules_json or {}),
            "row_filters": list(policy.row_filters_json or []),
            "allow_dml": policy.allow_dml,
        }

    def build_dataset_definition_snapshot(self, dataset: DatasetMetadata) -> dict[str, Any]:
        relation_identity, execution_capabilities = self.resolve_dataset_descriptor_snapshot(dataset)
        return {
            "id": str(dataset.id),
            "workspace_id": str(dataset.workspace_id),
            "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
            "name": dataset.name,
            "description": dataset.description,
            "tags": list(dataset.tags_json or []),
            "dataset_type": dataset.dataset_type_value,
            "materialization_mode": dataset.materialization_mode_value,
            "source": dataset.source_json,
            "sync": dataset.sync_json,
            "source_kind": dataset.source_kind_value,
            "connector_kind": dataset.connector_kind,
            "storage_kind": dataset.storage_kind_value,
            "dialect": dataset.dialect,
            "storage_uri": dataset.storage_uri,
            "catalog_name": dataset.catalog_name,
            "schema_name": dataset.schema_name,
            "table_name": dataset.table_name,
            "sql_text": dataset.sql_text,
            "referenced_dataset_ids": list(dataset.referenced_dataset_ids_json or []),
            "federated_plan": dataset.federated_plan_json,
            "file_config": dataset.file_config_json,
            "relation_identity": relation_identity,
            "execution_capabilities": execution_capabilities,
            "status": dataset.status_value,
        }

    def build_dataset_source_bindings(self, dataset: DatasetMetadata) -> list[dict[str, Any]]:
        file_config = dict(dataset.file_config_json or {})
        sync_meta = dict(dataset.sync_json or {})
        storage_uri = str(dataset.storage_uri or "").strip()
        relation_identity, execution_capabilities = self.resolve_dataset_descriptor_snapshot(dataset)
        bindings: list[dict[str, Any]] = [
            {
                "source_type": "dataset_contract",
                "dataset_id": str(dataset.id),
                "materialization_mode": dataset.materialization_mode_value,
                "source": dataset.source_json,
                "sync": dataset.sync_json,
                "source_kind": dataset.source_kind_value,
                "connector_kind": dataset.connector_kind,
                "storage_kind": dataset.storage_kind_value,
                "relation_identity": relation_identity,
                "execution_capabilities": execution_capabilities,
            }
        ]
        if dataset.connection_id is not None:
            bindings.append(
                {
                    "source_type": "connection",
                    "connection_id": str(dataset.connection_id),
                }
            )
        if sync_meta:
            sync_source = dict(sync_meta.get("source") or {})
            source_binding_type = "sync_source"
            if sync_source.get("resource"):
                source_binding_type = "api_resource"
            elif sync_source.get("request"):
                source_binding_type = "api_request"
            elif sync_source.get("table"):
                source_binding_type = "source_table"
            elif sync_source.get("sql"):
                source_binding_type = "sql_query"
            bindings.append(
                {
                    "source_type": source_binding_type,
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "source": sync_source,
                    "source_key": self._source_resolver.sync_source_key(
                        DatasetSource.model_validate(sync_source)
                    ),
                    "strategy": sync_meta.get("strategy"),
                    "cadence": sync_meta.get("cadence"),
                    "cursor_field": sync_meta.get("cursor_field"),
                }
            )
        if storage_uri:
            bindings.append(
                {
                    "source_type": "file_resource",
                    "storage_uri": storage_uri,
                    "file_config": file_config,
                }
            )
        return bindings

    def resolve_dataset_descriptor_snapshot(
        self,
        dataset: DatasetMetadata,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        relation_identity = dict(dataset.relation_identity_json or {})
        execution_capabilities = dict(dataset.execution_capabilities_json or {})
        return relation_identity, execution_capabilities

    def apply_dataset_descriptor_metadata(self, *, dataset: DatasetMetadata) -> None:
        if dataset.source_kind is None:
            raise ValueError("Synced datasets must set source_kind explicitly before descriptor refresh.")
        if dataset.storage_kind is None:
            raise ValueError("Synced datasets must set storage_kind explicitly before descriptor refresh.")
        if not str(dataset.connector_kind or "").strip():
            raise ValueError("Synced datasets must set connector_kind explicitly before descriptor refresh.")
        source_kind = dataset.source_kind
        storage_kind = dataset.storage_kind
        connector_kind = str(dataset.connector_kind or "").strip().lower() or None
        relation_identity = build_dataset_relation_identity(
            dataset_id=dataset.id,
            connector_id=dataset.connection_id,
            dataset_name=dataset.name,
            catalog_name=dataset.catalog_name,
            schema_name=dataset.schema_name,
            table_name=dataset.table_name,
            storage_uri=dataset.storage_uri,
            source_kind=source_kind,
            storage_kind=storage_kind,
            existing_payload=dict(dataset.relation_identity_json or {}),
        )
        execution_capabilities = build_dataset_execution_capabilities(
            source_kind=source_kind,
            storage_kind=storage_kind,
            existing_payload=dict(dataset.execution_capabilities_json or {}),
        )

        dataset.source_kind = source_kind
        dataset.connector_kind = connector_kind
        dataset.storage_kind = storage_kind
        dataset.relation_identity = relation_identity.model_dump(mode="json")
        dataset.execution_capabilities = execution_capabilities.model_dump(mode="json")

    def dataset_description(self, connector_name: str, source_label: str) -> str:
        return f"Managed dataset synced from connector '{connector_name}' {source_label}."

    def dataset_sql_alias(self, name: str) -> str:
        alias = _RESOURCE_SANITIZER.sub("_", str(name or "").strip().lower()).strip("_")
        alias = re.sub(r"_+", "_", alias)
        if not alias:
            return "dataset"
        if alias[0].isdigit():
            return f"dataset_{alias}"
        return alias

    def dataset_tags(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        source: DatasetSource,
    ) -> list[str]:
        if source.resource:
            return [
                "api-connector",
                connector_type.value.lower(),
                f"resource:{str(source.resource).strip().lower()}",
                "managed",
            ]
        if source.request:
            return [
                "api-connector",
                connector_type.value.lower(),
                "request-sync",
                "managed",
            ]
        if source.table:
            return [
                "database-connector",
                connector_type.value.lower(),
                f"table:{str(source.table).strip().lower()}",
                "managed",
            ]
        if source.sql:
            return [
                "database-connector",
                connector_type.value.lower(),
                "sql-sync",
                "managed",
            ]
        return [connector_type.value.lower(), "managed"]

    def merge_tags(self, *, existing: list[str], required: list[str]) -> list[str]:
        seen: set[str] = set()
        merged: list[str] = []
        for tag in [*(existing or []), *(required or [])]:
            normalized = str(tag or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
        return merged

    def sync_meta(self, dataset: DatasetMetadata) -> Mapping[str, Any]:
        payload = dict(dataset.sync_json or {})
        source_payload = payload.get("source")
        if isinstance(source_payload, dict):
            merged_payload = dict(payload)
            for key, value in source_payload.items():
                merged_payload.setdefault(str(key), value)
            return merged_payload
        return payload
