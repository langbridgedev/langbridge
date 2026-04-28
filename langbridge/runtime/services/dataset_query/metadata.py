from typing import Any

from langbridge.runtime.models import DatasetMetadata, DatasetPolicyMetadata
from langbridge.runtime.models.metadata import DatasetType


class DatasetQueryMetadataBuilder:
    """Builds revision snapshots and source bindings for dataset query mutations."""

    def definition_snapshot(self, dataset: DatasetMetadata) -> dict[str, Any]:
        return {
            "id": str(dataset.id),
            "workspace_id": str(dataset.workspace_id),
            "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
            "name": dataset.name,
            "description": dataset.description,
            "tags": list(dataset.tags_json or []),
            "dataset_type": dataset.dataset_type_value,
            "materialization_mode": dataset.materialization_mode_value,
            "source_kind": dataset.source_kind_value,
            "connector_kind": dataset.connector_kind,
            "storage_kind": dataset.storage_kind_value,
            "dialect": dataset.dialect,
            "storage_uri": dataset.storage_uri,
            "catalog_name": dataset.catalog_name,
            "schema_name": dataset.schema_name,
            "table_name": dataset.table_name,
            "sql_text": dataset.sql_text,
            "relation_identity": dataset.relation_identity_json,
            "execution_capabilities": dataset.execution_capabilities_json,
            "referenced_dataset_ids": list(dataset.referenced_dataset_ids_json or []),
            "federated_plan": dataset.federated_plan_json,
            "file_config": dataset.file_config_json,
            "source": dataset.source_json,
            "sync": dataset.sync_json,
            "status": dataset.status_value,
        }

    def source_bindings(self, dataset: DatasetMetadata) -> list[dict[str, Any]]:
        dataset_type = dataset.dataset_type
        if dataset_type == DatasetType.TABLE:
            return [
                {
                    "source_type": "connection",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "materialization_mode": dataset.materialization_mode_value,
                    "source": dataset.source_json,
                    "sync": dataset.sync_json,
                },
                {
                    "source_type": "source_table",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "materialization_mode": dataset.materialization_mode_value,
                    "catalog_name": dataset.catalog_name,
                    "schema_name": dataset.schema_name,
                    "table_name": dataset.table_name,
                    "source": dataset.source_json,
                    "sync": dataset.sync_json,
                },
            ]
        if dataset_type == DatasetType.API:
            source = dict(dataset.source_json or {})
            return [
                {
                    "source_type": "connection",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "materialization_mode": dataset.materialization_mode_value,
                    "source": dataset.source_json,
                    "sync": dataset.sync_json,
                },
                {
                    "source_type": "api_resource",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "materialization_mode": dataset.materialization_mode_value,
                    "resource_name": str(source.get("resource") or "").strip() or None,
                    "source": dataset.source_json,
                    "sync": dataset.sync_json,
                },
            ]
        if dataset_type == DatasetType.FILE:
            storage_uri = self.file_storage_uri(dataset)
            return [
                {
                    "source_type": "file_resource",
                    "materialization_mode": dataset.materialization_mode_value,
                    "storage_uri": storage_uri,
                    "file_config": dict(dataset.file_config_json or {}),
                    "source": dataset.source_json,
                    "sync": dataset.sync_json,
                }
            ]
        if dataset_type == DatasetType.FEDERATED:
            return self._federated_source_bindings(dataset)
        return []

    def policy_snapshot(self, policy: DatasetPolicyMetadata) -> dict[str, Any]:
        return {
            "max_rows_preview": policy.max_rows_preview,
            "max_export_rows": policy.max_export_rows,
            "redaction_rules": dict(policy.redaction_rules_json or {}),
            "row_filters": list(policy.row_filters_json or []),
            "allow_dml": policy.allow_dml,
        }

    def file_storage_uri(self, dataset: DatasetMetadata) -> str | None:
        return (
            str((dataset.source_json or {}).get("storage_uri") or "").strip()
            or str((dataset.file_config_json or {}).get("source_storage_uri") or "").strip()
            or str((dataset.file_config_json or {}).get("storage_uri") or "").strip()
            or dataset.storage_uri
        )

    def _federated_source_bindings(self, dataset: DatasetMetadata) -> list[dict[str, Any]]:
        bindings: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw_value in dataset.referenced_dataset_ids_json or []:
            value = str(raw_value)
            if not value or value in seen:
                continue
            seen.add(value)
            bindings.append(
                {
                    "source_type": "dataset",
                    "dataset_id": value,
                    "materialization_mode": dataset.materialization_mode_value,
                }
            )
        plan = dataset.federated_plan_json if isinstance(dataset.federated_plan_json, dict) else {}
        tables_payload = plan.get("tables")
        iterable = tables_payload.values() if isinstance(tables_payload, dict) else tables_payload or []
        for item in iterable:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("dataset_id") or item.get("datasetId")
            if raw_id is None:
                continue
            value = str(raw_id)
            if not value or value in seen:
                continue
            seen.add(value)
            bindings.append(
                {
                    "source_type": "dataset",
                    "dataset_id": value,
                    "materialization_mode": dataset.materialization_mode_value,
                }
            )
        return bindings
