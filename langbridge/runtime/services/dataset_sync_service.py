from __future__ import annotations

import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from langbridge.runtime.utils.lineage import (
    LineageEdgeType,
    LineageNodeType,
    build_api_resource_id,
    build_file_resource_id,
    stable_payload_hash,
)
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    resolve_dataset_source_kind,
    resolve_dataset_storage_kind,
)
from langbridge.connectors.base import ApiResource
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.runtime.models import (
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    LineageEdge,
)
from langbridge.runtime.ports import (
    ConnectorSyncStateStore,
    DatasetCatalogStore,
    DatasetColumnStore,
    DatasetPolicyStore,
    DatasetRevisionStore,
    LineageEdgeStore,
)
from langbridge.runtime.settings import runtime_settings as settings

_RESOURCE_SANITIZER = re.compile(r"[^0-9A-Za-z_]+")
_SYNC_MODE_INCREMENTAL = "INCREMENTAL"
_SYNC_MODE_FULL_REFRESH = "FULL_REFRESH"
_SYNC_STATUS_NEVER_SYNCED = "never_synced"
_SYNC_STATUS_SUCCEEDED = "succeeded"
_SYNC_STATUS_FAILED = "failed"


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


@dataclass(slots=True)
class MaterializedDatasetResult:
    dataset_id: uuid.UUID
    dataset_name: str
    resource_name: str
    row_count: int
    bytes_written: int | None
    schema_drift: dict[str, Any] | None = None


class ConnectorSyncRuntime:
    def __init__(
        self,
        *,
        connector_sync_state_repository: ConnectorSyncStateStore,
        dataset_repository: DatasetCatalogStore,
        dataset_column_repository: DatasetColumnStore,
        dataset_policy_repository: DatasetPolicyStore,
        dataset_revision_repository: DatasetRevisionStore | None = None,
        lineage_edge_repository: LineageEdgeStore | None = None,
    ) -> None:
        self._connector_sync_state_repository = connector_sync_state_repository
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._dataset_revision_repository = dataset_revision_repository
        self._lineage_edge_repository = lineage_edge_repository

    async def get_or_create_state(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_type: ConnectorRuntimeType,
        resource_name: str,
        sync_mode: Any,
    ) -> ConnectorSyncState:
        sync_mode_value = _enum_value(sync_mode)
        state = await self._connector_sync_state_repository.get_for_resource(
            workspace_id=workspace_id,
            connection_id=connection_id,
            resource_name=resource_name,
        )
        if state is not None:
            return state
        state = ConnectorSyncState(
            id=uuid.uuid4(),
            workspace_id=workspace_id,
            connection_id=connection_id,
            connector_type=connector_type.value,
            resource_name=resource_name,
            sync_mode=sync_mode_value,
            last_cursor=None,
            last_sync_at=None,
            state={},
            status=_SYNC_STATUS_NEVER_SYNCED,
            error_message=None,
            records_synced=0,
            bytes_synced=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._connector_sync_state_repository.add(state)
        return state

    async def sync_resource(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_record,
        connector_type: ConnectorRuntimeType,
        resource: ApiResource,
        api_connector,
        state: ConnectorSyncState,
        sync_mode: Any,
    ) -> dict[str, Any]:
        sync_mode_value = _enum_value(sync_mode)
        effective_sync_mode = sync_mode_value
        if sync_mode_value == _SYNC_MODE_INCREMENTAL and not resource.supports_incremental:
            effective_sync_mode = _SYNC_MODE_FULL_REFRESH

        since = None
        if effective_sync_mode == _SYNC_MODE_INCREMENTAL and resource.supports_incremental:
            since = state.last_cursor

        page_cursor: str | None = None
        page_count = 0
        parent_rows: list[dict[str, Any]] = []
        child_rows: dict[str, list[dict[str, Any]]] = {}
        checkpoint_cursor = state.last_cursor

        while True:
            extract_result = await api_connector.extract_resource(
                resource.name,
                since=since,
                cursor=page_cursor,
                limit=None,
            )
            parent_rows.extend(list(extract_result.records or []))
            for child_name, rows in (extract_result.child_records or {}).items():
                child_rows.setdefault(child_name, []).extend(list(rows or []))
            checkpoint_cursor = self._pick_newer_cursor(checkpoint_cursor, extract_result.checkpoint_cursor)
            page_count += 1
            page_cursor = extract_result.next_cursor
            if not page_cursor:
                break

        now = datetime.now(timezone.utc)
        materialized: list[MaterializedDatasetResult] = []
        materialized.append(
            await self._materialize_dataset(
                workspace_id=workspace_id,
                actor_id=actor_id,
                connection_id=connection_id,
                connector_record=connector_record,
                connector_type=connector_type,
                root_resource_name=resource.name,
                resource_name=resource.name,
                parent_resource_name=None,
                rows=parent_rows,
                primary_key=resource.primary_key,
                sync_mode=effective_sync_mode,
                checkpoint_cursor=checkpoint_cursor,
                last_sync_at=now,
            )
        )
        for child_name, rows in child_rows.items():
            materialized.append(
                await self._materialize_dataset(
                    workspace_id=workspace_id,
                    actor_id=actor_id,
                    connection_id=connection_id,
                    connector_record=connector_record,
                    connector_type=connector_type,
                    root_resource_name=resource.name,
                    resource_name=child_name,
                    parent_resource_name=resource.name,
                    rows=rows,
                    primary_key=self._child_primary_key(rows),
                    sync_mode=effective_sync_mode,
                    checkpoint_cursor=checkpoint_cursor,
                    last_sync_at=now,
                )
            )

        bytes_synced = sum(item.bytes_written or 0 for item in materialized) or None
        state.sync_mode = effective_sync_mode
        state.last_cursor = (
            checkpoint_cursor
            if effective_sync_mode == _SYNC_MODE_INCREMENTAL and resource.supports_incremental
            else state.last_cursor
        )
        state.last_sync_at = now
        state.status = _SYNC_STATUS_SUCCEEDED
        state.error_message = None
        state.records_synced = len(parent_rows) + sum(len(rows) for rows in child_rows.values())
        state.bytes_synced = bytes_synced
        state.state = {
            "page_count": page_count,
            "dataset_ids": [str(item.dataset_id) for item in materialized],
            "dataset_names": [item.dataset_name for item in materialized],
            "resource_dataset_map": {
                item.resource_name: str(item.dataset_id)
                for item in materialized
            },
            "schema_drift": {
                item.resource_name: item.schema_drift
                for item in materialized
                if item.schema_drift
            },
            "root_resource_name": resource.name,
            "last_sync_at": now.isoformat(),
        }
        state.updated_at = now
        await self._connector_sync_state_repository.save(state)

        return {
            "resource_name": resource.name,
            "sync_mode": effective_sync_mode,
            "records_synced": int(state.records_synced or 0),
            "bytes_synced": bytes_synced,
            "last_cursor": state.last_cursor,
            "dataset_ids": [str(item.dataset_id) for item in materialized],
            "dataset_names": [item.dataset_name for item in materialized],
        }

    async def mark_failed(self, *, state: ConnectorSyncState, error_message: str) -> None:
        state.status = _SYNC_STATUS_FAILED
        state.error_message = error_message
        state.updated_at = datetime.now(timezone.utc)
        await self._connector_sync_state_repository.save(state)

    async def _materialize_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_record,
        connector_type: ConnectorRuntimeType,
        root_resource_name: str,
        resource_name: str,
        parent_resource_name: str | None,
        rows: list[dict[str, Any]],
        primary_key: str | None,
        sync_mode: str,
        checkpoint_cursor: str | None,
        last_sync_at: datetime,
    ) -> MaterializedDatasetResult:
        dataset_name = self._dataset_name(
            connector_type=connector_type,
            connection_id=connection_id,
            resource_name=resource_name,
        )
        dataset = await self._dataset_repository.find_file_dataset_for_connection(
            workspace_id=workspace_id,
            connection_id=connection_id,
            table_name=dataset_name,
        )
        parquet_path = self._dataset_parquet_path(
            workspace_id=workspace_id,
            connection_id=connection_id,
            dataset_name=dataset_name,
        )
        existing_rows, existing_schema = self._read_existing_rows(parquet_path)
        merged_rows = self._merge_rows(
            existing_rows=existing_rows,
            new_rows=rows,
            primary_key=primary_key,
            full_refresh=sync_mode == _SYNC_MODE_FULL_REFRESH,
        )
        table = self._rows_to_table(rows=merged_rows, existing_schema=existing_schema)
        schema_drift = self._describe_schema_drift(existing_schema=existing_schema, next_schema=table.schema)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, parquet_path)
        storage_uri = parquet_path.resolve().as_uri()
        bytes_written = parquet_path.stat().st_size if parquet_path.exists() else None
        now = datetime.now(timezone.utc)

        file_config = {
            "format": "parquet",
            "storage_uri": storage_uri,
            "managed_dataset": True,
            "connector_sync": {
                "connector_id": str(connection_id),
                "connector_type": connector_type.value,
                "resource_name": resource_name,
                "root_resource_name": root_resource_name,
                "parent_resource_name": parent_resource_name,
                "sync_mode": sync_mode,
                "last_sync_at": last_sync_at.isoformat(),
                "last_cursor": checkpoint_cursor,
                "primary_key": primary_key,
                "schema_drift": schema_drift,
            },
        }

        if dataset is None:
            dataset = DatasetMetadata(
                id=uuid.uuid4(),
                workspace_id=workspace_id,
                connection_id=connection_id,
                created_by=actor_id,
                updated_by=actor_id,
                name=dataset_name,
                sql_alias=self._dataset_sql_alias(dataset_name),
                description=self._dataset_description(connector_record.name, resource_name, parent_resource_name),
                tags=self._dataset_tags(connector_type=connector_type, resource_name=resource_name),
                dataset_type="FILE",
                dialect="duckdb",
                catalog_name=None,
                schema_name=None,
                table_name=dataset_name,
                storage_uri=storage_uri,
                sql_text=None,
                referenced_dataset_ids=[],
                federated_plan=None,
                file_config=file_config,
                status="published",
                revision_id=None,
                row_count_estimate=len(merged_rows),
                bytes_estimate=bytes_written,
                last_profiled_at=None,
                created_at=now,
                updated_at=now,
            )
            self._apply_dataset_descriptor_metadata(
                dataset=dataset,
                connection_connector_type=connector_type.value,
            )
            self._dataset_repository.add(dataset)
            change_summary = f"Initial sync materialized {resource_name}."
        else:
            dataset.connection_id = connection_id
            dataset.updated_by = actor_id
            dataset.name = dataset_name
            dataset.description = self._dataset_description(
                connector_record.name,
                resource_name,
                parent_resource_name,
            )
            dataset.tags = self._merge_tags(
                existing=list(dataset.tags_json or []),
                required=self._dataset_tags(connector_type=connector_type, resource_name=resource_name),
            )
            dataset.dataset_type = "FILE"
            dataset.dialect = "duckdb"
            dataset.schema_name = None
            dataset.table_name = dataset_name
            dataset.storage_uri = storage_uri
            dataset.file_config = file_config
            dataset.status = "published"
            dataset.row_count_estimate = len(merged_rows)
            dataset.bytes_estimate = bytes_written
            dataset.updated_at = now
            self._apply_dataset_descriptor_metadata(
                dataset=dataset,
                connection_connector_type=connector_type.value,
            )
            change_summary = f"{sync_mode.replace('_', ' ').title()} sync updated {resource_name}."

        await self._replace_columns(dataset=dataset, table=table)
        policy = await self._get_or_create_policy(dataset=dataset)
        await self._create_dataset_revision(
            dataset=dataset,
            policy=policy,
            created_by=actor_id,
            change_summary=change_summary,
        )
        await self._replace_dataset_lineage(dataset=dataset)
        await self._dataset_repository.save(dataset)

        return MaterializedDatasetResult(
            dataset_id=dataset.id,
            dataset_name=dataset.name,
            resource_name=resource_name,
            row_count=len(merged_rows),
            bytes_written=bytes_written,
            schema_drift=schema_drift,
        )

    async def _replace_columns(self, *, dataset: DatasetMetadata, table: pa.Table) -> None:
        await self._dataset_column_repository.delete_for_dataset(dataset_id=dataset.id)
        now = datetime.now(timezone.utc)
        for ordinal, field in enumerate(table.schema):
            self._dataset_column_repository.add(
                DatasetColumnMetadata(
                    id=uuid.uuid4(),
                    dataset_id=dataset.id,
                    workspace_id=dataset.workspace_id,
                    name=str(field.name),
                    data_type=str(field.type),
                    nullable=field.nullable,
                    ordinal_position=ordinal,
                    description=None,
                    is_allowed=True,
                    is_computed=False,
                    expression=None,
                    created_at=now,
                    updated_at=now,
                )
            )

    async def _get_or_create_policy(self, *, dataset: DatasetMetadata) -> DatasetPolicyMetadata:
        existing = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if existing is not None:
            existing.allow_dml = False
            existing.updated_at = datetime.now(timezone.utc)
            await self._dataset_policy_repository.save(existing)
            return existing
        policy = DatasetPolicyMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            max_rows_preview=1000,
            max_export_rows=100000,
            redaction_rules={},
            row_filters=[],
            allow_dml=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._dataset_policy_repository.add(policy)
        return policy

    async def _create_dataset_revision(
        self,
        *,
        dataset: DatasetMetadata,
        policy: DatasetPolicyMetadata,
        created_by: uuid.UUID,
        change_summary: str,
    ) -> None:
        if self._dataset_revision_repository is None:
            return
        columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        next_revision = await self._dataset_revision_repository.next_revision_number(dataset_id=dataset.id)
        definition = self._build_dataset_definition_snapshot(dataset)
        schema_snapshot = [self._column_snapshot(column) for column in columns]
        policy_snapshot = self._policy_snapshot(policy)
        source_bindings = self._build_dataset_source_bindings(dataset)
        relation_identity, execution_capabilities = self._resolve_dataset_descriptor_snapshot(dataset)
        execution_characteristics = {
            "row_count_estimate": dataset.row_count_estimate,
            "bytes_estimate": dataset.bytes_estimate,
            "last_profiled_at": dataset.last_profiled_at.isoformat() if dataset.last_profiled_at else None,
            "relation_identity": relation_identity,
            "execution_capabilities": execution_capabilities,
        }
        snapshot = {
            "dataset": definition,
            "columns": schema_snapshot,
            "policy": policy_snapshot,
            "source_bindings": source_bindings,
            "execution_characteristics": execution_characteristics,
        }
        revision_id = uuid.uuid4()
        self._dataset_revision_repository.add(
            DatasetRevision(
                id=revision_id,
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                revision_number=next_revision,
                revision_hash=stable_payload_hash(snapshot),
                change_summary=change_summary,
                definition=definition,
                schema_snapshot=schema_snapshot,
                policy=policy_snapshot,
                source_bindings=source_bindings,
                execution_characteristics=execution_characteristics,
                status=dataset.status,
                snapshot=snapshot,
                note=change_summary,
                created_by=created_by,
                created_at=datetime.now(timezone.utc),
            )
        )
        dataset.revision_id = revision_id

    async def _replace_dataset_lineage(self, *, dataset: DatasetMetadata) -> None:
        if self._lineage_edge_repository is None:
            return
        await self._lineage_edge_repository.delete_for_target(
            workspace_id=dataset.workspace_id,
            target_type=LineageNodeType.DATASET.value,
            target_id=str(dataset.id),
        )

        file_config = dict(dataset.file_config_json or {})
        sync_meta = file_config.get("connector_sync") if isinstance(file_config.get("connector_sync"), dict) else {}
        if sync_meta is None:
            sync_meta = {}
        storage_uri = str(file_config.get("storage_uri") or dataset.storage_uri or "").strip()

        edges: list[LineageEdge] = []
        if dataset.connection_id is not None:
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.CONNECTION.value,
                    source_id=str(dataset.connection_id),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.FEEDS.value,
                    metadata={"connection_id": str(dataset.connection_id)},
                )
            )
        resource_name = str(sync_meta.get("resource_name") or "").strip()
        if dataset.connection_id is not None and resource_name:
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.API_RESOURCE.value,
                    source_id=build_api_resource_id(
                        connection_id=dataset.connection_id,
                        resource_name=resource_name,
                    ),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata={
                        "connection_id": str(dataset.connection_id),
                        "connector_type": sync_meta.get("connector_type"),
                        "resource_name": resource_name,
                        "root_resource_name": sync_meta.get("root_resource_name"),
                        "parent_resource_name": sync_meta.get("parent_resource_name"),
                    },
                )
            )
        if storage_uri:
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.FILE_RESOURCE.value,
                    source_id=build_file_resource_id(storage_uri),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata={
                        "storage_uri": storage_uri,
                        "file_config": file_config,
                    },
                )
            )
        for edge in edges:
            self._lineage_edge_repository.add(edge)

    @staticmethod
    def _column_snapshot(column: DatasetColumnMetadata) -> dict[str, Any]:
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

    @staticmethod
    def _policy_snapshot(policy: DatasetPolicyMetadata) -> dict[str, Any]:
        return {
            "max_rows_preview": policy.max_rows_preview,
            "max_export_rows": policy.max_export_rows,
            "redaction_rules": dict(policy.redaction_rules_json or {}),
            "row_filters": list(policy.row_filters_json or []),
            "allow_dml": policy.allow_dml,
        }

    @staticmethod
    def _build_dataset_definition_snapshot(dataset: DatasetMetadata) -> dict[str, Any]:
        relation_identity, execution_capabilities = ConnectorSyncRuntime._resolve_dataset_descriptor_snapshot(
            dataset
        )
        return {
            "id": str(dataset.id),
            "workspace_id": str(dataset.workspace_id),
            "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
            "name": dataset.name,
            "description": dataset.description,
            "tags": list(dataset.tags_json or []),
            "dataset_type": dataset.dataset_type,
            "source_kind": dataset.source_kind,
            "connector_kind": dataset.connector_kind,
            "storage_kind": dataset.storage_kind,
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
            "status": dataset.status,
        }

    @staticmethod
    def _build_dataset_source_bindings(dataset: DatasetMetadata) -> list[dict[str, Any]]:
        file_config = dict(dataset.file_config_json or {})
        sync_meta = file_config.get("connector_sync") if isinstance(file_config.get("connector_sync"), dict) else {}
        storage_uri = str(file_config.get("storage_uri") or dataset.storage_uri or "").strip()
        relation_identity, execution_capabilities = ConnectorSyncRuntime._resolve_dataset_descriptor_snapshot(
            dataset
        )
        bindings: list[dict[str, Any]] = [
            {
                "source_type": "dataset_contract",
                "dataset_id": str(dataset.id),
                "source_kind": dataset.source_kind,
                "connector_kind": dataset.connector_kind,
                "storage_kind": dataset.storage_kind,
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
            bindings.append(
                {
                    "source_type": "api_resource",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "connector_type": sync_meta.get("connector_type"),
                    "resource_name": sync_meta.get("resource_name"),
                    "root_resource_name": sync_meta.get("root_resource_name"),
                    "parent_resource_name": sync_meta.get("parent_resource_name"),
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

    @staticmethod
    def _resolve_dataset_descriptor_snapshot(dataset: DatasetMetadata) -> tuple[dict[str, Any], dict[str, Any]]:
        relation_identity = dict(dataset.relation_identity_json or {})
        execution_capabilities = dict(dataset.execution_capabilities_json or {})
        return relation_identity, execution_capabilities

    @staticmethod
    def _apply_dataset_descriptor_metadata(
        *,
        dataset: DatasetMetadata,
        connection_connector_type: str | None,
    ) -> None:
        connector_kind = str(connection_connector_type or "").strip().lower() or None
        source_kind = resolve_dataset_source_kind(
            explicit_source_kind=dataset.source_kind,
            legacy_dataset_type=dataset.dataset_type,
            connector_kind=connector_kind,
            file_config=dict(dataset.file_config_json or {}),
        )
        storage_kind = resolve_dataset_storage_kind(
            explicit_storage_kind="parquet",
            legacy_dataset_type=dataset.dataset_type,
            file_config=dict(dataset.file_config_json or {}),
            storage_uri=dataset.storage_uri,
        )
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

        dataset.source_kind = source_kind.value
        dataset.connector_kind = connector_kind
        dataset.storage_kind = storage_kind.value
        dataset.relation_identity = relation_identity.model_dump(mode="json")
        dataset.execution_capabilities = execution_capabilities.model_dump(mode="json")

    @staticmethod
    def _dataset_name(
        *,
        connector_type: ConnectorRuntimeType,
        connection_id: uuid.UUID,
        resource_name: str,
    ) -> str:
        sanitized_resource = _RESOURCE_SANITIZER.sub("_", resource_name.strip().lower()).strip("_")
        sanitized_resource = re.sub(r"_+", "_", sanitized_resource)
        return f"{connector_type.value.lower()}_{connection_id.hex[:8]}_{sanitized_resource or 'resource'}"

    @staticmethod
    def _dataset_description(connector_name: str, resource_name: str, parent_resource_name: str | None) -> str:
        if parent_resource_name:
            return (
                f"Managed dataset synced from connector '{connector_name}' resource "
                f"'{parent_resource_name}' child collection '{resource_name}'."
            )
        return f"Managed dataset synced from connector '{connector_name}' resource '{resource_name}'."

    @staticmethod
    def _dataset_sql_alias(name: str) -> str:
        alias = _RESOURCE_SANITIZER.sub("_", str(name or "").strip().lower()).strip("_")
        alias = re.sub(r"_+", "_", alias)
        if not alias:
            return "dataset"
        if alias[0].isdigit():
            return f"dataset_{alias}"
        return alias

    @staticmethod
    def _dataset_tags(
        *,
        connector_type: ConnectorRuntimeType,
        resource_name: str,
    ) -> list[str]:
        return [
            "api-connector",
            connector_type.value.lower(),
            f"resource:{resource_name.strip().lower()}",
            "managed",
        ]

    @staticmethod
    def _merge_tags(*, existing: list[str], required: list[str]) -> list[str]:
        seen: set[str] = set()
        merged: list[str] = []
        for tag in [*(existing or []), *(required or [])]:
            normalized = str(tag or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
        return merged

    @staticmethod
    def _dataset_parquet_path(
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        dataset_name: str,
    ) -> Path:
        return (
            Path(settings.DATASET_FILE_LOCAL_DIR)
            / "api-connectors"
            / str(workspace_id)
            / str(connection_id)
            / f"{dataset_name}.parquet"
        )

    @staticmethod
    def _read_existing_rows(path: Path) -> tuple[list[dict[str, Any]], pa.Schema | None]:
        if not path.exists():
            return [], None
        table = pq.read_table(path)
        return table.to_pylist(), table.schema

    @staticmethod
    def _merge_rows(
        *,
        existing_rows: list[dict[str, Any]],
        new_rows: list[dict[str, Any]],
        primary_key: str | None,
        full_refresh: bool,
    ) -> list[dict[str, Any]]:
        if full_refresh:
            return list(new_rows)
        if not primary_key:
            return [*existing_rows, *new_rows]

        merged: dict[str, dict[str, Any]] = {}
        extras: list[dict[str, Any]] = []
        for row in existing_rows:
            key = ConnectorSyncRuntime._row_identity(row, primary_key)
            if key is None:
                extras.append(dict(row))
            else:
                merged[key] = dict(row)
        for row in new_rows:
            key = ConnectorSyncRuntime._row_identity(row, primary_key)
            if key is None:
                extras.append(dict(row))
            else:
                merged[key] = dict(row)
        return [*merged.values(), *extras]

    @staticmethod
    def _rows_to_table(*, rows: list[dict[str, Any]], existing_schema: pa.Schema | None) -> pa.Table:
        ConnectorSyncRuntime._ensure_pyarrow_compatible_pandas_stub()
        normalized_rows = ConnectorSyncRuntime._normalize_rows_for_arrow(rows)
        if normalized_rows:
            try:
                return pa.Table.from_pylist(normalized_rows)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                stringified_rows = [
                    {key: (None if value is None else str(value)) for key, value in row.items()}
                    for row in normalized_rows
                ]
                return pa.Table.from_pylist(stringified_rows)
        if existing_schema is not None:
            return pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in existing_schema],
                schema=existing_schema,
            )
        return pa.table({})

    @staticmethod
    def _ensure_pyarrow_compatible_pandas_stub() -> None:
        pandas_module = sys.modules.get("pandas")
        if pandas_module is not None and not hasattr(pandas_module, "__version__"):
            setattr(pandas_module, "__version__", "0.0.0")

    @staticmethod
    def _normalize_rows_for_arrow(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        columns: set[str] = set()
        for row in rows:
            columns.update(str(key) for key in row.keys())
        ordered_columns = sorted(columns)

        category_map: dict[str, set[str]] = {column: set() for column in ordered_columns}
        for row in rows:
            for column in ordered_columns:
                value = row.get(column)
                category = ConnectorSyncRuntime._value_category(value)
                if category is not None:
                    category_map[column].add(category)

        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized_row: dict[str, Any] = {}
            for column in ordered_columns:
                value = row.get(column)
                categories = category_map[column]
                if value is None:
                    normalized_row[column] = None
                elif categories <= {"int"}:
                    normalized_row[column] = int(value)
                elif categories <= {"int", "float"}:
                    normalized_row[column] = float(value)
                elif categories <= {"bool"}:
                    normalized_row[column] = bool(value)
                else:
                    normalized_row[column] = str(value)
            normalized.append(normalized_row)
        return normalized

    @staticmethod
    def _value_category(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        return "string"

    @staticmethod
    def _row_identity(row: dict[str, Any], primary_key: str) -> str | None:
        if primary_key == "_child_identity":
            parent_id = row.get("_parent_id")
            child_index = row.get("_child_index")
            if parent_id is None or child_index is None:
                return None
            return f"{parent_id}:{child_index}"
        value = row.get(primary_key)
        if value is not None and str(value).strip():
            return str(value)
        return None

    @staticmethod
    def _child_primary_key(rows: list[dict[str, Any]]) -> str | None:
        if any("id" in row for row in rows):
            return "id"
        if any("_parent_id" in row for row in rows) and any("_child_index" in row for row in rows):
            return "_child_identity"
        return None

    @staticmethod
    def _pick_newer_cursor(current: str | None, candidate: str | None) -> str | None:
        if not candidate:
            return current
        if not current:
            return candidate
        if current.isdigit() and candidate.isdigit():
            return str(max(int(current), int(candidate)))
        return max(current, candidate)

    @staticmethod
    def _describe_schema_drift(
        *,
        existing_schema: pa.Schema | None,
        next_schema: pa.Schema,
    ) -> dict[str, Any] | None:
        if existing_schema is None:
            return None

        previous_fields = {field.name: str(field.type) for field in existing_schema}
        next_fields = {field.name: str(field.type) for field in next_schema}

        added_columns = sorted(name for name in next_fields if name not in previous_fields)
        removed_columns = sorted(name for name in previous_fields if name not in next_fields)
        type_changes = [
            {
                "column": name,
                "before": previous_fields[name],
                "after": next_fields[name],
            }
            for name in sorted(previous_fields.keys() & next_fields.keys())
            if previous_fields[name] != next_fields[name]
        ]
        if not added_columns and not removed_columns and not type_changes:
            return None
        return {
            "added_columns": added_columns,
            "removed_columns": removed_columns,
            "type_changes": type_changes,
        }


DatasetSyncService = ConnectorSyncRuntime
