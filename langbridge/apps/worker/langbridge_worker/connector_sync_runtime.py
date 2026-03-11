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

from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectorSyncMode,
    ConnectorSyncStatus,
)
from langbridge.packages.common.langbridge_common.db.connector_sync import (
    ConnectorSyncStateRecord,
)
from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.packages.common.langbridge_common.db.lineage import LineageEdgeRecord
from langbridge.packages.common.langbridge_common.repositories.connector_sync_repository import (
    ConnectorSyncStateRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetColumnRepository,
    DatasetPolicyRepository,
    DatasetRepository,
    DatasetRevisionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.lineage_repository import (
    LineageEdgeRepository,
)
from langbridge.packages.common.langbridge_common.utils.lineage import (
    LineageEdgeType,
    LineageNodeType,
    build_api_resource_id,
    build_file_resource_id,
    stable_payload_hash,
)
from langbridge.packages.common.langbridge_common.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    resolve_dataset_source_kind,
    resolve_dataset_storage_kind,
)
from langbridge.packages.connectors.langbridge_connectors.api import ApiResource
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType

_RESOURCE_SANITIZER = re.compile(r"[^0-9A-Za-z_]+")


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
        connector_sync_state_repository: ConnectorSyncStateRepository,
        dataset_repository: DatasetRepository,
        dataset_column_repository: DatasetColumnRepository,
        dataset_policy_repository: DatasetPolicyRepository,
        dataset_revision_repository: DatasetRevisionRepository | None = None,
        lineage_edge_repository: LineageEdgeRepository | None = None,
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
        sync_mode: ConnectorSyncMode,
    ) -> ConnectorSyncStateRecord:
        state = await self._connector_sync_state_repository.get_for_resource(
            workspace_id=workspace_id,
            connection_id=connection_id,
            resource_name=resource_name,
        )
        if state is not None:
            return state
        state = ConnectorSyncStateRecord(
            id=uuid.uuid4(),
            workspace_id=workspace_id,
            connection_id=connection_id,
            connector_type=connector_type.value,
            resource_name=resource_name,
            sync_mode=sync_mode.value,
            last_cursor=None,
            last_sync_at=None,
            state_json={},
            status=ConnectorSyncStatus.NEVER_SYNCED.value,
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
        project_id: uuid.UUID | None,
        user_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_record,
        connector_type: ConnectorRuntimeType,
        resource: ApiResource,
        api_connector,
        state: ConnectorSyncStateRecord,
        sync_mode: ConnectorSyncMode,
    ) -> dict[str, Any]:
        effective_sync_mode = sync_mode
        if sync_mode == ConnectorSyncMode.INCREMENTAL and not resource.supports_incremental:
            effective_sync_mode = ConnectorSyncMode.FULL_REFRESH

        since = None
        if effective_sync_mode == ConnectorSyncMode.INCREMENTAL and resource.supports_incremental:
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
                project_id=project_id,
                user_id=user_id,
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
                    project_id=project_id,
                    user_id=user_id,
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
        state.sync_mode = effective_sync_mode.value
        state.last_cursor = (
            checkpoint_cursor
            if effective_sync_mode == ConnectorSyncMode.INCREMENTAL and resource.supports_incremental
            else state.last_cursor
        )
        state.last_sync_at = now
        state.status = ConnectorSyncStatus.SUCCEEDED.value
        state.error_message = None
        state.records_synced = len(parent_rows) + sum(len(rows) for rows in child_rows.values())
        state.bytes_synced = bytes_synced
        state.state_json = {
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

        return {
            "resource_name": resource.name,
            "sync_mode": effective_sync_mode.value,
            "records_synced": int(state.records_synced or 0),
            "bytes_synced": bytes_synced,
            "last_cursor": state.last_cursor,
            "dataset_ids": [str(item.dataset_id) for item in materialized],
            "dataset_names": [item.dataset_name for item in materialized],
        }

    async def mark_failed(self, *, state: ConnectorSyncStateRecord, error_message: str) -> None:
        state.status = ConnectorSyncStatus.FAILED.value
        state.error_message = error_message
        state.updated_at = datetime.now(timezone.utc)

    async def _materialize_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_record,
        connector_type: ConnectorRuntimeType,
        root_resource_name: str,
        resource_name: str,
        parent_resource_name: str | None,
        rows: list[dict[str, Any]],
        primary_key: str | None,
        sync_mode: ConnectorSyncMode,
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
            full_refresh=sync_mode == ConnectorSyncMode.FULL_REFRESH,
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
                "sync_mode": sync_mode.value,
                "last_sync_at": last_sync_at.isoformat(),
                "last_cursor": checkpoint_cursor,
                "primary_key": primary_key,
                "schema_drift": schema_drift,
            },
        }

        if dataset is None:
            dataset = DatasetRecord(
                id=uuid.uuid4(),
                workspace_id=workspace_id,
                project_id=project_id,
                connection_id=connection_id,
                created_by=user_id,
                updated_by=user_id,
                name=dataset_name,
                sql_alias=self._dataset_sql_alias(dataset_name),
                description=self._dataset_description(connector_record.name, resource_name, parent_resource_name),
                tags_json=self._dataset_tags(connector_type=connector_type, resource_name=resource_name),
                dataset_type="FILE",
                dialect="duckdb",
                catalog_name=None,
                schema_name=None,
                table_name=dataset_name,
                storage_uri=storage_uri,
                sql_text=None,
                referenced_dataset_ids_json=[],
                federated_plan_json=None,
                file_config_json=file_config,
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
            dataset.project_id = project_id
            dataset.connection_id = connection_id
            dataset.updated_by = user_id
            dataset.name = dataset_name
            dataset.description = self._dataset_description(
                connector_record.name,
                resource_name,
                parent_resource_name,
            )
            dataset.tags_json = self._merge_tags(
                existing=list(dataset.tags_json or []),
                required=self._dataset_tags(connector_type=connector_type, resource_name=resource_name),
            )
            dataset.dataset_type = "FILE"
            dataset.dialect = "duckdb"
            dataset.schema_name = None
            dataset.table_name = dataset_name
            dataset.storage_uri = storage_uri
            dataset.file_config_json = file_config
            dataset.status = "published"
            dataset.row_count_estimate = len(merged_rows)
            dataset.bytes_estimate = bytes_written
            dataset.updated_at = now
            self._apply_dataset_descriptor_metadata(
                dataset=dataset,
                connection_connector_type=connector_type.value,
            )
            change_summary = f"{sync_mode.value.replace('_', ' ').title()} sync updated {resource_name}."

        await self._replace_columns(dataset=dataset, table=table)
        policy = await self._get_or_create_policy(dataset=dataset)
        await self._create_dataset_revision(
            dataset=dataset,
            policy=policy,
            created_by=user_id,
            change_summary=change_summary,
        )
        await self._replace_dataset_lineage(dataset=dataset)

        return MaterializedDatasetResult(
            dataset_id=dataset.id,
            dataset_name=dataset.name,
            resource_name=resource_name,
            row_count=len(merged_rows),
            bytes_written=bytes_written,
            schema_drift=schema_drift,
        )

    async def _replace_columns(self, *, dataset: DatasetRecord, table: pa.Table) -> None:
        await self._dataset_column_repository.delete_for_dataset(dataset_id=dataset.id)
        now = datetime.now(timezone.utc)
        for ordinal, field in enumerate(table.schema):
            self._dataset_column_repository.add(
                DatasetColumnRecord(
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

    async def _get_or_create_policy(self, *, dataset: DatasetRecord) -> DatasetPolicyRecord:
        existing = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if existing is not None:
            existing.allow_dml = False
            existing.updated_at = datetime.now(timezone.utc)
            return existing
        policy = DatasetPolicyRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            max_rows_preview=1000,
            max_export_rows=100000,
            redaction_rules_json={},
            row_filters_json=[],
            allow_dml=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._dataset_policy_repository.add(policy)
        return policy

    async def _create_dataset_revision(
        self,
        *,
        dataset: DatasetRecord,
        policy: DatasetPolicyRecord,
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
            DatasetRevisionRecord(
                id=revision_id,
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                revision_number=next_revision,
                revision_hash=stable_payload_hash(snapshot),
                change_summary=change_summary,
                definition_json=definition,
                schema_json=schema_snapshot,
                policy_json=policy_snapshot,
                source_bindings_json=source_bindings,
                execution_characteristics_json=execution_characteristics,
                status=dataset.status,
                snapshot_json=snapshot,
                note=change_summary,
                created_by=created_by,
                created_at=datetime.now(timezone.utc),
            )
        )
        dataset.revision_id = revision_id

    async def _replace_dataset_lineage(self, *, dataset: DatasetRecord) -> None:
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

        edges: list[LineageEdgeRecord] = []
        if dataset.connection_id is not None:
            edges.append(
                LineageEdgeRecord(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.CONNECTION.value,
                    source_id=str(dataset.connection_id),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.FEEDS.value,
                    metadata_json={"connection_id": str(dataset.connection_id)},
                )
            )
        resource_name = str(sync_meta.get("resource_name") or "").strip()
        if dataset.connection_id is not None and resource_name:
            edges.append(
                LineageEdgeRecord(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.API_RESOURCE.value,
                    source_id=build_api_resource_id(
                        connection_id=dataset.connection_id,
                        resource_name=resource_name,
                    ),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata_json={
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
                LineageEdgeRecord(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.FILE_RESOURCE.value,
                    source_id=build_file_resource_id(storage_uri),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata_json={
                        "storage_uri": storage_uri,
                        "file_config": file_config,
                    },
                )
            )
        for edge in edges:
            self._lineage_edge_repository.add(edge)

    @staticmethod
    def _column_snapshot(column: DatasetColumnRecord) -> dict[str, Any]:
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
    def _policy_snapshot(policy: DatasetPolicyRecord) -> dict[str, Any]:
        return {
            "max_rows_preview": policy.max_rows_preview,
            "max_export_rows": policy.max_export_rows,
            "redaction_rules": dict(policy.redaction_rules_json or {}),
            "row_filters": list(policy.row_filters_json or []),
            "allow_dml": policy.allow_dml,
        }

    @staticmethod
    def _build_dataset_definition_snapshot(dataset: DatasetRecord) -> dict[str, Any]:
        relation_identity, execution_capabilities = ConnectorSyncRuntime._resolve_dataset_descriptor_snapshot(
            dataset
        )
        return {
            "id": str(dataset.id),
            "workspace_id": str(dataset.workspace_id),
            "project_id": str(dataset.project_id) if dataset.project_id else None,
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
    def _build_dataset_source_bindings(dataset: DatasetRecord) -> list[dict[str, Any]]:
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
    def _resolve_dataset_descriptor_snapshot(dataset: DatasetRecord) -> tuple[dict[str, Any], dict[str, Any]]:
        relation_identity = dict(dataset.relation_identity_json or {})
        execution_capabilities = dict(dataset.execution_capabilities_json or {})
        return relation_identity, execution_capabilities

    @staticmethod
    def _apply_dataset_descriptor_metadata(
        *,
        dataset: DatasetRecord,
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
        dataset.relation_identity_json = relation_identity.model_dump(mode="json")
        dataset.execution_capabilities_json = execution_capabilities.model_dump(mode="json")

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
