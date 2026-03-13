from __future__ import annotations

import uuid
from typing import Any

from langbridge.packages.common.langbridge_common.db.connector_sync import ConnectorSyncStateRecord
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelRecordResponse,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.connector_sync_repository import (
    ConnectorSyncStateRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetColumnRepository,
    DatasetPolicyRepository,
    DatasetRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)


class RuntimeMetadataService:
    def __init__(
        self,
        *,
        dataset_repository: DatasetRepository,
        dataset_column_repository: DatasetColumnRepository,
        dataset_policy_repository: DatasetPolicyRepository,
        connector_repository: ConnectorRepository,
        semantic_model_repository: SemanticModelRepository,
        connector_sync_state_repository: ConnectorSyncStateRepository,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._connector_repository = connector_repository
        self._semantic_model_repository = semantic_model_repository
        self._connector_sync_state_repository = connector_sync_state_repository

    async def get_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_id: uuid.UUID,
    ) -> dict[str, Any] | None:
        dataset = await self._dataset_repository.get_for_workspace(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
        )
        if dataset is None:
            return None
        return self._serialize_dataset(dataset)

    async def get_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: list[uuid.UUID],
    ) -> list[dict[str, Any]]:
        rows = await self._dataset_repository.get_by_ids_for_workspace(
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
        )
        return [self._serialize_dataset(row) for row in rows]

    async def get_dataset_columns(self, *, dataset_id: uuid.UUID) -> list[dict[str, Any]]:
        rows = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset_id)
        return [self._serialize_dataset_column(row) for row in rows]

    async def get_dataset_policy(self, *, dataset_id: uuid.UUID) -> dict[str, Any] | None:
        policy = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset_id)
        if policy is None:
            return None
        return self._serialize_dataset_policy(policy)

    async def get_connector(self, *, connector_id: uuid.UUID) -> dict[str, Any] | None:
        connector = await self._connector_repository.get_by_id(connector_id)
        if connector is None:
            return None
        return ConnectorResponse.from_connector(connector).model_dump(mode="json")

    async def get_semantic_model(
        self,
        *,
        organization_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
    ) -> dict[str, Any] | None:
        record = await self._semantic_model_repository.get_for_scope(
            model_id=semantic_model_id,
            organization_id=organization_id,
        )
        if record is None:
            return None
        return SemanticModelRecordResponse.model_validate(record).model_dump(mode="json")

    async def get_sync_state(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        resource_name: str,
    ) -> dict[str, Any] | None:
        state = await self._connector_sync_state_repository.get_for_resource(
            workspace_id=workspace_id,
            connection_id=connection_id,
            resource_name=resource_name,
        )
        if state is None:
            return None
        return self._serialize_sync_state(state)

    async def upsert_sync_state(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_type: str,
        resource_name: str,
        sync_mode: str = "INCREMENTAL",
    ) -> dict[str, Any]:
        state = await self._connector_sync_state_repository.get_for_resource(
            workspace_id=workspace_id,
            connection_id=connection_id,
            resource_name=resource_name,
        )
        if state is None:
            state = ConnectorSyncStateRecord(
                workspace_id=workspace_id,
                connection_id=connection_id,
                connector_type=connector_type,
                resource_name=resource_name,
                sync_mode=sync_mode,
            )
            self._connector_sync_state_repository.add(state)
        return self._serialize_sync_state(state)

    async def mark_sync_state_failed(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        resource_name: str,
        error_message: str,
        status: str = "failed",
    ) -> dict[str, Any] | None:
        state = await self._connector_sync_state_repository.get_for_resource(
            workspace_id=workspace_id,
            connection_id=connection_id,
            resource_name=resource_name,
        )
        if state is None:
            return None
        state.status = status
        state.error_message = error_message
        return self._serialize_sync_state(state)

    @staticmethod
    def _serialize_sync_state(state: Any) -> dict[str, Any]:
        return {
            "id": str(state.id),
            "workspace_id": str(state.workspace_id),
            "connection_id": str(state.connection_id),
            "connector_type": state.connector_type,
            "resource_name": state.resource_name,
            "sync_mode": state.sync_mode,
            "last_cursor": state.last_cursor,
            "last_sync_at": state.last_sync_at.isoformat() if state.last_sync_at else None,
            "state_json": dict(state.state_json or {}),
            "status": state.status,
            "error_message": state.error_message,
            "records_synced": int(state.records_synced or 0),
            "bytes_synced": int(state.bytes_synced) if state.bytes_synced is not None else None,
            "created_at": state.created_at.isoformat(),
            "updated_at": state.updated_at.isoformat(),
        }

    @staticmethod
    def _serialize_dataset(dataset: Any) -> dict[str, Any]:
        return {
            "id": str(dataset.id),
            "workspace_id": str(dataset.workspace_id),
            "project_id": str(dataset.project_id) if dataset.project_id else None,
            "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
            "created_by": str(dataset.created_by) if dataset.created_by else None,
            "updated_by": str(dataset.updated_by) if dataset.updated_by else None,
            "name": dataset.name,
            "sql_alias": dataset.sql_alias,
            "description": dataset.description,
            "tags_json": list(dataset.tags_json or []),
            "dataset_type": dataset.dataset_type,
            "source_kind": dataset.source_kind,
            "connector_kind": dataset.connector_kind,
            "storage_kind": dataset.storage_kind,
            "dialect": dataset.dialect,
            "catalog_name": dataset.catalog_name,
            "schema_name": dataset.schema_name,
            "table_name": dataset.table_name,
            "storage_uri": dataset.storage_uri,
            "sql_text": dataset.sql_text,
            "relation_identity_json": dict(dataset.relation_identity_json or {})
            if isinstance(dataset.relation_identity_json, dict)
            else dataset.relation_identity_json,
            "execution_capabilities_json": dict(dataset.execution_capabilities_json or {})
            if isinstance(dataset.execution_capabilities_json, dict)
            else dataset.execution_capabilities_json,
            "referenced_dataset_ids_json": list(dataset.referenced_dataset_ids_json or []),
            "federated_plan_json": dict(dataset.federated_plan_json or {})
            if isinstance(dataset.federated_plan_json, dict)
            else dataset.federated_plan_json,
            "file_config_json": dict(dataset.file_config_json or {})
            if isinstance(dataset.file_config_json, dict)
            else dataset.file_config_json,
            "status": dataset.status,
            "revision_id": str(dataset.revision_id) if dataset.revision_id else None,
            "row_count_estimate": int(dataset.row_count_estimate) if dataset.row_count_estimate is not None else None,
            "bytes_estimate": int(dataset.bytes_estimate) if dataset.bytes_estimate is not None else None,
            "last_profiled_at": dataset.last_profiled_at.isoformat() if dataset.last_profiled_at else None,
            "created_at": dataset.created_at.isoformat(),
            "updated_at": dataset.updated_at.isoformat(),
        }

    @staticmethod
    def _serialize_dataset_column(column: Any) -> dict[str, Any]:
        return {
            "id": str(column.id),
            "dataset_id": str(column.dataset_id),
            "workspace_id": str(column.workspace_id),
            "name": column.name,
            "data_type": column.data_type,
            "nullable": bool(column.nullable),
            "ordinal_position": int(column.ordinal_position),
            "description": column.description,
            "is_allowed": bool(column.is_allowed),
            "is_computed": bool(column.is_computed),
            "expression": column.expression,
            "created_at": column.created_at.isoformat(),
            "updated_at": column.updated_at.isoformat(),
        }

    @staticmethod
    def _serialize_dataset_policy(policy: Any) -> dict[str, Any]:
        return {
            "id": str(policy.id),
            "dataset_id": str(policy.dataset_id),
            "workspace_id": str(policy.workspace_id),
            "max_rows_preview": int(policy.max_rows_preview),
            "max_export_rows": int(policy.max_export_rows),
            "redaction_rules_json": dict(policy.redaction_rules_json or {}),
            "row_filters_json": list(policy.row_filters_json or []),
            "allow_dml": bool(policy.allow_dml),
            "created_at": policy.created_at.isoformat(),
            "updated_at": policy.updated_at.isoformat(),
        }
