from __future__ import annotations

import json
import os
import uuid
from typing import Any

from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService
from langbridge.apps.api.langbridge_api.services.jobs.connector_sync_job_request_service import (
    ConnectorSyncJobRequestService,
)
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectorResourceListResponse,
    ConnectorResourceResponse,
    ConnectorSyncHistoryItemResponse,
    ConnectorSyncHistoryResponse,
    ConnectorSyncMode,
    ConnectorSyncRequest,
    ConnectorSyncStartResponse,
    ConnectorSyncStateListResponse,
    ConnectorSyncStateResponse,
    ConnectorSyncStatus,
    ConnectorTestResponse,
    SecretReference,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.connector_job import (
    CreateConnectorSyncJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.connector import APIConnector as ApiConnectorRecord
from langbridge.packages.common.langbridge_common.db.connector_sync import ConnectorSyncStateRecord
from langbridge.packages.common.langbridge_common.db.dataset import DatasetRecord
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.connector_sync_repository import (
    ConnectorSyncStateRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.utils.connector_runtime import (
    build_connector_runtime_payload,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType


class ConnectorSyncService:
    def __init__(
        self,
        *,
        connector_repository: ConnectorRepository,
        connector_sync_state_repository: ConnectorSyncStateRepository,
        dataset_repository: DatasetRepository,
        job_repository: JobRepository,
        connector_service: ConnectorService,
        connector_sync_job_request_service: ConnectorSyncJobRequestService,
    ) -> None:
        self._connector_repository = connector_repository
        self._connector_sync_state_repository = connector_sync_state_repository
        self._dataset_repository = dataset_repository
        self._job_repository = job_repository
        self._connector_service = connector_service
        self._connector_sync_job_request_service = connector_sync_job_request_service

    async def test_connection(
        self,
        *,
        organization_id: uuid.UUID,
        connector_id: uuid.UUID,
    ) -> ConnectorTestResponse:
        connector_record, _ = await self._get_runtime_api_connector(
            organization_id=organization_id,
            connector_id=connector_id,
        )
        return ConnectorTestResponse(
            status="ok",
            message=f"Connector '{connector_record.name}' validated successfully.",
        )

    async def list_resources(
        self,
        *,
        organization_id: uuid.UUID,
        connector_id: uuid.UUID,
    ) -> ConnectorResourceListResponse:
        connector_record, runtime_connector = await self._get_runtime_api_connector(
            organization_id=organization_id,
            connector_id=connector_id,
        )
        resources = await runtime_connector.discover_resources()
        states = await self._connector_sync_state_repository.list_for_connection(
            workspace_id=organization_id,
            connection_id=connector_id,
        )
        datasets = await self._dataset_repository.list_for_connection(
            workspace_id=organization_id,
            connection_id=connector_id,
            dataset_types=["FILE"],
        )
        state_by_name = {state.resource_name: state for state in states}
        dataset_map = self._datasets_by_resource(datasets)

        items = []
        for resource in resources:
            state = state_by_name.get(resource.name)
            dataset_items = dataset_map.get(resource.name, [])
            items.append(
                ConnectorResourceResponse(
                    name=resource.name,
                    label=resource.label,
                    primary_key=resource.primary_key,
                    parent_resource=resource.parent_resource,
                    cursor_field=resource.cursor_field,
                    incremental_cursor_field=resource.incremental_cursor_field,
                    supports_incremental=bool(resource.supports_incremental),
                    default_sync_mode=(
                        ConnectorSyncMode(resource.default_sync_mode)
                        if str(resource.default_sync_mode or "").strip()
                        else ConnectorSyncMode.FULL_REFRESH
                    ),
                    status=self._to_sync_status(state.status if state is not None else None),
                    last_cursor=state.last_cursor if state is not None else None,
                    last_sync_at=state.last_sync_at if state is not None else None,
                    dataset_ids=[dataset.id for dataset in dataset_items],
                    dataset_names=[dataset.name for dataset in dataset_items],
                    records_synced=state.records_synced if state is not None else None,
                )
            )
        return ConnectorResourceListResponse(connector_id=connector_id, items=items)

    async def list_sync_states(
        self,
        *,
        organization_id: uuid.UUID,
        connector_id: uuid.UUID,
    ) -> ConnectorSyncStateListResponse:
        await self._get_connector_record(organization_id=organization_id, connector_id=connector_id)
        states = await self._connector_sync_state_repository.list_for_connection(
            workspace_id=organization_id,
            connection_id=connector_id,
        )
        return ConnectorSyncStateListResponse(
            connection_id=connector_id,
            items=[self._to_state_response(state) for state in states],
        )

    async def trigger_sync(
        self,
        *,
        organization_id: uuid.UUID,
        connector_id: uuid.UUID,
        user_id: uuid.UUID,
        request: ConnectorSyncRequest,
    ) -> ConnectorSyncStartResponse:
        connector_record = await self._get_connector_record(
            organization_id=organization_id,
            connector_id=connector_id,
        )
        project_id = connector_record.projects[0].id if getattr(connector_record, "projects", None) else None
        job = await self._connector_sync_job_request_service.create_sync_job(
            CreateConnectorSyncJobRequest(
                workspace_id=organization_id,
                project_id=project_id,
                user_id=user_id,
                connection_id=connector_id,
                resource_names=list(request.resources),
                sync_mode=request.sync_mode,
                force_full_refresh=bool(request.force_full_refresh),
            )
        )
        return ConnectorSyncStartResponse(job_id=job.id, job_status=job.status.value)

    async def list_sync_history(
        self,
        *,
        organization_id: uuid.UUID,
        connector_id: uuid.UUID,
        limit: int = 20,
    ) -> ConnectorSyncHistoryResponse:
        await self._get_connector_record(organization_id=organization_id, connector_id=connector_id)
        jobs = await self._job_repository.list_for_organisation_and_type(
            organisation_id=str(organization_id),
            job_type=JobType.CONNECTOR_SYNC.value,
            limit=max(limit * 3, limit),
        )
        items: list[ConnectorSyncHistoryItemResponse] = []
        for job in jobs:
            payload = dict(job.payload or {})
            if str(payload.get("connection_id") or "") != str(connector_id):
                continue
            items.append(
                ConnectorSyncHistoryItemResponse(
                    job_id=job.id,
                    status=job.status.value if hasattr(job.status, "value") else str(job.status),
                    progress=int(job.progress or 0),
                    status_message=job.status_message,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    finished_at=job.finished_at,
                    error=dict(job.error or {}) if isinstance(job.error, dict) else None,
                    payload=payload,
                )
            )
            if len(items) >= limit:
                break
        return ConnectorSyncHistoryResponse(connection_id=connector_id, items=items)

    async def _get_connector_record(
        self,
        *,
        organization_id: uuid.UUID,
        connector_id: uuid.UUID,
    ) -> ApiConnectorRecord:
        connector = await self._connector_repository.get_by_id(connector_id)
        if connector is None:
            raise BusinessValidationError("Connector not found.")
        org_ids = {str(item.id) for item in getattr(connector, "organizations", [])}
        if org_ids and str(organization_id) not in org_ids:
            raise BusinessValidationError("Connector does not belong to the requested organization.")
        if not isinstance(connector, ApiConnectorRecord) and str(getattr(connector, "type", "")).lower() != "api_connector":
            raise BusinessValidationError("Sync is only available for API connectors.")
        return connector

    async def _get_runtime_api_connector(
        self,
        *,
        organization_id: uuid.UUID,
        connector_id: uuid.UUID,
    ):
        connector_record = await self._get_connector_record(
            organization_id=organization_id,
            connector_id=connector_id,
        )
        connector_type = ConnectorRuntimeType(str(connector_record.connector_type).upper())
        runtime_payload = build_connector_runtime_payload(
            config_json=connector_record.config_json,
            connection_metadata=connector_record.connection_metadata_json,
            secret_references=connector_record.secret_references_json,
            secret_resolver=self._resolve_secret_reference,
        )
        runtime_connector = await self._connector_service.async_create_api_connector(
            connector_type,
            runtime_payload,
        )
        return connector_record, runtime_connector

    @staticmethod
    def _datasets_by_resource(datasets: list[DatasetRecord]) -> dict[str, list[DatasetRecord]]:
        results: dict[str, list[DatasetRecord]] = {}
        for dataset in datasets:
            file_config = dict(dataset.file_config_json or {})
            sync_meta = file_config.get("connector_sync")
            if not isinstance(sync_meta, dict):
                continue
            resource_name = str(
                sync_meta.get("root_resource_name")
                or sync_meta.get("resource_name")
                or ""
            ).strip()
            if not resource_name:
                continue
            results.setdefault(resource_name, []).append(dataset)
        return results

    def _resolve_secret_reference(self, reference: SecretReference) -> str:
        if reference.provider_type != "env":
            raise BusinessValidationError(
                f"Connector sync API only supports env-backed secret references for live validation. Unsupported provider '{reference.provider_type}'."
            )
        raw = os.environ.get(reference.identifier)
        if raw is None:
            raise BusinessValidationError(
                f"Environment secret '{reference.identifier}' was not found."
            )
        if reference.key:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise BusinessValidationError(
                    f"Environment secret '{reference.identifier}' is not valid JSON."
                ) from exc
            if not isinstance(payload, dict) or reference.key not in payload:
                raise BusinessValidationError(
                    f"Environment secret '{reference.identifier}' does not contain key '{reference.key}'."
                )
            value = payload.get(reference.key)
            if value is None:
                raise BusinessValidationError(
                    f"Environment secret '{reference.identifier}' key '{reference.key}' is empty."
                )
            return str(value)
        return raw

    @staticmethod
    def _to_sync_status(raw_status: str | None) -> ConnectorSyncStatus:
        normalized = str(raw_status or "").strip().lower()
        for candidate in ConnectorSyncStatus:
            if candidate.value == normalized:
                return candidate
        return ConnectorSyncStatus.NEVER_SYNCED

    def _to_state_response(self, state: ConnectorSyncStateRecord) -> ConnectorSyncStateResponse:
        state_payload = dict(state.state_json or {})
        raw_dataset_ids = state_payload.get("dataset_ids") if isinstance(state_payload.get("dataset_ids"), list) else []
        dataset_ids: list[uuid.UUID] = []
        for raw_id in raw_dataset_ids:
            try:
                dataset_ids.append(uuid.UUID(str(raw_id)))
            except (TypeError, ValueError):
                continue

        return ConnectorSyncStateResponse(
            id=state.id,
            workspace_id=state.workspace_id,
            connection_id=state.connection_id,
            connector_type=state.connector_type,
            resource_name=state.resource_name,
            sync_mode=ConnectorSyncMode(str(state.sync_mode).upper()),
            last_cursor=state.last_cursor,
            last_sync_at=state.last_sync_at,
            state=state_payload,
            status=self._to_sync_status(state.status),
            error_message=state.error_message,
            records_synced=int(state.records_synced or 0),
            bytes_synced=int(state.bytes_synced) if state.bytes_synced is not None else None,
            created_at=state.created_at,
            updated_at=state.updated_at,
            dataset_ids=dataset_ids,
        )
