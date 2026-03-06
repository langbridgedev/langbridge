from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from langbridge.apps.worker.langbridge_worker.connector_sync_runtime import (
    ConnectorSyncRuntime,
)
from langbridge.apps.worker.langbridge_worker.secrets import SecretProviderRegistry
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectorSyncMode,
    ConnectorSyncStatus,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.connector_job import (
    CreateConnectorSyncJobRequest,
)
from langbridge.packages.common.langbridge_common.db.job import (
    JobEventRecord,
    JobEventVisibility,
    JobStatus,
)
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
    DatasetColumnRepository,
    DatasetPolicyRepository,
    DatasetRepository,
    DatasetRevisionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.lineage_repository import (
    LineageEdgeRepository,
)
from langbridge.packages.common.langbridge_common.utils.connector_runtime import (
    build_connector_runtime_payload,
)
from langbridge.packages.connectors.langbridge_connectors.api import (
    ApiConnectorFactory,
    get_connector_config_factory,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.connector_job import (
    ConnectorSyncJobRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler


class ConnectorSyncJobRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.CONNECTOR_SYNC_JOB_REQUEST

    def __init__(
        self,
        job_repository: JobRepository,
        connector_repository: ConnectorRepository,
        connector_sync_state_repository: ConnectorSyncStateRepository,
        dataset_repository: DatasetRepository,
        dataset_column_repository: DatasetColumnRepository,
        dataset_policy_repository: DatasetPolicyRepository,
        dataset_revision_repository: DatasetRevisionRepository | None = None,
        lineage_edge_repository: LineageEdgeRepository | None = None,
        secret_provider_registry: SecretProviderRegistry | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._job_repository = job_repository
        self._connector_repository = connector_repository
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()
        self._api_connector_factory = ApiConnectorFactory()
        self._runtime = ConnectorSyncRuntime(
            connector_sync_state_repository=connector_sync_state_repository,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
            dataset_revision_repository=dataset_revision_repository,
            lineage_edge_repository=lineage_edge_repository,
        )

    async def handle(self, payload: ConnectorSyncJobRequestMessage) -> None:
        request = self._parse_request(payload)
        job_record = await self._job_repository.get_by_id(payload.job_id)
        if job_record is None:
            raise BusinessValidationError(f"Job with ID {payload.job_id} does not exist.")
        if job_record.status in {JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled}:
            self._logger.info("Connector sync job %s already terminal (%s).", job_record.id, job_record.status)
            return None

        connector_record = await self._connector_repository.get_by_id(request.connection_id)
        if connector_record is None:
            raise BusinessValidationError("Connector not found.")

        connector_type = ConnectorRuntimeType(str(connector_record.connector_type).upper())
        runtime_payload = build_connector_runtime_payload(
            config_json=connector_record.config_json,
            connection_metadata=connector_record.connection_metadata_json,
            secret_references=connector_record.secret_references_json,
            secret_resolver=self._secret_provider_registry.resolve,
        )
        config_factory = get_connector_config_factory(connector_type)
        api_connector = self._api_connector_factory.create_api_connector(
            connector_type,
            config_factory.create(runtime_payload.get("config") or {}),
            logger=self._logger,
        )
        await api_connector.test_connection()

        discovered_resources = {resource.name: resource for resource in await api_connector.discover_resources()}
        unknown_resources = [name for name in request.resource_names if name not in discovered_resources]
        if unknown_resources:
            raise BusinessValidationError(
                f"Unsupported resource(s) requested for sync: {', '.join(sorted(unknown_resources))}."
            )

        job_record.status = JobStatus.running
        job_record.progress = 5
        job_record.status_message = "Connector sync started."
        if job_record.started_at is None:
            job_record.started_at = datetime.now(timezone.utc)

        active_state = None
        summaries: list[dict] = []
        try:
            total_resources = max(1, len(request.resource_names))
            for index, resource_name in enumerate(request.resource_names):
                job_record.progress = max(5, 5 + int((index / total_resources) * 85))
                job_record.status_message = f"Syncing {resource_name} ({index + 1}/{total_resources})."
                active_state = await self._runtime.get_or_create_state(
                    workspace_id=request.workspace_id,
                    connection_id=request.connection_id,
                    connector_type=connector_type,
                    resource_name=resource_name,
                    sync_mode=request.sync_mode,
                )
                active_state.status = ConnectorSyncStatus.RUNNING.value
                active_state.sync_mode = request.sync_mode.value
                active_state.error_message = None
                active_state.updated_at = datetime.now(timezone.utc)

                summary = await self._runtime.sync_resource(
                    workspace_id=request.workspace_id,
                    project_id=request.project_id,
                    user_id=request.user_id,
                    connection_id=request.connection_id,
                    connector_record=connector_record,
                    connector_type=connector_type,
                    resource=discovered_resources[resource_name],
                    api_connector=api_connector,
                    state=active_state,
                    sync_mode=(
                        ConnectorSyncMode.FULL_REFRESH
                        if request.force_full_refresh
                        else request.sync_mode
                    ),
                )
                summaries.append(summary)
                job_record.job_events.append(
                    JobEventRecord(
                        job_id=job_record.id,
                        event_type="ConnectorSyncResourceCompleted",
                        visibility=JobEventVisibility.public,
                        details=summary,
                    )
                )

            job_record.status = JobStatus.succeeded
            job_record.progress = 100
            job_record.status_message = "Connector sync completed."
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = None
            job_record.result = {
                "result": {"resources": summaries},
                "summary": f"Connector sync completed for {len(summaries)} resource(s).",
            }
        except Exception as exc:
            self._logger.exception("Connector sync job %s failed: %s", job_record.id, exc)
            if active_state is not None:
                await self._runtime.mark_failed(state=active_state, error_message=str(exc))
            job_record.status = JobStatus.failed
            job_record.progress = 100
            job_record.status_message = "Connector sync failed."
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = {"message": str(exc)}
        return None

    @staticmethod
    def _parse_request(payload: ConnectorSyncJobRequestMessage) -> CreateConnectorSyncJobRequest:
        try:
            return CreateConnectorSyncJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid connector sync request payload.") from exc
