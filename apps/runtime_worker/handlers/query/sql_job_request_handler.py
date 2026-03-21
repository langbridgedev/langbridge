import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from langbridge.runtime.persistence import (
    RepositoryDatasetCatalogStore,
    RepositorySqlJobArtifactStore,
    RepositorySqlJobStore,
)
from langbridge.runtime.persistence.mappers import from_sql_job_record
from langbridge.runtime.models import (
    CreateSqlJobRequest,
    SqlJob,
)
from langbridge.runtime.errors import BusinessValidationError
from langbridge.runtime.persistence.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.runtime.persistence.repositories.dataset_repository import (
    DatasetRepository,
)
from langbridge.runtime.persistence.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
)
from ...messaging.contracts.base import MessageType
from ...messaging.contracts.jobs.sql_job import (
    SqlJobRequestMessage,
)
from ...messaging.handler import BaseMessageHandler
from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.execution import FederatedQueryTool
from langbridge.runtime.providers import RepositoryConnectorMetadataProvider
from langbridge.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.runtime.security import SecretProviderRegistry
from langbridge.runtime.services.sql_query_service import SqlQueryService


class SqlJobRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.SQL_JOB_REQUEST

    def __init__(
        self,
        sql_job_repository: SqlJobRepository,
        sql_job_result_artifact_repository: SqlJobResultArtifactRepository,
        connector_repository: ConnectorRepository,
        dataset_repository: DatasetRepository | None = None,
        secret_provider_registry: SecretProviderRegistry | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._sql_job_repository = sql_job_repository
        self._sql_job_store = RepositorySqlJobStore(repository=sql_job_repository)
        self._sql_query_service = SqlQueryService(
            sql_job_result_artifact_store=RepositorySqlJobArtifactStore(
                repository=sql_job_result_artifact_repository
            ),
            connector_provider=RepositoryConnectorMetadataProvider(
                connector_repository=connector_repository
            ),
            dataset_repository=(
                RepositoryDatasetCatalogStore(repository=dataset_repository)
                if dataset_repository is not None
                else None
            ),
            secret_provider_registry=secret_provider_registry,
            federated_query_tool=federated_query_tool,
        )

    async def handle(self, payload: SqlJobRequestMessage) -> None:
        request = self._parse_request(payload)
        legacy_job = await self._sql_job_repository.get_by_id_for_workspace(
            sql_job_id=request.sql_job_id,
            workspace_id=request.workspace_id,
        )
        if legacy_job is None:
            raise BusinessValidationError("SQL job not found.")
        job: SqlJob | None = from_sql_job_record(legacy_job)
        if job is None:
            raise BusinessValidationError("SQL job could not be loaded.")

        if job.status in {"succeeded", "failed", "cancelled"}:
            self._logger.info("SQL job %s already terminal (%s).", job.id, job.status)
            return None

        job.status = "running"
        if job.started_at is None:
            job.started_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)
        runtime = RuntimeHost(
            context=RuntimeContext.build(
                workspace_id=request.workspace_id,
                actor_id=request.actor_id,
                request_id=request.correlation_id,
            ),
            providers=RuntimeProviders(),
            services=RuntimeServices(sql_query=self._sql_query_service),
        )
        result = await runtime.execute_sql(
            request=request,
            job=job,
            create_sql_connector=self._create_sql_connector,
            resolve_connector_config=self._resolve_connector_config,
        )
        await self._sql_job_store.save(job)
        return result

    def _parse_request(self, payload: SqlJobRequestMessage) -> CreateSqlJobRequest:
        try:
            return CreateSqlJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid SQL job request payload.") from exc

    async def _create_sql_connector(self, **kwargs):
        return await self._sql_query_service._create_sql_connector(**kwargs)

    def _resolve_connector_config(self, connector):
        return self._sql_query_service._resolve_connector_config(connector)
