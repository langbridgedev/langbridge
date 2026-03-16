import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from langbridge.packages.common.langbridge_common.contracts.jobs.sql_job import (
    CreateSqlJobRequest,
)
from langbridge.packages.common.langbridge_common.db.sql import SqlJobRecord
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.sql_job import (
    SqlJobRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler
from langbridge.packages.runtime.context import RuntimeContext
from langbridge.packages.runtime.execution import FederatedQueryTool
from langbridge.packages.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.packages.runtime.security import SecretProviderRegistry
from langbridge.packages.runtime.services.sql_query_service import SqlQueryService


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
        self._sql_query_service = SqlQueryService(
            sql_job_result_artifact_repository=sql_job_result_artifact_repository,
            connector_repository=connector_repository,
            dataset_repository=dataset_repository,
            secret_provider_registry=secret_provider_registry,
            federated_query_tool=federated_query_tool,
        )

    async def handle(self, payload: SqlJobRequestMessage) -> None:
        request = self._parse_request(payload)
        job: SqlJobRecord = await self._sql_job_repository.get_by_id_for_workspace(
            sql_job_id=request.sql_job_id,
            workspace_id=request.workspace_id,
        )
        if job is None:
            raise BusinessValidationError("SQL job not found.")

        if job.status in {"succeeded", "failed", "cancelled"}:
            self._logger.info("SQL job %s already terminal (%s).", job.id, job.status)
            return None

        job.status = "running"
        if job.started_at is None:
            job.started_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)
        runtime = RuntimeHost(
            context=RuntimeContext.build(
                tenant_id=request.workspace_id,
                workspace_id=request.workspace_id,
                user_id=request.user_id,
                request_id=request.correlation_id,
            ),
            providers=RuntimeProviders(),
            services=RuntimeServices(sql_query=self._sql_query_service),
        )
        return await runtime.execute_sql(
            request=request,
            job=job,
            create_sql_connector=self._create_sql_connector,
            resolve_connector_config=self._resolve_connector_config,
        )

    def _parse_request(self, payload: SqlJobRequestMessage) -> CreateSqlJobRequest:
        try:
            return CreateSqlJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid SQL job request payload.") from exc

    async def _create_sql_connector(self, **kwargs):
        return await self._sql_query_service._create_sql_connector(**kwargs)

    def _resolve_connector_config(self, connector):
        return self._sql_query_service._resolve_connector_config(connector)
