import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from langbridge.packages.common.langbridge_common.contracts.jobs.dataset_job import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetCsvIngestJobRequest,
    CreateDatasetPreviewJobRequest,
    CreateDatasetProfileJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.job import JobStatus
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
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
from langbridge.packages.common.langbridge_common.utils.sql import sanitize_sql_error_message
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.dataset_job import (
    DatasetJobRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler
from langbridge.packages.runtime.context import RuntimeContext
from langbridge.packages.runtime.execution import FederatedQueryTool
from langbridge.packages.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.packages.runtime.services.dataset_query_service import (
    DatasetExecutionRequest,
    DatasetQueryService,
)


class DatasetJobRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.DATASET_JOB_REQUEST

    def __init__(
        self,
        job_repository: JobRepository,
        dataset_repository: DatasetRepository,
        dataset_column_repository: DatasetColumnRepository,
        dataset_policy_repository: DatasetPolicyRepository,
        dataset_revision_repository: DatasetRevisionRepository | None = None,
        lineage_edge_repository: LineageEdgeRepository | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._job_repository = job_repository
        self._dataset_query_service = DatasetQueryService(
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
            dataset_revision_repository=dataset_revision_repository,
            lineage_edge_repository=lineage_edge_repository,
            federated_query_tool=federated_query_tool,
        )

    async def handle(self, payload: DatasetJobRequestMessage) -> None:
        job_record = await self._job_repository.get_by_id(payload.job_id)
        if job_record is None:
            raise BusinessValidationError(f"Job with ID {payload.job_id} does not exist.")
        if job_record.status in {JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled}:
            self._logger.info("Dataset job %s already terminal (%s).", job_record.id, job_record.status)
            return None

        job_record.status = JobStatus.running
        job_record.progress = 5
        job_record.status_message = "Dataset execution started."
        if job_record.started_at is None:
            job_record.started_at = datetime.now(timezone.utc)

        if payload.job_type not in {
            JobType.DATASET_PREVIEW,
            JobType.DATASET_PROFILE,
            JobType.DATASET_CSV_INGEST,
            JobType.DATASET_BULK_CREATE,
        }:
            raise BusinessValidationError(f"Unsupported dataset job type '{payload.job_type.value}'.")

        try:
            request = self._parse_request(payload)
        except Exception as exc:
            self._logger.exception("Dataset job %s failed during request parsing: %s", job_record.id, exc)
            self._mark_job_failed(job_record, exc)
            return None

        runtime = RuntimeHost(
            context=RuntimeContext.build(
                tenant_id=request.workspace_id,
                workspace_id=request.workspace_id,
                user_id=request.user_id,
                request_id=str(job_record.id),
            ),
            providers=RuntimeProviders(),
            services=RuntimeServices(dataset_query=self._dataset_query_service),
        )
        return await runtime.query_dataset(
            request=request,
            job_record=job_record,
        )

    def _parse_request(self, payload: DatasetJobRequestMessage) -> DatasetExecutionRequest:
        if payload.job_type == JobType.DATASET_PREVIEW:
            return self._parse_preview_request(payload)
        if payload.job_type == JobType.DATASET_PROFILE:
            return self._parse_profile_request(payload)
        if payload.job_type == JobType.DATASET_CSV_INGEST:
            return self._parse_csv_ingest_request(payload)
        if payload.job_type == JobType.DATASET_BULK_CREATE:
            return self._parse_bulk_create_request(payload)
        raise BusinessValidationError(f"Unsupported dataset job type '{payload.job_type.value}'.")

    @staticmethod
    def _parse_preview_request(payload: DatasetJobRequestMessage) -> CreateDatasetPreviewJobRequest:
        try:
            return CreateDatasetPreviewJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid dataset preview request payload.") from exc

    @staticmethod
    def _parse_profile_request(payload: DatasetJobRequestMessage) -> CreateDatasetProfileJobRequest:
        try:
            return CreateDatasetProfileJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid dataset profile request payload.") from exc

    @staticmethod
    def _parse_csv_ingest_request(payload: DatasetJobRequestMessage) -> CreateDatasetCsvIngestJobRequest:
        try:
            return CreateDatasetCsvIngestJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid CSV ingest request payload.") from exc

    @staticmethod
    def _parse_bulk_create_request(payload: DatasetJobRequestMessage) -> CreateDatasetBulkCreateJobRequest:
        try:
            return CreateDatasetBulkCreateJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid dataset bulk-create request payload.") from exc

    @staticmethod
    def _mark_job_failed(job_record, exc: Exception) -> None:
        job_record.status = JobStatus.failed
        job_record.progress = 100
        job_record.status_message = "Dataset execution failed."
        job_record.finished_at = datetime.now(timezone.utc)
        job_record.error = {"message": sanitize_sql_error_message(str(exc))}
