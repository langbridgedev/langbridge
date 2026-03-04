from __future__ import annotations

import logging
import uuid

from langbridge.apps.api.langbridge_api.services.task_dispatch_service import TaskDispatchService
from langbridge.packages.common.langbridge_common.contracts.jobs.dataset_job import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetPreviewJobRequest,
    CreateDatasetProfileJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.job import (
    JobEventRecord,
    JobEventVisibility,
    JobRecord,
    JobStatus,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.dataset_job import (
    DatasetJobRequestMessage,
)


class DatasetJobRequestService:
    def __init__(
        self,
        *,
        job_repository: JobRepository,
        task_dispatch_service: TaskDispatchService,
    ) -> None:
        self._job_repository = job_repository
        self._task_dispatch_service = task_dispatch_service
        self._logger = logging.getLogger(__name__)

    async def create_preview_job(self, request: CreateDatasetPreviewJobRequest) -> JobRecord:
        return await self._create_job(
            job_type=JobType.DATASET_PREVIEW,
            workspace_id=request.workspace_id,
            payload=request.model_dump(mode="json"),
            status_message="Dataset preview queued.",
            event_type="DatasetPreviewQueued",
            event_message="Dataset preview queued.",
        )

    async def create_profile_job(self, request: CreateDatasetProfileJobRequest) -> JobRecord:
        return await self._create_job(
            job_type=JobType.DATASET_PROFILE,
            workspace_id=request.workspace_id,
            payload=request.model_dump(mode="json"),
            status_message="Dataset profiling queued.",
            event_type="DatasetProfileQueued",
            event_message="Dataset profiling queued.",
        )

    async def create_bulk_create_job(self, request: CreateDatasetBulkCreateJobRequest) -> JobRecord:
        return await self._create_job(
            job_type=JobType.DATASET_BULK_CREATE,
            workspace_id=request.workspace_id,
            payload=request.model_dump(mode="json"),
            status_message="Bulk dataset creation queued.",
            event_type="DatasetBulkCreateQueued",
            event_message="Bulk dataset creation queued.",
        )

    async def _create_job(
        self,
        *,
        job_type: JobType,
        workspace_id: uuid.UUID,
        payload: dict,
        status_message: str,
        event_type: str,
        event_message: str,
    ) -> JobRecord:
        job_id = uuid.uuid4()
        job_record = JobRecord(
            id=job_id,
            job_type=job_type.value,
            payload=payload,
            headers={},
            organisation_id=str(workspace_id),
            status=JobStatus.queued,
            progress=0,
            status_message=status_message,
            job_events=[
                JobEventRecord(
                    event_type=event_type,
                    visibility=JobEventVisibility.public,
                    details={
                        "visibility": "public",
                        "message": event_message,
                        "source": "api",
                        "details": payload,
                    },
                )
            ],
        )
        self._job_repository.add(job_record)
        self._logger.info("Created dataset job %s (%s).", job_id, job_type.value)

        message = DatasetJobRequestMessage(
            job_id=job_id,
            job_type=job_type,
            job_request=payload,
        )
        await self._task_dispatch_service.dispatch_job_message(
            tenant_id=workspace_id,
            payload=message,
            required_tags=["dataset"],
        )
        return job_record
