from __future__ import annotations

import logging
import uuid

from langbridge.apps.api.langbridge_api.services.task_dispatch_service import TaskDispatchService
from langbridge.packages.common.langbridge_common.contracts.jobs.connector_job import (
    CreateConnectorSyncJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.job import (
    JobEventRecord,
    JobEventVisibility,
    JobRecord,
    JobStatus,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.connector_job import (
    ConnectorSyncJobRequestMessage,
)


class ConnectorSyncJobRequestService:
    def __init__(
        self,
        *,
        job_repository: JobRepository,
        task_dispatch_service: TaskDispatchService,
    ) -> None:
        self._job_repository = job_repository
        self._task_dispatch_service = task_dispatch_service
        self._logger = logging.getLogger(__name__)

    async def create_sync_job(self, request: CreateConnectorSyncJobRequest) -> JobRecord:
        job_id = uuid.uuid4()
        payload = request.model_dump(mode="json")
        job_record = JobRecord(
            id=job_id,
            job_type=JobType.CONNECTOR_SYNC.value,
            payload=payload,
            headers={},
            organisation_id=str(request.workspace_id),
            status=JobStatus.queued,
            progress=0,
            status_message="Connector sync queued.",
            job_events=[
                JobEventRecord(
                    event_type="ConnectorSyncQueued",
                    visibility=JobEventVisibility.public,
                    details={
                        "visibility": "public",
                        "message": "Connector sync queued.",
                        "source": "api",
                        "details": payload,
                    },
                )
            ],
        )
        self._job_repository.add(job_record)
        self._logger.info("Created connector sync job %s for connection %s.", job_id, request.connection_id)

        message = ConnectorSyncJobRequestMessage(
            job_id=job_id,
            job_type=JobType.CONNECTOR_SYNC,
            job_request=payload,
        )
        await self._task_dispatch_service.dispatch_job_message(
            tenant_id=request.workspace_id,
            payload=message,
            required_tags=["connector-sync"],
        )
        return job_record
