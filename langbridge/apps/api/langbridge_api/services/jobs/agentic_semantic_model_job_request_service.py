from __future__ import annotations

import logging
import uuid

from langbridge.apps.api.langbridge_api.services.task_dispatch_service import TaskDispatchService
from langbridge.packages.common.langbridge_common.contracts.jobs.agentic_semantic_model_job import (
    CreateAgenticSemanticModelJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.job import (
    JobEventRecord,
    JobEventVisibility,
    JobRecord,
    JobStatus,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.agentic_semantic_model_job import (
    AgenticSemanticModelJobRequestMessage,
)


class AgenticSemanticModelJobRequestService:
    def __init__(
        self,
        job_repository: JobRepository,
        task_dispatch_service: TaskDispatchService,
    ) -> None:
        self._job_repository = job_repository
        self._task_dispatch_service = task_dispatch_service
        self._logger = logging.getLogger(__name__)

    async def create_agentic_semantic_model_job_request(
        self,
        request: CreateAgenticSemanticModelJobRequest,
    ) -> JobRecord:
        job_id = uuid.uuid4()
        payload = request.model_dump(mode="json")
        job_record = JobRecord(
            id=job_id,
            job_type=JobType.AGENTIC_SEMANTIC_MODEL.value,
            payload=payload,
            headers={},
            organisation_id=str(request.organisation_id),
            status=JobStatus.queued,
            progress=0,
            status_message="Agentic semantic model generation queued.",
            job_events=[
                JobEventRecord(
                    event_type="AgenticSemanticModelQueued",
                    visibility=JobEventVisibility.public,
                    details={
                        "visibility": "public",
                        "message": "Agentic semantic model generation queued.",
                        "source": "api",
                        "details": {
                            "semantic_model_id": str(request.semantic_model_id),
                            "connector_id": str(request.connector_id),
                            "selected_table_count": len(request.selected_tables),
                            "question_prompt_count": len(request.question_prompts),
                        },
                    },
                )
            ],
        )
        self._job_repository.add(job_record)

        self._logger.info(
            "Created agentic semantic model job %s for semantic model %s",
            job_id,
            request.semantic_model_id,
        )

        message = AgenticSemanticModelJobRequestMessage(
            job_id=job_id,
            job_type=JobType.AGENTIC_SEMANTIC_MODEL,
            job_request=payload,
        )
        await self._task_dispatch_service.dispatch_job_message(
            tenant_id=request.organisation_id,
            payload=message,
        )
        return job_record
