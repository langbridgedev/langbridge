from __future__ import annotations

from langbridge.apps.api.langbridge_api.services.task_dispatch_service import TaskDispatchService
from langbridge.packages.common.langbridge_common.contracts.jobs.sql_job import CreateSqlJobRequest
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.sql_job import (
    SqlJobRequestMessage,
)


class SqlJobRequestService:
    def __init__(self, task_dispatch_service: TaskDispatchService) -> None:
        self._task_dispatch_service = task_dispatch_service

    async def dispatch_sql_job(self, request: CreateSqlJobRequest) -> None:
        message = SqlJobRequestMessage(
            sql_job_id=request.sql_job_id,
            job_type=JobType.SQL,
            job_request=request.model_dump(mode="json"),
        )
        await self._task_dispatch_service.dispatch_job_message(
            tenant_id=request.workspace_id,
            payload=message,
            required_tags=["sql"],
        )

