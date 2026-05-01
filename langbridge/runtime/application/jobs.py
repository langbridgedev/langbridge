import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from langbridge.runtime.models import (
    CreateRuntimeJobRequest,
    RuntimeJob,
    RuntimeJobCancelRequest,
    RuntimeJobStreamEvent,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


class JobApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    async def create_job(self, *, request: CreateRuntimeJobRequest) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            job = await self._host.services.jobs.create_job(
                workspace_id=self._host.context.workspace_id,
                actor_id=self._host.context.actor_id,
                request=request,
            )
            if uow is not None:
                await uow.commit()
        self._host.wake_job_processor()
        return self._serialize_job(job)

    async def get_job(self, *, job_id: uuid.UUID) -> dict[str, Any]:
        async with self._host._runtime_operation_scope():
            job = await self._host.services.jobs.get_job_for_workspace(
                job_id=job_id,
                workspace_id=self._host.context.workspace_id,
            )
        return self._serialize_job(job)

    async def list_jobs(
        self,
        *,
        job_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope():
            jobs = await self._host.services.jobs.list_jobs(
                workspace_id=self._host.context.workspace_id,
                job_type=job_type,
                status=status,
                limit=limit,
            )
        return {
            "items": [self._serialize_job(job) for job in jobs],
            "total": len(jobs),
        }

    async def cancel_job(
        self,
        *,
        job_id: uuid.UUID,
        request: RuntimeJobCancelRequest,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            await self._host.services.jobs.get_job_for_workspace(
                job_id=job_id,
                workspace_id=self._host.context.workspace_id,
            )
            job = await self._host.services.jobs.cancel_job(
                job_id=job_id,
                reason=request.reason,
            )
            if uow is not None:
                await uow.commit()
        return self._serialize_job(job)

    async def stream_job(
        self,
        *,
        job_id: uuid.UUID,
        after_sequence: int = 0,
        heartbeat_interval: float = 10.0,
    ) -> AsyncIterator[RuntimeJobStreamEvent | None]:
        async with self._host._runtime_operation_scope():
            await self._host.services.jobs.get_job_for_workspace(
                job_id=job_id,
                workspace_id=self._host.context.workspace_id,
            )
        return await self._host.services.jobs.stream_events(
            job_id=job_id,
            after_sequence=after_sequence,
            heartbeat_interval=heartbeat_interval,
        )

    def _serialize_job(self, job: RuntimeJob) -> dict[str, Any]:
        return job.model_dump(mode="json")
