from __future__ import annotations

import traceback
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from langbridge.runtime.models import (
    CreateRuntimeJobRequest,
    RuntimeJob,
    RuntimeJobArtifact,
    RuntimeJobEvent,
    RuntimeJobStatus,
    RuntimeJobTask,
    RuntimeJobStreamEvent,
)
from langbridge.runtime.services.jobs.event_stream import RuntimeJobEventStream
from langbridge.runtime.services.jobs.mappers import RuntimeJobMapper


class RuntimeJobService:
    def __init__(self, *, repository: Any, mapper: RuntimeJobMapper | None = None) -> None:
        self._repository = repository
        self._mapper = mapper or RuntimeJobMapper()
        self._event_stream = RuntimeJobEventStream(repository=repository, mapper=self._mapper)

    async def create_job(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        request: CreateRuntimeJobRequest,
        job_id: uuid.UUID | None = None,
    ) -> RuntimeJob:
        idempotency_key = str(request.idempotency_key or "").strip() or None
        if idempotency_key is not None:
            existing = await self._repository.get_by_idempotency_key(
                workspace_id=workspace_id,
                idempotency_key=idempotency_key,
            )
            if existing is not None:
                return self._mapper.to_job(existing)

        job = self._repository.create_job(
            job_id=job_id or uuid.uuid4(),
            workspace_id=workspace_id,
            job_type=request.job_type,
            actor_id=actor_id,
            subject_type=request.subject_type,
            subject_id=request.subject_id,
            queue_name=request.queue_name,
            priority=str(request.priority.value),
            required_capabilities=list(request.required_capabilities),
            runtime_pool_id=request.runtime_pool_id,
            affinity_key=request.affinity_key,
            concurrency_key=request.concurrency_key,
            idempotency_key=idempotency_key,
            max_attempts=request.max_attempts,
            scheduled_at=request.scheduled_at,
            payload=dict(request.payload or {}),
        )
        created = self._mapper.to_job(job)
        await self.append_event(
            job_id=created.id,
            task_id=None,
            event_type="job.created",
            status=RuntimeJobStatus.queued.value,
            stage="queued",
            message="Job queued.",
            visibility="internal",
            terminal=False,
            source="runtime",
            details={"job_type": created.job_type},
        )
        return await self.get_job(job_id=created.id)

    async def get_job(self, *, job_id: uuid.UUID) -> RuntimeJob:
        job = await self._repository.get_by_id(job_id)
        if job is None:
            raise KeyError(str(job_id))
        return self._mapper.to_job(job)

    async def get_job_for_workspace(self, *, job_id: uuid.UUID, workspace_id: uuid.UUID) -> RuntimeJob:
        job = await self._repository.get_by_id_for_workspace(
            job_id=job_id,
            workspace_id=workspace_id,
        )
        if job is None:
            raise KeyError(str(job_id))
        return self._mapper.to_job(job)

    async def list_jobs(
        self,
        *,
        workspace_id: uuid.UUID,
        job_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[RuntimeJob]:
        return [
            self._mapper.to_job(item)
            for item in await self._repository.list_for_workspace(
                workspace_id=workspace_id,
                job_type=job_type,
                status=status,
                limit=limit,
            )
        ]

    async def claim_next(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        job_types: set[str] | None = None,
        queue_name: str | None = None,
    ) -> RuntimeJob | None:
        claimed = await self._repository.claim_next(
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            job_types=job_types,
            queue_name=queue_name,
        )
        if claimed is None:
            return None
        job = self._mapper.to_job(claimed)
        await self.append_event(
            job_id=job.id,
            task_id=None,
            event_type="job.started",
            status=RuntimeJobStatus.running.value,
            stage="running",
            message="Job execution started.",
            visibility="public",
            terminal=False,
            source="runtime",
            details={"attempt": job.attempt, "worker_id": worker_id},
        )
        return await self.get_job(job_id=job.id)

    async def heartbeat_job(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        heartbeat = getattr(self._repository, "heartbeat_job", None)
        if heartbeat is None:
            return False
        return bool(
            await heartbeat(
                job_id=job_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
            )
        )

    async def start_job(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str = "inline",
        event_type: str = "job.started",
        event_status: str = RuntimeJobStatus.running.value,
        stage: str = "running",
        message: str = "Job execution started.",
        visibility: str = "public",
        event_details: dict[str, Any] | None = None,
    ) -> RuntimeJob:
        job = await self._repository.get_by_id(job_id)
        if job is None:
            raise KeyError(str(job_id))
        now = datetime.now(timezone.utc)
        if self._status_value(getattr(job, "status", "")) != RuntimeJobStatus.running.value:
            job.status = RuntimeJobStatus.running.value
            job.attempt = int(getattr(job, "attempt", 0) or 0) + 1
            job.started_at = getattr(job, "started_at", None) or now
        job.lock_owner = worker_id
        job.heartbeat_at = now
        job.updated_at = now
        await self._repository.save_job(job)
        await self.append_event(
            job_id=job_id,
            task_id=None,
            event_type=event_type,
            status=event_status,
            stage=stage,
            message=message,
            visibility=visibility,
            terminal=False,
            source="runtime",
            details=dict(
                event_details
                or {"attempt": int(getattr(job, "attempt", 0) or 0), "worker_id": worker_id}
            ),
        )
        return await self.get_job(job_id=job_id)

    async def complete_job(
        self,
        *,
        job_id: uuid.UUID,
        result: dict[str, Any] | None = None,
        message: str = "Job completed.",
        event_type: str = "job.succeeded",
        event_status: str = RuntimeJobStatus.succeeded.value,
        stage: str = "completed",
        visibility: str = "public",
        event_details: dict[str, Any] | None = None,
    ) -> RuntimeJob:
        job = await self._repository.get_by_id(job_id)
        if job is None:
            raise KeyError(str(job_id))
        now = datetime.now(timezone.utc)
        job.status = RuntimeJobStatus.succeeded.value
        job.result = dict(result or {})
        job.error = None
        job.status_message = message
        job.completed_at = now
        job.lock_owner = None
        job.locked_until = None
        job.updated_at = now
        await self._repository.save_job(job)
        await self.append_event(
            job_id=job_id,
            task_id=None,
            event_type=event_type,
            status=event_status,
            stage=stage,
            message=message,
            visibility=visibility,
            terminal=True,
            source="runtime",
            details=dict(event_details or {"result": dict(result or {})}),
        )
        return await self.get_job(job_id=job_id)

    async def fail_job(
        self,
        *,
        job_id: uuid.UUID,
        exc: BaseException | None = None,
        error: dict[str, Any] | None = None,
        message: str | None = None,
        event_type: str = "job.failed",
        event_status: str = RuntimeJobStatus.failed.value,
        stage: str = "failed",
        visibility: str = "public",
        event_details: dict[str, Any] | None = None,
    ) -> RuntimeJob:
        job = await self._repository.get_by_id(job_id)
        if job is None:
            raise KeyError(str(job_id))
        error_payload = dict(error or self._error_payload(exc))
        failure_message = message or str(error_payload.get("message") or "Job failed.")
        now = datetime.now(timezone.utc)
        job.status = RuntimeJobStatus.failed.value
        job.error = error_payload
        job.status_message = failure_message
        job.failed_at = now
        job.lock_owner = None
        job.locked_until = None
        job.updated_at = now
        await self._repository.save_job(job)
        await self.append_event(
            job_id=job_id,
            task_id=None,
            event_type=event_type,
            status=event_status,
            stage=stage,
            message=failure_message,
            visibility=visibility,
            terminal=True,
            source="runtime",
            details=dict(event_details or {"error": error_payload}),
        )
        return await self.get_job(job_id=job_id)

    async def cancel_job(self, *, job_id: uuid.UUID, reason: str | None = None) -> RuntimeJob:
        job = await self._repository.get_by_id(job_id)
        if job is None:
            raise KeyError(str(job_id))
        if self._status_value(job.status) in {
            RuntimeJobStatus.succeeded.value,
            RuntimeJobStatus.failed.value,
            RuntimeJobStatus.cancelled.value,
        }:
            return self._mapper.to_job(job)
        now = datetime.now(timezone.utc)
        job.status = RuntimeJobStatus.cancelled.value
        job.status_message = str(reason or "").strip() or "Job cancelled."
        job.cancelled_at = now
        job.lock_owner = None
        job.locked_until = None
        job.updated_at = now
        await self._repository.save_job(job)
        await self.append_event(
            job_id=job_id,
            task_id=None,
            event_type="job.cancelled",
            status=RuntimeJobStatus.cancelled.value,
            stage="cancelled",
            message=job.status_message,
            visibility="public",
            terminal=True,
            source="runtime",
            details={"reason": reason},
        )
        return await self.get_job(job_id=job_id)

    async def append_event(
        self,
        *,
        job_id: uuid.UUID,
        task_id: uuid.UUID | None,
        event_type: str,
        status: str,
        stage: str,
        message: str,
        visibility: str = "internal",
        terminal: bool = False,
        source: str | None = None,
        raw_event_type: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RuntimeJobEvent:
        event = await self._repository.append_event(
            job_id=job_id,
            task_id=task_id,
            event_type=event_type,
            status=status,
            stage=stage,
            message=message,
            visibility=visibility,
            terminal=terminal,
            source=source,
            raw_event_type=raw_event_type,
            details=details,
        )
        await self._event_stream.notify()
        return self._mapper.to_event(event)

    async def upsert_task(
        self,
        *,
        job_id: uuid.UUID,
        task_key: str,
        task_type: str,
        status: str,
        attempt: int | None = None,
        max_attempts: int | None = None,
        resume_policy: str | None = None,
        reuse_policy: str | None = None,
        input: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> RuntimeJobTask:
        task = await self._repository.upsert_task(
            job_id=job_id,
            task_key=task_key,
            task_type=task_type,
            status=status,
            attempt=attempt,
            max_attempts=max_attempts,
            resume_policy=resume_policy,
            reuse_policy=reuse_policy,
            input=input,
            state=state,
            result=result,
            error=error,
            diagnostics=diagnostics,
        )
        return self._mapper.to_task(task)

    async def add_artifact(
        self,
        *,
        job_id: uuid.UUID,
        task_id: uuid.UUID | None,
        artifact_key: str,
        artifact_type: str,
        title: str | None,
        storage_kind: str,
        storage_uri: str | None,
        data: Any | None,
        schema: dict[str, Any] | None,
        formatting: dict[str, Any] | None,
        metadata: dict[str, Any] | None,
    ) -> RuntimeJobArtifact:
        artifact = await self._repository.add_artifact(
            job_id=job_id,
            task_id=task_id,
            artifact_key=artifact_key,
            artifact_type=artifact_type,
            title=title,
            storage_kind=storage_kind,
            storage_uri=storage_uri,
            data=data,
            schema=schema,
            formatting=formatting,
            metadata=metadata,
        )
        return self._mapper.to_artifact(artifact)

    async def stream_events(
        self,
        *,
        job_id: uuid.UUID,
        after_sequence: int = 0,
        heartbeat_interval: float = 10.0,
    ) -> AsyncIterator[RuntimeJobStreamEvent | None]:
        return self._event_stream.stream(
            job_id=job_id,
            after_sequence=after_sequence,
            heartbeat_interval=heartbeat_interval,
        )

    def _error_payload(self, exc: BaseException | None) -> dict[str, Any]:
        if exc is None:
            return {"message": "Job failed."}
        return {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        }

    def _status_value(self, status: Any) -> str:
        return str(getattr(status, "value", status) or "").strip()
