from __future__ import annotations

from typing import Any

from langbridge.runtime.models import (
    RuntimeJob,
    RuntimeJobArtifact,
    RuntimeJobEvent,
    RuntimeJobTask,
)


class RuntimeJobMapper:
    def to_job(self, value: Any) -> RuntimeJob:
        if isinstance(value, RuntimeJob):
            return value
        return RuntimeJob(
            id=value.id,
            workspace_id=value.workspace_id,
            job_type=value.job_type,
            status=value.status,
            priority=value.priority,
            actor_id=value.actor_id,
            subject_type=value.subject_type,
            subject_id=value.subject_id,
            queue_name=value.queue_name,
            required_capabilities=list(value.required_capabilities or []),
            runtime_pool_id=value.runtime_pool_id,
            affinity_key=value.affinity_key,
            concurrency_key=value.concurrency_key,
            idempotency_key=value.idempotency_key,
            payload=dict(value.payload or {}),
            result=value.result,
            error=value.error,
            progress=self._progress_payload(value.progress),
            status_message=value.status_message,
            last_sequence=int(value.last_sequence or 0),
            terminal_sequence=value.terminal_sequence,
            attempt=int(value.attempt or 0),
            max_attempts=int(value.max_attempts or 1),
            lock_owner=value.lock_owner,
            locked_until=value.locked_until,
            heartbeat_at=value.heartbeat_at,
            scheduled_at=value.scheduled_at,
            queued_at=value.queued_at,
            started_at=value.started_at,
            completed_at=value.completed_at,
            failed_at=value.failed_at,
            cancelled_at=value.cancelled_at,
            created_at=value.created_at,
            updated_at=value.updated_at,
            tasks=[
                self.to_task(item)
                for item in list(getattr(value, "job_tasks", None) or getattr(value, "tasks", []) or [])
            ],
            events=[
                self.to_event(item)
                for item in list(getattr(value, "job_events", None) or getattr(value, "events", []) or [])
            ],
            artifacts=[
                self.to_artifact(item)
                for item in list(getattr(value, "job_artifacts", None) or getattr(value, "artifacts", []) or [])
            ],
        )

    def to_task(self, value: Any) -> RuntimeJobTask:
        if isinstance(value, RuntimeJobTask):
            return value
        return RuntimeJobTask(
            id=value.id,
            job_id=value.job_id,
            task_key=value.task_key,
            task_type=value.task_type,
            status=value.status,
            attempt=int(value.attempt or 0),
            max_attempts=int(value.max_attempts or 1),
            resume_policy=value.resume_policy,
            reuse_policy=value.reuse_policy,
            input=dict(value.input or {}),
            state=dict(value.state or {}),
            result=value.result,
            error=value.error,
            diagnostics=dict(value.diagnostics or {}),
            started_sequence=value.started_sequence,
            last_sequence=value.last_sequence,
            terminal_sequence=value.terminal_sequence,
            started_at=value.started_at,
            completed_at=value.completed_at,
            failed_at=value.failed_at,
            updated_at=value.updated_at,
        )

    def _progress_payload(self, value: Any) -> dict[str, Any] | int:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, int):
            return value
        return {}

    def to_event(self, value: Any) -> RuntimeJobEvent:
        if isinstance(value, RuntimeJobEvent):
            return value
        return RuntimeJobEvent(
            id=value.id,
            job_id=value.job_id,
            task_id=value.task_id,
            sequence=int(value.sequence or 0),
            event_type=value.event_type,
            status=value.status,
            stage=value.stage,
            message=value.message,
            visibility=value.visibility,
            terminal=bool(value.terminal),
            source=value.source,
            raw_event_type=value.raw_event_type,
            details=dict(value.details or {}),
            created_at=value.created_at,
        )

    def to_artifact(self, value: Any) -> RuntimeJobArtifact:
        if isinstance(value, RuntimeJobArtifact):
            return value
        return RuntimeJobArtifact(
            id=value.id,
            job_id=value.job_id,
            task_id=value.task_id,
            artifact_key=value.artifact_key,
            artifact_type=value.artifact_type,
            title=value.title,
            storage_kind=value.storage_kind,
            storage_uri=value.storage_uri,
            data=value.data,
            artifact_schema=dict(getattr(value, "schema", None) or getattr(value, "artifact_schema", None) or {}),
            formatting=dict(value.formatting or {}),
            metadata=dict(getattr(value, "metadata_json", None) or getattr(value, "metadata", None) or {}),
            created_at=value.created_at,
            updated_at=value.updated_at,
        )
