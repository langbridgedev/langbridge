from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from langbridge.runtime.models import RuntimeJob, RuntimeJobArtifact, RuntimeJobEvent, RuntimeJobTask


@dataclass(slots=True)
class JobExecutionContext:
    job: RuntimeJob
    worker_id: str
    _service: Any

    async def emit(
        self,
        *,
        event_type: str,
        message: str,
        status: str | None = None,
        stage: str | None = None,
        task_id: uuid.UUID | None = None,
        visibility: str = "internal",
        terminal: bool = False,
        source: str | None = None,
        raw_event_type: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RuntimeJobEvent:
        event = await self._service.append_event(
            job_id=self.job.id,
            task_id=task_id,
            event_type=event_type,
            status=status or str(self.job.status),
            stage=stage or event_type,
            message=message,
            visibility=visibility,
            terminal=terminal,
            source=source,
            raw_event_type=raw_event_type,
            details=details,
        )
        self.job.last_sequence = max(int(self.job.last_sequence or 0), int(event.sequence or 0))
        if terminal:
            self.job.terminal_sequence = event.sequence
        return event

    async def upsert_task(
        self,
        *,
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
        return await self._service.upsert_task(
            job_id=self.job.id,
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

    async def add_artifact(
        self,
        *,
        artifact_key: str,
        artifact_type: str,
        title: str | None = None,
        task_id: uuid.UUID | None = None,
        storage_kind: str = "inline",
        storage_uri: str | None = None,
        data: Any | None = None,
        schema: dict[str, Any] | None = None,
        formatting: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeJobArtifact:
        return await self._service.add_artifact(
            job_id=self.job.id,
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
