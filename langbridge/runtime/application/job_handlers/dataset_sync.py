import uuid
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.models.state import ConnectorSyncMode
from langbridge.runtime.services.jobs.context import JobExecutionContext
from langbridge.runtime.services.jobs.handlers import RuntimeJobHandler

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


DATASET_SYNC_JOB_TYPE = "dataset.sync"


@dataclass(slots=True, frozen=True)
class _DatasetSyncJobPayload:
    dataset_ref: str
    sync_mode: ConnectorSyncMode
    force_full_refresh: bool


class DatasetSyncJobHandler(RuntimeJobHandler):
    def __init__(self, *, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    @property
    def job_type(self) -> str:
        return DATASET_SYNC_JOB_TYPE

    async def handle(self, context: JobExecutionContext) -> dict[str, Any]:
        payload = self._parse_payload(context.job.payload)
        job_host = self._host.with_context(self._job_runtime_context(context))
        task = await context.upsert_task(
            task_key="dataset_sync",
            task_type=self.job_type,
            status="running",
            attempt=int(context.job.attempt or 1),
            max_attempts=int(context.job.max_attempts or 1),
            input=self._task_input(payload),
            state={"stage": "syncing"},
        )
        await context.emit(
            event_type="dataset.sync.started",
            message=f"Dataset sync started for '{payload.dataset_ref}'.",
            status="running",
            stage="syncing",
            task_id=task.id,
            visibility="public",
            source="dataset-sync",
            details=self._task_input(payload),
        )

        try:
            raw_result = await job_host.execute_dataset_sync(
                dataset_ref=payload.dataset_ref,
                sync_mode=payload.sync_mode.value,
                force_full_refresh=payload.force_full_refresh,
            )
        except Exception as exc:
            error = {"type": type(exc).__name__, "message": str(exc)}
            await context.upsert_task(
                task_key="dataset_sync",
                task_type=self.job_type,
                status="failed",
                attempt=int(context.job.attempt or 1),
                max_attempts=int(context.job.max_attempts or 1),
                input=self._task_input(payload),
                state={"stage": "failed"},
                error=error,
            )
            await context.emit(
                event_type="dataset.sync.failed",
                message=str(exc) or "Dataset sync failed.",
                status="failed",
                stage="failed",
                task_id=task.id,
                visibility="public",
                source="dataset-sync",
                details={"error": error, **self._task_input(payload)},
            )
            raise
        result = self._json_safe_mapping(raw_result)

        await context.add_artifact(
            artifact_key="sync_result",
            artifact_type="json",
            title="Dataset sync result",
            task_id=task.id,
            data=result,
            metadata={"dataset_ref": payload.dataset_ref},
        )
        await context.upsert_task(
            task_key="dataset_sync",
            task_type=self.job_type,
            status="succeeded",
            attempt=int(context.job.attempt or 1),
            max_attempts=int(context.job.max_attempts or 1),
            input=self._task_input(payload),
            state={"stage": "completed"},
            result=result,
        )
        await context.emit(
            event_type="dataset.sync.succeeded",
            message=result.get("summary") or f"Dataset sync completed for '{payload.dataset_ref}'.",
            status="succeeded",
            stage="completed",
            task_id=task.id,
            visibility="public",
            source="dataset-sync",
            details={
                "dataset_id": str(result.get("dataset_id")) if result.get("dataset_id") else None,
                "dataset_name": result.get("dataset_name"),
                "resources": result.get("resources") or [],
            },
        )
        return result

    def _parse_payload(self, payload: dict[str, Any]) -> _DatasetSyncJobPayload:
        dataset_ref = str(payload.get("dataset_ref") or "").strip()
        if not dataset_ref:
            dataset_id = str(payload.get("dataset_id") or "").strip()
            dataset_ref = dataset_id
        if not dataset_ref:
            raise ValueError("Dataset sync job payload requires dataset_ref or dataset_id.")
        return _DatasetSyncJobPayload(
            dataset_ref=dataset_ref,
            sync_mode=self._parse_sync_mode(payload.get("sync_mode")),
            force_full_refresh=bool(payload.get("force_full_refresh")),
        )

    def _parse_sync_mode(self, value: Any) -> ConnectorSyncMode:
        normalized = str(
            getattr(value, "value", value) or ConnectorSyncMode.INCREMENTAL.value
        ).strip().upper()
        return ConnectorSyncMode(normalized)

    def _job_runtime_context(self, context: JobExecutionContext) -> RuntimeContext:
        return RuntimeContext.build(
            workspace_id=context.job.workspace_id,
            actor_id=context.job.actor_id,
            roles=self._host.context.roles,
            request_id=f"job:{context.job.id}",
        )

    def _task_input(self, payload: _DatasetSyncJobPayload) -> dict[str, Any]:
        return {
            "dataset_ref": payload.dataset_ref,
            "sync_mode": payload.sync_mode.value,
            "force_full_refresh": payload.force_full_refresh,
        }

    def _json_safe_mapping(self, value: dict[str, Any]) -> dict[str, Any]:
        return {
            str(key): self._json_safe_value(item)
            for key, item in dict(value or {}).items()
        }

    def _json_safe_value(self, value: Any) -> Any:
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {
                str(key): self._json_safe_value(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._json_safe_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._json_safe_value(item) for item in value]
        return value
