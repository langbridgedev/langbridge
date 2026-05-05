import uuid
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.models import RuntimeJobStatus, SqlQueryRequest
from langbridge.runtime.services.jobs.context import JobExecutionContext
from langbridge.runtime.services.jobs.handlers import RuntimeJobHandler

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


SQL_QUERY_JOB_TYPE = "sql.query"


@dataclass(slots=True, frozen=True)
class _SqlQueryJobPayload:
    request: SqlQueryRequest


class SqlQueryJobHandler(RuntimeJobHandler):
    def __init__(self, *, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    @property
    def job_type(self) -> str:
        return SQL_QUERY_JOB_TYPE

    async def handle(self, context: JobExecutionContext) -> dict[str, Any]:
        payload = self._parse_payload(context.job.payload)
        job_host = self._host.with_context(self._job_runtime_context(context))
        task = await context.upsert_task(
            task_key="sql_query",
            task_type=self.job_type,
            status=RuntimeJobStatus.running.value,
            attempt=int(context.job.attempt or 1),
            max_attempts=int(context.job.max_attempts or 1),
            input=self._task_input(payload),
            state={"stage": "executing"},
        )
        await context.emit(
            event_type="sql.query.started",
            message="SQL query execution started.",
            status=RuntimeJobStatus.running.value,
            stage="executing",
            task_id=task.id,
            visibility="public",
            source="sql-query",
            details=self._task_input(payload),
        )

        try:
            raw_result = await job_host.execute_sql_query(request=payload.request)
        except Exception as exc:
            error = {"type": type(exc).__name__, "message": str(exc)}
            await context.upsert_task(
                task_key="sql_query",
                task_type=self.job_type,
                status=RuntimeJobStatus.failed.value,
                attempt=int(context.job.attempt or 1),
                max_attempts=int(context.job.max_attempts or 1),
                input=self._task_input(payload),
                state={"stage": "failed"},
                error=error,
            )
            await context.emit(
                event_type="sql.query.failed",
                message=str(exc) or "SQL query execution failed.",
                status=RuntimeJobStatus.failed.value,
                stage="failed",
                task_id=task.id,
                visibility="public",
                source="sql-query",
                details={"error": error, **self._task_input(payload)},
            )
            raise

        result = self._json_safe_mapping(raw_result)
        await self._record_artifacts(context=context, task_id=task.id, result=result)
        await context.upsert_task(
            task_key="sql_query",
            task_type=self.job_type,
            status=RuntimeJobStatus.succeeded.value,
            attempt=int(context.job.attempt or 1),
            max_attempts=int(context.job.max_attempts or 1),
            input=self._task_input(payload),
            state={"stage": "completed"},
            result=result,
            diagnostics=self._diagnostics(result),
        )
        await context.emit(
            event_type="sql.query.succeeded",
            message=self._success_message(result),
            status=RuntimeJobStatus.succeeded.value,
            stage="completed",
            task_id=task.id,
            visibility="public",
            source="sql-query",
            details=self._diagnostics(result),
        )
        return result

    def _parse_payload(self, payload: dict[str, Any]) -> _SqlQueryJobPayload:
        raw_request = payload.get("request") if isinstance(payload.get("request"), dict) else payload
        return _SqlQueryJobPayload(request=SqlQueryRequest.model_validate(raw_request))

    def _job_runtime_context(self, context: JobExecutionContext) -> RuntimeContext:
        return RuntimeContext.build(
            workspace_id=context.job.workspace_id,
            actor_id=context.job.actor_id,
            roles=self._host.context.roles,
            request_id=f"job:{context.job.id}",
        )

    def _task_input(self, payload: _SqlQueryJobPayload) -> dict[str, Any]:
        request = payload.request
        return {
            "query_scope": request.query_scope.value,
            "query": request.query,
            "connection_id": str(request.connection_id) if request.connection_id else None,
            "connection_name": request.connection_name,
            "selected_datasets": [str(dataset_id) for dataset_id in request.selected_datasets],
            "query_dialect": request.query_dialect,
            "requested_limit": request.requested_limit,
            "requested_timeout_seconds": request.requested_timeout_seconds,
            "explain": request.explain,
        }

    async def _record_artifacts(
        self,
        *,
        context: JobExecutionContext,
        task_id: uuid.UUID,
        result: dict[str, Any],
    ) -> None:
        columns = list(result.get("columns") or [])
        rows = list(result.get("rows") or [])
        await context.add_artifact(
            artifact_key="result_table",
            artifact_type="table",
            title="SQL query result",
            task_id=task_id,
            data={
                "columns": columns,
                "rows": rows,
                "row_count_preview": int(result.get("row_count_preview") or len(rows)),
            },
            schema={"columns": columns},
            metadata={
                "query_scope": result.get("query_scope"),
                "row_count_preview": int(result.get("row_count_preview") or len(rows)),
            },
        )
        await context.add_artifact(
            artifact_key="sql_diagnostics",
            artifact_type="json",
            title="SQL diagnostics",
            task_id=task_id,
            data=self._diagnostics(result),
            metadata={"query_scope": result.get("query_scope")},
        )

    def _diagnostics(self, result: dict[str, Any]) -> dict[str, Any]:
        return {
            "query_scope": result.get("query_scope"),
            "sql_job_id": result.get("sql_job_id"),
            "generated_sql": result.get("generated_sql"),
            "query": result.get("query"),
            "row_count_preview": result.get("row_count_preview"),
            "total_rows_estimate": result.get("total_rows_estimate"),
            "bytes_scanned": result.get("bytes_scanned"),
            "duration_ms": result.get("duration_ms"),
            "redaction_applied": bool(result.get("redaction_applied")),
            "federation_diagnostics": result.get("federation_diagnostics"),
        }

    def _success_message(self, result: dict[str, Any]) -> str:
        row_count = int(result.get("row_count_preview") or 0)
        query_scope = str(result.get("query_scope") or "sql")
        return f"SQL query completed through {query_scope} scope with {row_count} preview rows."

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
