from __future__ import annotations

import asyncio
import inspect
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Protocol

import httpx
from pydantic import Field

from langbridge.packages.contracts.base import _Base
from langbridge.packages.contracts.datasets import (
    DatasetListResponse,
    DatasetPreviewColumn,
    DatasetPreviewRequest,
)
from langbridge.packages.contracts.jobs.agent_job import (
    AgentJobStateResponse,
    CreateAgentJobRequest,
    JobEventResponse,
)
from langbridge.packages.contracts.jobs.dataset_job import (
    CreateDatasetPreviewJobRequest,
)
from langbridge.packages.contracts.jobs.sql_job import (
    CreateSqlJobRequest,
)
from langbridge.packages.contracts.jobs.type import JobType
from langbridge.packages.contracts.sql import (
    SqlColumnMetadata,
    SqlDialect,
    SqlExecuteRequest,
    SqlJobResponse,
    SqlJobResultsResponse,
    SqlSelectedDataset,
    SqlWorkbenchMode,
)
from langbridge.packages.contracts.threads import (
    ThreadChatRequest,
    ThreadCreateRequest,
    ThreadResponse,
)
from langbridge.packages.common.langbridge_common.db.threads import (
    Role,
    Thread,
    ThreadMessage,
    ThreadState,
)


_TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}


class _AwaitableModel(_Base):
    def __await__(self):
        async def _resolve() -> "_AwaitableModel":
            return self

        return _resolve().__await__()


class DatasetSummary(_AwaitableModel):
    id: uuid.UUID | None = None
    name: str
    label: str | None = None
    description: str | None = None
    connector: str | None = None
    semantic_model: str | None = None


class DatasetListResult(_AwaitableModel):
    items: list[DatasetSummary] = Field(default_factory=list)
    total: int = 0


class DatasetQueryResult(_AwaitableModel):
    dataset_id: uuid.UUID | None = None
    dataset_name: str | None = None
    status: str
    columns: list[DatasetPreviewColumn] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count_preview: int = 0
    effective_limit: int | None = None
    redaction_applied: bool = False
    duration_ms: int | None = None
    bytes_scanned: int | None = None
    generated_sql: str | None = None
    error: str | None = None
    job_id: uuid.UUID | None = None


class SqlQueryResult(_AwaitableModel):
    sql_job_id: uuid.UUID | None = None
    status: str
    columns: list[SqlColumnMetadata] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count_preview: int = 0
    total_rows_estimate: int | None = None
    bytes_scanned: int | None = None
    duration_ms: int | None = None
    redaction_applied: bool = False
    error: dict[str, Any] | None = None
    query: str | None = None
    generated_sql: str | None = None


class AgentAskResult(_AwaitableModel):
    thread_id: uuid.UUID | None = None
    status: str
    job_id: uuid.UUID | None = None
    summary: str | None = None
    result: Any | None = None
    visualization: Any | None = None
    error: dict[str, Any] | None = None
    events: list[JobEventResponse] = Field(default_factory=list)

    @property
    def text(self) -> str | None:
        if self.summary:
            return self.summary
        if isinstance(self.result, dict):
            value = self.result.get("text")
            return str(value) if value is not None else None
        return None


class _SdkAdapter(Protocol):
    def list_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        search: str | None,
    ) -> DatasetListResult: ...

    def query_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        limit: int | None,
        filters: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
        user_context: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult: ...

    def query_dataset_semantic(
        self,
        *,
        dataset_name: str,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        metrics: list[str] | None,
        dimensions: list[str] | None,
        filters: list[dict[str, Any]] | None,
        time_dimensions: list[dict[str, Any]] | None,
        limit: int | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult: ...

    def query_sql(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        query: str,
        connection_id: uuid.UUID | None,
        connection_name: str | None,
        selected_datasets: list[SqlSelectedDataset] | None,
        query_dialect: SqlDialect | str,
        params: dict[str, Any] | None,
        requested_limit: int | None,
        requested_timeout_seconds: int | None,
        explain: bool,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SqlQueryResult: ...

    def ask_agent(
        self,
        *,
        organization_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        message: str,
        agent_id: uuid.UUID | None,
        agent_name: str | None,
        thread_id: uuid.UUID | None,
        title: str | None,
        metadata_json: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> AgentAskResult: ...

    def close(self) -> None: ...


def _run_awaitable(awaitable: Any) -> Any:
    if not inspect.isawaitable(awaitable):
        return awaitable
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def _target() -> None:
        try:
            result["value"] = asyncio.run(awaitable)
        except BaseException as exc:  # pragma: no cover - defensive
            error["exc"] = exc

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join()
    if "exc" in error:
        raise error["exc"]
    return result.get("value")


def _call_maybe_async(func: Any, *args: Any, **kwargs: Any) -> Any:
    return _run_awaitable(func(*args, **kwargs))


def _coalesce_uuid(value: uuid.UUID | None, fallback: uuid.UUID | None, field_name: str) -> uuid.UUID:
    resolved = value or fallback
    if resolved is None:
        raise ValueError(f"{field_name} is required.")
    return resolved


def _normalize_selected_datasets(
    selected_datasets: list[SqlSelectedDataset | dict[str, Any]] | None,
) -> list[SqlSelectedDataset]:
    normalized: list[SqlSelectedDataset] = []
    for item in selected_datasets or []:
        if isinstance(item, SqlSelectedDataset):
            normalized.append(item)
        else:
            normalized.append(SqlSelectedDataset.model_validate(item))
    return normalized


def _coerce_sql_dialect(value: SqlDialect | str) -> SqlDialect:
    if isinstance(value, SqlDialect):
        return value
    return SqlDialect(str(value).strip().lower())


def _wait_for_terminal(
    fetch_status,
    *,
    timeout_s: float,
    poll_interval_s: float,
):
    deadline = time.monotonic() + timeout_s
    response = fetch_status()
    while _status_value(getattr(response, "status", "")) not in _TERMINAL_STATUSES:
        if time.monotonic() >= deadline:
            raise TimeoutError("Timed out waiting for Langbridge job completion.")
        time.sleep(max(poll_interval_s, 0.05))
        response = fetch_status()
    return response


def _status_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw).strip().lower()


class RemoteApiAdapter:
    def __init__(
        self,
        *,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(base_url=self._base_url, timeout=timeout)
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def list_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        search: str | None,
    ) -> DatasetListResult:
        payload = DatasetListResponse.model_validate(
            self._request(
                "GET",
                "/api/v1/datasets",
                params={
                    "workspace_id": str(workspace_id),
                    **({"project_id": str(project_id)} if project_id else {}),
                    **({"search": search} if search else {}),
                },
            )
        )
        return DatasetListResult(
            items=[
                DatasetSummary(
                    id=item.id,
                    name=item.name,
                    label=item.name,
                    description=item.description,
                )
                for item in payload.items
            ],
            total=payload.total,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any:
        response = self._client.request(
            method,
            path,
            params=params,
            json=json,
            headers=self._headers,
        )
        response.raise_for_status()
        if not response.content:
            return None
        return response.json()

    def query_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        limit: int | None,
        filters: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
        user_context: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult:
        payload = DatasetPreviewRequest(
            workspace_id=workspace_id,
            project_id=project_id,
            limit=limit,
            filters=filters or {},
            sort=sort or [],
            user_context=user_context or {},
        ).model_dump(mode="json")
        initial = DatasetQueryResult.model_validate(
            self._request("POST", f"/api/v1/datasets/{dataset_id}/preview", json=payload)
        )
        if initial.job_id is None:
            return initial

        def _fetch() -> DatasetQueryResult:
            return DatasetQueryResult.model_validate(
                self._request(
                    "GET",
                    f"/api/v1/datasets/{dataset_id}/preview/jobs/{initial.job_id}",
                    params={"workspace_id": str(workspace_id)},
                )
            )

        return _wait_for_terminal(_fetch, timeout_s=timeout_s, poll_interval_s=poll_interval_s)

    def query_dataset_semantic(
        self,
        *,
        dataset_name: str,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        metrics: list[str] | None,
        dimensions: list[str] | None,
        filters: list[dict[str, Any]] | None,
        time_dimensions: list[dict[str, Any]] | None,
        limit: int | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult:
        raise NotImplementedError(
            "Semantic-style dataset queries are currently supported by the local runtime adapter only."
        )

    def query_sql(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        query: str,
        connection_id: uuid.UUID | None,
        connection_name: str | None,
        selected_datasets: list[SqlSelectedDataset] | None,
        query_dialect: SqlDialect | str,
        params: dict[str, Any] | None,
        requested_limit: int | None,
        requested_timeout_seconds: int | None,
        explain: bool,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SqlQueryResult:
        normalized_datasets = _normalize_selected_datasets(selected_datasets)
        workbench_mode = (
            SqlWorkbenchMode.dataset if normalized_datasets else SqlWorkbenchMode.direct_sql
        )
        execute_request = SqlExecuteRequest(
            workspace_id=workspace_id,
            project_id=project_id,
            workbench_mode=workbench_mode,
            connection_id=connection_id,
            query=query,
            query_dialect=_coerce_sql_dialect(query_dialect),
            params=params or {},
            requested_limit=requested_limit,
            requested_timeout_seconds=requested_timeout_seconds,
            explain=explain,
            selected_datasets=normalized_datasets,
            federated_datasets=normalized_datasets,
        )
        initial = self._request(
            "POST",
            "/api/v1/sql/execute",
            json=execute_request.model_dump(mode="json"),
        )
        sql_job_id = uuid.UUID(str(initial["sql_job_id"]))

        def _fetch_job() -> SqlJobResponse:
            return SqlJobResponse.model_validate(
                self._request(
                    "GET",
                    f"/api/v1/sql/jobs/{sql_job_id}",
                    params={"workspace_id": str(workspace_id)},
                )
            )

        job = _wait_for_terminal(_fetch_job, timeout_s=timeout_s, poll_interval_s=poll_interval_s)
        if job.status != "succeeded":
            return SqlQueryResult(
                sql_job_id=job.id,
                status=job.status,
                bytes_scanned=job.bytes_scanned,
                duration_ms=job.duration_ms,
                redaction_applied=job.redaction_applied,
                error=job.error,
                query=job.query,
            )

        results = SqlJobResultsResponse.model_validate(
            self._request(
                "GET",
                f"/api/v1/sql/jobs/{sql_job_id}/results",
                params={"workspace_id": str(workspace_id)},
            )
        )
        return SqlQueryResult(
            sql_job_id=job.id,
            status=results.status,
            columns=results.columns,
            rows=results.rows,
            row_count_preview=results.row_count_preview,
            total_rows_estimate=results.total_rows_estimate,
            bytes_scanned=job.bytes_scanned,
            duration_ms=job.duration_ms,
            redaction_applied=job.redaction_applied,
            error=job.error,
            query=job.query,
        )

    def ask_agent(
        self,
        *,
        organization_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        message: str,
        agent_id: uuid.UUID | None,
        agent_name: str | None,
        thread_id: uuid.UUID | None,
        title: str | None,
        metadata_json: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> AgentAskResult:
        if agent_id is None:
            raise ValueError("agent_id is required for remote API agent execution.")
        resolved_thread_id = thread_id
        if resolved_thread_id is None:
            thread = ThreadResponse.model_validate(
                self._request(
                    "POST",
                    f"/api/v1/thread/{organization_id}/",
                    json=ThreadCreateRequest(
                        project_id=project_id,
                        title=title,
                        metadata_json=metadata_json,
                    ).model_dump(mode="json"),
                )
            )
            if thread.id is None:
                raise ValueError("Thread creation did not return a thread id.")
            resolved_thread_id = thread.id

        chat = self._request(
            "POST",
            f"/api/v1/thread/{organization_id}/{resolved_thread_id}/chat",
            json=ThreadChatRequest(message=message, agent_id=agent_id).model_dump(mode="json"),
        )
        job_id = uuid.UUID(str(chat["job_id"]))

        def _fetch_job() -> AgentJobStateResponse:
            return AgentJobStateResponse.model_validate(
                self._request("GET", f"/api/v1/jobs/{organization_id}/{job_id}")
            )

        job = _wait_for_terminal(_fetch_job, timeout_s=timeout_s, poll_interval_s=poll_interval_s)
        final_response = job.final_response
        return AgentAskResult(
            thread_id=resolved_thread_id,
            status=job.status,
            job_id=job.id,
            summary=final_response.summary if final_response else None,
            result=final_response.result if final_response else None,
            visualization=final_response.visualization if final_response else None,
            error=job.error,
            events=job.events,
        )


class LocalRuntimeAdapter:
    def __init__(
        self,
        *,
        runtime_host: Any,
        thread_repository: Any | None = None,
        thread_message_repository: Any | None = None,
    ) -> None:
        self._runtime_host = runtime_host
        self._thread_repository = thread_repository
        self._thread_message_repository = thread_message_repository

    def close(self) -> None:
        return None

    def list_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        search: str | None,
    ) -> DatasetListResult:
        list_method = getattr(self._runtime_host, "list_datasets", None)
        if list_method is None:
            raise ValueError("Local runtime host does not expose list_datasets().")
        payload = _run_awaitable(list_method())
        items = [DatasetSummary.model_validate(item) for item in (payload or [])]
        if search:
            filtered = [
                item
                for item in items
                if search.lower() in item.name.lower()
                or search.lower() in str(item.label or "").lower()
            ]
            return DatasetListResult(items=filtered, total=len(filtered))
        return DatasetListResult(items=items, total=len(items))

    def query_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        limit: int | None,
        filters: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
        user_context: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult:
        resolved_user_id = _coalesce_uuid(
            user_id,
            getattr(getattr(self._runtime_host, "context", None), "user_id", None),
            "user_id",
        )
        request = CreateDatasetPreviewJobRequest(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            project_id=project_id,
            user_id=resolved_user_id,
            requested_limit=limit,
            enforced_limit=limit or 100,
            filters=filters or {},
            sort=sort or [],
            user_context=user_context or {},
            correlation_id=getattr(getattr(self._runtime_host, "context", None), "request_id", None),
        )
        try:
            payload = _run_awaitable(self._runtime_host.query_dataset(request=request))
        except Exception as exc:
            return DatasetQueryResult(
                dataset_id=dataset_id,
                status="failed",
                error=str(exc),
            )
        return DatasetQueryResult(
            dataset_id=dataset_id,
            dataset_name=payload.get("dataset_name"),
            status="succeeded",
            columns=[DatasetPreviewColumn.model_validate(item) for item in payload.get("columns", [])],
            rows=list(payload.get("rows", [])),
            row_count_preview=int(payload.get("row_count_preview") or 0),
            effective_limit=payload.get("effective_limit"),
            redaction_applied=bool(payload.get("redaction_applied")),
            duration_ms=payload.get("duration_ms"),
            bytes_scanned=payload.get("bytes_scanned"),
            generated_sql=payload.get("generated_sql"),
        )

    def query_dataset_semantic(
        self,
        *,
        dataset_name: str,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        metrics: list[str] | None,
        dimensions: list[str] | None,
        filters: list[dict[str, Any]] | None,
        time_dimensions: list[dict[str, Any]] | None,
        limit: int | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult:
        query_method = getattr(self._runtime_host, "query_dataset_by_name", None)
        if query_method is None:
            raise ValueError(
                "Semantic-style dataset queries require a config-backed local runtime host."
            )
        try:
            payload = _run_awaitable(
                query_method(
                    dataset_name=dataset_name,
                    metrics=metrics,
                    dimensions=dimensions,
                    filters=filters,
                    time_dimensions=time_dimensions,
                    limit=limit,
                    order=order,
                )
            )
        except Exception as exc:
            return DatasetQueryResult(
                dataset_name=dataset_name,
                status="failed",
                error=str(exc),
            )
        return DatasetQueryResult(
            dataset_id=payload.get("dataset_id"),
            dataset_name=payload.get("dataset_name") or dataset_name,
            status="succeeded",
            columns=[DatasetPreviewColumn.model_validate(item) for item in payload.get("columns", [])],
            rows=list(payload.get("rows", [])),
            row_count_preview=int(payload.get("row_count_preview") or 0),
            effective_limit=payload.get("effective_limit"),
            redaction_applied=bool(payload.get("redaction_applied")),
            duration_ms=payload.get("duration_ms"),
            bytes_scanned=payload.get("bytes_scanned"),
            generated_sql=payload.get("generated_sql"),
        )

    def query_sql(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        query: str,
        connection_id: uuid.UUID | None,
        connection_name: str | None,
        selected_datasets: list[SqlSelectedDataset] | None,
        query_dialect: SqlDialect | str,
        params: dict[str, Any] | None,
        requested_limit: int | None,
        requested_timeout_seconds: int | None,
        explain: bool,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SqlQueryResult:
        execute_sql_text = getattr(self._runtime_host, "execute_sql_text", None)
        if execute_sql_text is not None:
            try:
                payload = _run_awaitable(
                    execute_sql_text(
                        query=query,
                        connection_name=connection_name,
                        requested_limit=requested_limit,
                    )
                )
            except Exception as exc:
                return SqlQueryResult(
                    sql_job_id=uuid.uuid4(),
                    status="failed",
                    error={"message": str(exc)},
                    query=query,
                )
            return SqlQueryResult(
                sql_job_id=uuid.uuid4(),
                status="succeeded",
                columns=[SqlColumnMetadata.model_validate(item) for item in payload.get("columns", [])],
                rows=list(payload.get("rows", [])),
                row_count_preview=int(payload.get("row_count_preview") or 0),
                total_rows_estimate=payload.get("total_rows_estimate"),
                bytes_scanned=payload.get("bytes_scanned"),
                duration_ms=payload.get("duration_ms"),
                redaction_applied=bool(payload.get("redaction_applied")),
                query=query,
                generated_sql=payload.get("generated_sql"),
            )

        normalized_datasets = _normalize_selected_datasets(selected_datasets)
        resolved_user_id = _coalesce_uuid(
            user_id,
            getattr(getattr(self._runtime_host, "context", None), "user_id", None),
            "user_id",
        )
        workbench_mode = (
            SqlWorkbenchMode.dataset if normalized_datasets else SqlWorkbenchMode.direct_sql
        )
        sql_job_id = uuid.uuid4()
        request = CreateSqlJobRequest(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
            project_id=project_id,
            user_id=resolved_user_id,
            workbench_mode=workbench_mode,
            connection_id=connection_id,
            execution_mode=("federated" if normalized_datasets else "single"),
            query=query,
            query_dialect=_coerce_sql_dialect(query_dialect).value,
            params=params or {},
            requested_limit=requested_limit,
            requested_timeout_seconds=requested_timeout_seconds,
            enforced_limit=requested_limit or 100,
            enforced_timeout_seconds=requested_timeout_seconds or 30,
            allow_dml=False,
            allow_federation=bool(normalized_datasets),
            selected_datasets=normalized_datasets,
            federated_datasets=normalized_datasets,
            explain=explain,
            correlation_id=getattr(getattr(self._runtime_host, "context", None), "request_id", None),
        )
        try:
            payload = _run_awaitable(self._runtime_host.execute_sql(request=request))
        except Exception as exc:
            return SqlQueryResult(
                sql_job_id=sql_job_id,
                status="failed",
                error={"message": str(exc)},
                query=query,
            )
        return SqlQueryResult(
            sql_job_id=sql_job_id,
            status="succeeded",
            columns=[SqlColumnMetadata.model_validate(item) for item in payload.get("columns", [])],
            rows=list(payload.get("rows", [])),
            row_count_preview=int(payload.get("row_count_preview") or 0),
            total_rows_estimate=payload.get("total_rows_estimate"),
            bytes_scanned=payload.get("bytes_scanned"),
            duration_ms=payload.get("duration_ms"),
            redaction_applied=bool(payload.get("redaction_applied")),
            query=query,
            generated_sql=payload.get("generated_sql"),
        )

    def ask_agent(
        self,
        *,
        organization_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID | None,
        message: str,
        agent_id: uuid.UUID | None,
        agent_name: str | None,
        thread_id: uuid.UUID | None,
        title: str | None,
        metadata_json: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> AgentAskResult:
        ask_agent_method = getattr(self._runtime_host, "ask_agent", None)
        if ask_agent_method is not None:
            try:
                payload = _run_awaitable(
                    ask_agent_method(
                        prompt=message,
                        agent_name=agent_name,
                    )
                )
            except Exception as exc:
                return AgentAskResult(
                    status="failed",
                    error={"message": str(exc)},
                )
            return AgentAskResult(
                thread_id=thread_id,
                status="succeeded",
                summary=payload.get("summary"),
                result=payload.get("result"),
                visualization=payload.get("visualization"),
                error=payload.get("error"),
                events=[],
            )

        if self._thread_repository is None or self._thread_message_repository is None:
            raise ValueError(
                "Local agent execution requires thread repositories unless the runtime host exposes ask_agent()."
            )
        if agent_id is None:
            raise ValueError("agent_id is required for repository-backed local agent execution.")

        resolved_user_id = _coalesce_uuid(
            user_id,
            getattr(getattr(self._runtime_host, "context", None), "user_id", None),
            "user_id",
        )
        resolved_project_id = _coalesce_uuid(project_id, None, "project_id")

        thread = None
        if thread_id is not None:
            thread = _call_maybe_async(self._thread_repository.get_by_id, thread_id)
        if thread is None:
            thread = Thread(
                id=thread_id or uuid.uuid4(),
                organization_id=organization_id,
                project_id=resolved_project_id,
                title=title,
                created_by=resolved_user_id,
                state=ThreadState.awaiting_user_input,
                metadata_json=metadata_json,
            )
            self._thread_repository.add(thread)

        message_id = uuid.uuid4()
        user_message = ThreadMessage(
            id=message_id,
            thread_id=thread.id,
            role=Role.user,
            content={"text": message},
        )
        self._thread_message_repository.add(user_message)
        thread.last_message_id = message_id
        thread.updated_at = datetime.now(timezone.utc)
        thread.state = ThreadState.processing

        job_id = uuid.uuid4()
        request = CreateAgentJobRequest(
            job_type=JobType.AGENT,
            agent_definition_id=agent_id,
            organisation_id=organization_id,
            project_id=resolved_project_id,
            user_id=resolved_user_id,
            thread_id=thread.id,
        )
        try:
            execution = _run_awaitable(
                self._runtime_host.create_agent(
                    job_id=job_id,
                    request=request,
                    event_emitter=None,
                )
            )
        except Exception as exc:
            return AgentAskResult(
                thread_id=thread.id,
                status="failed",
                job_id=job_id,
                error={"message": str(exc)},
            )

        response = getattr(execution, "response", {}) or {}
        return AgentAskResult(
            thread_id=thread.id,
            status="succeeded",
            job_id=job_id,
            summary=response.get("summary"),
            result=response.get("result"),
            visualization=response.get("visualization"),
            error=response.get("error"),
            events=[],
        )


class _DatasetClient:
    def __init__(self, owner: "LangbridgeClient") -> None:
        self._owner = owner

    def list(
        self,
        *,
        workspace_id: uuid.UUID | None = None,
        project_id: uuid.UUID | None = None,
        search: str | None = None,
    ) -> DatasetListResult:
        return self._owner._adapter.list_datasets(
            workspace_id=_coalesce_uuid(workspace_id, self._owner.default_workspace_id, "workspace_id"),
            project_id=project_id or self._owner.default_project_id,
            search=search,
        )

    def query(
        self,
        dataset: str | uuid.UUID | None = None,
        *,
        dataset_id: uuid.UUID | None = None,
        workspace_id: uuid.UUID | None = None,
        project_id: uuid.UUID | None = None,
        user_id: uuid.UUID | None = None,
        limit: int | None = None,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        sort: list[dict[str, Any]] | None = None,
        user_context: dict[str, Any] | None = None,
        timeout_s: float = 30.0,
        poll_interval_s: float = 0.2,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        time_dimensions: list[dict[str, Any]] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
    ) -> DatasetQueryResult:
        resolved_workspace_id = _coalesce_uuid(
            workspace_id,
            self._owner.default_workspace_id,
            "workspace_id",
        )
        semantic_query_requested = isinstance(dataset, str) or metrics is not None or dimensions is not None
        if semantic_query_requested:
            dataset_name = str(dataset or "").strip()
            if not dataset_name:
                raise ValueError("dataset name is required for semantic-style dataset queries.")
            normalized_filters = (
                filters
                if isinstance(filters, list)
                else [
                    {"member": key, "operator": "equals", "values": [str(value)]}
                    for key, value in (filters or {}).items()
                ]
            )
            return self._owner._adapter.query_dataset_semantic(
                dataset_name=dataset_name,
                workspace_id=resolved_workspace_id,
                project_id=project_id or self._owner.default_project_id,
                user_id=user_id or self._owner.default_user_id,
                metrics=metrics,
                dimensions=dimensions,
                filters=normalized_filters,
                time_dimensions=time_dimensions,
                limit=limit,
                order=order,
                timeout_s=timeout_s,
                poll_interval_s=poll_interval_s,
            )

        resolved_dataset_id = dataset_id
        if resolved_dataset_id is None and isinstance(dataset, uuid.UUID):
            resolved_dataset_id = dataset
        if resolved_dataset_id is None:
            raise ValueError("dataset_id is required for preview-style dataset queries.")
        return self._owner._adapter.query_dataset(
            dataset_id=resolved_dataset_id,
            workspace_id=resolved_workspace_id,
            project_id=project_id or self._owner.default_project_id,
            user_id=user_id or self._owner.default_user_id,
            limit=limit,
            filters=filters if isinstance(filters, dict) else None,
            sort=sort,
            user_context=user_context,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )


class _SqlClient:
    def __init__(self, owner: "LangbridgeClient") -> None:
        self._owner = owner

    def query(
        self,
        *,
        query: str,
        workspace_id: uuid.UUID | None = None,
        project_id: uuid.UUID | None = None,
        user_id: uuid.UUID | None = None,
        connection_id: uuid.UUID | None = None,
        connection_name: str | None = None,
        selected_datasets: list[SqlSelectedDataset | dict[str, Any]] | None = None,
        query_dialect: SqlDialect | str = SqlDialect.tsql,
        params: dict[str, Any] | None = None,
        requested_limit: int | None = None,
        requested_timeout_seconds: int | None = None,
        explain: bool = False,
        timeout_s: float = 30.0,
        poll_interval_s: float = 0.2,
    ) -> SqlQueryResult:
        return self._owner._adapter.query_sql(
            workspace_id=_coalesce_uuid(workspace_id, self._owner.default_workspace_id, "workspace_id"),
            project_id=project_id or self._owner.default_project_id,
            user_id=user_id or self._owner.default_user_id,
            query=query,
            connection_id=connection_id,
            connection_name=connection_name,
            selected_datasets=_normalize_selected_datasets(selected_datasets),
            query_dialect=query_dialect,
            params=params,
            requested_limit=requested_limit,
            requested_timeout_seconds=requested_timeout_seconds,
            explain=explain,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )


class _AgentClient:
    def __init__(self, owner: "LangbridgeClient") -> None:
        self._owner = owner

    def ask(
        self,
        message: str,
        *,
        agent_id: uuid.UUID | None = None,
        agent_name: str | None = None,
        organization_id: uuid.UUID | None = None,
        project_id: uuid.UUID | None = None,
        user_id: uuid.UUID | None = None,
        thread_id: uuid.UUID | None = None,
        title: str | None = None,
        metadata_json: dict[str, Any] | None = None,
        timeout_s: float = 60.0,
        poll_interval_s: float = 0.5,
    ) -> AgentAskResult:
        return self._owner._adapter.ask_agent(
            organization_id=_coalesce_uuid(
                organization_id or self._owner.default_organization_id,
                self._owner.default_workspace_id,
                "organization_id",
            ),
            project_id=project_id or self._owner.default_project_id,
            user_id=user_id or self._owner.default_user_id,
            agent_id=agent_id,
            agent_name=agent_name,
            message=message,
            thread_id=thread_id,
            title=title,
            metadata_json=metadata_json,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )


class LangbridgeClient:
    def __init__(
        self,
        *,
        adapter: _SdkAdapter,
        default_workspace_id: uuid.UUID | None = None,
        default_organization_id: uuid.UUID | None = None,
        default_project_id: uuid.UUID | None = None,
        default_user_id: uuid.UUID | None = None,
    ) -> None:
        self._adapter = adapter
        self.default_workspace_id = default_workspace_id
        self.default_organization_id = default_organization_id
        self.default_project_id = default_project_id
        self.default_user_id = default_user_id
        self.datasets = _DatasetClient(self)
        self.sql = _SqlClient(self)
        self.agents = _AgentClient(self)

    @classmethod
    def remote(
        cls,
        *,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        http_client: httpx.Client | None = None,
        default_workspace_id: uuid.UUID | None = None,
        default_organization_id: uuid.UUID | None = None,
        default_project_id: uuid.UUID | None = None,
        default_user_id: uuid.UUID | None = None,
    ) -> "LangbridgeClient":
        return cls.for_remote_api(
            base_url=base_url,
            token=token,
            timeout=timeout,
            http_client=http_client,
            default_workspace_id=default_workspace_id,
            default_organization_id=default_organization_id,
            default_project_id=default_project_id,
            default_user_id=default_user_id,
        )

    @classmethod
    def local(
        cls,
        *,
        config_path: str,
        user_id: uuid.UUID | None = None,
        workspace_id: uuid.UUID | None = None,
        tenant_id: uuid.UUID | None = None,
        project_id: uuid.UUID | None = None,
        request_id: str | None = None,
        roles: list[str] | tuple[str, ...] | None = None,
    ) -> "LangbridgeClient":
        from langbridge.packages.runtime import build_configured_local_runtime

        runtime_host = build_configured_local_runtime(
            config_path=config_path,
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            user_id=user_id,
            roles=roles,
            request_id=request_id,
        )
        return cls.for_local_runtime(
            runtime_host=runtime_host,
            default_workspace_id=runtime_host.context.workspace_id,
            default_organization_id=runtime_host.context.workspace_id,
            default_project_id=project_id,
            default_user_id=runtime_host.context.user_id,
        )

    @classmethod
    def for_remote_api(
        cls,
        *,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        http_client: httpx.Client | None = None,
        default_workspace_id: uuid.UUID | None = None,
        default_organization_id: uuid.UUID | None = None,
        default_project_id: uuid.UUID | None = None,
        default_user_id: uuid.UUID | None = None,
    ) -> "LangbridgeClient":
        return cls(
            adapter=RemoteApiAdapter(
                base_url=base_url,
                token=token,
                timeout=timeout,
                client=http_client,
            ),
            default_workspace_id=default_workspace_id,
            default_organization_id=default_organization_id,
            default_project_id=default_project_id,
            default_user_id=default_user_id,
        )

    @classmethod
    def for_local_runtime(
        cls,
        *,
        runtime_host: Any,
        thread_repository: Any | None = None,
        thread_message_repository: Any | None = None,
        default_workspace_id: uuid.UUID | None = None,
        default_organization_id: uuid.UUID | None = None,
        default_project_id: uuid.UUID | None = None,
        default_user_id: uuid.UUID | None = None,
    ) -> "LangbridgeClient":
        return cls(
            adapter=LocalRuntimeAdapter(
                runtime_host=runtime_host,
                thread_repository=thread_repository,
                thread_message_repository=thread_message_repository,
            ),
            default_workspace_id=default_workspace_id,
            default_organization_id=default_organization_id,
            default_project_id=default_project_id,
            default_user_id=default_user_id,
        )

    def close(self) -> None:
        self._adapter.close()

    def __enter__(self) -> "LangbridgeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


__all__ = [
    "AgentAskResult",
    "DatasetListResult",
    "DatasetQueryResult",
    "DatasetSummary",
    "LangbridgeClient",
    "LocalRuntimeAdapter",
    "RemoteApiAdapter",
    "SqlQueryResult",
]
