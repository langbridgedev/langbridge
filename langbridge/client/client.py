from __future__ import annotations

import asyncio
import inspect
import threading
import time
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import Field

if TYPE_CHECKING:
    import httpx
else:  # pragma: no cover - exercised indirectly in local-only environments
    try:
        import httpx
    except ModuleNotFoundError:  # pragma: no cover - depends on install extras
        httpx = None

from langbridge.contracts.base import _Base
from langbridge.contracts.datasets import (
    DatasetListResponse,
    DatasetPreviewColumn,
    DatasetPreviewRequest,
)
from langbridge.contracts.jobs.agent_job import (
    AgentJobStateResponse,
    JobEventResponse,
)
from langbridge.contracts.sql import (
    SqlColumnMetadata,
    SqlDialect,
    SqlExecuteRequest,
    SqlJobResponse,
    SqlJobResultsResponse,
    SqlSelectedDataset,
)
from langbridge.contracts.threads import (
    ThreadChatRequest,
    ThreadCreateRequest,
    ThreadResponse,
)
from langbridge.runtime.models import (
    CreateDatasetPreviewJobRequest,
    CreateSqlJobRequest,
    SqlWorkbenchMode,
)
from langbridge.runtime.hosting.api_models import (
    RuntimeConnectorListResponse,
    RuntimeDatasetListResponse,
    RuntimeSyncResourceListResponse,
    RuntimeSyncResponse,
    RuntimeSyncStateListResponse,
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
    managed: bool = False


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


class SemanticQueryResult(_AwaitableModel):
    status: str
    semantic_model_id: uuid.UUID | None = None
    semantic_model_ids: list[uuid.UUID] = Field(default_factory=list)
    connector_id: uuid.UUID | None = None
    data: list[dict[str, Any]] = Field(default_factory=list)
    annotations: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] | None = None
    generated_sql: str | None = None
    error: str | None = None

    @property
    def rows(self) -> list[dict[str, Any]]:
        return list(self.data)


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


class ConnectorSummary(_AwaitableModel):
    id: uuid.UUID | None = None
    name: str
    description: str | None = None
    connector_type: str | None = None
    supports_sync: bool = False
    supported_resources: list[str] = Field(default_factory=list)
    sync_strategy: str | None = None
    managed: bool = False


class ConnectorListResult(_AwaitableModel):
    items: list[ConnectorSummary] = Field(default_factory=list)
    total: int = 0


class SyncResourceResult(_AwaitableModel):
    name: str
    label: str | None = None
    primary_key: str | None = None
    parent_resource: str | None = None
    cursor_field: str | None = None
    incremental_cursor_field: str | None = None
    supports_incremental: bool = False
    default_sync_mode: str | None = None
    status: str | None = None
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)
    records_synced: int = 0
    bytes_synced: int | None = None


class SyncResourceListResult(_AwaitableModel):
    items: list[SyncResourceResult] = Field(default_factory=list)
    total: int = 0


class SyncStateResult(_AwaitableModel):
    id: uuid.UUID | None = None
    workspace_id: uuid.UUID | None = None
    connection_id: uuid.UUID | None = None
    connector_name: str | None = None
    connector_type: str | None = None
    resource_name: str
    sync_mode: str | None = None
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    status: str | None = None
    error_message: str | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SyncStateListResult(_AwaitableModel):
    items: list[SyncStateResult] = Field(default_factory=list)
    total: int = 0


class SyncRunResourceResult(_AwaitableModel):
    resource_name: str
    sync_mode: str | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    last_cursor: str | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)


class SyncRunResult(_AwaitableModel):
    status: str
    connector_id: uuid.UUID | None = None
    connector_name: str | None = None
    sync_mode: str | None = None
    resources: list[SyncRunResourceResult] = Field(default_factory=list)
    summary: str | None = None
    error: str | None = None


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
        search: str | None,
    ) -> DatasetListResult: ...

    def query_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        limit: int | None,
        filters: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
        user_context: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult: ...

    def query_semantic(
        self,
        *,
        semantic_models: list[str],
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        measures: list[str] | None,
        dimensions: list[str] | None,
        filters: list[dict[str, Any]] | None,
        time_dimensions: list[dict[str, Any]] | None,
        limit: int | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SemanticQueryResult: ...

    def query_sql(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
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
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        message: str,
        agent_id: uuid.UUID | None,
        agent_name: str | None,
        thread_id: uuid.UUID | None,
        title: str | None,
        metadata_json: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> AgentAskResult: ...

    def list_connectors(self) -> ConnectorListResult: ...

    def list_sync_resources(
        self,
        *,
        connector_name: str,
    ) -> SyncResourceListResult: ...

    def list_sync_states(
        self,
        *,
        connector_name: str,
    ) -> SyncStateListResult: ...

    def sync_connector(
        self,
        *,
        connector_name: str,
        resource_names: list[str],
        sync_mode: str,
        force_full_refresh: bool,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SyncRunResult: ...

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


class _BaseHttpApiAdapter:
    def __init__(
        self,
        *,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        if httpx is None and client is None:
            raise ModuleNotFoundError(
                "httpx is required for LangbridgeClient.remote(...) and LangbridgeClient.for_remote_api(...)."
            )
        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(base_url=self._base_url, timeout=timeout)
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

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

    def _request_optional(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any | None:
        try:
            return self._request(method, path, params=params, json=json)
        except Exception as exc:
            response = getattr(exc, "response", None)
            if getattr(response, "status_code", None) == 404:
                return None
            raise


class RuntimeHostApiAdapter(_BaseHttpApiAdapter, _SdkAdapter):
    def list_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        search: str | None,
    ) -> DatasetListResult:
        payload = RuntimeDatasetListResponse.model_validate(
            self._request(
                "GET",
                "/api/runtime/v1/datasets",
            )
        )
        items = [
            DatasetSummary.model_validate(item.model_dump(mode="json"))
            for item in payload.items
        ]
        if search:
            filtered = [
                item
                for item in items
                if search.lower() in item.name.lower()
                or search.lower() in str(item.label or "").lower()
            ]
            return DatasetListResult(items=filtered, total=len(filtered))
        return DatasetListResult(items=items, total=payload.total)

    def query_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        limit: int | None,
        filters: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
        user_context: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult:
        payload = {
            "limit": limit,
            "filters": filters or {},
            "sort": sort or [],
            "user_context": user_context or {},
        }
        return DatasetQueryResult.model_validate(
            self._request(
                "POST",
                f"/api/runtime/v1/datasets/{dataset_id}/preview",
                json=payload,
            )
        )

    def query_semantic(
        self,
        *,
        semantic_models: list[str],
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        measures: list[str] | None,
        dimensions: list[str] | None,
        filters: list[dict[str, Any]] | None,
        time_dimensions: list[dict[str, Any]] | None,
        limit: int | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SemanticQueryResult:
        payload = {
            "semantic_models": semantic_models,
            "measures": measures or [],
            "dimensions": dimensions or [],
            "filters": filters or [],
            "time_dimensions": time_dimensions or [],
            "limit": limit,
            **({"order": order} if order is not None else {}),
        }
        return SemanticQueryResult.model_validate(
            self._request(
                "POST",
                "/api/runtime/v1/semantic/query",
                json=payload,
            )
        )

    def query_sql(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
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
        payload = {
            "query": query,
            **({"connection_id": str(connection_id)} if connection_id else {}),
            **({"connection_name": connection_name} if connection_name else {}),
            "selected_datasets": [item.model_dump(mode="json") for item in normalized_datasets],
            "query_dialect": _coerce_sql_dialect(query_dialect).value,
            "params": params or {},
            **({"requested_limit": requested_limit} if requested_limit is not None else {}),
            **(
                {"requested_timeout_seconds": requested_timeout_seconds}
                if requested_timeout_seconds is not None
                else {}
            ),
            "explain": explain,
        }
        return SqlQueryResult.model_validate(
            self._request(
                "POST",
                "/api/runtime/v1/sql/query",
                json=payload,
            )
        )

    def ask_agent(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        message: str,
        agent_id: uuid.UUID | None,
        agent_name: str | None,
        thread_id: uuid.UUID | None,
        title: str | None,
        metadata_json: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> AgentAskResult:
        return AgentAskResult.model_validate(
            self._request(
                "POST",
                "/api/runtime/v1/agents/ask",
                json={
                    "message": message,
                    **({"agent_id": str(agent_id)} if agent_id else {}),
                    **({"agent_name": agent_name} if agent_name else {}),
                    **({"thread_id": str(thread_id)} if thread_id else {}),
                    **({"title": title} if title else {}),
                    **({"metadata_json": metadata_json} if metadata_json else {}),
                },
            )
        )

    def list_connectors(self) -> ConnectorListResult:
        payload = RuntimeConnectorListResponse.model_validate(
            self._request("GET", "/api/runtime/v1/connectors")
        )
        return ConnectorListResult(
            items=[
                ConnectorSummary.model_validate(item.model_dump(mode="json"))
                for item in payload.items
            ],
            total=payload.total,
        )

    def list_sync_resources(
        self,
        *,
        connector_name: str,
    ) -> SyncResourceListResult:
        payload = RuntimeSyncResourceListResponse.model_validate(
            self._request(
                "GET",
                f"/api/runtime/v1/connectors/{connector_name}/sync/resources",
            )
        )
        return SyncResourceListResult(
            items=[
                SyncResourceResult.model_validate(item.model_dump(mode="json"))
                for item in payload.items
            ],
            total=payload.total,
        )

    def list_sync_states(
        self,
        *,
        connector_name: str,
    ) -> SyncStateListResult:
        payload = RuntimeSyncStateListResponse.model_validate(
            self._request(
                "GET",
                f"/api/runtime/v1/connectors/{connector_name}/sync/states",
            )
        )
        return SyncStateListResult(
            items=[
                SyncStateResult.model_validate(item.model_dump(mode="json"))
                for item in payload.items
            ],
            total=payload.total,
        )

    def sync_connector(
        self,
        *,
        connector_name: str,
        resource_names: list[str],
        sync_mode: str,
        force_full_refresh: bool,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SyncRunResult:
        payload = RuntimeSyncResponse.model_validate(
            self._request(
                "POST",
                f"/api/runtime/v1/connectors/{connector_name}/sync",
                json={
                    "resource_names": list(resource_names or []),
                    "sync_mode": str(sync_mode or "INCREMENTAL").strip().upper() or "INCREMENTAL",
                    "force_full_refresh": bool(force_full_refresh),
                },
            )
        )
        return SyncRunResult.model_validate(payload.model_dump(mode="json"))


class RemoteApiAdapter(_BaseHttpApiAdapter, _SdkAdapter):
    def list_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        search: str | None,
    ) -> DatasetListResult:
        payload = DatasetListResponse.model_validate(
            self._request(
                "GET",
                "/api/v1/datasets",
                params={
                    "workspace_id": str(workspace_id),
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

    def query_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        limit: int | None,
        filters: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
        user_context: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult:
        payload = DatasetPreviewRequest(
            workspace_id=workspace_id,
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

    def query_semantic(
        self,
        *,
        semantic_models: list[str],
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        measures: list[str] | None,
        dimensions: list[str] | None,
        filters: list[dict[str, Any]] | None,
        time_dimensions: list[dict[str, Any]] | None,
        limit: int | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SemanticQueryResult:
        raise ValueError(
            "Cloud API semantic queries are not exposed through LangbridgeClient.for_remote_api(...). "
            "Use LangbridgeClient.remote(...) against a runtime host, or LangbridgeClient.for_runtime_host(...)."
        )

    def query_sql(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
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
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
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
                    f"/api/v1/thread/{workspace_id}/",
                    json=ThreadCreateRequest(
                        workspace_id=workspace_id,
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
            f"/api/v1/thread/{workspace_id}/{resolved_thread_id}/chat",
            json=ThreadChatRequest(message=message, agent_id=agent_id).model_dump(mode="json"),
        )
        job_id = uuid.UUID(str(chat["job_id"]))

        def _fetch_job() -> AgentJobStateResponse:
            return AgentJobStateResponse.model_validate(
                self._request("GET", f"/api/v1/jobs/{workspace_id}/{job_id}")
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

    def list_connectors(self) -> ConnectorListResult:
        raise ValueError(
            "Cloud API connector management is not exposed through LangbridgeClient.for_remote_api(...). "
            "Use LangbridgeClient.remote(...) against a runtime host, or LangbridgeClient.for_runtime_host(...)."
        )

    def list_sync_resources(
        self,
        *,
        connector_name: str,
    ) -> SyncResourceListResult:
        raise ValueError(
            "Cloud API sync operations are not exposed through LangbridgeClient.for_remote_api(...). "
            "Use LangbridgeClient.remote(...) against a runtime host, or LangbridgeClient.for_runtime_host(...)."
        )

    def list_sync_states(
        self,
        *,
        connector_name: str,
    ) -> SyncStateListResult:
        raise ValueError(
            "Cloud API sync operations are not exposed through LangbridgeClient.for_remote_api(...). "
            "Use LangbridgeClient.remote(...) against a runtime host, or LangbridgeClient.for_runtime_host(...)."
        )

    def sync_connector(
        self,
        *,
        connector_name: str,
        resource_names: list[str],
        sync_mode: str,
        force_full_refresh: bool,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SyncRunResult:
        raise ValueError(
            "Cloud API sync operations are not exposed through LangbridgeClient.for_remote_api(...). "
            "Use LangbridgeClient.remote(...) against a runtime host, or LangbridgeClient.for_runtime_host(...)."
        )


class LocalRuntimeAdapter(_SdkAdapter):
    def __init__(
        self,
        *,
        runtime_host: Any,
    ) -> None:
        self._runtime_host = runtime_host

    def close(self) -> None:
        return None

    def list_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
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
        actor_id: uuid.UUID | None,
        limit: int | None,
        filters: dict[str, Any] | None,
        sort: list[dict[str, Any]] | None,
        user_context: dict[str, Any] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> DatasetQueryResult:
        resolved_actor_id = _coalesce_uuid(
            actor_id,
            getattr(getattr(self._runtime_host, "context", None), "actor_id", None),
            "actor_id",
        )
        request = CreateDatasetPreviewJobRequest(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            actor_id=resolved_actor_id,
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

    def query_semantic(
        self,
        *,
        semantic_models: list[str],
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        measures: list[str] | None,
        dimensions: list[str] | None,
        filters: list[dict[str, Any]] | None,
        time_dimensions: list[dict[str, Any]] | None,
        limit: int | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SemanticQueryResult:
        query_method = getattr(self._runtime_host, "query_semantic_models", None)
        if query_method is None:
            raise ValueError(
                "Local runtime host does not expose query_semantic_models()."
            )
        try:
            payload = _run_awaitable(
                query_method(
                    semantic_models=semantic_models,
                    measures=measures,
                    dimensions=dimensions,
                    filters=filters,
                    time_dimensions=time_dimensions,
                    limit=limit,
                    order=order,
                )
            )
        except Exception as exc:
            return SemanticQueryResult(
                status="failed",
                error=str(exc),
            )
        semantic_model_ids = []
        for value in payload.get("semantic_model_ids", []):
            try:
                semantic_model_ids.append(uuid.UUID(str(value)))
            except (TypeError, ValueError):
                continue
        semantic_model_id = payload.get("semantic_model_id")
        connector_id = payload.get("connector_id")
        return SemanticQueryResult(
            status="succeeded",
            semantic_model_id=(
                uuid.UUID(str(semantic_model_id))
                if semantic_model_id is not None
                else None
            ),
            semantic_model_ids=semantic_model_ids,
            connector_id=(
                uuid.UUID(str(connector_id))
                if connector_id is not None
                else None
            ),
            data=list(payload.get("rows", [])),
            annotations=list(payload.get("annotations", [])),
            metadata=payload.get("metadata"),
            generated_sql=payload.get("generated_sql"),
        )

    def query_sql(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
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
        if execute_sql_text is not None and not selected_datasets:
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
        resolved_actor_id = _coalesce_uuid(
            actor_id,
            getattr(getattr(self._runtime_host, "context", None), "actor_id", None),
            "actor_id",
        )
        workbench_mode = (
            SqlWorkbenchMode.dataset if normalized_datasets else SqlWorkbenchMode.direct_sql
        )
        sql_job_id = uuid.uuid4()
        request = CreateSqlJobRequest(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
            actor_id=resolved_actor_id,
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
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
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
        if ask_agent_method is None:
            raise ValueError("Local runtime host does not expose ask_agent().")
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
        payload_thread_id = payload.get("thread_id")
        payload_job_id = payload.get("job_id")
        return AgentAskResult(
            thread_id=(
                uuid.UUID(str(payload_thread_id))
                if payload_thread_id is not None
                else thread_id
            ),
            status="succeeded",
            job_id=(
                uuid.UUID(str(payload_job_id))
                if payload_job_id is not None
                else None
            ),
            summary=payload.get("summary"),
            result=payload.get("result"),
            visualization=payload.get("visualization"),
            error=payload.get("error"),
            events=[],
        )

    def list_connectors(self) -> ConnectorListResult:
        list_method = getattr(self._runtime_host, "list_connectors", None)
        if list_method is None:
            raise ValueError("Local runtime host does not expose list_connectors().")
        payload = _run_awaitable(list_method())
        items = [ConnectorSummary.model_validate(item) for item in (payload or [])]
        return ConnectorListResult(items=items, total=len(items))

    def list_sync_resources(
        self,
        *,
        connector_name: str,
    ) -> SyncResourceListResult:
        list_method = getattr(self._runtime_host, "list_sync_resources", None)
        if list_method is None:
            raise ValueError("Local runtime host does not expose list_sync_resources().")
        payload = _run_awaitable(list_method(connector_name=connector_name))
        items = [SyncResourceResult.model_validate(item) for item in (payload or [])]
        return SyncResourceListResult(items=items, total=len(items))

    def list_sync_states(
        self,
        *,
        connector_name: str,
    ) -> SyncStateListResult:
        list_method = getattr(self._runtime_host, "list_sync_states", None)
        if list_method is None:
            raise ValueError("Local runtime host does not expose list_sync_states().")
        payload = _run_awaitable(list_method(connector_name=connector_name))
        items = [SyncStateResult.model_validate(item) for item in (payload or [])]
        return SyncStateListResult(items=items, total=len(items))

    def sync_connector(
        self,
        *,
        connector_name: str,
        resource_names: list[str],
        sync_mode: str,
        force_full_refresh: bool,
        timeout_s: float,
        poll_interval_s: float,
    ) -> SyncRunResult:
        sync_method = getattr(self._runtime_host, "sync_connector_resources", None)
        if sync_method is None:
            raise ValueError("Local runtime host does not expose sync_connector_resources().")
        try:
            payload = _run_awaitable(
                sync_method(
                    connector_name=connector_name,
                    resources=list(resource_names or []),
                    sync_mode=sync_mode,
                    force_full_refresh=force_full_refresh,
                )
            )
        except Exception as exc:
            return SyncRunResult(
                status="failed",
                connector_name=connector_name,
                sync_mode=str(sync_mode or "INCREMENTAL").strip().upper() or "INCREMENTAL",
                error=str(exc),
            )
        return SyncRunResult.model_validate(payload)


class _ConnectorClient:
    def __init__(self, owner: "LangbridgeClient") -> None:
        self._owner = owner

    def list(self) -> ConnectorListResult:
        return self._owner._adapter.list_connectors()


class _SyncClient:
    def __init__(self, owner: "LangbridgeClient") -> None:
        self._owner = owner

    def resources(
        self,
        *,
        connector_name: str,
    ) -> SyncResourceListResult:
        return self._owner._adapter.list_sync_resources(connector_name=connector_name)

    def states(
        self,
        *,
        connector_name: str,
    ) -> SyncStateListResult:
        return self._owner._adapter.list_sync_states(connector_name=connector_name)

    def run(
        self,
        *,
        connector_name: str,
        resource_names: list[str] | tuple[str, ...],
        sync_mode: str = "INCREMENTAL",
        force_full_refresh: bool = False,
        timeout_s: float = 300.0,
        poll_interval_s: float = 0.5,
    ) -> SyncRunResult:
        return self._owner._adapter.sync_connector(
            connector_name=connector_name,
            resource_names=[str(item) for item in resource_names],
            sync_mode=sync_mode,
            force_full_refresh=force_full_refresh,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )


class _DatasetClient:
    def __init__(self, owner: "LangbridgeClient") -> None:
        self._owner = owner

    def list(
        self,
        *,
        workspace_id: uuid.UUID | None = None,
        search: str | None = None,
    ) -> DatasetListResult:
        return self._owner._adapter.list_datasets(
            workspace_id=_coalesce_uuid(workspace_id, self._owner.default_workspace_id, "workspace_id"),
            search=search,
        )

    def query(
        self,
        dataset: str | uuid.UUID | None = None,
        *,
        dataset_id: uuid.UUID | None = None,
        workspace_id: uuid.UUID | None = None,
        actor_id: uuid.UUID | None = None,
        limit: int | None = None,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        sort: list[dict[str, Any]] | None = None,
        user_context: dict[str, Any] | None = None,
        timeout_s: float = 30.0,
        poll_interval_s: float = 0.2,
    ) -> DatasetQueryResult:
        resolved_workspace_id = _coalesce_uuid(
            workspace_id,
            self._owner.default_workspace_id,
            "workspace_id",
        )
        if filters is not None and not isinstance(filters, dict):
            raise ValueError(
                "Dataset queries accept filters as a simple column-to-value mapping. "
                "Use client.semantic.query(...) for semantic filter objects."
            )

        resolved_dataset_id = dataset_id
        if resolved_dataset_id is None and isinstance(dataset, uuid.UUID):
            resolved_dataset_id = dataset
        if resolved_dataset_id is None and isinstance(dataset, str):
            dataset_name = str(dataset).strip()
            matches = [
                item
                for item in self.list(
                    workspace_id=resolved_workspace_id,
                ).items
                if item.id is not None and item.name == dataset_name
            ]
            if not matches:
                raise ValueError(f"Unknown dataset '{dataset_name}'.")
            if len(matches) > 1:
                raise ValueError(f"Dataset name '{dataset_name}' is ambiguous; use dataset_id instead.")
            resolved_dataset_id = matches[0].id
        if resolved_dataset_id is None:
            raise ValueError("dataset_id or dataset name is required for dataset queries.")
        return self._owner._adapter.query_dataset(
            dataset_id=resolved_dataset_id,
            workspace_id=resolved_workspace_id,
            actor_id=actor_id or self._owner.default_actor_id,
            limit=limit,
            filters=filters if isinstance(filters, dict) else None,
            sort=sort,
            user_context=user_context,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )
        
        
class _SemanticQueryClient:
    def __init__(self, owner: "LangbridgeClient") -> None:
        self._owner = owner
        
    def query(
        self,
        semantic_models: list[str] | str | None = None,
        *,
        workspace_id: uuid.UUID | None = None,
        actor_id: uuid.UUID | None = None,
        measures: list[str] | None = None,
        dimensions: list[str] | None = None,
        time_dimensions: list[dict[str, Any]] | None = None,
        filters: list[dict[str, Any]] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
        limit: int | None = None,
        timeout_s: float = 30.0,
        poll_interval_s: float = 0.2,
    ) -> SemanticQueryResult:
        normalized_models = (
            [semantic_models]
            if isinstance(semantic_models, str)
            else list(semantic_models or [])
        )
        return self._owner._adapter.query_semantic(
            semantic_models=normalized_models,
            workspace_id=_coalesce_uuid(workspace_id, self._owner.default_workspace_id, "workspace_id"),
            actor_id=actor_id or self._owner.default_actor_id,
            measures=measures,
            dimensions=dimensions,
            filters=filters,
            time_dimensions=time_dimensions,
            order=order,
            limit=limit,
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
        actor_id: uuid.UUID | None = None,
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
            actor_id=actor_id or self._owner.default_actor_id,
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
        workspace_id: uuid.UUID | None = None,
        actor_id: uuid.UUID | None = None,
        thread_id: uuid.UUID | None = None,
        title: str | None = None,
        metadata_json: dict[str, Any] | None = None,
        timeout_s: float = 60.0,
        poll_interval_s: float = 0.5,
    ) -> AgentAskResult:
        return self._owner._adapter.ask_agent(
            workspace_id=_coalesce_uuid(
                workspace_id,
                self._owner.default_workspace_id,
                "workspace_id",
            ),
            actor_id=actor_id or self._owner.default_actor_id,
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
        default_actor_id: uuid.UUID | None = None,
    ) -> None:
        self._adapter = adapter
        self.default_workspace_id = default_workspace_id
        self.default_actor_id = default_actor_id
        self.connectors = _ConnectorClient(self)
        self.sync = _SyncClient(self)
        self.datasets = _DatasetClient(self)
        self.semantic = _SemanticQueryClient(self)
        self.sql = _SqlClient(self)
        self.agents = _AgentClient(self)

    def close(self) -> None:
        self._adapter.close()

    def __enter__(self) -> "LangbridgeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @classmethod
    def remote(
        cls,
        *,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        http_client: httpx.Client | None = None,
        default_workspace_id: uuid.UUID | None = None,
        default_actor_id: uuid.UUID | None = None,
    ) -> "LangbridgeClient":
        runtime_host_defaults = _discover_runtime_host_defaults(
            base_url=base_url,
            token=token,
            timeout=timeout,
            http_client=http_client,
        )
        if runtime_host_defaults:
            return cls.for_runtime_host(
                base_url=base_url,
                token=token,
                timeout=timeout,
                http_client=http_client,
                default_workspace_id=default_workspace_id or runtime_host_defaults.get("workspace_id"),
                default_actor_id=default_actor_id or runtime_host_defaults.get("actor_id"),
            )
        return cls.for_remote_api(
            base_url=base_url,
            token=token,
            timeout=timeout,
            http_client=http_client,
            default_workspace_id=default_workspace_id,
            default_actor_id=default_actor_id,
        )

    @classmethod
    def local(
        cls,
        *,
        config_path: str,
        actor_id: uuid.UUID | None = None,
        workspace_id: uuid.UUID | None = None,
        request_id: str | None = None,
        roles: list[str] | tuple[str, ...] | None = None,
    ) -> "LangbridgeClient":
        from langbridge.runtime.local_config import build_configured_local_runtime

        runtime_host = build_configured_local_runtime(
            config_path=config_path,
            workspace_id=workspace_id,
            actor_id=actor_id,
            roles=roles,
            request_id=request_id,
        )
        return cls.for_local_runtime(
            runtime_host=runtime_host,
            default_workspace_id=runtime_host.context.workspace_id,
            default_actor_id=runtime_host.context.actor_id,
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
        default_actor_id: uuid.UUID | None = None,
    ) -> "LangbridgeClient":
        return cls(
            adapter=RemoteApiAdapter(
                base_url=base_url,
                token=token,
                timeout=timeout,
                client=http_client,
            ),
            default_workspace_id=default_workspace_id,
            default_actor_id=default_actor_id,
        )

    @classmethod
    def for_runtime_host(
        cls,
        *,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        http_client: httpx.Client | None = None,
        default_workspace_id: uuid.UUID | None = None,
        default_actor_id: uuid.UUID | None = None,
    ) -> "LangbridgeClient":
        discovered_defaults = _discover_runtime_host_defaults(
            base_url=base_url,
            token=token,
            timeout=timeout,
            http_client=http_client,
        )
        return cls(
            adapter=RuntimeHostApiAdapter(
                base_url=base_url,
                token=token,
                timeout=timeout,
                client=http_client,
            ),
            default_workspace_id=default_workspace_id or discovered_defaults.get("workspace_id"),
            default_actor_id=default_actor_id or discovered_defaults.get("actor_id"),
        )

    @classmethod
    def for_local_runtime(
        cls,
        *,
        runtime_host: Any,
        default_workspace_id: uuid.UUID | None = None,
        default_actor_id: uuid.UUID | None = None,
    ) -> "LangbridgeClient":
        return cls(
            adapter=LocalRuntimeAdapter(
                runtime_host=runtime_host,
            ),
            default_workspace_id=default_workspace_id,
            default_actor_id=default_actor_id,
        )


def _discover_runtime_host_defaults(
    *,
    base_url: str,
    token: str | None,
    timeout: float,
    http_client: httpx.Client | None,
) -> dict[str, uuid.UUID]:
    if httpx is None and http_client is None:
        return {}
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    owns_client = http_client is None
    client = http_client or httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout, headers=headers)
    try:
        response = client.get("/api/runtime/v1/info", headers=headers if http_client is not None else None)
        if response.status_code != 200:
            return {}
        payload = response.json()
    except Exception:
        return {}
    finally:
        if owns_client:
            client.close()
    defaults: dict[str, uuid.UUID] = {}
    for key in ("workspace_id", "actor_id"):
        try:
            defaults[key] = uuid.UUID(str(payload.get(key)))
        except (TypeError, ValueError, AttributeError):
            continue
    return defaults


__all__ = [
    "AgentAskResult",
    "DatasetListResult",
    "DatasetQueryResult",
    "DatasetSummary",
    "LangbridgeClient",
    "LocalRuntimeAdapter",
    "RemoteApiAdapter",
    "RuntimeHostApiAdapter",
    "SemanticQueryResult",
    "SqlQueryResult",
]
