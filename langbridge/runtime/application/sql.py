import uuid
from typing import TYPE_CHECKING, Any

from langbridge.runtime.models import (
    CreateSqlJobRequest,
    SqlQueryRequest,
    SqlQueryScope,
    SqlWorkbenchMode,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


class SqlApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    async def query_sql(self, *, request: SqlQueryRequest) -> dict[str, Any]:
        if request.query_scope == SqlQueryScope.semantic:
            payload = await self._host._applications.semantic.query_semantic_sql(request=request)
            normalized = dict(payload or {})
            normalized["query_scope"] = request.query_scope.value
            return normalized

        job_request = self._build_job_request(request=request)
        payload = await self.execute_sql(request=job_request)
        normalized = dict(payload or {})
        normalized["sql_job_id"] = job_request.sql_job_id
        normalized["query_scope"] = request.query_scope.value
        return normalized

    async def execute_sql(self, *, request: CreateSqlJobRequest) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            payload = await self._host._runtime_host.execute_sql(request=request)
            if uow is not None:
                await uow.commit()
            normalized = dict(payload or {})
            stats = normalized.get("stats")
            if isinstance(stats, dict):
                normalized.setdefault("generated_sql", stats.get("query_sql"))
            return normalized

    async def execute_sql_text(
        self,
        *,
        query: str,
        connection_name: str | None = None,
        requested_limit: int | None = None,
    ) -> dict[str, Any]:
        connector = self._host._resolve_connector(connection_name)
        payload = await self.query_sql(
            request=SqlQueryRequest(
                query_scope=SqlQueryScope.source,
                query=str(query or "").strip(),
                connection_name=connector.name,
                query_dialect=self._host._connector_dialect(connector.connector_type or ""),
                params={},
                requested_limit=requested_limit,
                requested_timeout_seconds=None,
                explain=False,
            )
        )
        payload.setdefault("generated_sql", None)
        return payload

    def _build_job_request(self, *, request: SqlQueryRequest) -> CreateSqlJobRequest:
        connector_id = None
        execution_mode = "federated"
        workbench_mode = SqlWorkbenchMode.dataset
        allow_federation = True
        selected_datasets = list(request.selected_datasets or [])

        if request.query_scope == SqlQueryScope.source:
            connector = self._resolve_source_connector(request=request)
            connector_id = connector.id
            execution_mode = "single"
            workbench_mode = SqlWorkbenchMode.direct_sql
            allow_federation = False
            selected_datasets = []

        return CreateSqlJobRequest(
            sql_job_id=uuid.uuid4(),
            workspace_id=self._host.context.workspace_id,
            actor_id=self._host.context.actor_id,
            workbench_mode=workbench_mode,
            connection_id=connector_id,
            execution_mode=execution_mode,
            query=request.query,
            query_dialect=request.query_dialect,
            params=dict(request.params or {}),
            requested_limit=request.requested_limit,
            requested_timeout_seconds=request.requested_timeout_seconds,
            enforced_limit=request.requested_limit or 100,
            enforced_timeout_seconds=request.requested_timeout_seconds or 30,
            allow_dml=False,
            allow_federation=allow_federation,
            selected_datasets=selected_datasets,
            explain=bool(request.explain),
            correlation_id=self._host.context.request_id,
        )

    def _resolve_source_connector(self, *, request: SqlQueryRequest):
        if request.connection_name:
            return self._host._resolve_connector(request.connection_name)
        connector = self._host._connector_for_id(request.connection_id)
        if connector is None:
            raise ValueError(f"Unknown connector '{request.connection_id}'.")
        return connector
