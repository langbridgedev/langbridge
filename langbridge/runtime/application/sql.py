
import uuid
from typing import TYPE_CHECKING, Any

from langbridge.runtime.models import CreateSqlJobRequest, SqlWorkbenchMode

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


class SqlApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    async def execute_sql(self, *, request) -> dict[str, Any]:
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
        request = CreateSqlJobRequest(
            sql_job_id=uuid.uuid4(),
            workspace_id=self._host.context.workspace_id,
            actor_id=self._host.context.actor_id,
            workbench_mode=SqlWorkbenchMode.direct_sql,
            connection_id=connector.id,
            execution_mode="single",
            query=str(query or "").strip(),
            query_dialect=self._host._connector_dialect(connector.connector_type or ""),
            params={},
            requested_limit=requested_limit,
            requested_timeout_seconds=None,
            enforced_limit=int(requested_limit or 100),
            enforced_timeout_seconds=30,
            allow_dml=False,
            allow_federation=False,
            selected_datasets=[],
            federated_datasets=[],
            explain=False,
            correlation_id=self._host.context.request_id,
        )
        payload = await self.execute_sql(request=request)
        payload.setdefault("generated_sql", None)
        return payload
