import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.hosting.auth import RuntimeAuthConfig, RuntimeAuthMode
from langbridge.runtime.hosting.odbc import (
    RuntimeOdbcEndpoint,
    RuntimeOdbcEndpointConfig,
    RuntimeOdbcQueryGateway,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@dataclass
class _RecordedRequest:
    query: str
    execution_mode: str
    query_dialect: str
    requested_limit: int | None


class _FakeRuntimeHost:
    def __init__(
        self,
        *,
        context: RuntimeContext,
        calls: list[_RecordedRequest] | None = None,
    ) -> None:
        self.context = context
        self.calls = calls if calls is not None else []
        self._dataset_id = uuid.uuid4()

    def with_context(self, context: RuntimeContext) -> "_FakeRuntimeHost":
        return _FakeRuntimeHost(context=context, calls=self.calls)

    async def execute_sql(self, *, request) -> dict[str, Any]:
        self.calls.append(
            _RecordedRequest(
                query=request.query,
                execution_mode=request.execution_mode,
                query_dialect=request.query_dialect,
                requested_limit=request.requested_limit,
            )
        )
        return {
            "columns": [
                {"name": "id"},
                {"name": "total"},
            ],
            "rows": [
                {"id": 1, "total": 12.5},
            ],
        }

    async def list_datasets(self) -> list[dict[str, Any]]:
        return [
            {
                "id": self._dataset_id,
                "name": "orders",
            }
        ]

    async def get_dataset(self, *, dataset_ref: str) -> dict[str, Any]:
        assert dataset_ref == str(self._dataset_id)
        return {
            "id": self._dataset_id,
            "name": "orders",
            "sql_alias": "orders",
            "columns": [
                {"name": "id", "data_type": "integer"},
                {"name": "total", "data_type": "number"},
            ],
        }


@pytest.mark.anyio
async def test_runtime_odbc_gateway_routes_queries_to_federated_sql() -> None:
    host = _FakeRuntimeHost(
        context=RuntimeContext.build(
            workspace_id=uuid.uuid4(),
            actor_id=uuid.uuid4(),
            roles=("analyst",),
        )
    )
    gateway = RuntimeOdbcQueryGateway(
        runtime_host=host,
        context=host.context,
        config=RuntimeOdbcEndpointConfig(max_rows=250),
    )

    result = await gateway.execute('select id, total from public.orders where id = 1')

    assert result.rows == [(1, 12.5)]
    assert len(host.calls) == 1
    request = host.calls[0]
    assert request.execution_mode == "federated"
    assert request.query_dialect == "postgres"
    assert request.requested_limit == 250
    assert "public." not in request.query.lower()
    assert "orders" in request.query.lower()


@pytest.mark.anyio
async def test_runtime_odbc_gateway_serves_information_schema_from_runtime_datasets() -> None:
    host = _FakeRuntimeHost(
        context=RuntimeContext.build(
            workspace_id=uuid.uuid4(),
            actor_id=uuid.uuid4(),
        )
    )
    gateway = RuntimeOdbcQueryGateway(
        runtime_host=host,
        context=host.context,
    )

    result = await gateway.execute(
        "select table_schema, table_name from information_schema.tables where table_schema = 'public'"
    )

    assert ("public", "orders") in result.rows
    assert host.calls == []


@pytest.mark.anyio
async def test_runtime_odbc_endpoint_accepts_psycopg_queries() -> None:
    host = _FakeRuntimeHost(
        context=RuntimeContext.build(
            workspace_id=uuid.uuid4(),
            actor_id=uuid.uuid4(),
        )
    )
    endpoint = RuntimeOdbcEndpoint(
        runtime_host=host,
        auth_config=RuntimeAuthConfig(mode=RuntimeAuthMode.none),
        config=RuntimeOdbcEndpointConfig(host="127.0.0.1", port=0, max_rows=500),
    )
    await endpoint.start()
    try:
        port = endpoint.bound_port
        assert port is not None
        show_rows, rows = await asyncio.to_thread(_run_psycopg_queries, port)
        assert show_rows == [("public",)]
        assert rows == [(1, 12.5)]
    finally:
        await endpoint.close()

    assert len(host.calls) == 1
    assert "public." not in host.calls[0].query.lower()


def _run_psycopg_queries(port: int) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    with psycopg.connect(
        host="127.0.0.1",
        port=port,
        dbname="langbridge",
        user="runtime",
        autocommit=True,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("show search_path")
            show_rows = cursor.fetchall()
            cursor.execute("select id, total from public.orders")
            rows = cursor.fetchall()
    return show_rows, rows
