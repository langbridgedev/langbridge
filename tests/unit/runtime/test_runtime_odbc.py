import asyncio
import socket
import struct
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
async def test_runtime_odbc_gateway_routes_pg_catalog_bootstrap_query_to_metadata_path() -> None:
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
        """
        /*** Load all supported types ***/
        SELECT count(*) AS type_count
        FROM pg_type AS a
        JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
        JOIN pg_proc ON pg_proc.oid = a.typreceive
        LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
        """
    )

    assert result.columns[0]["name"] == "type_count"
    assert host.calls == []


@pytest.mark.anyio
async def test_runtime_odbc_gateway_rewrites_catalog_oid_projection_for_duckdb_compat() -> None:
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
        """
        SELECT pg_type.oid, enumlabel
        FROM pg_enum
        JOIN pg_type ON pg_type.oid = enumtypid
        ORDER BY oid, enumsortorder
        """
    )

    assert result.columns[0]["name"] == "oid"
    assert host.calls == []


@pytest.mark.anyio
async def test_runtime_odbc_endpoint_simple_query_streams_multiple_statement_results() -> None:
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
        message_types = await asyncio.to_thread(_run_simple_query_sequence, port, "select 1; select 2")
        assert message_types.count("T") == 2
        assert message_types.count("D") == 2
        assert message_types.count("C") == 2
        assert message_types[-1] == "Z"
    finally:
        await endpoint.close()


@pytest.mark.anyio
async def test_runtime_odbc_endpoint_extended_query_describe_returns_row_description_but_execute_does_not() -> None:
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
        describe_types, execute_types = await asyncio.to_thread(_run_describe_portal_sequence, port)
        assert "T" in describe_types
        assert "T" not in execute_types
        assert "D" in execute_types
        assert "C" in execute_types
        assert "Z" in execute_types
    finally:
        await endpoint.close()


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


def _run_describe_portal_sequence(port: int) -> tuple[list[str], list[str]]:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.settimeout(0.2)
        _send_startup(sock, user="runtime", database="langbridge")
        _drain_until_ready(sock)
        _send_message(sock, "P", _cstring("stmt1") + _cstring("select id, total from public.orders") + struct.pack("!H", 0))
        _send_message(
            sock,
            "B",
            _cstring("portal1") + _cstring("stmt1") + struct.pack("!H", 0) + struct.pack("!H", 0) + struct.pack("!H", 0),
        )
        _send_message(sock, "D", b"P" + _cstring("portal1"))
        _send_message(sock, "H", b"")
        describe_types = _read_available_messages(sock)
        _send_message(sock, "E", _cstring("portal1") + struct.pack("!I", 0))
        _send_message(sock, "S", b"")
        execute_types = _drain_until_ready(sock)
        return describe_types, execute_types


def _run_simple_query_sequence(port: int, query: str) -> list[str]:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        _send_startup(sock, user="runtime", database="langbridge")
        _drain_until_ready(sock)
        _send_message(sock, "Q", _cstring(query))
        return _drain_until_ready(sock)


def _send_startup(sock: socket.socket, *, user: str, database: str) -> None:
    payload = (
        struct.pack("!I", 196608)
        + _cstring("user")
        + _cstring(user)
        + _cstring("database")
        + _cstring(database)
        + b"\x00"
    )
    sock.sendall(struct.pack("!I", len(payload) + 4) + payload)


def _send_message(sock: socket.socket, message_type: str, payload: bytes) -> None:
    sock.sendall(message_type.encode("ascii") + struct.pack("!I", len(payload) + 4) + payload)


def _drain_until_ready(sock: socket.socket) -> list[str]:
    message_types: list[str] = []
    while True:
        message_type = sock.recv(1).decode("ascii")
        length = struct.unpack("!I", _recv_exact(sock, 4))[0]
        _recv_exact(sock, length - 4)
        message_types.append(message_type)
        if message_type == "Z":
            return message_types


def _read_available_messages(sock: socket.socket) -> list[str]:
    message_types: list[str] = []
    while True:
        try:
            message_type = sock.recv(1)
        except socket.timeout:
            return message_types
        if not message_type:
            raise EOFError("Socket closed before the PostgreSQL message stream completed.")
        length = struct.unpack("!I", _recv_exact(sock, 4))[0]
        _recv_exact(sock, length - 4)
        message_types.append(message_type.decode("ascii"))


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise EOFError("Socket closed before the PostgreSQL message was complete.")
        chunks.extend(chunk)
    return bytes(chunks)


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"
