
import pytest

from langbridge.connectors.base.errors import ConnectorError
from langbridge.connectors.builtin.postgres.config import (
    PostgresConnectorConfig,
)
from langbridge.connectors.builtin.postgres.connector import (
    PostgresConnector,
)
from langbridge.connectors.builtin.postgres import connector as postgres_connector_module


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build_connector() -> PostgresConnector:
    return PostgresConnector(
        PostgresConnectorConfig(
            host="db",
            port=5432,
            database="langbridge",
            user="langbridge",
            password="secret",
        )
    )


class _FakeCursor:
    def __init__(self, rows=None, description=None) -> None:
        self.rows = rows or []
        self.description = description or []
        self.executed: list[tuple[str, object]] = []

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))

    async def fetchall(self):
        return self.rows


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    async def __aenter__(self) -> _FakeConnection:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


@pytest.mark.anyio
async def test_postgres_connect_uses_psycopg_async_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _build_connector()
    captured: dict[str, object] = {}
    fake_connection = _FakeConnection(_FakeCursor())

    async def fake_connect(**kwargs):
        captured.update(kwargs)
        return fake_connection

    fake_psycopg = type(
        "FakePsycopg",
        (),
        {"AsyncConnection": type("AsyncConnection", (), {"connect": staticmethod(fake_connect)})},
    )
    monkeypatch.setattr(postgres_connector_module, "psycopg", fake_psycopg)

    result = await connector._connect()

    assert result is fake_connection
    assert captured["host"] == "db"
    assert captured["port"] == 5432
    assert captured["dbname"] == "langbridge"
    assert captured["user"] == "langbridge"
    assert captured["password"] == "secret"
    assert captured["autocommit"] is True


@pytest.mark.anyio
async def test_postgres_test_connection_uses_async_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _build_connector()
    cursor = _FakeCursor()
    fake_connection = _FakeConnection(cursor)

    async def fake_connect():
        return fake_connection

    monkeypatch.setattr(connector, "_connect", fake_connect)

    await connector.test_connection()

    assert cursor.executed == [("SELECT 1", None)]


@pytest.mark.anyio
async def test_postgres_fetch_schemas_raises_if_psycopg_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _build_connector()
    monkeypatch.setattr(postgres_connector_module, "psycopg", None)

    with pytest.raises(ConnectorError, match="psycopg is required for PostgreSQL support."):
        await connector._connect()
