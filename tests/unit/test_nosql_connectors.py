from __future__ import annotations

import uuid

import pytest

from langbridge.connectors.nosql.mongodb import (
    MongoDBConnectorConfigFactory as LegacyMongoDBConnectorConfigFactory,
)
from langbridge.connectors.base import (
    NoSqlConnectorFactory,
    SqlConnectorFactory,
)
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.nosql.mongodb import (
    MongoDBConnector,
    MongoDBConnectorConfig,
    MongoDBConnectorConfigFactory,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class _Cursor:
    def __init__(self, documents: list[dict[str, object]]) -> None:
        self._documents = documents

    def sort(self, _spec: list[tuple[str, int]]) -> "_Cursor":
        return self

    def limit(self, limit: int) -> "_Cursor":
        self._documents = self._documents[:limit]
        return self

    def __iter__(self):
        return iter(self._documents)


class _Collection:
    def __init__(self, documents: list[dict[str, object]]) -> None:
        self._documents = documents

    def find(self, *, filter: dict[str, object], projection=None) -> _Cursor:
        assert filter == {"active": True}
        assert projection == {"name": True}
        return _Cursor(self._documents)


class _Database:
    def __init__(self, collection: _Collection) -> None:
        self._collection = collection

    def command(self, name: str) -> dict[str, int]:
        assert name == "ping"
        return {"ok": 1}

    def list_collection_names(self) -> list[str]:
        return ["users"]

    def __getitem__(self, name: str) -> _Collection:
        assert name == "users"
        return self._collection


class _Client:
    def __init__(self, database: _Database) -> None:
        self._database = database
        self.closed = False

    def __getitem__(self, name: str) -> _Database:
        assert name == "analytics"
        return self._database

    def close(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_mongodb_connector_queries_documents_via_nosql_base() -> None:
    client = _Client(
        _Database(
            _Collection(
                [
                    {
                        "_id": uuid.uuid4(),
                        "name": "Ada",
                        "profile": {"country": "UK"},
                    }
                ]
            )
        )
    )
    connector = MongoDBConnector(
        config=MongoDBConnectorConfig(
            connection_uri="mongodb://localhost:27017",
            database="analytics",
        ),
        client_factory=lambda: client,
    )

    await connector.test_connection()
    assert await connector.list_collections() == ["users"]

    result = await connector.query_documents(
        collection="users",
        query={"active": True},
        projection=["name"],
        limit=1,
    )

    assert result.collection == "users"
    assert result.rowcount == 1
    assert result.documents[0]["name"] == "Ada"
    assert isinstance(result.documents[0]["_id"], str)


def test_mongodb_connector_registration_uses_nosql_factory() -> None:
    connector_class = NoSqlConnectorFactory.get_no_sql_connector_class_reference(
        ConnectorRuntimeType.MONGODB
    )

    assert connector_class is MongoDBConnector
    with pytest.raises(ValueError, match="No SQL connector found"):
        SqlConnectorFactory.get_sql_connector_class_reference(
            ConnectorRuntimeType.MONGODB
        )


def test_legacy_mongodb_config_import_is_a_compatibility_alias() -> None:
    assert LegacyMongoDBConnectorConfigFactory is MongoDBConnectorConfigFactory
