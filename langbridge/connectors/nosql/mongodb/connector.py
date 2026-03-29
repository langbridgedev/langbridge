
from typing import Any, Mapping, Sequence

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import NoSqlConnector, run_sync
from langbridge.connectors.base.errors import ConnectorError

from .config import MongoDBConnectorConfig


class MongoDBConnector(NoSqlConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.MONGODB

    def __init__(
        self,
        config: MongoDBConnectorConfig,
        logger=None,
        *,
        client_factory: Any | None = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._client_factory = client_factory

    async def test_connection(self) -> None:
        await run_sync(self._with_database, lambda database: database.command("ping"))

    async def list_collections(self) -> list[str]:
        return await run_sync(
            self._with_database,
            lambda database: database.list_collection_names(),
        )

    async def _query_documents(
        self,
        *,
        collection: str,
        query: Mapping[str, Any] | None = None,
        projection: Sequence[str] | Mapping[str, int | bool] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
        limit: int | None = 100,
    ) -> list[dict[str, Any]]:
        return await run_sync(
            self._query_documents_sync,
            collection,
            query,
            projection,
            sort,
            limit,
        )

    def _query_documents_sync(
        self,
        collection: str,
        query: Mapping[str, Any] | None,
        projection: Sequence[str] | Mapping[str, int | bool] | None,
        sort: Sequence[tuple[str, int]] | None,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        def _run(database: Any) -> list[dict[str, Any]]:
            projection_payload: Mapping[str, int | bool] | None
            if projection is None:
                projection_payload = None
            elif isinstance(projection, Mapping):
                projection_payload = projection
            else:
                projection_payload = {field: True for field in projection}

            cursor = database[collection].find(
                filter=dict(query or {}),
                projection=projection_payload,
            )
            if sort:
                cursor = cursor.sort(list(sort))
            if limit is not None:
                cursor = cursor.limit(max(1, int(limit)))
            return [dict(document) for document in cursor]

        return self._with_database(_run)

    def _build_client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory()

        try:
            from pymongo import MongoClient
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise ConnectorError(
                "pymongo is required to use the MongoDB connector."
            ) from exc

        config = self.config
        kwargs: dict[str, Any] = {}
        if config.username:
            kwargs["username"] = config.username
        if config.password:
            kwargs["password"] = config.password
        if config.auth_source:
            kwargs["authSource"] = config.auth_source

        return MongoClient(config.connection_uri, **kwargs)

    def _with_database(self, fn):
        client = self._build_client()
        try:
            database = client[self.config.database]
            return fn(database)
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
