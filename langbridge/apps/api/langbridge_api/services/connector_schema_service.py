import json
import logging
from uuid import UUID

from langbridge.packages.connectors.langbridge_connectors.api import (
    BaseConnectorConfig,
    ColumnMetadata,
    ConnectorRuntimeTypeSqlDialectMap,
    SqlConnector,
    SqlConnectorFactory,
    build_connector_config,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorType
from langbridge.packages.common.langbridge_common.db.connector import Connector
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.repositories.connector_repository import ConnectorRepository


class ConnectorSchemaService:
    def __init__(self, connector_repository: ConnectorRepository) -> None:
        self._connector_repository = connector_repository
        self._sql_connector_factory = SqlConnectorFactory()
        self._logger = logging.getLogger(__name__)

    async def _get_connector(self, connector_id: UUID) -> Connector:
        connector = await self._connector_repository.get_by_id(connector_id)
        if not connector:
            raise BusinessValidationError("Connector not found")
        return connector

    def _build_connector_config(self, connector: Connector) -> BaseConnectorConfig:
        payload = connector.config_json
        config_payload = json.loads(payload if isinstance(payload, str) else payload.value)
        if hasattr(config_payload, "to_dict"):
            config_payload = config_payload.to_dict()
        connector_type = ConnectorType(connector.connector_type)
        return build_connector_config(connector_type, config_payload["config"])

    async def _create_sql_connector(
        self,
        connector_type: ConnectorType,
        config: BaseConnectorConfig,
    ) -> SqlConnector:
        dialect = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
        if dialect is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} does not support schema introspection."
            )
        sql_connector = self._sql_connector_factory.create_sql_connector(
            dialect,
            config,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    async def _get_sql_connector(self, connector_id: UUID) -> SqlConnector:
        connector = await self._get_connector(connector_id)
        connector_type = ConnectorType(connector.connector_type)
        config = self._build_connector_config(connector)
        return await self._create_sql_connector(connector_type, config)

    async def get_schemas(self, connector_id: UUID) -> list[str]:
        sql_connector = await self._get_sql_connector(connector_id)
        return await sql_connector.fetch_schemas()

    async def get_tables(self, connector_id: UUID, schema: str) -> list[str]:
        sql_connector = await self._get_sql_connector(connector_id)
        return await sql_connector.fetch_tables(schema=schema)

    async def get_columns(
        self,
        connector_id: UUID,
        schema: str,
        table: str,
    ) -> list[ColumnMetadata]:
        sql_connector = await self._get_sql_connector(connector_id)
        return await sql_connector.fetch_columns(schema=schema, table=table)
