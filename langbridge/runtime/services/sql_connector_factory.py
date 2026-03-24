
import logging
from typing import Any

from runtime.services.errors import ExecutionValidationError
from langbridge.connectors.base import (
    SqlConnector,
    SqlConnectorFactory,
    get_connector_config_factory,
)
from connectors.base.config import ConnectorRuntimeType

class SqlConnectorFactory:
    @staticmethod
    async def create_sql_connector(
        *,
        connector_type: ConnectorRuntimeType,
        connector_config: dict[str, Any],
        sql_connector_factory: SqlConnectorFactory,
        logger: logging.Logger,
    ) -> SqlConnector:
        try:
            sql_connector_factory.get_sql_connector_class_reference(connector_type)
        except ValueError as exc:
            raise ExecutionValidationError(
                f"Connector type {connector_type.value} does not support SQL operations."
            ) from exc
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_config.get("config", {}))
        sql_connector = sql_connector_factory.create_sql_connector(
            connector_type,
            config_instance,
            logger=logger,
        )
        await sql_connector.test_connection()
        return sql_connector