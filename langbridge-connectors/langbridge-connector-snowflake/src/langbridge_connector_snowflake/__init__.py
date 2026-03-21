from langbridge.plugins import (
    ConnectorFamily,
    ConnectorPlugin,
    ConnectorRuntimeType,
    register_connector_plugin,
)

from .config import (
    SnowflakeConnectorConfig,
    SnowflakeConnectorConfigFactory,
    SnowflakeConnectorConfigSchemaFactory,
)
from .connector import SnowflakeConnector
from .metadata import SnowflakeMetadataExtractor

PLUGIN = ConnectorPlugin(
    connector_type=ConnectorRuntimeType.SNOWFLAKE,
    connector_family=ConnectorFamily.DATABASE,
    config_factory=SnowflakeConnectorConfigFactory,
    config_schema_factory=SnowflakeConnectorConfigSchemaFactory,
)
register_connector_plugin(PLUGIN)

__all__ = [
    "PLUGIN",
    "SnowflakeConnector",
    "SnowflakeConnectorConfig",
    "SnowflakeConnectorConfigFactory",
    "SnowflakeConnectorConfigSchemaFactory",
    "SnowflakeMetadataExtractor",
    "get_connector_plugin",
]


def get_connector_plugin() -> ConnectorPlugin:
    return PLUGIN
