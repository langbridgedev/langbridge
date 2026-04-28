from langbridge.plugins import (
    ConnectorCapabilities,
    ConnectorFamily,
    ConnectorPlugin,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
    register_connector_plugin,
)

from .config import (
    BasicHttpConnectorConfig,
    BasicHttpConnectorConfigFactory,
    BasicHttpConnectorConfigSchemaFactory,
)
from .connector import BasicHttpConnector

PLUGIN = register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.BASIC_HTTP,
        connector_family=ConnectorFamily.API,
        capabilities=ConnectorCapabilities(
            supports_live_datasets=True,
            supports_synced_datasets=True,
            supports_incremental_sync=True,
            supports_federated_execution=True,
        ),
        supported_resources=(),
        auth_schema=(),
        default_sync_strategy=ConnectorSyncStrategy.FULL_REFRESH,
        config_factory=BasicHttpConnectorConfigFactory,
        config_schema_factory=BasicHttpConnectorConfigSchemaFactory,
        api_connector_class=BasicHttpConnector,
    )
)

__all__ = [
    "BasicHttpConnector",
    "BasicHttpConnectorConfig",
    "BasicHttpConnectorConfigFactory",
    "BasicHttpConnectorConfigSchemaFactory",
    "PLUGIN",
    "get_connector_plugin",
    "register_plugin",
]


def get_connector_plugin() -> ConnectorPlugin:
    return PLUGIN


def register_plugin() -> ConnectorPlugin:
    return PLUGIN
