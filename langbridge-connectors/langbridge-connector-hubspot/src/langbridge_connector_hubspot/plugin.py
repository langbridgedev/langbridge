
from langbridge.connectors.base.config import ConnectorCapabilities, ConnectorFamily, ConnectorRuntimeType
from langbridge.plugins import ConnectorPlugin, register_connector_plugin

from .config import (
    HUBSPOT_AUTH_SCHEMA,
    HUBSPOT_SUPPORTED_RESOURCES,
    HUBSPOT_SYNC_STRATEGY,
    HubSpotDeclarativeConnectorConfigFactory,
    HubSpotDeclarativeConnectorConfigSchemaFactory,
)
from .connector import HubSpotDeclarativeApiConnector

PLUGIN = register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.HUBSPOT,
        connector_family=ConnectorFamily.API,
        capabilities=ConnectorCapabilities(
            supports_synced_datasets=True,
            supports_incremental_sync=True,
        ),
        supported_resources=HUBSPOT_SUPPORTED_RESOURCES,
        auth_schema=HUBSPOT_AUTH_SCHEMA,
        sync_strategy=HUBSPOT_SYNC_STRATEGY,
        config_factory=HubSpotDeclarativeConnectorConfigFactory,
        config_schema_factory=HubSpotDeclarativeConnectorConfigSchemaFactory,
        api_connector_class=HubSpotDeclarativeApiConnector,
    )
)


def get_connector_plugin() -> ConnectorPlugin:
    return PLUGIN


def register_plugin() -> ConnectorPlugin:
    return PLUGIN
