from langbridge.plugins import (
    ConnectorPlugin,
    ConnectorFamily,
    ConnectorRuntimeType,
    register_connector_plugin,
)

from .config import (
    HUBSPOT_AUTH_SCHEMA,
    HUBSPOT_SUPPORTED_RESOURCES,
    HUBSPOT_SYNC_STRATEGY,
    HubSpotConnectorConfig,
    HubSpotConnectorConfigFactory,
    HubSpotConnectorConfigSchemaFactory,
)
from .connector import HubSpotApiConnector

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.HUBSPOT,
        connector_family=ConnectorFamily.API,
        supported_resources=HUBSPOT_SUPPORTED_RESOURCES,
        auth_schema=HUBSPOT_AUTH_SCHEMA,
        sync_strategy=HUBSPOT_SYNC_STRATEGY,
        config_factory=HubSpotConnectorConfigFactory,
        config_schema_factory=HubSpotConnectorConfigSchemaFactory,
        api_connector_class=HubSpotApiConnector,
    )
)

__all__ = [
    "HubSpotApiConnector",
    "HubSpotConnectorConfig",
    "HubSpotConnectorConfigFactory",
    "HubSpotConnectorConfigSchemaFactory",
]
