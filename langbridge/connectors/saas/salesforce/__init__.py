from langbridge.plugins import (
    ConnectorCapabilities,
    ConnectorPlugin,
    ConnectorFamily,
    ConnectorRuntimeType,
    register_connector_plugin,
)

from .config import (
    SALESFORCE_AUTH_SCHEMA,
    SALESFORCE_SUPPORTED_RESOURCES,
    SALESFORCE_SYNC_STRATEGY,
    SalesforceConnectorConfig,
    SalesforceConnectorConfigFactory,
    SalesforceConnectorConfigSchemaFactory,
)
from .connector import SalesforceApiConnector

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.SALESFORCE,
        connector_family=ConnectorFamily.API,
        capabilities=ConnectorCapabilities(
            supports_synced_datasets=True,
            supports_incremental_sync=True,
        ),
        supported_resources=SALESFORCE_SUPPORTED_RESOURCES,
        auth_schema=SALESFORCE_AUTH_SCHEMA,
        sync_strategy=SALESFORCE_SYNC_STRATEGY,
        config_factory=SalesforceConnectorConfigFactory,
        config_schema_factory=SalesforceConnectorConfigSchemaFactory,
        api_connector_class=SalesforceApiConnector,
    )
)

__all__ = [
    "SalesforceApiConnector",
    "SalesforceConnectorConfig",
    "SalesforceConnectorConfigFactory",
    "SalesforceConnectorConfigSchemaFactory",
]
