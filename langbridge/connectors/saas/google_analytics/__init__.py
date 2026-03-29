from langbridge.plugins import (
    ConnectorCapabilities,
    ConnectorPlugin,
    ConnectorFamily,
    ConnectorRuntimeType,
    register_connector_plugin,
)

from .config import (
    GOOGLE_ANALYTICS_AUTH_SCHEMA,
    GOOGLE_ANALYTICS_SUPPORTED_RESOURCES,
    GOOGLE_ANALYTICS_SYNC_STRATEGY,
    GoogleAnalyticsConnectorConfig,
    GoogleAnalyticsConnectorConfigFactory,
    GoogleAnalyticsConnectorConfigSchemaFactory,
)
from .connector import GoogleAnalyticsApiConnector

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS,
        connector_family=ConnectorFamily.API,
        capabilities=ConnectorCapabilities(
            supports_synced_datasets=True,
            supports_incremental_sync=False,
        ),
        supported_resources=GOOGLE_ANALYTICS_SUPPORTED_RESOURCES,
        auth_schema=GOOGLE_ANALYTICS_AUTH_SCHEMA,
        sync_strategy=GOOGLE_ANALYTICS_SYNC_STRATEGY,
        config_factory=GoogleAnalyticsConnectorConfigFactory,
        config_schema_factory=GoogleAnalyticsConnectorConfigSchemaFactory,
        api_connector_class=GoogleAnalyticsApiConnector,
    )
)

__all__ = [
    "GoogleAnalyticsApiConnector",
    "GoogleAnalyticsConnectorConfig",
    "GoogleAnalyticsConnectorConfigFactory",
    "GoogleAnalyticsConnectorConfigSchemaFactory",
]
