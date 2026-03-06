from langbridge.packages.connectors.langbridge_connectors.api.config import (
    ConnectorFamily,
    ConnectorRuntimeType,
)
from langbridge.packages.connectors.langbridge_connectors.api.registry import (
    ConnectorPlugin,
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
