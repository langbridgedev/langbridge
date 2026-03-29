from langbridge.plugins import (
    ConnectorCapabilities,
    ConnectorPlugin,
    ConnectorFamily,
    ConnectorRuntimeType,
    register_connector_plugin,
)

from .config import (
    SHOPIFY_AUTH_SCHEMA,
    SHOPIFY_SUPPORTED_RESOURCES,
    SHOPIFY_SYNC_STRATEGY,
    ShopifyConnectorConfig,
    ShopifyConnectorConfigFactory,
    ShopifyConnectorConfigSchemaFactory,
)
from .connector import ShopifyApiConnector

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.SHOPIFY,
        connector_family=ConnectorFamily.API,
        capabilities=ConnectorCapabilities(
            supports_synced_datasets=True,
            supports_incremental_sync=True,
        ),
        supported_resources=SHOPIFY_SUPPORTED_RESOURCES,
        auth_schema=SHOPIFY_AUTH_SCHEMA,
        sync_strategy=SHOPIFY_SYNC_STRATEGY,
        config_factory=ShopifyConnectorConfigFactory,
        config_schema_factory=ShopifyConnectorConfigSchemaFactory,
        api_connector_class=ShopifyApiConnector,
    )
)

__all__ = [
    "ShopifyApiConnector",
    "ShopifyConnectorConfig",
    "ShopifyConnectorConfigFactory",
    "ShopifyConnectorConfigSchemaFactory",
]
