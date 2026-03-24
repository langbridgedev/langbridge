from pydantic import Field

from langbridge.config import settings
from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorAuthFieldSchema,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorFamily,
    ConnectorPluginMetadata,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)

SHOPIFY_SUPPORTED_RESOURCES = ("orders", "customers", "products")
SHOPIFY_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="shop_domain",
        label="Shop Domain",
        description="Shopify shop domain, for example `acme.myshopify.com`.",
        type="string",
        required=True,
    ),
    ConnectorAuthFieldSchema(
        field="shopify_app_client_id",
        label="Shopify App Client ID",
        description="Client ID of the Shopify app used for authentication. This app must have the necessary permissions to access the desired resources.",
        type="string",
        required=True,
    ),
    ConnectorAuthFieldSchema(
        field="shopify_app_client_secret",
        label="Shopify App Client Secret",
        description="Client Secret of the Shopify app used for authentication.",
        type="string",
        required=True,
    ),
)
SHOPIFY_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL


class ShopifyConnectorConfig(BaseConnectorConfig):
    shop_domain: str
    shopify_app_client_id: str = Field(default_factory=lambda: settings.SHOPIFY_APP_CLIENT_ID)
    shopify_app_client_secret: str = Field(default_factory=lambda: settings.SHOPIFY_APP_CLIENT_SECRET)


class ShopifyConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.SHOPIFY

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return ShopifyConnectorConfig(**config)


class ShopifyConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.SHOPIFY

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Shopify",
            description="Connect to Shopify Admin API resources and ingest them as datasets.",
            version="0.1.0",
            config=[        
                ConnectorConfigEntrySchema(
                    field="shop_domain",
                    label="Shop Domain",
                    description="Shopify shop domain, for example `acme.myshopify.com`.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="shopify_app_client_id",
                    label="Shopify App Client ID",
                    description="Client ID of the Shopify app used for authentication. This app must have the necessary permissions to access the desired resources.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="shopify_app_client_secret",
                    label="Shopify App Client Secret",
                    description="Client Secret of the Shopify app used for authentication.",
                    type="string",
                    required=True,
                ),
            ],
            plugin_metadata=ConnectorPluginMetadata(
                connector_type=ConnectorRuntimeType.SHOPIFY.value,
                connector_family=ConnectorFamily.API,
                supported_resources=list(SHOPIFY_SUPPORTED_RESOURCES),
                auth_schema=list(SHOPIFY_AUTH_SCHEMA),
                sync_strategy=SHOPIFY_SYNC_STRATEGY,
            ),
        )
