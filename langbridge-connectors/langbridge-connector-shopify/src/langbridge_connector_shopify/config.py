
from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorAuthFieldSchema,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)
from langbridge.connectors.saas.declarative import (
    build_declarative_config_entries,
    build_declarative_plugin_metadata,
    load_declarative_connector_manifest,
)

_MANIFEST = load_declarative_connector_manifest(
    "langbridge_connector_shopify.manifests",
    "shopify.yaml",
)


class ShopifyDeclarativeConnectorConfig(BaseConnectorConfig):
    access_token: str
    shop_domain: str
    api_base_url: str | None = None


class ShopifyDeclarativeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.SHOPIFY

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return ShopifyDeclarativeConnectorConfig(**config)


class ShopifyDeclarativeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.SHOPIFY

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name=_MANIFEST.display_name,
            description=(
                f"{_MANIFEST.description} The package derives the shop-specific Admin API "
                "base URL from `shop_domain` unless `api_base_url` is explicitly overridden."
            ),
            version=_MANIFEST.schema_version,
            config=[
                ConnectorConfigEntrySchema(
                    field="shop_domain",
                    label="Shop Domain",
                    description="Shopify shop domain, for example `acme.myshopify.com`.",
                    type="string",
                    required=True,
                ),
                *build_declarative_config_entries(
                    _MANIFEST,
                    token_description="Shopify Admin API access token.",
                    include_base_url=False,
                ),
                ConnectorConfigEntrySchema(
                    field="api_base_url",
                    label="API Base URL",
                    description=(
                        "Optional Shopify Admin API base URL override. Defaults to "
                        "`https://{shop_domain}/admin/api/2026-01`."
                    ),
                    type="string",
                    required=False,
                ),
            ],
            plugin_metadata=build_declarative_plugin_metadata(
                _MANIFEST,
                connector_type=ConnectorRuntimeType.SHOPIFY,
                auth_schema=SHOPIFY_AUTH_SCHEMA,
                sync_strategy=ConnectorSyncStrategy.INCREMENTAL,
            ),
        )


SHOPIFY_MANIFEST = _MANIFEST
SHOPIFY_SUPPORTED_RESOURCES = _MANIFEST.resource_keys
SHOPIFY_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL
SHOPIFY_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="shop_domain",
        label="Shop Domain",
        description="Shopify shop domain, for example `acme.myshopify.com`.",
        type="string",
        required=True,
        secret=False,
    ),
    ConnectorAuthFieldSchema(
        field="access_token",
        label="Access Token",
        description="Shopify Admin API access token.",
        type="password",
        required=True,
        secret=True,
    ),
)
