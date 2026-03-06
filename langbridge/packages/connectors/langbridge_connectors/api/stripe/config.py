from langbridge.packages.connectors.langbridge_connectors.api.config import (
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

STRIPE_SUPPORTED_RESOURCES = ("customers", "charges", "invoices", "subscriptions")
STRIPE_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="api_key",
        label="API Key",
        description="Stripe secret API key.",
        type="password",
        required=True,
        secret=True,
    ),
    ConnectorAuthFieldSchema(
        field="account_id",
        label="Connected Account ID",
        description="Optional Stripe connected account identifier.",
        type="string",
        required=False,
    ),
)
STRIPE_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL


class StripeConnectorConfig(BaseConnectorConfig):
    api_key: str
    account_id: str | None = None
    api_base_url: str = "https://api.stripe.com"


class StripeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.STRIPE

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return StripeConnectorConfig(**config)


class StripeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.STRIPE

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Stripe",
            description="Connect to Stripe resources and ingest them as datasets.",
            version="0.1.0",
            label="Stripe",
            icon="stripe.png",
            connector_type="api",
            config=[
                ConnectorConfigEntrySchema(
                    field="api_key",
                    label="API Key",
                    description="Stripe secret API key.",
                    type="password",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="account_id",
                    label="Connected Account ID",
                    description="Optional Stripe connected account identifier.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="api_base_url",
                    label="API Base URL",
                    description="Optional Stripe API base URL override.",
                    type="string",
                    required=False,
                    default="https://api.stripe.com",
                ),
            ],
            plugin_metadata=ConnectorPluginMetadata(
                connector_type=ConnectorRuntimeType.STRIPE.value,
                connector_family=ConnectorFamily.API,
                supported_resources=list(STRIPE_SUPPORTED_RESOURCES),
                auth_schema=list(STRIPE_AUTH_SCHEMA),
                sync_strategy=STRIPE_SYNC_STRATEGY,
            ),
        )
