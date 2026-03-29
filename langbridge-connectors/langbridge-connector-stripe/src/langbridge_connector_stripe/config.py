
from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)
from langbridge.connectors.saas.declarative import (
    build_declarative_auth_schema,
    build_declarative_connector_config_schema,
    load_declarative_connector_manifest,
)

_MANIFEST = load_declarative_connector_manifest(
    "langbridge_connector_stripe.manifests",
    "stripe.yaml",
)


class StripeDeclarativeConnectorConfig(BaseConnectorConfig):
    api_key: str
    account_id: str | None = None
    api_base_url: str = "https://api.stripe.com"


class StripeDeclarativeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.STRIPE

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return StripeDeclarativeConnectorConfig(**config)


class StripeDeclarativeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.STRIPE

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return build_declarative_connector_config_schema(
            _MANIFEST,
            connector_type=ConnectorRuntimeType.STRIPE,
            sync_strategy=ConnectorSyncStrategy.INCREMENTAL,
            auth_schema=STRIPE_AUTH_SCHEMA,
            description_suffix=(
                "Resources declared in the manifest execute through the core declarative "
                "HTTP SaaS runtime and materialize runtime-managed datasets through sync."
            ),
            token_description="Stripe secret API key used for bearer-token authentication.",
            field_labels={"account_id": "Connected Account ID"},
            field_descriptions={
                "account_id": "Optional Stripe connected account identifier for Stripe-Account routing.",
            },
            base_url_description="Optional Stripe API base URL override.",
        )


STRIPE_MANIFEST = _MANIFEST
STRIPE_SUPPORTED_RESOURCES = _MANIFEST.resource_keys
STRIPE_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL
STRIPE_AUTH_SCHEMA = build_declarative_auth_schema(
    _MANIFEST,
    token_description="Stripe secret API key.",
)
