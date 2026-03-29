
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
    "langbridge_connector_hubspot.manifests",
    "hubspot.yaml",
)


class HubSpotDeclarativeConnectorConfig(BaseConnectorConfig):
    access_token: str
    api_base_url: str = "https://api.hubapi.com"


class HubSpotDeclarativeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.HUBSPOT

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return HubSpotDeclarativeConnectorConfig(**config)


class HubSpotDeclarativeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.HUBSPOT

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return build_declarative_connector_config_schema(
            _MANIFEST,
            connector_type=ConnectorRuntimeType.HUBSPOT,
            sync_strategy=ConnectorSyncStrategy.INCREMENTAL,
            auth_schema=HUBSPOT_AUTH_SCHEMA,
            description_suffix=(
                "Manifest-defined HubSpot CRM resources execute through the core declarative "
                "HTTP SaaS runtime."
            ),
            token_description="HubSpot private app token used for bearer-token authentication.",
            base_url_description="Optional HubSpot API base URL override.",
        )


HUBSPOT_MANIFEST = _MANIFEST
HUBSPOT_SUPPORTED_RESOURCES = _MANIFEST.resource_keys
HUBSPOT_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL
HUBSPOT_AUTH_SCHEMA = build_declarative_auth_schema(
    _MANIFEST,
    token_description="HubSpot private app access token.",
)
