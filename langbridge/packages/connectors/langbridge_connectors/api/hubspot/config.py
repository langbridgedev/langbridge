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

HUBSPOT_SUPPORTED_RESOURCES = ("contacts", "companies", "deals", "tickets")
HUBSPOT_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="access_token",
        label="Private App Token",
        description="HubSpot private app access token.",
        type="password",
        required=True,
        secret=True,
    ),
    ConnectorAuthFieldSchema(
        field="portal_id",
        label="Portal ID",
        description="Optional HubSpot portal identifier.",
        type="string",
        required=False,
    ),
)
HUBSPOT_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL


class HubSpotConnectorConfig(BaseConnectorConfig):
    access_token: str
    portal_id: str | None = None
    api_base_url: str = "https://api.hubapi.com"


class HubSpotConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.HUBSPOT

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return HubSpotConnectorConfig(**config)


class HubSpotConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.HUBSPOT

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="HubSpot",
            description="Connect to HubSpot CRM resources and ingest them as datasets.",
            version="0.1.0",
            label="HubSpot",
            icon="hubspot.png",
            connector_type="api",
            config=[
                ConnectorConfigEntrySchema(
                    field="access_token",
                    label="Private App Token",
                    description="HubSpot private app access token.",
                    type="password",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="portal_id",
                    label="Portal ID",
                    description="Optional HubSpot portal identifier.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="api_base_url",
                    label="API Base URL",
                    description="Optional HubSpot API base URL override.",
                    type="string",
                    required=False,
                    default="https://api.hubapi.com",
                ),
            ],
            plugin_metadata=ConnectorPluginMetadata(
                connector_type=ConnectorRuntimeType.HUBSPOT.value,
                connector_family=ConnectorFamily.API,
                supported_resources=list(HUBSPOT_SUPPORTED_RESOURCES),
                auth_schema=list(HUBSPOT_AUTH_SCHEMA),
                sync_strategy=HUBSPOT_SYNC_STRATEGY,
            ),
        )
