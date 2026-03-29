from pydantic import AliasChoices, Field

from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorAuthFieldSchema,
    ConnectorCapabilities,
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
        field="service_key",
        label="Service Key",
        description="HubSpot account service key.",
        type="password",
        required=True,
        secret=True,
    ),
    ConnectorAuthFieldSchema(
        field="portal_id",
        label="Portal ID",
        description="Optional HubSpot account or portal identifier.",
        type="string",
        required=False,
    ),
)
HUBSPOT_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL


class HubSpotConnectorConfig(BaseConnectorConfig):
    service_key: str = Field(validation_alias=AliasChoices("service_key", "access_token"))
    portal_id: str | None = None
    api_base_url: str = "https://api.hubapi.com"

    @property
    def access_token(self) -> str:
        return self.service_key


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
            config=[
                ConnectorConfigEntrySchema(
                    field="service_key",
                    label="Service Key",
                    description="HubSpot account service key.",
                    type="password",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="portal_id",
                    label="Portal ID",
                    description="Optional HubSpot account or portal identifier.",
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
                capabilities=ConnectorCapabilities(
                    supports_synced_datasets=True,
                    supports_incremental_sync=True,
                ),
            ),
        )
