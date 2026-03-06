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

SALESFORCE_SUPPORTED_RESOURCES = ("accounts", "contacts", "leads", "opportunities")
SALESFORCE_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="instance_url",
        label="Instance URL",
        description="Salesforce instance URL.",
        type="string",
        required=True,
    ),
    ConnectorAuthFieldSchema(
        field="client_id",
        label="Client ID",
        description="Connected app client ID.",
        type="string",
        required=True,
    ),
    ConnectorAuthFieldSchema(
        field="client_secret",
        label="Client Secret",
        description="Connected app client secret.",
        type="password",
        required=True,
        secret=True,
    ),
    ConnectorAuthFieldSchema(
        field="refresh_token",
        label="Refresh Token",
        description="OAuth refresh token for API access.",
        type="password",
        required=True,
        secret=True,
    ),
)
SALESFORCE_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL


class SalesforceConnectorConfig(BaseConnectorConfig):
    instance_url: str
    client_id: str
    client_secret: str
    refresh_token: str
    api_version: str = "v61.0"


class SalesforceConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.SALESFORCE

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return SalesforceConnectorConfig(**config)


class SalesforceConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.SALESFORCE

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Salesforce",
            description="Connect to Salesforce objects and ingest them as datasets.",
            version="0.1.0",
            label="Salesforce",
            icon="salesforce.png",
            connector_type="api",
            config=[
                ConnectorConfigEntrySchema(
                    field="instance_url",
                    label="Instance URL",
                    description="Salesforce instance URL.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="client_id",
                    label="Client ID",
                    description="Connected app client ID.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="client_secret",
                    label="Client Secret",
                    description="Connected app client secret.",
                    type="password",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="refresh_token",
                    label="Refresh Token",
                    description="OAuth refresh token for API access.",
                    type="password",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="api_version",
                    label="API Version",
                    description="Optional Salesforce REST API version.",
                    type="string",
                    required=False,
                    default="v61.0",
                ),
            ],
            plugin_metadata=ConnectorPluginMetadata(
                connector_type=ConnectorRuntimeType.SALESFORCE.value,
                connector_family=ConnectorFamily.API,
                supported_resources=list(SALESFORCE_SUPPORTED_RESOURCES),
                auth_schema=list(SALESFORCE_AUTH_SCHEMA),
                sync_strategy=SALESFORCE_SYNC_STRATEGY,
            ),
        )
