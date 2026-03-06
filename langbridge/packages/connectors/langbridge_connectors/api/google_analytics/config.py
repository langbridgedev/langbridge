from typing import Any

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

GOOGLE_ANALYTICS_SUPPORTED_RESOURCES = (
    "events",
    "pages",
    "sessions",
    "traffic_sources",
)
GOOGLE_ANALYTICS_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="property_id",
        label="Property ID",
        description="Google Analytics property identifier.",
        type="string",
        required=True,
    ),
    ConnectorAuthFieldSchema(
        field="credentials_json",
        label="Service Account Credentials JSON",
        description="Serialized Google service account credentials JSON.",
        type="json",
        required=True,
        secret=True,
    ),
)
GOOGLE_ANALYTICS_SYNC_STRATEGY = ConnectorSyncStrategy.WINDOWED_INCREMENTAL


class GoogleAnalyticsConnectorConfig(BaseConnectorConfig):
    property_id: str
    credentials_json: str | dict[str, Any]
    api_base_url: str = "https://analyticsdata.googleapis.com"


class GoogleAnalyticsConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.GOOGLE_ANALYTICS

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return GoogleAnalyticsConnectorConfig(**config)


class GoogleAnalyticsConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.GOOGLE_ANALYTICS

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Google Analytics",
            description="Connect to Google Analytics reporting resources and ingest them as datasets.",
            version="0.1.0",
            label="Google Analytics",
            icon="google-analytics.png",
            connector_type="api",
            config=[
                ConnectorConfigEntrySchema(
                    field="property_id",
                    label="Property ID",
                    description="Google Analytics property identifier.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="credentials_json",
                    label="Service Account Credentials JSON",
                    description="Serialized Google service account credentials JSON.",
                    type="json",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="api_base_url",
                    label="API Base URL",
                    description="Optional Google Analytics Data API base URL override.",
                    type="string",
                    required=False,
                    default="https://analyticsdata.googleapis.com",
                ),
            ],
            plugin_metadata=ConnectorPluginMetadata(
                connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS.value,
                connector_family=ConnectorFamily.API,
                supported_resources=list(GOOGLE_ANALYTICS_SUPPORTED_RESOURCES),
                auth_schema=list(GOOGLE_ANALYTICS_AUTH_SCHEMA),
                sync_strategy=GOOGLE_ANALYTICS_SYNC_STRATEGY,
            ),
        )
