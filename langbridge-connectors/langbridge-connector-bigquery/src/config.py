from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class BigQueryConnectorConfig(BaseConnectorConfig):
    project_id: str
    dataset: str
    credentials_json: str
    location: str | None = None


class BigQueryConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.BIGQUERY

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return BigQueryConnectorConfig(**config)


class BigQueryConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.BIGQUERY

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Google BigQuery",
            description="Connect to a Google BigQuery project using a service account.",
            version="1.0.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="project_id",
                    label="Project ID",
                    description="Google Cloud project identifier.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="dataset",
                    label="Dataset",
                    description="Default dataset to use.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="credentials_json",
                    label="Service account key",
                    description="Paste the JSON credentials for the service account.",
                    type="textarea",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="location",
                    label="Location",
                    description="Optional dataset location (for example: US or EU).",
                    type="string",
                    required=False,
                ),
            ],
        )
