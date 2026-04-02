from pydantic import model_validator

from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class GcsStorageConnectorConfig(BaseConnectorConfig):
    project: str | None = None
    service_account_json_path: str | None = None
    hmac_key_id: str | None = None
    hmac_secret: str | None = None
    use_credential_chain: bool = False
    endpoint_url: str | None = None
    use_ssl: bool = True

    @model_validator(mode="after")
    def _validate_hmac(self) -> "GcsStorageConnectorConfig":
        if bool(self.hmac_key_id) != bool(self.hmac_secret):
            raise ValueError("GCS storage connector requires both hmac_key_id and hmac_secret together.")
        return self


class GcsStorageConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.GCS

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return GcsStorageConnectorConfig(**config)


class GcsStorageConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.GCS

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="GCS Storage",
            description="Google Cloud Storage connector.",
            version="1.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="project",
                    label="Project",
                    description="Optional Google Cloud project id for SDK operations.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="service_account_json_path",
                    label="Service Account JSON Path",
                    description="Optional service account JSON path for SDK operations.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="hmac_key_id",
                    label="HMAC Key ID",
                    description="HMAC key id used by DuckDB CONFIG secrets for GCS parquet access.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="hmac_secret",
                    label="HMAC Secret",
                    description="HMAC secret used by DuckDB CONFIG secrets for GCS parquet access.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="use_credential_chain",
                    label="Use Credential Chain",
                    description="Use DuckDB credential_chain provider for GCS parquet access.",
                    type="boolean",
                    required=False,
                    default="false",
                ),
                ConnectorConfigEntrySchema(
                    field="endpoint_url",
                    label="Endpoint URL",
                    description="Optional GCS-compatible endpoint override.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="use_ssl",
                    label="Use SSL",
                    description="Whether to use TLS for GCS requests.",
                    type="boolean",
                    required=False,
                    default="true",
                ),
            ],
        )
