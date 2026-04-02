from pydantic import model_validator

from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class S3StorageConnectorConfig(BaseConnectorConfig):
    region_name: str | None = None
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    profile_name: str | None = None
    url_style: str | None = None
    use_ssl: bool = True

    @model_validator(mode="after")
    def _validate_credentials(self) -> "S3StorageConnectorConfig":
        if bool(self.access_key_id) != bool(self.secret_access_key):
            raise ValueError("S3 storage connector requires both access_key_id and secret_access_key together.")
        return self


class S3StorageConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.S3

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return S3StorageConnectorConfig(**config)


class S3StorageConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.S3

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="S3 Storage",
            description="Amazon S3-compatible object storage connector.",
            version="1.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="region_name",
                    label="Region",
                    description="AWS region used for S3 access.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="endpoint_url",
                    label="Endpoint URL",
                    description="Optional S3-compatible endpoint override.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="access_key_id",
                    label="Access Key ID",
                    description="Static AWS access key id for explicit credential configuration.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="secret_access_key",
                    label="Secret Access Key",
                    description="Static AWS secret access key for explicit credential configuration.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="session_token",
                    label="Session Token",
                    description="Optional AWS session token for temporary credentials.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="profile_name",
                    label="Profile Name",
                    description="Optional local AWS profile name for SDK operations.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="url_style",
                    label="URL Style",
                    description="Optional S3 URL style override such as path or vhost.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="use_ssl",
                    label="Use SSL",
                    description="Whether to use TLS for S3 requests.",
                    type="boolean",
                    required=False,
                    default="true",
                ),
            ],
        )
