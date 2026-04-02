from pydantic import model_validator

from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class AzureBlobStorageConnectorConfig(BaseConnectorConfig):
    connection_string: str | None = None
    account_name: str | None = None
    account_url: str | None = None
    account_key: str | None = None
    sas_token: str | None = None
    use_credential_chain: bool = False
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    @model_validator(mode="after")
    def _validate_auth(self) -> "AzureBlobStorageConnectorConfig":
        if self.account_key and not (self.account_name or self.account_url):
            raise ValueError("Azure Blob storage connector requires account_name or account_url with account_key.")
        if self.client_secret and not (self.account_name and self.client_id and self.tenant_id):
            raise ValueError(
                "Azure Blob service principal auth requires account_name, tenant_id, client_id, and client_secret."
            )
        return self


class AzureBlobStorageConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.AZURE_BLOB

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return AzureBlobStorageConnectorConfig(**config)


class AzureBlobStorageConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.AZURE_BLOB

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Azure Blob Storage",
            description="Azure Blob / ADLS-compatible storage connector.",
            version="1.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="connection_string",
                    label="Connection String",
                    description="Azure Storage connection string.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="account_name",
                    label="Account Name",
                    description="Azure storage account name.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="account_url",
                    label="Account URL",
                    description="Optional Azure Blob account URL override.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="account_key",
                    label="Account Key",
                    description="Azure storage account key.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="sas_token",
                    label="SAS Token",
                    description="Optional SAS token for SDK access.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="use_credential_chain",
                    label="Use Credential Chain",
                    description="Use DuckDB and Azure SDK default credential chains.",
                    type="boolean",
                    required=False,
                    default="false",
                ),
                ConnectorConfigEntrySchema(
                    field="tenant_id",
                    label="Tenant ID",
                    description="Tenant id for Azure service principal auth.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="client_id",
                    label="Client ID",
                    description="Client id for Azure service principal auth.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="client_secret",
                    label="Client Secret",
                    description="Client secret for Azure service principal auth.",
                    type="password",
                    required=False,
                ),
            ],
        )
