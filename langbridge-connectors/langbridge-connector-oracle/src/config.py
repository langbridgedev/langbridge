from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class OracleConnectorConfig(BaseConnectorConfig):
    host: str
    port: int = 1521
    service_name: str
    username: str
    password: str
    wallet_path: str | None = None


class OracleConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.ORACLE

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return OracleConnectorConfig(**config)


class OracleConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.ORACLE

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Oracle Database",
            description="Connect to an Oracle database using a service name.",
            version="1.0.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="host",
                    label="Host",
                    description="Database host address.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="port",
                    label="Port",
                    description="Database listener port.",
                    type="number",
                    required=True,
                    default="1521",
                ),
                ConnectorConfigEntrySchema(
                    field="service_name",
                    label="Service name",
                    description="Oracle service name or SID.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="username",
                    label="Username",
                    description="Database username.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="password",
                    label="Password",
                    description="Database password.",
                    type="password",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="wallet_path",
                    label="Wallet path",
                    description="Optional Oracle wallet directory for TLS connections.",
                    type="string",
                    required=False,
                ),
            ],
        )
