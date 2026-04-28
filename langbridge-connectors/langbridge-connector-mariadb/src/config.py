from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class MariaDBConnectorConfig(BaseConnectorConfig):
    host: str
    port: int = 3306
    database: str
    user: str
    password: str
    ssl_mode: str | None = None


class MariaDBConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.MARIADB

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return MariaDBConnectorConfig(**config)


class MariaDBConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.MARIADB

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="MariaDB",
            description="Connect to a MariaDB database.",
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
                    description="Database port number.",
                    type="number",
                    required=True,
                    default="3306",
                ),
                ConnectorConfigEntrySchema(
                    field="database",
                    label="Database",
                    description="Database name.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="user",
                    label="User",
                    description="Database user.",
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
                    field="ssl_mode",
                    label="SSL mode",
                    description="Optional SSL mode.",
                    type="string",
                    required=False,
                    value_list=["disabled", "preferred", "required", "verify_ca", "verify_identity"],
                ),
            ],
        )
