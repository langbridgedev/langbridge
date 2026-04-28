from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class RedshiftConnectorConfig(BaseConnectorConfig):
    host: str
    port: int = 5439
    database: str
    user: str
    password: str
    cluster_identifier: str | None = None
    ssl: bool | None = None


class RedshiftConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.REDSHIFT

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return RedshiftConnectorConfig(**config)


class RedshiftConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.REDSHIFT

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Amazon Redshift",
            description="Connect to an Amazon Redshift cluster.",
            version="1.0.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="host",
                    label="Host",
                    description="Cluster endpoint hostname.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="port",
                    label="Port",
                    description="Cluster endpoint port.",
                    type="number",
                    required=True,
                    default="5439",
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
                    field="cluster_identifier",
                    label="Cluster identifier",
                    description="Optional cluster identifier.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="ssl",
                    label="Use SSL",
                    description="Enable SSL for the connection.",
                    type="boolean",
                    required=False,
                ),
            ],
        )
