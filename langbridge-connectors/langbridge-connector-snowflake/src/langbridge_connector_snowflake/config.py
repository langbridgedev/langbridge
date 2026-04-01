from pydantic import Field

from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
)


class SnowflakeConnectorConfig(BaseConnectorConfig):
    account: str
    user: str
    password: str
    database: str
    warehouse: str
    schema_name: str = Field(alias="schema")
    role: str

    @classmethod
    def create_from_dict(cls, data: dict[str, str]) -> "SnowflakeConnectorConfig":
        return cls(
            account=data.get("account", ""),
            user=data.get("user", ""),
            password=data.get("password", ""),
            database=data.get("database", ""),
            warehouse=data.get("warehouse", ""),
            schema=data.get("schema", ""),
            role=data.get("role", ""),
        )


class SnowflakeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.SNOWFLAKE

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return SnowflakeConnectorConfig.create_from_dict(config)


class SnowflakeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.SNOWFLAKE

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Snowflake",
            description="Snowflake Connector",
            version="1.0.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="account",
                    label="Account",
                    description="Snowflake Account",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="user",
                    label="User",
                    description="Snowflake User",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="password",
                    label="Password",
                    description="Snowflake Password",
                    type="password",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="database",
                    label="Database",
                    description="Snowflake Database",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="warehouse",
                    label="Warehouse",
                    description="Snowflake Warehouse",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="schema",
                    label="Schema",
                    description="Snowflake Schema",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="role",
                    label="Role",
                    description="Snowflake Role",
                    type="string",
                    required=True,
                ),
            ],
        )
