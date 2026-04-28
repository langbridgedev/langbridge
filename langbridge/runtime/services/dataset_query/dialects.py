import uuid

from langbridge.runtime.providers import ConnectorMetadataProvider


class DatasetConnectorDialectResolver:
    """Resolves connector metadata into SQL dialect names used by query generation."""

    def __init__(self, *, connector_provider: ConnectorMetadataProvider | None) -> None:
        self._connector_provider = connector_provider

    def connector_dialect(self, connector_type: str | None) -> str:
        normalized = str(connector_type or "").strip().upper()
        dialect_map = {
            "POSTGRES": "postgres",
            "MYSQL": "mysql",
            "MARIADB": "mysql",
            "SNOWFLAKE": "snowflake",
            "BIGQUERY": "bigquery",
            "SQLSERVER": "tsql",
            "REDSHIFT": "postgres",
            "ORACLE": "oracle",
            "SQLITE": "sqlite",
        }
        return dialect_map.get(normalized, normalized.lower() or "tsql")

    async def connector_runtime_kind(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
    ) -> str | None:
        if self._connector_provider is None:
            return None
        connector = await self._connector_provider.get_connector(
            workspace_id=workspace_id,
            connector_id=connection_id,
        )
        if connector is None or connector.connector_type is None:
            return None
        return connector.connector_type_value.lower()
