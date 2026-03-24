from typing import Dict, List

from langbridge.connectors.base.config import BaseConnectorConfig, ConnectorRuntimeType
from langbridge.connectors.base.errors import ConnectorError
from langbridge.connectors.base.metadata import (
    BaseMetadataExtractor,
    ColumnMetadata,
    TableMetadata,
)

from .config import SnowflakeConnectorConfig

try:  # pragma: no cover - optional dependency
    from snowflake.connector import DatabaseError, OperationalError, ProgrammingError, connect  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    connect = None  # type: ignore
    ProgrammingError = OperationalError = DatabaseError = Exception  # type: ignore


class SnowflakeMetadataExtractor(BaseMetadataExtractor):
    type = ConnectorRuntimeType.SNOWFLAKE

    def fetch_metadata(self, config: BaseConnectorConfig) -> List[TableMetadata]:
        if not isinstance(config, SnowflakeConnectorConfig):
            raise ConnectorError(
                "Invalid config type: expected SnowflakeConnectorConfig, "
                f"got {type(config).__name__}"
            )
        if connect is None:
            raise ConnectorError(
                "snowflake-connector-python is required for Snowflake support."
            )

        try:
            conn = connect(
                user=config.user,
                password=config.password,
                account=config.account,
                database=config.database,
                warehouse=config.warehouse,
                schema=config.schema,
                role=config.role,
            )
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            raise ConnectorError(f"Unable to connect to Snowflake: {exc}") from exc

        tables: Dict[tuple[str, str], List[ColumnMetadata]] = {}
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE DATABASE {config.database}")
            base_query = """
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
            """
            params: List[str] = []
            if config.schema:
                base_query += " WHERE table_schema = %s"
                params.append(config.schema)

            cursor.execute(base_query, params)
            for schema_name, table_name, column_name, data_type in cursor.fetchall():
                key = (schema_name, table_name)
                tables.setdefault(key, []).append(
                    ColumnMetadata(name=column_name, data_type=data_type)
                )
        finally:
            cursor.close()
            conn.close()

        return [
            TableMetadata(schema=schema, name=table, columns=columns)
            for (schema, table), columns in tables.items()
        ]
