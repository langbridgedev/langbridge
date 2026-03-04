import logging
from typing import Any, Dict, Optional
from langbridge.packages.connectors.langbridge_connectors.api.connector import SqlConnector
from langbridge.packages.connectors.langbridge_connectors.api import SqlDialetcs
from langbridge.packages.common.langbridge_common.errors.connector_errors import ConnectorError
from langbridge.packages.connectors.langbridge_connectors.api.metadata import ColumnMetadata, ForeignKeyMetadata, TableMetadata
from .config import SqliteConnectorConfig
from sqlite3 import connect, OperationalError, DatabaseError, ProgrammingError

class SqliteConnector(SqlConnector):
    """
    SQLite connector implementation.
    """
    DIALECT = SqlDialetcs.SQLITE
    
    def __init__(
        self,
        config: SqliteConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self.database_path = config.location
        
        
    async def test_connection(self) -> None:
        try:
            conn = connect(self.database_path)
            conn.close()
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            self.logger.error("Connection test failed: %s", exc)
            raise ConnectorError(f"Unable to connect to SQLite database: {exc}") from exc
        
    async def fetch_schemas(self) -> list[str]:
        return ["main"]
    
    async def fetch_tables(self, schema:str) -> list[str]:
        try:
            conn = connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            self.logger.error("Failed to fetch tables: %s", exc)
            raise ConnectorError(f"Unable to fetch tables from SQLite database: {exc}") from exc
        
    async def fetch_table_metadata(self, schema:str, table:str) -> TableMetadata:
        try:
            conn = connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info('{table}');")
            columns = []
            for row in cursor.fetchall():
                column = ColumnMetadata(
                    name=row[1],
                    data_type=row[2],
                    is_nullable=not bool(row[3]),
                    is_primary_key=bool(row[5]),
                )
                columns.append(column)
            cursor.close()
            conn.close()
            return TableMetadata(schema=schema, name=table)
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            self.logger.error("Failed to fetch table metadata: %s", exc)
            raise ConnectorError(f"Unable to fetch table metadata from SQLite database: {exc}") from exc
        
    async def fetch_columns(self, schema:str, table:str) -> list[ColumnMetadata]:
        try:
            conn = connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info('{table}');")
            columns = []
            for row in cursor.fetchall():
                column = ColumnMetadata(
                    name=row[1],
                    data_type=row[2],
                    is_nullable=not bool(row[3]),
                    is_primary_key=bool(row[5]),
                )
                columns.append(column)
            cursor.close()
            conn.close()
            return columns
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            self.logger.error("Failed to fetch columns: %s", exc)
            raise ConnectorError(f"Unable to fetch columns from SQLite database: {exc}") from exc
    
    async def fetch_foreign_keys(self, schema:str, table:str) -> list[ForeignKeyMetadata]:
        try:
            conn = connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            foreign_keys = []
            for row in cursor.fetchall():
                foreign_key = ForeignKeyMetadata(
                    schema=schema,
                    table=row[2],
                    name="fk_" + row[3],
                    column=row[3],
                    foreign_key=row[4],
                )
                foreign_keys.append(foreign_key)
            cursor.close()
            conn.close()
            return foreign_keys
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            self.logger.error("Failed to fetch foreign keys: %s", exc)
            raise ConnectorError(f"Unable to fetch foreign keys from SQLite database: {exc}") from exc
        
    async def _execute_select(
        self,
        sql: str,
        params: Dict[str, Any],
        *,
        timeout_s: Optional[int] = 30,
    ) -> tuple[list[str], list[tuple]]:
        try:
            sql = sql.replace("sqlite_main_", "main.")
            conn = connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return columns, rows
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            self.logger.error("SQL execution failed: %s", exc)
            raise ConnectorError(f"SQL execution failed on SQLite database: {exc}") from exc