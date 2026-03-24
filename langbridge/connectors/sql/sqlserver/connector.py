import logging
from typing import Any, Dict, Optional

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import SqlConnector
from langbridge.connectors.base.metadata import ColumnMetadata, ForeignKeyMetadata, TableMetadata
from langbridge.connectors.base.errors import ConnectorError

from .config import SQLServerConnectorConfig

try:  # pragma: no cover - optional dependency
    import pyodbc  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    pyodbc = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import pymssql  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    pymssql = None  # type: ignore


class SQLServerConnector(SqlConnector):
    """
    Microsoft SQL Server connector implementation.
    """

    RUNTIME_TYPE = ConnectorRuntimeType.SQLSERVER
    SQLGLOT_DIALECT = "tsql"

    def __init__(
        self,
        config: SQLServerConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._config = config
        self._driver = self._select_driver()

    def _select_driver(self) -> str:
        if pyodbc is not None:
            return "pyodbc"
        if pymssql is not None:
            return "pymssql"
        raise ConnectorError(
            "Install pyodbc or pymssql to enable SQL Server support."
        )

    def _connect(self):
        if self._driver == "pyodbc":
            encrypt = "yes" if self._config.encrypt else "no"
            trust = "yes" if self._config.trust_server_certificate else "no"
            driver = "ODBC Driver 18 for SQL Server"
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={self._config.host},{self._config.port};"
                f"DATABASE={self._config.database};"
                f"UID={self._config.username};"
                f"PWD={self._config.password};"
                f"Encrypt={encrypt};"
                f"TrustServerCertificate={trust}"
            )
            return pyodbc.connect(conn_str)  # type: ignore[union-attr]
        return pymssql.connect(  # type: ignore[union-attr]
            server=self._config.host,
            port=self._config.port,
            user=self._config.username,
            password=self._config.password,
            database=self._config.database,
        )

    async def test_connection(self) -> None:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
        except Exception as exc:
            self.logger.error("Connection test failed: %s", exc)
            raise ConnectorError(f"Unable to connect to SQL Server: {exc}") from exc

    async def fetch_schemas(self) -> list[str]:
        sql = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql)
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return schemas
        except Exception as exc:
            self.logger.error("Failed to fetch schemas: %s", exc)
            raise ConnectorError(f"Unable to fetch schemas from SQL Server: {exc}") from exc

    async def fetch_tables(self, schema: str) -> list[str]:
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = {placeholder}
            ORDER BY table_name
        """
        placeholder = "?" if self._driver == "pyodbc" else "%s"
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql.format(placeholder=placeholder), (schema,))
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables
        except Exception as exc:
            self.logger.error("Failed to fetch tables: %s", exc)
            raise ConnectorError(f"Unable to fetch tables from SQL Server: {exc}") from exc

    def _fetch_primary_keys(self, conn, schema: str, table: str) -> set[str]:
        sql = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = {placeholder}
              AND tc.table_name = {placeholder}
        """
        placeholder = "?" if self._driver == "pyodbc" else "%s"
        cursor = conn.cursor()
        cursor.execute(sql.format(placeholder=placeholder), (schema, table))
        keys = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return keys

    async def fetch_columns(self, schema: str, table: str) -> list[ColumnMetadata]:
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = {placeholder} AND table_name = {placeholder}
            ORDER BY ordinal_position
        """
        placeholder = "?" if self._driver == "pyodbc" else "%s"
        try:
            conn = self._connect()
            primary_keys = self._fetch_primary_keys(conn, schema, table)
            cursor = conn.cursor()
            cursor.execute(sql.format(placeholder=placeholder), (schema, table))
            columns = []
            for name, data_type, is_nullable in cursor.fetchall():
                columns.append(
                    ColumnMetadata(
                        name=name,
                        data_type=data_type,
                        is_nullable=is_nullable == "YES",
                        is_primary_key=name in primary_keys,
                    )
                )
            cursor.close()
            conn.close()
            return columns
        except Exception as exc:
            self.logger.error("Failed to fetch columns: %s", exc)
            raise ConnectorError(f"Unable to fetch columns from SQL Server: {exc}") from exc

    async def fetch_table_metadata(self, schema: str, table: str) -> TableMetadata:
        columns = await self.fetch_columns(schema, table)
        return TableMetadata(schema=schema, name=table, columns=columns)

    async def fetch_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyMetadata]:
        sql = """
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = {placeholder}
              AND tc.table_name = {placeholder}
        """
        placeholder = "?" if self._driver == "pyodbc" else "%s"
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql.format(placeholder=placeholder), (schema, table))
            foreign_keys = [
                ForeignKeyMetadata(
                    name=row[0],
                    column=row[1],
                    schema=row[2],
                    table=row[3],
                    foreign_key=row[4],
                )
                for row in cursor.fetchall()
            ]
            cursor.close()
            conn.close()
            return foreign_keys
        except Exception as exc:
            self.logger.error("Failed to fetch foreign keys: %s", exc)
            raise ConnectorError(f"Unable to fetch foreign keys from SQL Server: {exc}") from exc

    async def _execute_select(
        self,
        sql: str,
        params: Dict[str, Any],
        *,
        timeout_s: Optional[int] = 30,
    ) -> tuple[list[str], list[tuple]]:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            if timeout_s and self._driver == "pyodbc":
                conn.timeout = int(timeout_s)
            cursor.execute(sql, params or None)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return columns, rows
        except Exception as exc:
            self.logger.error("SQL execution failed: %s", exc)
            raise ConnectorError(f"SQL execution failed on SQL Server: {exc}") from exc
