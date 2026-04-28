import logging
from typing import Any, Dict, Optional

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import SqlConnector
from langbridge.connectors.base.metadata import (
    ColumnMetadata,
    ForeignKeyMetadata,
    TableMetadata,
)
from langbridge.connectors.base.errors import ConnectorError

from .config import MariaDBConnectorConfig

try:  # pragma: no cover - optional dependency
    import mariadb  # type: ignore
    from mariadb import Error as MariaDbError  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    mariadb = None  # type: ignore
    MariaDbError = Exception  # type: ignore

try:  # pragma: no cover - optional dependency
    import pymysql  # type: ignore
    from pymysql.err import MySQLError as PyMySqlError  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    pymysql = None  # type: ignore
    PyMySqlError = Exception  # type: ignore


class MariaDBConnector(SqlConnector):
    """
    MariaDB connector implementation.
    """

    RUNTIME_TYPE = ConnectorRuntimeType.MARIADB
    SQLGLOT_DIALECT = "mysql"

    def __init__(
        self,
        config: MariaDBConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._config = config
        self._driver = self._select_driver()

    def _select_driver(self) -> str:
        if mariadb is not None:
            return "mariadb"
        if pymysql is not None:
            return "pymysql"
        raise ConnectorError(
            "Install mariadb or PyMySQL to enable MariaDB support."
        )

    def _connection_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "host": self._config.host,
            "port": self._config.port,
            "database": self._config.database,
            "user": self._config.user,
            "password": self._config.password,
        }
        return kwargs

    def _connect(self):
        if self._driver == "mariadb":
            return mariadb.connect(**self._connection_kwargs())  # type: ignore[union-attr]
        return pymysql.connect(**self._connection_kwargs())  # type: ignore[union-attr]

    async def test_connection(self) -> None:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
        except (MariaDbError, PyMySqlError, Exception) as exc:
            self.logger.error("Connection test failed: %s", exc)
            raise ConnectorError(f"Unable to connect to MariaDB: {exc}") from exc

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
        except (MariaDbError, PyMySqlError, Exception) as exc:
            self.logger.error("Failed to fetch schemas: %s", exc)
            raise ConnectorError(f"Unable to fetch schemas from MariaDB: {exc}") from exc

    async def fetch_tables(self, schema: str) -> list[str]:
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql, (schema,))
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables
        except (MariaDbError, PyMySqlError, Exception) as exc:
            self.logger.error("Failed to fetch tables: %s", exc)
            raise ConnectorError(f"Unable to fetch tables from MariaDB: {exc}") from exc

    def _fetch_primary_keys(self, conn, schema: str, table: str) -> set[str]:
        sql = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
        """
        cursor = conn.cursor()
        cursor.execute(sql, (schema, table))
        keys = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return keys

    async def fetch_columns(self, schema: str, table: str) -> list[ColumnMetadata]:
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        try:
            conn = self._connect()
            primary_keys = self._fetch_primary_keys(conn, schema, table)
            cursor = conn.cursor()
            cursor.execute(sql, (schema, table))
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
        except (MariaDbError, PyMySqlError, Exception) as exc:
            self.logger.error("Failed to fetch columns: %s", exc)
            raise ConnectorError(f"Unable to fetch columns from MariaDB: {exc}") from exc

    async def fetch_table_metadata(self, schema: str, table: str) -> TableMetadata:
        columns = await self.fetch_columns(schema, table)
        return TableMetadata(schema=schema, name=table, columns=columns)

    async def fetch_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyMetadata]:
        sql = """
            SELECT
                constraint_name,
                column_name,
                referenced_table_schema,
                referenced_table_name,
                referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = %s
              AND table_name = %s
              AND referenced_table_name IS NOT NULL
        """
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql, (schema, table))
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
        except (MariaDbError, PyMySqlError, Exception) as exc:
            self.logger.error("Failed to fetch foreign keys: %s", exc)
            raise ConnectorError(f"Unable to fetch foreign keys from MariaDB: {exc}") from exc

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
            cursor.execute(sql, params or None)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return columns, rows
        except (MariaDbError, PyMySqlError, Exception) as exc:
            self.logger.error("SQL execution failed: %s", exc)
            raise ConnectorError(f"SQL execution failed on MariaDB: {exc}") from exc
