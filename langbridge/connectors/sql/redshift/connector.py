import logging
from typing import Any, Dict, Optional

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import SqlConnector
from langbridge.connectors.base.metadata import ColumnMetadata, ForeignKeyMetadata, TableMetadata
from langbridge.connectors.base.errors import ConnectorError

from .config import RedshiftConnectorConfig

try:  # pragma: no cover - optional dependency
    import psycopg  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    psycopg2 = None  # type: ignore


class RedshiftConnector(SqlConnector):
    """
    Amazon Redshift connector implementation (PostgreSQL-compatible).
    """

    RUNTIME_TYPE = ConnectorRuntimeType.REDSHIFT
    SQLGLOT_DIALECT = "redshift"

    def __init__(
        self,
        config: RedshiftConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._config = config

    def _connection_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "host": self._config.host,
            "port": self._config.port,
            "dbname": self._config.database,
            "user": self._config.user,
            "password": self._config.password,
        }
        if self._config.ssl is True:
            kwargs["sslmode"] = "require"
        elif self._config.ssl is False:
            kwargs["sslmode"] = "disable"
        return kwargs

    def _connect(self):
        if psycopg is not None:
            return psycopg.connect(**self._connection_kwargs())  # type: ignore[attr-defined]
        if psycopg2 is not None:
            return psycopg2.connect(**self._connection_kwargs())  # type: ignore[attr-defined]
        raise ConnectorError("psycopg or psycopg2 is required for Redshift support.")

    async def test_connection(self) -> None:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
        except Exception as exc:
            self.logger.error("Connection test failed: %s", exc)
            raise ConnectorError(f"Unable to connect to Redshift: {exc}") from exc

    async def fetch_schemas(self) -> list[str]:
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name
        """
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
            raise ConnectorError(f"Unable to fetch schemas from Redshift: {exc}") from exc

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
        except Exception as exc:
            self.logger.error("Failed to fetch tables: %s", exc)
            raise ConnectorError(f"Unable to fetch tables from Redshift: {exc}") from exc

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
        except Exception as exc:
            self.logger.error("Failed to fetch columns: %s", exc)
            raise ConnectorError(f"Unable to fetch columns from Redshift: {exc}") from exc

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
              AND tc.table_schema = %s
              AND tc.table_name = %s
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
        except Exception as exc:
            self.logger.error("Failed to fetch foreign keys: %s", exc)
            raise ConnectorError(f"Unable to fetch foreign keys from Redshift: {exc}") from exc

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
            if timeout_s:
                cursor.execute("SET statement_timeout = %s", (int(timeout_s * 1000),))
            cursor.execute(sql, params or None)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return columns, rows
        except Exception as exc:
            self.logger.error("SQL execution failed: %s", exc)
            raise ConnectorError(f"SQL execution failed on Redshift: {exc}") from exc
