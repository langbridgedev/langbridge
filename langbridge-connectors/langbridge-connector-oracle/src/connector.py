import logging
import os
from typing import Any, Dict, Optional

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import SqlConnector
from langbridge.connectors.base.metadata import ColumnMetadata, ForeignKeyMetadata, TableMetadata
from langbridge.connectors.base.errors import ConnectorError

from .config import OracleConnectorConfig

try:  # pragma: no cover - optional dependency
    import oracledb  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    oracledb = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import cx_Oracle  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    cx_Oracle = None  # type: ignore


class OracleConnector(SqlConnector):
    """
    Oracle Database connector implementation.
    """

    RUNTIME_TYPE = ConnectorRuntimeType.ORACLE
    SQLGLOT_DIALECT = "oracle"

    def __init__(
        self,
        config: OracleConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._config = config
        self._driver = self._select_driver()

    def _select_driver(self) -> str:
        if oracledb is not None:
            return "oracledb"
        if cx_Oracle is not None:
            return "cx_oracle"
        raise ConnectorError(
            "Install oracledb (preferred) or cx_Oracle to enable Oracle support."
        )

    def _build_dsn(self):
        if self._driver == "oracledb":
            return oracledb.makedsn(  # type: ignore[union-attr]
                self._config.host,
                self._config.port,
                service_name=self._config.service_name,
            )
        return cx_Oracle.makedsn(  # type: ignore[union-attr]
            self._config.host,
            self._config.port,
            service_name=self._config.service_name,
        )

    def _connect(self):
        if self._config.wallet_path:
            os.environ.setdefault("TNS_ADMIN", self._config.wallet_path)
        dsn = self._build_dsn()
        if self._driver == "oracledb":
            return oracledb.connect(  # type: ignore[union-attr]
                user=self._config.username,
                password=self._config.password,
                dsn=dsn,
            )
        return cx_Oracle.connect(  # type: ignore[union-attr]
            user=self._config.username,
            password=self._config.password,
            dsn=dsn,
        )

    async def test_connection(self) -> None:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM dual")
            cursor.close()
            conn.close()
        except Exception as exc:
            self.logger.error("Connection test failed: %s", exc)
            raise ConnectorError(f"Unable to connect to Oracle: {exc}") from exc

    async def fetch_schemas(self) -> list[str]:
        sql = "SELECT username FROM all_users ORDER BY username"
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
            raise ConnectorError(f"Unable to fetch schemas from Oracle: {exc}") from exc

    async def fetch_tables(self, schema: str) -> list[str]:
        sql = "SELECT table_name FROM all_tables WHERE owner = :schema ORDER BY table_name"
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql, {"schema": schema.upper()})
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables
        except Exception as exc:
            self.logger.error("Failed to fetch tables: %s", exc)
            raise ConnectorError(f"Unable to fetch tables from Oracle: {exc}") from exc

    def _fetch_primary_keys(self, conn, schema: str, table: str) -> set[str]:
        sql = """
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols
              ON cons.owner = cols.owner
             AND cons.constraint_name = cols.constraint_name
            WHERE cons.constraint_type = 'P'
              AND cons.owner = :schema
              AND cons.table_name = :table
        """
        cursor = conn.cursor()
        cursor.execute(sql, {"schema": schema.upper(), "table": table.upper()})
        keys = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return keys

    async def fetch_columns(self, schema: str, table: str) -> list[ColumnMetadata]:
        sql = """
            SELECT column_name, data_type, nullable
            FROM all_tab_columns
            WHERE owner = :schema AND table_name = :table
            ORDER BY column_id
        """
        try:
            conn = self._connect()
            primary_keys = self._fetch_primary_keys(conn, schema, table)
            cursor = conn.cursor()
            cursor.execute(sql, {"schema": schema.upper(), "table": table.upper()})
            columns = []
            for name, data_type, nullable in cursor.fetchall():
                columns.append(
                    ColumnMetadata(
                        name=name,
                        data_type=data_type,
                        is_nullable=nullable == "Y",
                        is_primary_key=name in primary_keys,
                    )
                )
            cursor.close()
            conn.close()
            return columns
        except Exception as exc:
            self.logger.error("Failed to fetch columns: %s", exc)
            raise ConnectorError(f"Unable to fetch columns from Oracle: {exc}") from exc

    async def fetch_table_metadata(self, schema: str, table: str) -> TableMetadata:
        columns = await self.fetch_columns(schema, table)
        return TableMetadata(schema=schema, name=table, columns=columns)

    async def fetch_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyMetadata]:
        sql = """
            SELECT
                a.constraint_name,
                a.column_name,
                c_pk.owner AS foreign_schema,
                c_pk.table_name AS foreign_table,
                b.column_name AS foreign_column
            FROM all_cons_columns a
            JOIN all_constraints c
              ON a.owner = c.owner
             AND a.constraint_name = c.constraint_name
            JOIN all_constraints c_pk
              ON c.r_owner = c_pk.owner
             AND c.r_constraint_name = c_pk.constraint_name
            JOIN all_cons_columns b
              ON c_pk.owner = b.owner
             AND c_pk.constraint_name = b.constraint_name
             AND a.position = b.position
            WHERE c.constraint_type = 'R'
              AND c.owner = :schema
              AND c.table_name = :table
        """
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql, {"schema": schema.upper(), "table": table.upper()})
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
            raise ConnectorError(f"Unable to fetch foreign keys from Oracle: {exc}") from exc

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
            cursor.execute(sql, params or {})
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return columns, rows
        except Exception as exc:
            self.logger.error("SQL execution failed: %s", exc)
            raise ConnectorError(f"SQL execution failed on Oracle: {exc}") from exc
