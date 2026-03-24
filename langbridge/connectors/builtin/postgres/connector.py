import logging
from typing import Any, Dict, Optional

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import SqlConnector
from langbridge.connectors.base.metadata import ColumnMetadata, ForeignKeyMetadata, TableMetadata
from langbridge.connectors.base.errors import ConnectorError

from .config import PostgresConnectorConfig

from sqlglot import exp

try:  # pragma: no cover - optional dependency
    import psycopg  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore


class PostgresConnector(SqlConnector):
    """
    PostgreSQL connector implementation.
    """

    RUNTIME_TYPE = ConnectorRuntimeType.POSTGRES
    SQLGLOT_DIALECT = "postgres"
    EXPRESSION_REWRITE = True

    def __init__(
        self,
        config: PostgresConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._config = config

    def _connection_kwargs(self, timeout_s: Optional[int] = None) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "host": self._config.host,
            "port": self._config.port,
            "dbname": self._config.database,
            "user": self._config.user,
            "password": self._config.password,
            "connect_timeout": timeout_s,
            "autocommit": True,
        }
        if self._config.ssl_mode:
            kwargs["sslmode"] = self._config.ssl_mode
        return kwargs

    async def _connect(self, timeout_s: Optional[int] = None) -> Any:
        if psycopg is None:
            raise ConnectorError("psycopg is required for PostgreSQL support.")
        return await psycopg.AsyncConnection.connect(**self._connection_kwargs(timeout_s))  # type: ignore[attr-defined]

    async def test_connection(self) -> None:
        try:
            async with await self._connect() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
        except Exception as exc:
            self.logger.error("Connection test failed: %s", exc)
            raise ConnectorError(f"Unable to connect to PostgreSQL: {exc}") from exc

    async def fetch_schemas(self) -> list[str]:
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name
        """
        try:
            async with await self._connect() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql)
                    return [row[0] for row in await cursor.fetchall()]
        except Exception as exc:
            self.logger.error("Failed to fetch schemas: %s", exc)
            raise ConnectorError(f"Unable to fetch schemas from PostgreSQL: {exc}") from exc

    async def fetch_tables(self, schema: str) -> list[str]:
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """
        try:
            async with await self._connect() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, (schema,))
                    return [row[0] for row in await cursor.fetchall()]
        except Exception as exc:
            self.logger.error("Failed to fetch tables: %s", exc)
            raise ConnectorError(f"Unable to fetch tables from PostgreSQL: {exc}") from exc

    async def _fetch_primary_keys(self, conn, schema: str, table: str) -> set[str]:
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
        async with conn.cursor() as cursor:
            await cursor.execute(sql, (schema, table))
            return {row[0] for row in await cursor.fetchall()}

    async def fetch_columns(self, schema: str, table: str) -> list[ColumnMetadata]:
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        try:
            async with await self._connect() as conn:
                primary_keys = await self._fetch_primary_keys(conn, schema, table)
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, (schema, table))
                    columns = []
                    for name, data_type, is_nullable in await cursor.fetchall():
                        columns.append(
                            ColumnMetadata(
                                name=name,
                                data_type=data_type,
                                is_nullable=is_nullable == "YES",
                                is_primary_key=name in primary_keys,
                            )
                        )
                    return columns
        except Exception as exc:
            self.logger.error("Failed to fetch columns: %s", exc)
            raise ConnectorError(f"Unable to fetch columns from PostgreSQL: {exc}") from exc

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
            async with await self._connect() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, (schema, table))
                    return [
                        ForeignKeyMetadata(
                            name=row[0],
                            column=row[1],
                            schema=row[2],
                            table=row[3],
                            foreign_key=row[4],
                        )
                        for row in await cursor.fetchall()
                    ]
        except Exception as exc:
            self.logger.error("Failed to fetch foreign keys: %s", exc)
            raise ConnectorError(f"Unable to fetch foreign keys from PostgreSQL: {exc}") from exc

    async def _execute_select(
        self,
        sql: str,
        params: Dict[str, Any],
        *,
        timeout_s: Optional[int] = 60, # default timeout of 60 seconds for queries, can be overridden by caller
    ) -> tuple[list[str], list[tuple]]:
        try:
            async with await self._connect(timeout_s) as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, params or None)
                    columns = [description[0] for description in cursor.description or []]
                    rows = await cursor.fetchall()
                    return columns, rows
        except Exception as exc:
            self.logger.error("SQL execution failed: %s", exc)
            raise ConnectorError(f"SQL execution failed on PostgreSQL: {exc}") from exc
        
    def rewrite_expression(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.DateAdd):
            unit = node.args.get("unit")
            date = node.args.get("this")         # 3rd arg in TSQL DATEADD(unit, number, date)
            number = node.args.get("expression") # 2nd arg

            if (
                unit and unit.name and unit.name.lower() == "month"
                and self.__is_zero_literal(date)
                and isinstance(number, exp.DateDiff)
            ):
                dd_unit = number.args.get("unit")
                dd_start = number.args.get("this")       # start date (2nd arg in DATEDIFF)
                dd_end = number.args.get("expression")   # end date (3rd arg)

                if dd_unit and dd_unit.name and dd_unit.name.lower() == "month" and self.__is_zero_literal(dd_start):
                    return exp.DateTrunc(
                        this=exp.Literal.string("month"),
                        expression=dd_end,
                    )
        return node

    
    def __is_zero_literal(self, e):
        return isinstance(e, exp.Literal) and e.is_number and e.this == "0"
