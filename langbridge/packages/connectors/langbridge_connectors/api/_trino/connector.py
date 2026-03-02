import asyncio
from typing import Any, Dict, List, Optional, Tuple

import trino
from trino.auth import BasicAuthentication

from langbridge.packages.connectors.langbridge_connectors.api.connector import ManagedConnector, QueryResult, SqlDialetcs, ensure_select_statement
from langbridge.packages.connectors.langbridge_connectors.api.config import BaseConnectorConfig
from langbridge.packages.common.langbridge_common.errors.connector_errors import AuthError, ConnectorError, QueryValidationError
from langbridge.packages.connectors.langbridge_connectors.api.metadata import TableMetadata, ColumnMetadata, ForeignKeyMetadata


class TrinoConnectorConfig(BaseConnectorConfig):
    host: str = "localhost"
    port: int = 8080
    user: str = "trino"
    catalog: str = "system"
    schema: str = "information_schema"
    http_scheme: str = "http"
    password: Optional[str] = None
    verify: bool = True
    tenant: str = ""
    source: str = "langbridge"


class TrinoConnector(ManagedConnector):
    """
    Managed connector for Trino that can query across underlying databases via catalogs.
    Assumes the Trino Python client is available (we vendor a modified version locally).
    """

    DIALECT = SqlDialetcs.TRINO

    def __init__(self, 
                 config: TrinoConnectorConfig) -> None:
        self.config = config
        if self.config.tenant == "" or self.config.tenant is None:
            raise AuthError("Trino tenant must be set")

        self._conn = self._create_connection()

    def _create_connection(self):
        auth = BasicAuthentication(self.config.user, self.config.password) if self.config.password else None
        return trino.dbapi.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            catalog=self.config.catalog,
            schema=self.config.schema,
            http_scheme=self.config.http_scheme,
            auth=auth,
            extra_credential=[
                ("tenant", self.config.tenant)
            ],
            verify=self.config.verify
        )

    async def test_connection(self) -> None:
        try:
            await self._execute_select_async("SELECT 1")
        except Exception as exc:
            raise ConnectorError(f"Trino connection failed: {exc}") from exc

    async def _execute_select_async(
        self,
        sql: str,
        params: Dict[str, Any] | None = None,
        *,
        timeout_s: Optional[int] = 30,
    ) -> Tuple[List[str], List[List[Any]]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._execute_select_sync, sql, params or {})

    def _execute_select_sync(self, sql: str, params: Dict[str, Any]) -> Tuple[List[str], List[List[Any]]]:
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            columns = [col[0] for col in cur.description] if cur.description else []
            return columns, rows

    async def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        max_rows: Optional[int] = 5000,
        timeout_s: Optional[int] = 30,
    ) -> QueryResult:
        ensure_select_statement(sql)
        # prepared_sql = apply_limit(sql, max_rows)
        columns, rows = await self._execute_select_async(sql, params or {}, timeout_s=timeout_s)
        return QueryResult(
            columns=columns,
            rows=rows,
            rowcount=len(rows),
            elapsed_ms=0,
            sql=sql,
        )

    # Schema introspection helpers leveraging Trino's information_schema
    async def fetch_schemas(self) -> List[str]:
        sql = "SELECT schema_name FROM information_schema.schemata"
        cols, rows = await self._execute_select_async(sql)
        return [row[0] for row in rows]

    async def fetch_tables(self, schema: str) -> List[str]:
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema"
        cols, rows = await self._execute_select_async(sql, {"schema": schema})
        return [row[0] for row in rows]

    async def fetch_columns(self, schema: str, table: str) -> List[ColumnMetadata]:
        sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
        """
        _, rows = await self._execute_select_async(sql, {"schema": schema, "table": table})
        return [ColumnMetadata(name=row[0], data_type=row[1], is_nullable=(row[2] == "YES")) for row in rows]

    async def fetch_table_metadata(self, schema: str, table: str) -> TableMetadata:
        columns = await self.fetch_columns(schema, table)
        return TableMetadata(schema=schema, name=table, columns=columns)

    async def fetch_foreign_keys(self, schema: str, table: str) -> List[ForeignKeyMetadata]:
        sql = """
        SELECT constraint_name, column_name, foreign_table_schema, foreign_table_name, foreign_column_name
        FROM information_schema.referential_constraints rc
        JOIN information_schema.key_column_usage kcu
          ON rc.constraint_name = kcu.constraint_name
         AND rc.constraint_schema = kcu.constraint_schema
        WHERE rc.constraint_schema = :schema AND rc.constraint_name IN (
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_schema = :schema AND table_name = :table AND constraint_type = 'FOREIGN KEY'
        )
        """
        cols, rows = await self._execute_select_async(sql, {"schema": schema, "table": table})
        fks: List[ForeignKeyMetadata] = []
        for row in rows:
            fks.append(
                ForeignKeyMetadata(
                    schema=row[1],
                    table=row[2],
                    name=row[0],
                    column=row[3],
                    foreign_key=row[4],
                )
            )
        return fks

    # Synchronous wrappers  
    def test_connection_sync(self) -> None:
        return asyncio.run(self.test_connection())

    def fetch_schemas_sync(self) -> List[str]:
        return asyncio.run(self.fetch_schemas())

    def fetch_tables_sync(self, schema: str) -> List[str]:
        return asyncio.run(self.fetch_tables(schema))

    def fetch_columns_sync(self, schema: str, table: str) -> List[ColumnMetadata]:
        return asyncio.run(self.fetch_columns(schema, table))

    def fetch_table_metadata_sync(self, schema: str, table: str) -> TableMetadata:
        return asyncio.run(self.fetch_table_metadata(schema, table))

    def fetch_foreign_keys_sync(self, schema: str, table: str) -> List[ForeignKeyMetadata]:
        return asyncio.run(self.fetch_foreign_keys(schema, table))
