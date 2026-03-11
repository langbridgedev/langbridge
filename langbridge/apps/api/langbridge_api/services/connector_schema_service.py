import json
import logging
from uuid import UUID

from langbridge.packages.connectors.langbridge_connectors.api import (
    BaseConnectorConfig,
    ColumnMetadata,
    ConnectorRuntimeTypeSqlDialectMap,
    SqlConnector,
    SqlConnectorFactory,
    build_connector_config,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorType
from langbridge.packages.common.langbridge_common.db.connector import Connector
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectorCatalogColumnResponse,
    ConnectorCatalogResponse,
    ConnectorCatalogSchemaResponse,
    ConnectorCatalogSummary,
    ConnectorCatalogTableResponse,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.repositories.connector_repository import ConnectorRepository

_SYSTEM_SCHEMA_NAMES = {
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "pg_temp_1",
    "pg_toast_temp_1",
    "mysql",
    "performance_schema",
    "sys",
}


class ConnectorSchemaService:
    def __init__(self, connector_repository: ConnectorRepository) -> None:
        self._connector_repository = connector_repository
        self._sql_connector_factory = SqlConnectorFactory()
        self._logger = logging.getLogger(__name__)

    async def _get_connector(self, connector_id: UUID) -> Connector:
        connector = await self._connector_repository.get_by_id(connector_id)
        if not connector:
            raise BusinessValidationError("Connector not found")
        return connector

    def _build_connector_config(self, connector: Connector) -> BaseConnectorConfig:
        payload = connector.config_json
        config_payload = json.loads(payload if isinstance(payload, str) else payload.value)
        if hasattr(config_payload, "to_dict"):
            config_payload = config_payload.to_dict()
        connector_type = ConnectorType(connector.connector_type)
        return build_connector_config(connector_type, config_payload["config"])

    async def _create_sql_connector(
        self,
        connector_type: ConnectorType,
        config: BaseConnectorConfig,
    ) -> SqlConnector:
        dialect = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
        if dialect is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} does not support schema introspection."
            )
        sql_connector = self._sql_connector_factory.create_sql_connector(
            dialect,
            config,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    async def _get_sql_connector(self, connector_id: UUID) -> SqlConnector:
        connector = await self._get_connector(connector_id)
        connector_type = ConnectorType(connector.connector_type)
        config = self._build_connector_config(connector)
        return await self._create_sql_connector(connector_type, config)

    async def get_schemas(self, connector_id: UUID) -> list[str]:
        sql_connector = await self._get_sql_connector(connector_id)
        return await sql_connector.fetch_schemas()

    async def get_tables(self, connector_id: UUID, schema: str) -> list[str]:
        sql_connector = await self._get_sql_connector(connector_id)
        return await sql_connector.fetch_tables(schema=schema)

    async def get_columns(
        self,
        connector_id: UUID,
        schema: str,
        table: str,
    ) -> list[ColumnMetadata]:
        sql_connector = await self._get_sql_connector(connector_id)
        return await sql_connector.fetch_columns(schema=schema, table=table)

    def _is_system_schema(self, schema: str) -> bool:
        normalized = (schema or "").strip().lower()
        if not normalized:
            return True
        if normalized in _SYSTEM_SCHEMA_NAMES:
            return True
        return normalized.startswith("pg_")

    async def get_catalog_summary(
        self,
        connector_id: UUID,
        *,
        include_system_schemas: bool = False,
    ) -> ConnectorCatalogSummary:
        catalog = await self.get_catalog(
            connector_id=connector_id,
            include_system_schemas=include_system_schemas,
            include_columns=False,
            limit=5000,
            offset=0,
        )
        return ConnectorCatalogSummary(
            schema_count=catalog.schema_count,
            table_count=catalog.table_count,
            column_count=catalog.column_count,
        )

    async def get_catalog(
        self,
        *,
        connector_id: UUID,
        search: str | None = None,
        include_schemas: list[str] | None = None,
        exclude_schemas: list[str] | None = None,
        include_system_schemas: bool = False,
        include_columns: bool = True,
        limit: int = 200,
        offset: int = 0,
    ) -> ConnectorCatalogResponse:
        sql_connector = await self._get_sql_connector(connector_id)
        schemas = await sql_connector.fetch_schemas()

        include_schema_set = {
            value.strip().lower()
            for value in (include_schemas or [])
            if value and value.strip()
        }
        exclude_schema_set = {
            value.strip().lower()
            for value in (exclude_schemas or [])
            if value and value.strip()
        }
        search_token = (search or "").strip().lower()

        candidate_schemas: list[str] = []
        for schema in schemas:
            normalized_schema = schema.strip()
            if not normalized_schema:
                continue
            normalized_schema_key = normalized_schema.lower()
            if not include_system_schemas and self._is_system_schema(normalized_schema):
                continue
            if include_schema_set and normalized_schema_key not in include_schema_set:
                continue
            if normalized_schema_key in exclude_schema_set:
                continue
            candidate_schemas.append(normalized_schema)

        table_rows: list[tuple[str, str, list[ColumnMetadata] | None]] = []
        total_column_count = 0

        for schema in candidate_schemas:
            tables = await sql_connector.fetch_tables(schema=schema)
            for table in tables:
                matched = True
                loaded_columns: list[ColumnMetadata] | None = None
                if search_token:
                    schema_match = search_token in schema.lower()
                    table_match = search_token in table.lower()
                    matched = schema_match or table_match
                    if not matched and include_columns:
                        loaded_columns = await sql_connector.fetch_columns(schema=schema, table=table)
                        matched = any(search_token in column.name.lower() for column in loaded_columns)
                if not matched:
                    continue
                if include_columns and loaded_columns is None:
                    loaded_columns = await sql_connector.fetch_columns(schema=schema, table=table)
                if loaded_columns is not None:
                    total_column_count += len(loaded_columns)
                table_rows.append((schema, table, loaded_columns))

        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 1000))
        paged_rows = table_rows[safe_offset : safe_offset + safe_limit]

        grouped: dict[str, list[ConnectorCatalogTableResponse]] = {}
        for schema, table, columns in paged_rows:
            grouped.setdefault(schema, []).append(
                ConnectorCatalogTableResponse(
                    schema=schema,
                    name=table,
                    fully_qualified_name=f"{schema}.{table}",
                    columns=[
                        ConnectorCatalogColumnResponse(
                            name=column.name,
                            type=column.data_type,
                            nullable=getattr(column, "nullable", None),
                            primary_key=getattr(column, "primary_key", False),
                        )
                        for column in (columns or [])
                    ],
                )
            )

        schema_nodes = [
            ConnectorCatalogSchemaResponse(name=schema, tables=tables)
            for schema, tables in grouped.items()
        ]
        schema_nodes.sort(key=lambda item: item.name.lower())

        return ConnectorCatalogResponse(
            connector_id=connector_id,
            schemas=schema_nodes,
            schema_count=len(candidate_schemas),
            table_count=len(table_rows),
            column_count=total_column_count,
            offset=safe_offset,
            limit=safe_limit,
            has_more=safe_offset + safe_limit < len(table_rows),
        )
