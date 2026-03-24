import logging
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from langbridge.connectors.base import (
    ConnectorRuntimeType,
    ColumnMetadata,
    ForeignKeyMetadata,
    SqlConnector,
    SqlConnectorFactory,
    TableMetadata,
    get_connector_config_factory,
)
from langbridge.runtime.models import ConnectorMetadata
from langbridge.runtime.providers import RepositoryConnectorMetadataProvider
from langbridge.semantic import Dimension, Measure, Relationship, SemanticModel, Table
from langbridge.semantic.loader import load_semantic_model
from langbridge.semantic.model import MeasureAggregation

from .errors import ExecutionValidationError

logger = logging.getLogger(__name__)


TYPE_NUMERIC = {"number", "decimal", "numeric", "int", "integer", "float", "double", "real"}
TYPE_BOOLEAN = {"boolean", "bool"}
TYPE_DATE = {"date", "datetime", "timestamp", "time"}


@dataclass
class ScopedTableMetadata:
    schema: str
    table_metadata: TableMetadata
    columns: list[ColumnMetadata]
    foreign_keys: list[ForeignKeyMetadata]


class SemanticModelBuilder:
    """Builds a semantic model for a connector from inspected SQL metadata."""

    def __init__(
        self,
        repository: RepositoryConnectorMetadataProvider,
    ) -> None:
        self._repository = repository
        self._logger = logging.getLogger(__name__)
        self._sql_connector_factory = SqlConnectorFactory()

    async def build_for_scope(
        self,
        connector_id: UUID,
        workspace_id: UUID,
        scope_schemas: list[str] | None = None,
        scope_tables: list[tuple[str, str]] | None = None,
        scope_columns: list[tuple[str, str, str]] | None = None,
    ) -> SemanticModel:
        connector = await self._repository.get_connector(
            connector_id=connector_id,
            workspace_id=workspace_id,
        )
        if connector is None:
            raise ExecutionValidationError("Connector not found.")

        sql_connector = await self._get_sql_connector(connector)
        scope_metadata = await self._collect_scope_metadata(
            sql_connector=sql_connector,
            scope_schemas=scope_schemas,
            scope_tables=scope_tables,
            scope_columns=scope_columns,
        )

        tables = self._build_semantic_tables(connector, scope_metadata)
        connector_name = connector.name
        relationships = self._infer_relationships(
            connector_name=connector_name,
            tables=tables,
            scope_metadata=scope_metadata,
        )

        return SemanticModel(
            version="1.0",
            connector=connector_name,
            name=f"{connector_name} semantic model",
            description=f"Semantic model generated from {connector_name}",
            datasets=tables,
            relationships=relationships or None,
        )

    async def build_yaml_for_scope(
        self,
        connector_id: UUID,
        workspace_id: UUID,
        scope_schemas: list[str] | None = None,
        scope_tables: list[tuple[str, str]] | None = None,
        scope_columns: list[tuple[str, str, str]] | None = None,
    ) -> str:
        semantic_model = await self.build_for_scope(
            connector_id=connector_id,
            workspace_id=workspace_id,
            scope_schemas=scope_schemas,
            scope_tables=scope_tables,
            scope_columns=scope_columns,
        )
        return semantic_model.yml_dump()

    def parse_yaml_to_model(self, yaml_content: str) -> SemanticModel:
        return load_semantic_model(yaml_content)

    async def _collect_scope_metadata(
        self,
        *,
        sql_connector: SqlConnector,
        scope_schemas: list[str] | None,
        scope_tables: list[tuple[str, str]] | None,
        scope_columns: list[tuple[str, str, str]] | None,
    ) -> list[ScopedTableMetadata]:
        schemas = list(scope_schemas) if scope_schemas else await sql_connector.fetch_schemas()
        tables_by_schema: dict[str, list[str]] = {}

        if scope_tables:
            requested_schemas = {schema for schema, _table in scope_tables}
            schemas = [schema for schema in schemas if schema in requested_schemas] or sorted(requested_schemas)
            for schema, table in scope_tables:
                tables_by_schema.setdefault(schema, []).append(table)
        else:
            for schema in schemas:
                tables_by_schema[schema] = await sql_connector.fetch_tables(schema)

        selected_columns: dict[tuple[str, str], set[str]] = {}
        if scope_columns:
            for schema, table, column in scope_columns:
                selected_columns.setdefault((schema, table), set()).add(column)

        scoped_metadata: list[ScopedTableMetadata] = []
        for schema in schemas:
            for table_name in tables_by_schema.get(schema, []):
                columns = await sql_connector.fetch_columns(schema, table_name)
                allowed_columns = selected_columns.get((schema, table_name))
                if allowed_columns is not None:
                    columns = [column for column in columns if column.name in allowed_columns]

                foreign_keys = await sql_connector.fetch_foreign_keys(schema, table_name)
                scoped_metadata.append(
                    ScopedTableMetadata(
                        schema=schema,
                        table_metadata=TableMetadata(schema=schema, name=table_name),
                        columns=columns,
                        foreign_keys=foreign_keys,
                    )
                )

        return scoped_metadata

    def _build_semantic_tables(
        self,
        connector: ConnectorMetadata,
        scope_metadata: list[ScopedTableMetadata],
    ) -> dict[str, Table]:
        tables: dict[str, Table] = {}

        for scope in scope_metadata:
            table_key = self._make_table_key(
                connector_name=connector.name,
                schema=scope.schema,
                table_name=scope.table_metadata.name,
            )
            dimensions: list[Dimension] = []
            measures: list[Measure] = []

            for column in scope.columns:
                normalized_type = self._map_column_type(column.data_type)

                if normalized_type in {"integer", "decimal", "float"} and "_id" not in column.name.lower():
                    measures.append(
                        Measure(
                            name=column.name,
                            expression=column.name,
                            type=normalized_type,
                            aggregation=MeasureAggregation.sum.value,
                            description=f"Aggregate {column.name} from {scope.table_metadata.name}",
                            synonyms=[column.name],
                        )
                    )
                    continue

                dimensions.append(
                    Dimension(
                        name=column.name,
                        expression=column.name,
                        type=normalized_type,
                        primary_key=column.is_primary_key
                        or self._is_probable_primary_key(column.name, scope.table_metadata.name),
                        description=f"Column {column.name} from {scope.table_metadata.name}",
                        synonyms=[column.name],
                    )
                )

            tables[table_key] = Table(
                relation_name=scope.table_metadata.name,
                schema_name=scope.schema,
                description=f"Table {scope.table_metadata.name} from connector {connector.name}",
                synonyms=[
                    scope.table_metadata.name,
                    f"{scope.schema}.{scope.table_metadata.name}",
                ],
                dimensions=dimensions or None,
                measures=measures or None,
            )

        return tables

    def _infer_relationships(
        self,
        *,
        connector_name: str,
        tables: dict[str, Table],
        scope_metadata: list[ScopedTableMetadata],
    ) -> list[Relationship]:
        relationships: list[Relationship] = []

        for scoped in scope_metadata:
            source_table_key = self._make_table_key(
                connector_name=connector_name,
                schema=scoped.schema,
                table_name=scoped.table_metadata.name,
            )
            if source_table_key not in tables:
                continue

            for foreign_key in scoped.foreign_keys:
                target_table_key = self._make_table_key(
                    connector_name=connector_name,
                    schema=foreign_key.schema,
                    table_name=foreign_key.table,
                )
                if target_table_key not in tables:
                    continue

                relationships.append(
                    Relationship(
                        name=f"{source_table_key}_to_{target_table_key}_{foreign_key.column}",
                        source_dataset=source_table_key,
                        source_field=foreign_key.column,
                        target_dataset=target_table_key,
                        target_field=foreign_key.foreign_key,
                        type="many_to_one",
                    )
                )

        return relationships

    async def _get_sql_connector(self, connector: ConnectorMetadata) -> SqlConnector:
        if not connector.connector_type:
            raise ExecutionValidationError("Connector type is required.")

        connector_type = ConnectorRuntimeType(connector.connector_type.upper())
        try:
            self._sql_connector_factory.get_sql_connector_class_reference(connector_type)
        except ValueError as exc:
            raise ExecutionValidationError(
                f"Connector type {connector_type.value} does not support SQL metadata extraction."
            ) from exc

        connector_payload = self._resolve_connector_config(connector)
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_payload.get("config", {}))
        sql_connector = self._sql_connector_factory.create_sql_connector(
            connector_type,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    @staticmethod
    def _resolve_connector_config(connector: ConnectorMetadata) -> dict[str, Any]:
        resolved_payload = dict(connector.config or {})
        runtime_config = dict(resolved_payload.get("config") or {})

        if connector.connection_metadata is not None:
            metadata = connector.connection_metadata.model_dump(exclude_none=True, by_alias=True)
            extra = metadata.pop("extra", {})
            for key, value in metadata.items():
                runtime_config.setdefault(key, value)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    if value is not None:
                        runtime_config.setdefault(key, value)

        resolved_payload["config"] = runtime_config
        return resolved_payload

    @staticmethod
    def _make_table_key(connector_name: str, schema: str, table_name: str) -> str:
        sanitized = f"{connector_name}_{schema}_{table_name}"
        sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", sanitized)
        return sanitized.lower()

    @staticmethod
    def _map_column_type(data_type: str) -> str:
        normalized = data_type.lower()
        if any(token in normalized for token in TYPE_NUMERIC):
            if "int" in normalized and "point" not in normalized:
                return "integer"
            if any(token in normalized for token in ("double", "float")):
                return "float"
            return "decimal"
        if any(token == normalized or token in normalized for token in TYPE_BOOLEAN):
            return "boolean"
        if any(token == normalized or token in normalized for token in TYPE_DATE) or any(
            token in normalized for token in ("date", "time")
        ):
            return "date"
        return "string"

    @staticmethod
    def _is_probable_primary_key(column_name: str, table_name: str) -> bool:
        normalized_column = column_name.lower()
        normalized_table = re.sub(r"[^a-z0-9]", "", table_name.lower())
        if normalized_column == "id":
            return True
        if normalized_column == f"{normalized_table}id":
            return True
        if normalized_column.endswith("_id") and len(normalized_column) > 3:
            return True
        return False
