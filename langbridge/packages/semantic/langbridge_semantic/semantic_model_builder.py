from dataclasses import dataclass
import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Protocol, Tuple
from uuid import UUID

from langbridge.packages.connectors.langbridge_connectors.api import (
    ConnectorRuntimeType,
    TableMetadata,
    ColumnMetadata,
    ForeignKeyMetadata,
    SqlConnector,
)
from langbridge.packages.semantic.langbridge_semantic.model import MeasureAggregation
from langbridge.packages.semantic.langbridge_semantic import Dimension, Measure, Relationship, SemanticModel, Table
from langbridge.packages.semantic.langbridge_semantic.loader import load_semantic_model
from langbridge.packages.contracts.connectors import ConnectorResponse

logger = logging.getLogger(__name__)


TYPE_NUMERIC = {"number", "decimal", "numeric", "int", "integer", "float", "double", "real"}
TYPE_BOOLEAN = {"boolean", "bool"}
TYPE_DATE = {"date", "datetime", "timestamp", "time"}

@dataclass
class ScopedTableMetadata:
    schema: str
    table_metadata: TableMetadata
    columns: List[ColumnMetadata]
    foreign_keys: List[ForeignKeyMetadata]


class ConnectorCatalogService(Protocol):
    async def get_connector(self, connector_id: UUID) -> ConnectorResponse: ...

    async def async_create_sql_connector(
        self,
        runtime_type: ConnectorRuntimeType,
        connector_config: dict,
    ) -> SqlConnector: ...


class SemanticModelBuilder:
    """Builds a semantic data model across organization/project connectors."""

    def __init__(
        self,
        connector_service: ConnectorCatalogService,
    ) -> None:
        self._connector_service = connector_service
        self._logger = logging.getLogger(__name__)

    async def build_for_scope(
        self,
        connector_id: UUID,
        scope_schemas: Optional[List[str]] = None, # schema scope
        scope_tables: Optional[List[Tuple[str, str]]] = None, # schema, table scope
        scope_columns: Optional[List[Tuple[str, str, str]]] = None, # schema, table, column scope
    ) -> SemanticModel:
        connector = await self.__get_connector(connector_id)
        sql_connector: SqlConnector = await self._get_sql_connector(connector)

        scope_metadata: List[ScopedTableMetadata] = []

        if not scope_schemas:
            schemas = await sql_connector.fetch_schemas()
        for schema in schemas:
            if not scope_tables:
                scope_tables = [
                    (schema, table)
                    for table in await sql_connector.fetch_tables(schema)
                ]
            for table in scope_tables:
                columns_metadata: List[ColumnMetadata] = []
                table_metadata = TableMetadata(schema=schema, name=table[1])
                columns_metadata = await sql_connector.fetch_columns(schema, table[1])
                if scope_columns:
                    columns_metadata = [
                        column
                        for column in columns_metadata
                        if (column.name) in [col[2] for col in scope_columns]
                    ]
                    
                foreign_keys_metadata = await sql_connector.fetch_foreign_keys(schema, table[1])
                scope_metadata.append(
                    ScopedTableMetadata(
                        schema=schema,
                        table_metadata=table_metadata,
                        columns=columns_metadata,
                        foreign_keys=foreign_keys_metadata,
                    )
                )

        tables: Dict[str, Table] = self._build_semantic_tables(connector, scope_metadata)
        relationships: List[Relationship] = self._infer_relationships(connector.name, tables, scope_metadata)

        return SemanticModel(
            version="1.0",
            connector=connector.name if isinstance(connector.name, str) else connector.name.value,
            description=f"Semantic Model generated from {connector.name}",
            tables=tables,
            relationships=relationships or None,
        )

    async def build_yaml_for_scope(
        self,
        connector_id: UUID,
    ) -> str:
        semantic_model = await self.build_for_scope(connector_id)
        return semantic_model.yml_dump()
        
    def parse_yaml_to_model(self, yaml_content: str) -> SemanticModel:
        return load_semantic_model(yaml_content)
    
    def _build_semantic_tables(
        self,
        connector: ConnectorResponse,
        scope_metadata: List[ScopedTableMetadata],
    ) -> Dict[str, Table]:
        tables: Dict[str, Table] = {}
        connector_name: str = connector.name if isinstance(connector.name, str) else connector.name.value

        for scope in scope_metadata:
            table_key = self._make_table_key(connector_name, scope.schema, scope.table_metadata.name)
            dimensions: List[Dimension] = []
            dimensions_column_names = set()
            measures: List[Measure] = []

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
                        ) # type: ignore
                    )
                else:
                    is_pk = self._is_probable_primary_key(column.name, scope.table_metadata.name)
                    dimension = Dimension(
                        name=column.name,
                        expression=column.name,
                        type=normalized_type,
                        primary_key=is_pk,
                        description=f"Column {column.name} from {scope.table_metadata.name}",
                        synonyms=[column.name],
                    ) # type: ignore
                    dimensions.append(dimension)
                    dimensions_column_names.add(column.name.lower())

            tables[table_key] = Table(
                name=scope.table_metadata.name,
                schema=scope.schema,
                description=f"Table {scope.table_metadata.name} from connector {connector_name}",
                synonyms=[
                    scope.table_metadata.name,
                    f"{scope.schema}.{scope.table_metadata.name}"
                ],
                dimensions=dimensions or None,
                measures=measures or None,
            )

        return tables

    def _infer_relationships(self, connector_name: str, tables: Dict[str, Table], scope_metadata: List[ScopedTableMetadata]) -> List[Relationship]:
        relationships: List[Relationship] = []
        pk_index: Dict[str, List[Tuple[str, Dimension]]] = defaultdict(list)

        for table_name, table in tables.items():
            for dimension in table.dimensions or []:
                if dimension.primary_key:
                    pk_index[dimension.name.lower()].append((table_name, dimension))
        
        for scoped in scope_metadata:
            for foreign_key in scoped.foreign_keys:
                source_table_key = self._make_table_key(
                    connector_name=connector_name,
                    schema=foreign_key.schema,
                    table_name=scoped.table_metadata.name,
                )
                target_table_key = self._make_table_key(
                    connector_name=connector_name,
                    schema=foreign_key.schema,
                    table_name=foreign_key.table,
                )
                relationship_name = f"{source_table_key}_to_{target_table_key}"
                join_expression = f"{source_table_key}.{foreign_key.column} = {target_table_key}.{foreign_key.foreign_key}"
                relationships.append(
                    Relationship(
                        name=relationship_name,
                        from_=source_table_key,
                        to=target_table_key,
                        type="many_to_one",
                        join_on=join_expression,
                    )
                )
        
        return relationships
    
    async def __get_connector(self, connector_id: UUID) -> ConnectorResponse:
        return await self._connector_service.get_connector(connector_id)

    async def _get_sql_connector(self, connector: ConnectorResponse) -> SqlConnector:
        if not connector.connector_type:
            raise Exception("Connector type is required")
        runtime_type = ConnectorRuntimeType(connector.connector_type.upper())
        connector_config = connector.config or {}
        return await self._connector_service.async_create_sql_connector(
            runtime_type,
            connector_config,
        )

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

    @staticmethod
    def _infer_foreign_key_target(
        column_name: str,
        pk_index: Dict[str, List[Tuple[str, Dimension]]],
        current_table: str,
    ) -> Optional[Tuple[str, Dimension]]:
        key = column_name.lower()
        if key not in pk_index:
          return None

    @staticmethod
    def _build_metric_expression(table: Table, measure: Measure) -> str:
        table_ref = f"{table.schema}.{table.name}" if table.schema else table.name
        column_ref = f"{table_ref}.{measure.name}"
        aggregation = (measure.aggregation or "").strip().lower()
        if not aggregation:
            return column_ref
        if aggregation == "count":
            return f"COUNT({column_ref})"
        return f"{aggregation.upper()}({column_ref})"

        for table_name, dimension in pk_index[key]:
            if table_name == current_table:
                continue
            return table_name, dimension
        return None
