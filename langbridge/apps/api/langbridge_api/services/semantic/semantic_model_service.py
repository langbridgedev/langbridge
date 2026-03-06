import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Type
from uuid import UUID

import yaml

from langbridge.packages.connectors.langbridge_connectors.api import (
    ConnectorRuntimeType, 
    VectorDBConnectorFactory, 
    VectorDBType, 
    ManagedVectorDB
)
from langbridge.packages.common.langbridge_common.db.auth import Project
from langbridge.packages.common.langbridge_common.db.semantic import SemanticModelEntry
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.errors.connector_errors import ConnectorError
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.apps.api.langbridge_api.services.environment_service import EnvironmentService, EnvironmentSettingKey
from .semantic_search_sercice import SemanticSearchService
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationRepository,
    ProjectRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import SemanticModelRepository
from langbridge.packages.common.langbridge_common.utils.lineage import LineageNodeType
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelCatalogColumnResponse,
    SemanticModelCatalogResponse,
    SemanticModelCatalogSchemaResponse,
    SemanticModelCatalogTableResponse,
    SemanticModelRecordResponse,
    SemanticModelCreateRequest,
    SemanticModelSelectionGenerateResponse,
    SemanticModelUpdateRequest,
)
from langbridge.packages.semantic.langbridge_semantic.loader import SemanticModelError, load_semantic_model
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.semantic_model_builder import SemanticModelBuilder
from langbridge.apps.api.langbridge_api.services.agent_service import AgentService
from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService
from langbridge.apps.api.langbridge_api.services.lineage_service import LineageService
from langbridge.packages.common.langbridge_common.utils.embedding_provider import EmbeddingProvider, EmbeddingProviderError

VALUE_MAX_LENGTH = 256
TYPE_NUMERIC = {"number", "decimal", "numeric", "int", "integer", "float", "double", "real"}
TYPE_BOOLEAN = {"boolean", "bool"}
TYPE_DATE = {"date", "datetime", "timestamp", "time"}


class SemanticModelService:
    def __init__(
        self,
        repository: SemanticModelRepository,
        builder: SemanticModelBuilder,
        organization_repository: OrganizationRepository,
        project_repository: ProjectRepository,
        connector_service: ConnectorService,
        agent_service: AgentService,
        semantic_search_service: SemanticSearchService,
        emvironment_service: EnvironmentService,
        lineage_service: LineageService | None = None,
    ) -> None:
        self._repository = repository
        self._builder = builder
        self._organization_repository = organization_repository
        self._project_repository = project_repository
        self._connector_service = connector_service
        self._agent_service = agent_service
        self._semantic_search_service = semantic_search_service
        self._emvironment_service = emvironment_service
        self._lineage_service = lineage_service
        
        self._vector_factory = VectorDBConnectorFactory()

    async def generate_model_yaml(self, connector_id: UUID) -> str:
        return await self._builder.build_yaml_for_scope(connector_id)

    async def get_connector_catalog(self, connector_id: UUID) -> SemanticModelCatalogResponse:
        connector, sql_connector = await self._load_sql_connector(connector_id)
        schemas = await sql_connector.fetch_schemas()

        schema_responses: List[SemanticModelCatalogSchemaResponse] = []
        table_count = 0
        column_count = 0
        for schema_name in sorted(schemas):
            table_names = await sql_connector.fetch_tables(schema_name)
            table_responses: List[SemanticModelCatalogTableResponse] = []
            for table_name in sorted(table_names):
                raw_columns = await sql_connector.fetch_columns(schema_name, table_name)
                columns = [
                    SemanticModelCatalogColumnResponse(
                        name=column.name,
                        type=column.data_type,
                        nullable=getattr(column, "is_nullable", None),
                        primary_key=bool(getattr(column, "is_primary_key", False)),
                    )
                    for column in raw_columns
                ]
                table_reference = self._table_reference(schema_name, table_name)
                table_responses.append(
                    SemanticModelCatalogTableResponse(
                        schema=schema_name,
                        name=table_name,
                        fully_qualified_name=table_reference,
                        columns=columns,
                    )
                )
                table_count += 1
                column_count += len(columns)
            schema_responses.append(
                SemanticModelCatalogSchemaResponse(
                    name=schema_name,
                    tables=table_responses,
                )
            )

        return SemanticModelCatalogResponse(
            connector_id=connector_id,
            schemas=schema_responses,
            table_count=table_count,
            column_count=column_count,
        )

    async def generate_model_yaml_from_selection(
        self,
        *,
        connector_id: UUID,
        selected_tables: List[str],
        selected_columns: Dict[str, List[str]],
        include_sample_values: bool = False,
        description: str | None = None,
    ) -> SemanticModelSelectionGenerateResponse:
        connector, sql_connector = await self._load_sql_connector(connector_id)
        catalog = await self.get_connector_catalog(connector_id)
        table_blueprints = await self._build_selected_table_blueprints(
            sql_connector=sql_connector,
            catalog=catalog,
            selected_tables=selected_tables,
            selected_columns=selected_columns,
        )
        payload, warnings = self._build_payload_from_table_blueprints(
            connector_name=connector.name,
            table_blueprints=table_blueprints,
            description=description,
        )
        if include_sample_values:
            warnings.append("include_sample_values is not currently supported for semantic model generation.")
        self._validate_generated_payload(payload=payload, table_blueprints=table_blueprints)

        return SemanticModelSelectionGenerateResponse(
            yaml_text=yaml.safe_dump(payload, sort_keys=False),
            warnings=warnings,
        )

    async def list_models(
        self,
        organization_id: UUID,
        project_id: UUID | None = None,
        model_kind: Literal["all", "standard", "unified"] = "all",
    ) -> list[SemanticModelRecordResponse]:
        models = await self._repository.list_for_scope(
            organization_id=organization_id,
            project_id=project_id,
        )
        if model_kind != "all":
            models = [
                model
                for model in models
                if self._resolve_model_kind(model) == model_kind
            ]
        return [self._normalize_record(model) for model in models]

    async def list_all_models(self) -> list[SemanticModelRecordResponse]:
        models = await self._repository.get_all()
        return [self._normalize_record(model) for model in models]

    async def get_model(
        self,
        model_id: UUID,
        organization_id: UUID,
    ) -> SemanticModelRecordResponse:
        model = await self._get_model_entity(model_id=model_id, organization_id=organization_id)
        return self._normalize_record(model)

    async def delete_model(self, model_id: UUID, organization_id: UUID) -> None:
        model = await self._get_model_entity(model_id=model_id, organization_id=organization_id)
        if self._lineage_service is not None:
            await self._lineage_service.delete_node_lineage(
                workspace_id=organization_id,
                node_type=(
                    self._semantic_model_node_type(model)
                ),
                node_id=str(model.id),
            )
        await self._repository.delete(model)

    async def create_model(
        self,
        request: SemanticModelCreateRequest,
    ) -> SemanticModelRecordResponse:
        organization = await self._organization_repository.get_by_id(
            request.organization_id
        )
        if not organization:
            raise BusinessValidationError("Organization not found")

        project: Project | None = None
        if request.project_id:
            project: Project | None = await self._project_repository.get_by_id(request.project_id)
            if not project:
                raise BusinessValidationError("Project not found")
            if project.organization_id != organization.id:
                raise BusinessValidationError(
                    "Project does not belong to the specified organization"
                )

        raw_model_payload: Dict[str, Any] | None = None
        if request.auto_generate or not request.model_yaml:
            semantic_model: SemanticModel = await self._builder.build_for_scope(
                connector_id=request.connector_id
            )
        else:
            raw_model_payload = self._parse_yaml_payload(request.model_yaml)
            try:
                semantic_model = load_semantic_model(request.model_yaml)
            except SemanticModelError as exc:
                raise BusinessValidationError(
                    f"Semantic model failed validation: {exc}"
                ) from exc
        is_unified_model = self._is_unified_payload(raw_model_payload)

        connector = await self._connector_service.get_connector(request.connector_id)
        if connector and not semantic_model.connector and not is_unified_model:
            semantic_model.connector = connector.name if isinstance(connector.name, str) else connector.name.value

        if request.name and not semantic_model.name:
            semantic_model.name = request.name
        if request.description:
            semantic_model.description = request.description

        semantic_id: UUID = uuid.uuid4()

        if not is_unified_model:
            await self._populate_vector_indexes(semantic_model, request.connector_id, semantic_id)

        if is_unified_model and raw_model_payload is not None:
            payload = raw_model_payload
            if request.name and not payload.get("name"):
                payload["name"] = request.name
            if request.description and not payload.get("description"):
                payload["description"] = request.description
        else:
            payload = semantic_model.model_dump(by_alias=True, exclude_none=True)
        model_yaml = yaml.safe_dump(payload, sort_keys=False)
        content_json = json.dumps(payload)

        entry = SemanticModelEntry(
            id=semantic_id,
            connector_id=request.connector_id,
            organization_id=request.organization_id,
            project_id=request.project_id,
            name=request.name,
            description=request.description,
            content_yaml=model_yaml,
            content_json=content_json,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        self._repository.add(entry)
        if self._lineage_service is not None:
            await self._lineage_service.register_semantic_model_lineage(model=entry)
        return SemanticModelRecordResponse.model_validate(entry)

    async def update_model(
        self,
        model_id: UUID,
        organization_id: UUID,
        request: SemanticModelUpdateRequest,
    ) -> SemanticModelRecordResponse:
        model = await self._get_model_entity(model_id=model_id, organization_id=organization_id)
        organization = await self._organization_repository.get_by_id(organization_id)
        if not organization:
            raise BusinessValidationError("Organization not found")

        project_id = model.project_id
        if "project_id" in request.model_fields_set:
            project_id = request.project_id
        if project_id:
            project = await self._project_repository.get_by_id(project_id)
            if not project:
                raise BusinessValidationError("Project not found")
            if project.organization_id != organization.id:
                raise BusinessValidationError(
                    "Project does not belong to the specified organization"
                )

        connector_id = request.connector_id or model.connector_id

        if request.name is not None and not request.name.strip():
            raise BusinessValidationError("Semantic model name is required")

        existing_payload = self._parse_model_payload(model)
        raw_model_payload = self._parse_yaml_payload(request.model_yaml) if request.model_yaml is not None else None
        rebuild_content = bool(request.auto_generate or request.model_yaml is not None)
        is_unified_model = (
            self._is_unified_payload(raw_model_payload)
            if request.model_yaml is not None
            else self._is_unified_payload(existing_payload)
        )
        if rebuild_content:
            if request.auto_generate or not request.model_yaml:
                semantic_model = await self._builder.build_for_scope(
                    connector_id=connector_id
                )
            else:
                try:
                    semantic_model = load_semantic_model(request.model_yaml)
                except SemanticModelError as exc:
                    raise BusinessValidationError(
                        f"Semantic model failed validation: {exc}"
                    ) from exc
        else:
            try:
                semantic_model = load_semantic_model(model.content_yaml)
            except SemanticModelError as exc:
                raise BusinessValidationError(
                    f"Semantic model failed validation: {exc}"
                ) from exc

        connector = await self._connector_service.get_connector(connector_id)
        if (
            connector
            and (request.connector_id is not None or not semantic_model.connector)
            and not is_unified_model
        ):
            semantic_model.connector = connector.name if isinstance(connector.name, str) else connector.name.value

        if request.name is not None:
            model.name = request.name.strip()
            if model.name and not semantic_model.name:
                semantic_model.name = model.name
        if request.description is not None:
            model.description = request.description.strip() or None
            if model.description and not semantic_model.description:
                semantic_model.description = model.description

        model.connector_id = connector_id
        model.project_id = project_id

        if rebuild_content and not is_unified_model:
            await self._populate_vector_indexes(
                semantic_model,
                connector_id,
                model.id,
                reset_index=True,
            )

        if is_unified_model:
            if rebuild_content and raw_model_payload is not None:
                payload = raw_model_payload
            else:
                payload = existing_payload or semantic_model.model_dump(by_alias=True, exclude_none=True)
            if model.name and not payload.get("name"):
                payload["name"] = model.name
            if model.description and not payload.get("description"):
                payload["description"] = model.description
        else:
            payload = semantic_model.model_dump(by_alias=True, exclude_none=True)
        model.content_yaml = yaml.safe_dump(payload, sort_keys=False)
        model.content_json = json.dumps(payload)
        model.updated_at = datetime.now(timezone.utc)
        if self._lineage_service is not None:
            await self._lineage_service.register_semantic_model_lineage(model=model)

        return SemanticModelRecordResponse.model_validate(model)

    async def _load_sql_connector(self, connector_id: UUID):
        connector = await self._connector_service.get_connector(connector_id)
        if not connector.connector_type:
            raise BusinessValidationError("Connector type is required to build semantic models.")
        runtime_type = ConnectorRuntimeType(connector.connector_type.upper())
        sql_connector = await self._connector_service.async_create_sql_connector(
            runtime_type,
            connector.config or {},
        )
        return connector, sql_connector

    async def _build_selected_table_blueprints(
        self,
        *,
        sql_connector,
        catalog: SemanticModelCatalogResponse,
        selected_tables: List[str],
        selected_columns: Dict[str, List[str]],
    ) -> List[Dict[str, Any]]:
        if not selected_tables:
            raise BusinessValidationError("At least one table must be selected.")

        table_lookup: Dict[str, SemanticModelCatalogTableResponse] = {}
        table_name_lookup: Dict[str, List[SemanticModelCatalogTableResponse]] = {}
        for schema_entry in catalog.schemas:
            for table in schema_entry.tables:
                normalized_ref = table.fully_qualified_name.strip().lower()
                table_lookup[normalized_ref] = table
                bare_name = table.name.strip().lower()
                table_name_lookup.setdefault(bare_name, []).append(table)

        normalized_selected_columns: Dict[str, List[str]] = {}
        for table_key, columns in selected_columns.items():
            normalized_key = str(table_key).strip().lower()
            if not normalized_key:
                continue
            normalized_selected_columns[normalized_key] = [str(column).strip() for column in columns if str(column).strip()]

        entity_name_registry: set[str] = set()
        table_blueprints: List[Dict[str, Any]] = []
        for selected_table in selected_tables:
            table_reference = str(selected_table).strip()
            if not table_reference:
                continue
            normalized_reference = table_reference.lower()
            table = table_lookup.get(normalized_reference)
            if table is None:
                bare_name = normalized_reference.split(".")[-1]
                candidates = table_name_lookup.get(bare_name, [])
                if len(candidates) == 1:
                    table = candidates[0]
                else:
                    raise BusinessValidationError(
                        f"Selected table '{selected_table}' is unknown or ambiguous for this connector."
                    )

            column_lookup = {column.name.lower(): column for column in table.columns}
            selected_column_names = (
                normalized_selected_columns.get(normalized_reference)
                or normalized_selected_columns.get(table.fully_qualified_name.lower())
                or normalized_selected_columns.get(table.name.lower())
            )
            if not selected_column_names:
                selected_column_names = [column.name for column in table.columns]

            resolved_columns: List[SemanticModelCatalogColumnResponse] = []
            for column_name in selected_column_names:
                column = column_lookup.get(column_name.lower())
                if column is None:
                    raise BusinessValidationError(
                        f"Column '{column_name}' is not available on table '{table.fully_qualified_name}'."
                    )
                resolved_columns.append(column)

            foreign_keys = await sql_connector.fetch_foreign_keys(table.schema, table.name)
            entity_name = self._build_entity_name(
                schema=table.schema,
                table_name=table.name,
                registry=entity_name_registry,
            )
            table_blueprints.append(
                {
                    "entity_name": entity_name,
                    "schema": table.schema,
                    "table_name": table.name,
                    "table_reference": table.fully_qualified_name,
                    "selected_columns": resolved_columns,
                    "foreign_keys": foreign_keys,
                }
            )

        if not table_blueprints:
            raise BusinessValidationError("No valid table selections were provided.")
        return table_blueprints

    def _build_payload_from_table_blueprints(
        self,
        *,
        connector_name: str,
        table_blueprints: List[Dict[str, Any]],
        description: str | None = None,
    ) -> Tuple[Dict[str, Any], List[str]]:
        warnings: List[str] = []
        tables_payload: Dict[str, Any] = {}
        entity_lookup = {
            self._table_reference(blueprint["schema"], blueprint["table_name"]).lower(): blueprint["entity_name"]
            for blueprint in table_blueprints
        }
        relationship_payload: List[Dict[str, Any]] = []
        relationship_names: set[str] = set()

        for blueprint in table_blueprints:
            dimensions: List[Dict[str, Any]] = []
            measures: List[Dict[str, Any]] = []
            for column in blueprint["selected_columns"]:
                mapped_type = self._map_column_type(column.type)
                is_identifier = column.name.lower() == "id" or column.name.lower().endswith("_id")
                if mapped_type in {"integer", "decimal", "float"} and not is_identifier and not column.primary_key:
                    measures.append(
                        {
                            "name": column.name,
                            "expression": column.name,
                            "type": mapped_type,
                            "aggregation": "sum",
                            "description": f"Aggregate {column.name} from {blueprint['table_name']}",
                        }
                    )
                else:
                    dimensions.append(
                        {
                            "name": column.name,
                            "expression": column.name,
                            "type": mapped_type,
                            "primary_key": bool(column.primary_key),
                            "description": f"Column {column.name} from {blueprint['table_name']}",
                        }
                    )

            if not dimensions and measures:
                first_measure = measures.pop(0)
                dimensions.append(
                    {
                        "name": first_measure["name"],
                        "expression": first_measure["expression"],
                        "type": first_measure["type"],
                        "primary_key": False,
                        "description": first_measure["description"],
                    }
                )
                warnings.append(
                    f"Table '{blueprint['table_reference']}' had only numeric columns; converted one column to a dimension."
                )

            tables_payload[blueprint["entity_name"]] = {
                "schema": blueprint["schema"],
                "name": blueprint["table_name"],
                "description": f"Table {blueprint['table_name']} from connector {connector_name}",
                "dimensions": dimensions or None,
                "measures": measures or None,
            }

        for blueprint in table_blueprints:
            source_entity = blueprint["entity_name"]
            for foreign_key in blueprint["foreign_keys"]:
                target_reference = self._table_reference(foreign_key.schema, foreign_key.table).lower()
                target_entity = entity_lookup.get(target_reference)
                if not target_entity:
                    continue
                relationship_name = f"{source_entity}_to_{target_entity}"
                if relationship_name in relationship_names:
                    continue
                relationship_names.add(relationship_name)
                relationship_payload.append(
                    {
                        "name": relationship_name,
                        "from_": source_entity,
                        "to": target_entity,
                        "type": "many_to_one",
                        "join_on": f"{source_entity}.{foreign_key.column} = {target_entity}.{foreign_key.foreign_key}",
                    }
                )

        if not relationship_payload:
            warnings.append("No relationships were inferred from selected tables.")

        payload = {
            "version": "1.0",
            "connector": connector_name,
            "description": description or f"Semantic Model generated from {connector_name}",
            "tables": tables_payload,
            "relationships": relationship_payload or None,
        }
        return payload, warnings

    def _validate_generated_payload(
        self,
        *,
        payload: Dict[str, Any],
        table_blueprints: List[Dict[str, Any]],
    ) -> None:
        try:
            model = load_semantic_model(payload)
        except SemanticModelError as exc:
            raise BusinessValidationError(f"Generated semantic model is invalid: {exc}") from exc

        relationship_names = [relationship.name for relationship in model.relationships or []]
        if len(relationship_names) != len(set(relationship_names)):
            raise BusinessValidationError("Generated semantic model contains duplicate relationship names.")

        for blueprint in table_blueprints:
            entity_name = blueprint["entity_name"]
            table = model.tables.get(entity_name)
            if table is None:
                raise BusinessValidationError(
                    f"Generated semantic model is missing selected table '{entity_name}'."
                )
            selected_column_names = {column.name for column in blueprint["selected_columns"]}
            generated_column_names = {
                dimension.name for dimension in table.dimensions or []
            } | {
                measure.name for measure in table.measures or []
            }
            if selected_column_names != generated_column_names:
                raise BusinessValidationError(
                    f"Generated semantic model columns do not match selection for table '{blueprint['table_reference']}'."
                )

    @staticmethod
    def _table_reference(schema: str, table_name: str) -> str:
        schema_value = (schema or "").strip()
        table_value = (table_name or "").strip()
        if schema_value:
            return f"{schema_value}.{table_value}"
        return table_value

    @staticmethod
    def _build_entity_name(*, schema: str, table_name: str, registry: set[str]) -> str:
        base_name = re.sub(r"[^a-zA-Z0-9_]+", "_", f"{schema}_{table_name}".strip("_")).lower()
        root_name = base_name or "table"
        candidate = root_name
        suffix = 2
        while candidate in registry:
            candidate = f"{root_name}_{suffix}"
            suffix += 1
        registry.add(candidate)
        return candidate

    @staticmethod
    def _map_column_type(data_type: str) -> str:
        normalized = (data_type or "").lower()
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

    async def _get_model_entity(self, model_id: UUID, organization_id: UUID) -> SemanticModelEntry:
        model = await self._repository.get_for_scope(
            model_id=model_id,
            organization_id=organization_id,
        )
        if not model:
            raise BusinessValidationError("Semantic model not found")
        return model

    def _normalize_record(self, model: SemanticModelEntry) -> SemanticModelRecordResponse:
        response = SemanticModelRecordResponse.model_validate(model)
        if self._resolve_model_kind(model) == "unified":
            return response
        try:
            semantic_model = load_semantic_model(response.content_yaml)
        except SemanticModelError:
            return response
        if response.name and not semantic_model.name:
            semantic_model.name = response.name
        if response.description and not semantic_model.description:
            semantic_model.description = response.description
        response.content_yaml = semantic_model.yml_dump()
        return response

    @staticmethod
    def _resolve_model_kind(model: SemanticModelEntry) -> Literal["standard", "unified"]:
        payload = SemanticModelService._parse_model_payload(model)
        if payload is None:
            return "standard"
        has_unified_shape = isinstance(payload.get("semantic_models"), list) or isinstance(
            payload.get("source_models"), list
        )
        return "unified" if has_unified_shape else "standard"

    @staticmethod
    def _semantic_model_node_type(model: SemanticModelEntry) -> LineageNodeType:
        return (
            LineageNodeType.UNIFIED_SEMANTIC_MODEL
            if SemanticModelService._resolve_model_kind(model) == "unified"
            else LineageNodeType.SEMANTIC_MODEL
        )

    @staticmethod
    def _parse_model_payload(model: SemanticModelEntry) -> Dict[str, Any] | None:
        if model.content_json:
            try:
                parsed_json = json.loads(model.content_json)
                if isinstance(parsed_json, dict):
                    return parsed_json
            except Exception:
                pass
        if model.content_yaml:
            try:
                parsed_yaml = yaml.safe_load(model.content_yaml)
                if isinstance(parsed_yaml, dict):
                    return parsed_yaml
            except Exception:
                return None
        return None

    @staticmethod
    def _parse_yaml_payload(content_yaml: str | None) -> Dict[str, Any] | None:
        if not content_yaml:
            return None
        try:
            parsed = yaml.safe_load(content_yaml)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
        return None

    @staticmethod
    def _is_unified_payload(payload: Dict[str, Any] | None) -> bool:
        if payload is None:
            return False
        return isinstance(payload.get("semantic_models"), list) or isinstance(payload.get("source_models"), list)

    async def _populate_vector_indexes(
        self,
        semantic_model: SemanticModel,
        connector_id: UUID,
        semantic_id: UUID,
        reset_index: bool = False,
    ) -> None:
        vector_targets = self._discover_vectorized_dimensions(semantic_model)
        if not vector_targets:
            return

        connector = await self._connector_service.get_connector(connector_id)
        if not connector:
            raise BusinessValidationError("Connector not found for vectorization.")

        if not connector.connector_type:
            raise BusinessValidationError("Connector missing runtime type; cannot vectorize semantic model.")
        runtime = ConnectorRuntimeType(connector.connector_type.upper())
        sql_connector = await self._connector_service.async_create_sql_connector(
            runtime,
            connector.config or {},
        )

        embedder = await self._build_embedding_provider()

        vector_db_types: List[VectorDBType] = self._vector_factory.get_all_managed_vector_dbs()
        if not vector_db_types:
            raise BusinessValidationError(
                "No managed vector databases are configured; cannot vectorize semantic model."
            )

        vector_managed_instance, connector_response = await self.__get_default_semantic_vecotr_connnector(connector.organization_id, semantic_id)
        await vector_managed_instance.test_connection()
        if reset_index:
            # Ensure the managed index can be recreated when updating an existing model.
            try:
                await vector_managed_instance.delete_index()
            except Exception as exc:
                message = str(exc).lower()
                if "not found" not in message and "does not exist" not in message:
                    raise

        index_initialized = False
        index_dimension: Optional[int] = None

        for target in vector_targets:
            raw_values = await self._fetch_distinct_values(
                sql_connector,
                target["schema"],
                target["table"],
                target["column"],
            )
            values = self._prepare_vector_values(raw_values)
            if not values:
                target["dimension"].vector_index = None
                continue

            embeddings = await embedder.embed(values)
            if not embeddings:
                target["dimension"].vector_index = None
                continue

            vector_length = len(embeddings[0])
            if not index_initialized:
                await vector_managed_instance.create_index(dimension=vector_length)
                index_initialized = True
                index_dimension = vector_length
            elif index_dimension and vector_length != index_dimension:
                raise BusinessValidationError(
                    "Embedding dimension mismatch while populating vector index."
                )

            metadata_entries = [
                {
                    "entity": target["entity"],
                    "column": target["column"],
                    "value": value,
                }
                for value in values
            ]

            try:
                await vector_managed_instance.upsert_vectors(
                    embeddings,
                    metadata=metadata_entries,
                )
            except ConnectorError as exc:
                raise BusinessValidationError(
                    f"Failed to persist vectors for {target['entity']}.{target['column']}: {exc}"
                ) from exc

            vector_reference = self._build_vector_reference(
                vector_db_type=vector_managed_instance.VECTOR_DB_TYPE,
                connector_id=connector_id,
                entity=target["entity"],
                column=target["column"],
                vector_db_config=getattr(vector_managed_instance, "config", None),
            )

            vector_index_meta: Dict[str, Any] = {
                "model": embedder.embedding_model,
                "dimension": vector_length,
                "size": len(values),
                "vector_namespace": str(connector_response.id),
            }
            # Persist the backing vector store metadata so the orchestrator can evolve to read from it.
            vector_index_meta["vector_store"] = {
                "type": vector_managed_instance.VECTOR_DB_TYPE.value,
            }
            config_dict = getattr(vector_managed_instance, "config", None)
            location = getattr(config_dict, "location", None)
            if location:
                vector_index_meta["vector_store"]["location"] = location
            vector_index_meta["reference"] = {
                "entity": target["entity"],
                "column": target["column"],
                "vector_reference": vector_reference,
            }

            target["dimension"].vector_index = vector_index_meta
            target["dimension"].vector_reference = vector_reference

    async def __get_default_semantic_vecotr_connnector(
            self,
            organization_id: UUID,
            semantic_id: UUID
    ) -> Tuple[ManagedVectorDB, ConnectorResponse]:
        #TODO: revist this, currently only supports FAISS, will break on qdrant managed vector db

        default_vector_connector_id: str | None = await self._emvironment_service.get_setting(
            organization_id=organization_id,
            key=EnvironmentSettingKey.DEFAULT_SEMANTIC_VECTOR_CONNECTOR.value,
        )
        if not default_vector_connector_id:
            raise BusinessValidationError(
                "Default semantic vector connector not configured"
            )

        connector_response: ConnectorResponse = await self._connector_service.get_connector(UUID(default_vector_connector_id))

        vector_managed_class_ref: Type[ManagedVectorDB] = (
            self._vector_factory.get_managed_vector_db_class_reference(VectorDBType(connector_response.connector_type))    
        )
        vector_id: str = f"semantic_model_{connector_response.id.hex}_{semantic_id.hex}_idx" # type: ignore
        vector_managed_instance: ManagedVectorDB = await vector_managed_class_ref.create_managed_instance(
            kwargs={
                "index_name": vector_id
            },
        )

        return vector_managed_instance, connector_response

    def _discover_vectorized_dimensions(self, semantic_model: SemanticModel) -> List[Dict[str, Any]]:
        targets: List[Dict[str, Any]] = []
        for entity_name, table in semantic_model.tables.items():
            schema = table.schema or None
            table_name = table.name
            for dimension in table.dimensions or []:
                if not dimension.vectorized:
                    continue
                targets.append(
                    {
                        "entity": entity_name,
                        "schema": schema,
                        "table": table_name,
                        "column": dimension.name,
                        "dimension": dimension,
                    }
                )
        return targets

    def _build_vector_reference(
        self,
        *,
        vector_db_type: VectorDBType,
        connector_id: UUID,
        entity: str,
        column: str,
        vector_db_config: Any | None,
    ) -> str:
        """
        Build a stable reference string pointing to the managed vector index for a given entity/column pair.
        """
        location = getattr(vector_db_config, "location", None)
        location_token = str(location).strip() if location else "managed"
        entity_component = entity.replace(" ", "_")
        column_component = column.replace(" ", "_")
        return f"{vector_db_type.value}:{location_token}:{connector_id}:{entity_component}.{column_component}"

    async def _build_embedding_provider(self) -> EmbeddingProvider:
        connections = await self._agent_service.list_llm_connection_secrets()
        if not connections:
            raise BusinessValidationError(
                "No LLM connections configured; enable one before vectorizing semantic models."
            )
        connection = connections[0]
        try:
            return EmbeddingProvider.from_llm_connection(connection)
        except EmbeddingProviderError as exc:
            raise BusinessValidationError(f"Embedding provider misconfigured: {exc}") from exc

    async def _fetch_distinct_values(
        self,
        sql_connector,
        schema: Optional[str],
        table_name: str,
        column_name: str,
    ) -> List[Any]:
        attempts = self._build_identifier_attempts(schema, table_name, column_name)
        last_error: Exception | None = None
        for attempt in attempts:
            query = (
                f"SELECT DISTINCT {attempt['column']} "
                f"FROM {attempt['table']} "
                f"WHERE {attempt['column']} IS NOT NULL "
            )
            try:
                result = await sql_connector.execute(query)
            except (ConnectorError, Exception) as exc:  # pragma: no cover - depends on connector runtime
                last_error = exc
                continue
            return [row[0] for row in result.rows if row]

        if last_error:
            raise BusinessValidationError(
                f"Unable to fetch values for {table_name}.{column_name}: {last_error}"
            ) from last_error
        return []

    def _build_identifier_attempts(
        self,
        schema: Optional[str],
        table_name: str,
        column_name: str,
    ) -> List[Dict[str, str]]:
        attempts = []
        for left, right in (('"', '"'), ("`", "`"), ("[", "]"), (None, None)):
            column_expr = self._format_identifier(column_name, left, right)
            if schema:
                table_expr = (
                    f"{self._format_identifier(schema, left, right)}."
                    f"{self._format_identifier(table_name, left, right)}"
                )
            else:
                table_expr = self._format_identifier(table_name, left, right)
            attempts.append({"table": table_expr, "column": column_expr})
        return attempts

    @staticmethod
    def _format_identifier(value: str, left: Optional[str], right: Optional[str]) -> str:
        if not left and not right:
            return value
        left_token = left or ""
        right_token = right or ""
        escaped = value
        if right_token:
            escaped = escaped.replace(right_token, right_token * 2)
        elif left_token:
            escaped = escaped.replace(left_token, left_token * 2)
        return f"{left_token}{escaped}{right_token}"

    def _prepare_vector_values(self, values: List[Any]) -> List[str]:
        deduped: List[str] = []
        seen: set[str] = set()
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            if len(text) > VALUE_MAX_LENGTH:
                text = text[:VALUE_MAX_LENGTH]
            lowered = text.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(text)
        return deduped
