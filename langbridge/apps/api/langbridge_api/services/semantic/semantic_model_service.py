import inspect
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Literal, Mapping
from uuid import UUID

import yaml
from sqlalchemy.exc import IntegrityError

from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelCatalogDatasetResponse,
    SemanticModelCatalogFieldResponse,
    SemanticModelCatalogResponse,
    SemanticModelCreateRequest,
    SemanticModelRecordResponse,
    SemanticModelSelectionGenerateResponse,
    SemanticModelUpdateRequest,
)
from langbridge.packages.common.langbridge_common.db.agent import LLMConnection  # noqa: F401
from langbridge.packages.common.langbridge_common.db.dataset import DatasetRecord
from langbridge.packages.common.langbridge_common.db.semantic import SemanticModelEntry
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import DatasetRepository
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationRepository,
    ProjectRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import SemanticModelRepository
from langbridge.packages.common.langbridge_common.utils.lineage import LineageNodeType
from langbridge.packages.semantic.langbridge_semantic.errors import SemanticModelError
from langbridge.packages.semantic.langbridge_semantic.loader import (
    load_semantic_model,
    load_unified_semantic_model,
)

TYPE_NUMERIC = {"number", "decimal", "numeric", "int", "integer", "float", "double", "real", "bigint"}
TYPE_BOOLEAN = {"boolean", "bool"}
TYPE_DATE = {"date", "datetime", "timestamp", "time"}


class SemanticModelService:
    def __init__(
        self,
        repository: SemanticModelRepository,
        builder: Any = None,
        organization_repository: OrganizationRepository | None = None,
        project_repository: ProjectRepository | None = None,
        connector_service: Any = None,
        agent_service: Any = None,
        semantic_search_service: Any = None,
        emvironment_service: Any = None,
        lineage_service: Any | None = None,
        dataset_repository: DatasetRepository | None = None,
        dataset_column_repository: Any | None = None,
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
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository

    async def generate_model_yaml(self, connector_id: UUID) -> str:
        _ = connector_id
        raise BusinessValidationError(
            "Connector-scoped semantic model generation has been removed. Build semantic models from datasets instead."
        )

    async def get_connector_catalog(
        self,
        *,
        organization_id: UUID,
        project_id: UUID | None = None,
    ) -> SemanticModelCatalogResponse:
        dataset_repository = self._require_dataset_repository()
        datasets = await self._maybe_await(
            dataset_repository.list_for_workspace(
                workspace_id=organization_id,
                project_id=project_id,
            )
        )
        items = [self._to_catalog_item(dataset) for dataset in datasets]
        return SemanticModelCatalogResponse(workspace_id=organization_id, items=items)

    async def generate_model_yaml_from_selection(
        self,
        *,
        organization_id: UUID,
        selected_dataset_ids: list[UUID],
        selected_fields: Dict[str, list[str]] | None = None,
        include_sample_values: bool = False,
        description: str | None = None,
    ) -> SemanticModelSelectionGenerateResponse:
        normalized_selected_fields = dict(selected_fields or {})
        datasets = await self._load_selected_datasets(
            organization_id=organization_id,
            selected_dataset_ids=selected_dataset_ids,
        )
        payload, warnings = self._build_payload_from_datasets(
            datasets=datasets,
            selected_fields=normalized_selected_fields,
            description=description,
        )
        if include_sample_values:
            warnings.append("include_sample_values is not currently supported for semantic model generation.")
        self._validate_generated_payload(
            payload=payload,
            datasets=datasets,
            selected_fields=normalized_selected_fields,
        )
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
        models = await self._maybe_await(
            self._repository.list_for_scope(
                organization_id=organization_id,
                project_id=project_id,
            )
        )
        if model_kind != "all":
            models = [model for model in models if self._resolve_model_kind(model) == model_kind]
        return [self._normalize_record(model) for model in models]

    async def list_all_models(self) -> list[SemanticModelRecordResponse]:
        models = await self._maybe_await(self._repository.get_all())
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
            await self._maybe_await(
                self._lineage_service.delete_node_lineage(
                    workspace_id=organization_id,
                    node_type=self._semantic_model_node_type(model),
                    node_id=str(model.id),
                )
            )
        await self._maybe_await(self._repository.delete(model))

    async def create_model(self, request: SemanticModelCreateRequest) -> SemanticModelRecordResponse:
        await self._assert_organization_scope(
            organization_id=request.organization_id,
            project_id=request.project_id,
        )
        payload, connector_id = await self._resolve_payload_for_create(
            organization_id=request.organization_id,
            request=request,
        )
        entry = SemanticModelEntry(
            id=uuid.uuid4(),
            connector_id=connector_id,
            organization_id=request.organization_id,
            project_id=request.project_id,
            name=request.name.strip(),
            description=(request.description.strip() if request.description else None),
            content_yaml=yaml.safe_dump(payload, sort_keys=False),
            content_json=json.dumps(payload),
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._repository.add(entry)
        try:
            await self._flush_repository()
        except IntegrityError as exc:
            if connector_id is None and self._is_connector_null_integrity_error(exc):
                raise BusinessValidationError(
                    "semantic_models.connector_id is still enforced as NOT NULL in the running database. "
                    "Apply the latest migration with 'alembic upgrade head' and retry."
                ) from exc
            raise
        if self._lineage_service is not None:
            await self._maybe_await(self._lineage_service.register_semantic_model_lineage(model=entry))
        return self._normalize_record(entry)

    async def update_model(
        self,
        model_id: UUID,
        organization_id: UUID,
        request: SemanticModelUpdateRequest,
    ) -> SemanticModelRecordResponse:
        model = await self._get_model_entity(model_id=model_id, organization_id=organization_id)
        project_id = request.project_id if "project_id" in request.model_fields_set else model.project_id
        await self._assert_organization_scope(organization_id=organization_id, project_id=project_id)

        if request.name is not None:
            stripped_name = request.name.strip()
            if not stripped_name:
                raise BusinessValidationError("Semantic model name is required")
            model.name = stripped_name
        if request.description is not None:
            model.description = request.description.strip() or None
        model.project_id = project_id

        if request.auto_generate or request.model_yaml is not None or request.source_dataset_ids is not None:
            payload, connector_id = await self._resolve_payload_for_update(
                organization_id=organization_id,
                existing_model=model,
                request=request,
            )
            model.content_yaml = yaml.safe_dump(payload, sort_keys=False)
            model.content_json = json.dumps(payload)
            model.connector_id = connector_id

        model.updated_at = datetime.now(timezone.utc)
        if self._lineage_service is not None:
            await self._maybe_await(self._lineage_service.register_semantic_model_lineage(model=model))
        return self._normalize_record(model)

    async def _resolve_payload_for_create(
        self,
        *,
        organization_id: UUID,
        request: SemanticModelCreateRequest,
    ) -> tuple[dict[str, Any], UUID | None]:
        if request.auto_generate:
            if not request.source_dataset_ids:
                raise BusinessValidationError("source_dataset_ids are required when auto_generate is true.")
            generation = await self.generate_model_yaml_from_selection(
                organization_id=organization_id,
                selected_dataset_ids=request.source_dataset_ids,
                selected_fields={},
                include_sample_values=False,
                description=request.description,
            )
            payload = self._parse_yaml_payload(generation.yaml_text) or {}
        else:
            if not request.model_yaml:
                raise BusinessValidationError("model_yaml is required when auto_generate is false.")
            payload = self._parse_yaml_payload(request.model_yaml)
            if payload is None:
                raise BusinessValidationError("Semantic model YAML could not be parsed.")
            try:
                self._validate_model_payload(payload)
            except SemanticModelError as exc:
                raise BusinessValidationError(f"Semantic model failed validation: {exc}") from exc

        if request.name and not payload.get("name"):
            payload["name"] = request.name.strip()
        if request.description and not payload.get("description"):
            payload["description"] = request.description.strip()

        source_dataset_ids = (
            list(request.source_dataset_ids)
            if request.source_dataset_ids is not None
            else self._extract_source_dataset_ids_from_payload(payload)
        )
        connector_id = await self._resolve_connector_id(
            organization_id=organization_id,
            source_dataset_ids=source_dataset_ids,
            fallback=request.connector_id,
        )
        return payload, connector_id

    async def _resolve_payload_for_update(
        self,
        *,
        organization_id: UUID,
        existing_model: SemanticModelEntry,
        request: SemanticModelUpdateRequest,
    ) -> tuple[dict[str, Any], UUID | None]:
        if request.auto_generate:
            selected_dataset_ids = request.source_dataset_ids or self._extract_source_dataset_ids_from_payload(
                self._parse_model_payload(existing_model) or {}
            )
            if not selected_dataset_ids:
                raise BusinessValidationError("source_dataset_ids are required to auto-generate a semantic model.")
            generation = await self.generate_model_yaml_from_selection(
                organization_id=organization_id,
                selected_dataset_ids=selected_dataset_ids,
                selected_fields={},
                include_sample_values=False,
                description=request.description or existing_model.description,
            )
            payload = self._parse_yaml_payload(generation.yaml_text) or {}
        elif request.model_yaml is not None:
            payload = self._parse_yaml_payload(request.model_yaml)
            if payload is None:
                raise BusinessValidationError("Semantic model YAML could not be parsed.")
            try:
                self._validate_model_payload(payload)
            except SemanticModelError as exc:
                raise BusinessValidationError(f"Semantic model failed validation: {exc}") from exc
        else:
            payload = self._parse_model_payload(existing_model) or {}

        if existing_model.name and not payload.get("name"):
            payload["name"] = existing_model.name
        if existing_model.description and not payload.get("description"):
            payload["description"] = existing_model.description
        if request.name is not None:
            payload["name"] = request.name.strip()
        if request.description is not None:
            payload["description"] = request.description.strip() or None

        source_dataset_ids = (
            list(request.source_dataset_ids)
            if request.source_dataset_ids is not None
            else self._extract_source_dataset_ids_from_payload(payload)
        )
        connector_id = await self._resolve_connector_id(
            organization_id=organization_id,
            source_dataset_ids=source_dataset_ids,
            fallback=request.connector_id if request.connector_id is not None else existing_model.connector_id,
        )
        return payload, connector_id

    async def _assert_organization_scope(self, *, organization_id: UUID, project_id: UUID | None) -> None:
        if self._organization_repository is not None:
            organization = await self._maybe_await(self._organization_repository.get_by_id(organization_id))
            if not organization:
                raise BusinessValidationError("Organization not found")
        if project_id and self._project_repository is not None:
            project = await self._maybe_await(self._project_repository.get_by_id(project_id))
            if not project:
                raise BusinessValidationError("Project not found")
            if getattr(project, "organization_id", None) != organization_id:
                raise BusinessValidationError("Project does not belong to the specified organization")

    async def _load_selected_datasets(
        self,
        *,
        organization_id: UUID,
        selected_dataset_ids: Iterable[UUID],
    ) -> list[DatasetRecord]:
        dataset_repository = self._require_dataset_repository()
        dataset_ids = list(dict.fromkeys(selected_dataset_ids))
        if not dataset_ids:
            raise BusinessValidationError("At least one dataset must be selected.")
        datasets = await self._maybe_await(
            dataset_repository.get_by_ids_for_workspace(
                workspace_id=organization_id,
                dataset_ids=dataset_ids,
            )
        )
        if len(datasets) != len(dataset_ids):
            found = {dataset.id for dataset in datasets}
            missing = [str(dataset_id) for dataset_id in dataset_ids if dataset_id not in found]
            raise BusinessValidationError(
                f"Selected datasets were not found in this workspace: {', '.join(missing)}"
            )
        datasets_by_id = {dataset.id: dataset for dataset in datasets}
        return [datasets_by_id[dataset_id] for dataset_id in dataset_ids]

    def _build_payload_from_datasets(
        self,
        *,
        datasets: list[DatasetRecord],
        selected_fields: Mapping[str, list[str]],
        description: str | None,
    ) -> tuple[dict[str, Any], list[str]]:
        warnings: list[str] = []
        datasets_payload: Dict[str, Any] = {}
        dataset_key_lookup: dict[UUID, str] = {}
        registry: set[str] = set()

        for dataset in datasets:
            dataset_key = self._build_dataset_key(dataset=dataset, registry=registry)
            dataset_key_lookup[dataset.id] = dataset_key
            field_lookup = {
                column.name.lower(): column
                for column in list(getattr(dataset, "columns", []) or [])
                if getattr(column, "is_allowed", True)
            }
            selected_names = [field.strip() for field in selected_fields.get(str(dataset.id), []) if field and field.strip()]
            if not selected_names:
                selected_names = [column.name for column in field_lookup.values()]
            selected_columns = []
            for field_name in selected_names:
                column = field_lookup.get(field_name.lower())
                if column is None:
                    raise BusinessValidationError(
                        f"Field '{field_name}' is not available on dataset '{dataset.name}'."
                    )
                selected_columns.append(column)

            dimensions: list[dict[str, Any]] = []
            measures: list[dict[str, Any]] = []
            for column in selected_columns:
                mapped_type = self._map_column_type(getattr(column, "data_type", "string"))
                is_primary_key = self._is_probable_primary_key(column.name, dataset_key)
                is_identifier = column.name.lower() == "id" or column.name.lower().endswith("_id")
                if mapped_type in {"integer", "decimal", "float", "number"} and not is_identifier and not is_primary_key:
                    measures.append(
                        {
                            "name": column.name,
                            "expression": column.expression or column.name,
                            "type": mapped_type,
                            "aggregation": "sum",
                            "description": f"Aggregate {column.name} from {dataset.name}",
                        }
                    )
                else:
                    dimensions.append(
                        {
                            "name": column.name,
                            "expression": column.expression or column.name,
                            "type": mapped_type,
                            "primary_key": is_primary_key,
                            "description": f"Field {column.name} from {dataset.name}",
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
                    f"Dataset '{dataset.name}' had only numeric fields; converted one field to a dimension."
                )

            datasets_payload[dataset_key] = {
                "dataset_id": str(dataset.id),
                "relation_name": dataset_key,
                "description": dataset.description or f"Dataset {dataset.name}",
                "dimensions": dimensions or None,
                "measures": measures or None,
            }

        relationships = self._infer_dataset_relationships(datasets=datasets, dataset_key_lookup=dataset_key_lookup)
        if not relationships:
            warnings.append("No relationships were inferred from selected datasets.")

        payload = {
            "version": "1.0",
            "description": description or "Semantic model generated from selected datasets",
            "datasets": datasets_payload,
            "relationships": relationships or None,
        }
        return payload, warnings

    def _infer_dataset_relationships(
        self,
        *,
        datasets: list[DatasetRecord],
        dataset_key_lookup: Mapping[UUID, str],
    ) -> list[dict[str, Any]]:
        relationships: list[dict[str, Any]] = []
        signatures: set[tuple[str, str, str, str]] = set()
        primary_keys: dict[UUID, list[str]] = {}
        all_columns: dict[UUID, set[str]] = {}
        for dataset in datasets:
            column_names = [column.name for column in list(getattr(dataset, "columns", []) or []) if getattr(column, "is_allowed", True)]
            all_columns[dataset.id] = {name.lower() for name in column_names}
            primary_keys[dataset.id] = [name for name in column_names if self._is_probable_primary_key(name, dataset.name)]

        for dataset in datasets:
            source_key = dataset_key_lookup[dataset.id]
            column_names = [column.name for column in list(getattr(dataset, "columns", []) or []) if getattr(column, "is_allowed", True)]
            for column_name in column_names:
                lowered = column_name.lower()
                if lowered == "id" or not lowered.endswith("_id"):
                    continue
                for target in datasets:
                    if target.id == dataset.id:
                        continue
                    target_key = dataset_key_lookup[target.id]
                    target_pks = primary_keys.get(target.id) or []
                    target_fields = all_columns.get(target.id) or set()
                    target_field = None
                    if lowered in target_fields and lowered in {value.lower() for value in target_pks}:
                        target_field = column_name
                    elif "id" in {value.lower() for value in target_pks} and self._matches_target_name(lowered, target_key, target.name):
                        target_field = next((value for value in target_pks if value.lower() == "id"), target_pks[0] if target_pks else None)
                    elif lowered in target_fields:
                        target_field = column_name
                    if not target_field:
                        continue
                    signature = (source_key, column_name, target_key, target_field)
                    if signature in signatures:
                        continue
                    signatures.add(signature)
                    relationships.append(
                        {
                            "name": f"{source_key}_to_{target_key}",
                            "source_dataset": source_key,
                            "source_field": column_name,
                            "target_dataset": target_key,
                            "target_field": target_field,
                            "type": "many_to_one",
                        }
                    )
        return relationships

    def _validate_generated_payload(
        self,
        *,
        payload: Dict[str, Any],
        datasets: list[DatasetRecord],
        selected_fields: Mapping[str, list[str]],
    ) -> None:
        try:
            model = load_semantic_model(payload)
        except SemanticModelError as exc:
            raise BusinessValidationError(f"Generated semantic model is invalid: {exc}") from exc

        relationship_names = [relationship.name for relationship in model.relationships or []]
        if len(relationship_names) != len(set(relationship_names)):
            raise BusinessValidationError("Generated semantic model contains duplicate relationship names.")

        payload_keys = {str(dataset.dataset_id) for dataset in model.datasets.values()}
        expected_keys = {str(dataset.id) for dataset in datasets}
        if payload_keys != expected_keys:
            raise BusinessValidationError("Generated semantic model does not reference the selected datasets.")

        for dataset in datasets:
            selected_column_names = set(selected_fields.get(str(dataset.id), [])) or {
                column.name
                for column in list(getattr(dataset, "columns", []) or [])
                if getattr(column, "is_allowed", True)
            }
            matching = next(
                (
                    semantic_dataset
                    for semantic_dataset in model.datasets.values()
                    if str(semantic_dataset.dataset_id) == str(dataset.id)
                ),
                None,
            )
            if matching is None:
                raise BusinessValidationError(f"Generated semantic model is missing dataset '{dataset.name}'.")
            generated_column_names = {
                dimension.name for dimension in matching.dimensions or []
            } | {
                measure.name for measure in matching.measures or []
            }
            if selected_column_names != generated_column_names:
                raise BusinessValidationError(
                    f"Generated semantic model fields do not match selection for dataset '{dataset.name}'."
                )

    async def _get_model_entity(self, model_id: UUID, organization_id: UUID) -> SemanticModelEntry:
        model = await self._maybe_await(
            self._repository.get_for_scope(
                model_id=model_id,
                organization_id=organization_id,
            )
        )
        if not model:
            raise BusinessValidationError("Semantic model not found")
        return model

    def _normalize_record(self, model: SemanticModelEntry) -> SemanticModelRecordResponse:
        response = SemanticModelRecordResponse.model_validate(model)
        response.source_dataset_ids = self._extract_source_dataset_ids_from_payload(
            self._parse_model_payload(model) or {}
        )
        return response

    @staticmethod
    def _resolve_model_kind(model: SemanticModelEntry) -> Literal["standard", "unified"]:
        payload = SemanticModelService._parse_model_payload(model)
        if payload is None:
            return "standard"
        has_unified_shape = isinstance(payload.get("source_models"), list) or isinstance(payload.get("sourceModels"), list)
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
    def _validate_model_payload(payload: Mapping[str, Any]) -> None:
        if "source_models" in payload or "sourceModels" in payload:
            load_unified_semantic_model(payload)
            return
        load_semantic_model(payload)

    @staticmethod
    def _extract_source_dataset_ids_from_payload(payload: Mapping[str, Any]) -> list[UUID]:
        datasets = payload.get("datasets") if isinstance(payload.get("datasets"), Mapping) else payload.get("tables")
        if not isinstance(datasets, Mapping):
            return []
        values: list[UUID] = []
        seen: set[UUID] = set()
        for raw_dataset in datasets.values():
            if not isinstance(raw_dataset, Mapping):
                continue
            raw_dataset_id = raw_dataset.get("dataset_id") or raw_dataset.get("datasetId")
            if not raw_dataset_id:
                continue
            try:
                dataset_id = UUID(str(raw_dataset_id))
            except (TypeError, ValueError):
                continue
            if dataset_id in seen:
                continue
            seen.add(dataset_id)
            values.append(dataset_id)
        return values

    async def _resolve_connector_id(
        self,
        *,
        organization_id: UUID,
        source_dataset_ids: Iterable[UUID],
        fallback: UUID | None,
    ) -> UUID | None:
        dataset_ids = list(dict.fromkeys(source_dataset_ids))
        if not dataset_ids:
            return fallback
        if self._dataset_repository is None:
            return fallback
        datasets = await self._maybe_await(
            self._dataset_repository.get_by_ids_for_workspace(
                workspace_id=organization_id,
                dataset_ids=dataset_ids,
            )
        )
        connector_ids = {
            connection_id
            for dataset in datasets
            if (connection_id := getattr(dataset, "connection_id", None)) is not None
        }
        if len(connector_ids) == 1:
            return next(iter(connector_ids))
        if len(connector_ids) > 1:
            return None
        return fallback

    async def _flush_repository(self) -> None:
        flush = getattr(self._repository, "flush", None)
        if flush is None:
            return
        await self._maybe_await(flush())

    @staticmethod
    def _is_connector_null_integrity_error(exc: IntegrityError) -> bool:
        message = str(exc).lower()
        return "connector_id" in message and ("not-null" in message or "null value" in message)

    def _to_catalog_item(self, dataset: DatasetRecord) -> SemanticModelCatalogDatasetResponse:
        fields = [
            SemanticModelCatalogFieldResponse(
                name=column.name,
                type=column.data_type,
                nullable=column.nullable,
                primary_key=self._is_probable_primary_key(column.name, dataset.name),
            )
            for column in list(getattr(dataset, "columns", []) or [])
            if getattr(column, "is_allowed", True)
        ]
        return SemanticModelCatalogDatasetResponse(
            id=dataset.id,
            name=dataset.name,
            sql_alias=dataset.sql_alias,
            description=dataset.description,
            connection_id=dataset.connection_id,
            source_kind=dataset.source_kind,
            storage_kind=dataset.storage_kind,
            fields=fields,
        )

    def _build_dataset_key(self, *, dataset: DatasetRecord, registry: set[str]) -> str:
        base_name = re.sub(r"[^a-zA-Z0-9_]+", "_", (dataset.sql_alias or dataset.name or "dataset").strip()).strip("_").lower()
        candidate = base_name or f"dataset_{dataset.id.hex[:8]}"
        suffix = 2
        while candidate in registry:
            candidate = f"{base_name}_{suffix}"
            suffix += 1
        registry.add(candidate)
        return candidate

    @staticmethod
    def _map_column_type(data_type: str) -> str:
        normalized = (data_type or "").lower()
        if any(token in normalized for token in TYPE_NUMERIC):
            if "int" in normalized and "point" not in normalized:
                return "integer"
            if any(token in normalized for token in ("double", "float", "real")):
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
    def _is_probable_primary_key(column_name: str, dataset_name: str) -> bool:
        normalized_column = column_name.lower()
        normalized_dataset = re.sub(r"[^a-z0-9]", "", dataset_name.lower())
        if normalized_column == "id":
            return True
        if normalized_column == f"{normalized_dataset}id":
            return True
        if normalized_column == f"{normalized_dataset}_id":
            return True
        return False

    @staticmethod
    def _matches_target_name(column_name: str, dataset_key: str, dataset_name: str) -> bool:
        normalized_column = column_name.lower()
        base_candidates = {
            re.sub(r"[^a-z0-9]", "", dataset_key.lower()),
            re.sub(r"[^a-z0-9]", "", dataset_name.lower()),
        }
        candidates = {
            candidate_variant
            for candidate in base_candidates
            if candidate
            for candidate_variant in {candidate, candidate.rstrip("s")}
            if candidate_variant
        }
        for candidate in candidates:
            if normalized_column in {f"{candidate}_id", f"{candidate}s_id"}:
                return True
        return False

    def _require_dataset_repository(self) -> DatasetRepository:
        if self._dataset_repository is None:
            raise BusinessValidationError("Dataset repository is required for semantic model operations.")
        return self._dataset_repository

    @staticmethod
    async def _maybe_await(value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value
