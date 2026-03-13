from __future__ import annotations

import logging
import re
import uuid
from collections.abc import Iterable, Mapping
from typing import Any
from uuid import UUID

from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService
from langbridge.apps.api.langbridge_api.services.semantic.semantic_model_service import (
    SemanticModelService,
)
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelRecordResponse,
    SemanticQueryMetaResponse,
    SemanticQueryRequest,
    SemanticQueryResponse,
    UnifiedSemanticSourceModelRequest,
    UnifiedSemanticQueryMetaRequest,
    UnifiedSemanticQueryMetaResponse,
    UnifiedSemanticQueryRequest,
    UnifiedSemanticQueryResponse,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import (
    ConnectorRuntimeType,
)
from langbridge.packages.connectors.langbridge_connectors.api.connector import (
    QueryResult,
    SqlConnector,
)
from langbridge.packages.runtime.services import SemanticQueryExecutionService
from langbridge.packages.semantic.langbridge_semantic.loader import (
    SemanticModelError,
    load_semantic_model,
)
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery, SemanticQueryEngine
from langbridge.packages.semantic.langbridge_semantic.unified_query import (
    UnifiedSourceModel,
    build_unified_semantic_model,
)

_KEY_SANITIZER = re.compile(r"[^0-9A-Za-z_]+")


def _normalize_unified_relationship_payload(relationship: Any) -> dict[str, Any]:
    if hasattr(relationship, "model_dump"):
        return relationship.model_dump(exclude_none=True)
    if isinstance(relationship, Mapping):
        return dict(relationship)
    return dict(relationship)


class SemanticQueryService:
    def __init__(
        self,
        semantic_model_service: SemanticModelService,
        connector_service: ConnectorService,
        semantic_query_execution_service: SemanticQueryExecutionService | None = None,
    ):
        self._semantic_model_service = semantic_model_service
        self._connector_service = connector_service
        self._semantic_query_execution_service = semantic_query_execution_service
        self._engine = SemanticQueryEngine()
        self._logger = logging.getLogger(__name__)

    async def query_request(
        self,
        semantic_query_request: SemanticQueryRequest,
    ) -> SemanticQueryResponse:
        if self._semantic_query_execution_service is not None:
            semantic_query = self._load_query_payload(semantic_query_request.query)
            execution = await self._semantic_query_execution_service.execute_standard_query(
                organization_id=semantic_query_request.organization_id,
                project_id=semantic_query_request.project_id,
                semantic_model_id=semantic_query_request.semantic_model_id,
                semantic_query=semantic_query,
            )
            return execution.response

        semantic_model_record = await self._semantic_model_service.get_model(
            model_id=semantic_query_request.semantic_model_id,
            organization_id=semantic_query_request.organization_id,
        )
        semantic_model = self._load_model_payload(semantic_model_record.content_yaml)
        semantic_query = self._load_query_payload(semantic_query_request.query)

        if semantic_model_record.connector_id is None:
            raise BusinessValidationError(
                "This semantic model is dataset-backed and requires the federated execution runtime."
            )
        connector_response = await self._connector_service.get_connector(
            semantic_model_record.connector_id
        )
        sql_connector = await self._create_sql_connector(connector_response)
        plan, result = await self._compile_and_execute(
            semantic_model=semantic_model,
            semantic_query=semantic_query,
            sql_connector=sql_connector,
        )

        return SemanticQueryResponse(
            id=uuid.uuid4(),
            organization_id=semantic_query_request.organization_id,
            project_id=semantic_query_request.project_id,
            semantic_model_id=semantic_query_request.semantic_model_id,
            data=self._engine.format_rows(result.columns, result.rows),
            annotations=plan.annotations,
            metadata=plan.metadata,
        )

    async def query_unified_request(
        self,
        request: UnifiedSemanticQueryRequest,
    ) -> UnifiedSemanticQueryResponse:
        if self._semantic_query_execution_service is None:
            raise BusinessValidationError(
                "Unified semantic query execution service is not configured."
            )

        semantic_query = self._load_query_payload(request.query)
        execution = await self._semantic_query_execution_service.execute_unified_query(
            organization_id=request.organization_id,
            project_id=request.project_id,
            semantic_query=semantic_query,
            semantic_model_ids=request.semantic_model_ids,
            source_models=request.source_models,
            relationships=request.relationships,
            metrics=request.metrics,
        )
        return execution.response

    async def get_meta(
        self,
        semantic_model_id: UUID,
        organization_id: UUID,
    ) -> SemanticQueryMetaResponse:
        semantic_model_record = await self._semantic_model_service.get_model(
            model_id=semantic_model_id,
            organization_id=organization_id,
        )
        semantic_model = self._load_model_payload(semantic_model_record.content_yaml)

        payload = semantic_model.model_dump(by_alias=True, exclude_none=True)
        self._attach_full_column_paths(payload)
        return SemanticQueryMetaResponse(
            id=semantic_model_id,
            name=semantic_model_record.name,
            description=semantic_model_record.description,
            connector_id=semantic_model_record.connector_id,
            organization_id=semantic_model_record.organization_id,
            project_id=semantic_model_record.project_id,
            semantic_model=payload,
        )

    async def get_unified_meta(
        self,
        request: UnifiedSemanticQueryMetaRequest,
    ) -> UnifiedSemanticQueryMetaResponse:
        execution_connector_id = SemanticQueryExecutionService.build_unified_execution_connector_id(
            organization_id=request.organization_id
        )

        unified_model, _ = await self._build_unified_model_and_map(
            organization_id=request.organization_id,
            semantic_model_ids=request.semantic_model_ids,
            source_models=request.source_models,
            relationships=request.relationships,
            metrics=request.metrics,
        )

        payload = unified_model.model_dump(by_alias=True, exclude_none=True)
        self._attach_full_column_paths(payload)
        return UnifiedSemanticQueryMetaResponse(
            connector_id=execution_connector_id,
            organization_id=request.organization_id,
            project_id=request.project_id,
            semantic_model_ids=request.semantic_model_ids,
            semantic_model=payload,
        )

    async def _build_unified_model_and_map(
        self,
        *,
        organization_id: UUID,
        semantic_model_ids: Iterable[UUID],
        source_models: Iterable[UnifiedSemanticSourceModelRequest] | None,
        relationships: Iterable[Any] | None,
        metrics: Mapping[str, Any] | None,
    ) -> tuple[SemanticModel, dict[str, UUID]]:
        normalized_model_ids = self._normalize_model_ids(semantic_model_ids)
        loaded_source_models = await self._load_source_models(
            organization_id=organization_id,
            semantic_model_ids=normalized_model_ids,
            source_model_defs=source_models,
        )

        relationships_payload = [
            _normalize_unified_relationship_payload(relationship)
            for relationship in relationships or []
        ]
        metrics_payload: dict[str, Any] = {}
        for metric_name, metric_value in (metrics or {}).items():
            if hasattr(metric_value, "model_dump"):
                metrics_payload[metric_name] = metric_value.model_dump(
                    by_alias=True, exclude_none=True
                )
            elif isinstance(metric_value, Mapping):
                metrics_payload[metric_name] = dict(metric_value)
            else:
                metrics_payload[metric_name] = metric_value

        try:
            return build_unified_semantic_model(
                source_models=loaded_source_models,
                relationships=relationships_payload,
                metrics=metrics_payload or None,
            )
        except (SemanticModelError, ValueError) as exc:
            raise BusinessValidationError(
                f"Unified semantic model failed validation: {exc}"
            ) from exc

    async def _load_source_models(
        self,
        *,
        organization_id: UUID,
        semantic_model_ids: list[UUID],
        source_model_defs: Iterable[UnifiedSemanticSourceModelRequest] | None,
    ) -> list[UnifiedSourceModel]:
        source_model_defs_by_id = {
            source_model.id: source_model
            for source_model in (source_model_defs or [])
        }
        source_models: list[UnifiedSourceModel] = []
        seen_keys: set[str] = set()
        for semantic_model_id in semantic_model_ids:
            model_record: SemanticModelRecordResponse = await self._semantic_model_service.get_model(
                model_id=semantic_model_id,
                organization_id=organization_id,
            )
            source_model_def = source_model_defs_by_id.get(semantic_model_id)
            source_key = self._build_source_model_key(
                preferred_name=(
                    source_model_def.alias
                    if source_model_def is not None and source_model_def.alias
                    else model_record.name
                ),
                model_id=semantic_model_id,
                seen_keys=seen_keys,
            )
            source_models.append(
                UnifiedSourceModel(
                    model_id=model_record.id,
                    key=source_key,
                    model=self._load_model_payload(model_record.content_yaml),
                    connector_id=model_record.connector_id,
                    name=(
                        source_model_def.name
                        if source_model_def is not None and source_model_def.name
                        else model_record.name
                    ),
                    description=(
                        source_model_def.description
                        if source_model_def is not None and source_model_def.description
                        else model_record.description
                    ),
                )
            )
        return source_models

    async def _create_sql_connector(
        self,
        connector_response: ConnectorResponse,
    ) -> Any:
        if connector_response.connector_type is None:
            raise BusinessValidationError(
                "Connector type is required for semantic query execution."
            )

        connector_type = ConnectorRuntimeType(connector_response.connector_type.upper())
        sql_connector = await self._connector_service.async_create_sql_connector(
            connector_type,
            connector_response.config or {},
        )
        if not isinstance(sql_connector, SqlConnector):
            raise BusinessValidationError(
                "Only SQL connectors are supported for semantic queries."
            )
        return sql_connector

    async def _compile_and_execute(
        self,
        *,
        semantic_model: SemanticModel,
        semantic_query: SemanticQuery,
        sql_connector: Any,
    ) -> tuple[Any, QueryResult]:
        rewrite_expression = None
        if getattr(sql_connector, "EXPRESSION_REWRITE", False):
            rewrite_expression = getattr(sql_connector, "rewrite_expression", None)
            if rewrite_expression is None:
                raise BusinessValidationError(
                    "Semantic query translation failed: connector expression rewriter missing."
                )

        try:
            plan = self._engine.compile(
                semantic_query,
                semantic_model,
                dialect=sql_connector.DIALECT.value.lower(),
                rewrite_expression=rewrite_expression,
            )
        except Exception as exc:
            raise BusinessValidationError(
                f"Semantic query translation failed: {exc}"
            ) from exc

        self._logger.info("Translated SQL %s", plan.sql)
        result = await sql_connector.execute(plan.sql)
        return plan, result

    @staticmethod
    def _load_model_payload(content_yaml: str) -> SemanticModel:
        try:
            return load_semantic_model(content_yaml)
        except SemanticModelError as exc:
            raise BusinessValidationError(
                f"Semantic model failed validation: {exc}"
            ) from exc

    @staticmethod
    def _load_query_payload(query_payload: Mapping[str, Any] | dict[str, Any]) -> SemanticQuery:
        try:
            return SemanticQuery.model_validate(query_payload)
        except Exception as exc:
            raise BusinessValidationError(
                f"Semantic query payload failed validation: {exc}"
            ) from exc

    @staticmethod
    def _normalize_model_ids(semantic_model_ids: Iterable[UUID]) -> list[UUID]:
        ordered_unique: list[UUID] = []
        seen: set[UUID] = set()
        for model_id in semantic_model_ids:
            if model_id in seen:
                continue
            seen.add(model_id)
            ordered_unique.append(model_id)
        if not ordered_unique:
            raise BusinessValidationError(
                "semantic_model_ids must include at least one model id."
            )
        return ordered_unique

    @staticmethod
    def _build_source_model_key(
        *,
        preferred_name: str | None,
        model_id: UUID,
        seen_keys: set[str],
    ) -> str:
        base = _KEY_SANITIZER.sub("_", str(preferred_name or "").strip()).strip("_")
        if not base:
            base = f"model_{model_id.hex[:8]}"
        candidate = base
        if candidate in seen_keys:
            candidate = f"{base}_{model_id.hex[:8]}"
        seen_keys.add(candidate)
        return candidate

    @staticmethod
    def _attach_full_column_paths(payload: dict[str, Any]) -> None:
        datasets = payload.get("datasets")
        if not isinstance(datasets, dict):
            datasets = payload.get("tables")
        if not isinstance(datasets, dict):
            return
        for dataset in datasets.values():
            if not isinstance(dataset, dict):
                continue
            catalog = str(dataset.get("catalog_name") or dataset.get("catalog") or "").strip()
            schema = str(dataset.get("schema_name") or dataset.get("schema") or "").strip()
            relation_name = str(dataset.get("relation_name") or dataset.get("name") or "").strip()
            if not relation_name:
                continue
            base_parts = [part for part in [catalog, schema, relation_name] if part]
            if not base_parts:
                continue
            base = ".".join(base_parts)
            for collection_key in ("dimensions", "measures"):
                items = dataset.get(collection_key)
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    column_name = str(item.get("name") or "").strip()
                    if not column_name:
                        continue
                    item["full_path"] = f"{base}.{column_name}"


