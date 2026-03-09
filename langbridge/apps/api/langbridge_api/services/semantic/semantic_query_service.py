from __future__ import annotations

import logging
import uuid
from collections.abc import Iterable, Mapping
from typing import Any
from uuid import UUID

from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService
from langbridge.apps.api.langbridge_api.services.semantic.semantic_model_service import (
    SemanticModelService,
)
from langbridge.apps.worker.langbridge_worker.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelRecordResponse,
    SemanticQueryMetaResponse,
    SemanticQueryRequest,
    SemanticQueryResponse,
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
        semantic_model_record = await self._semantic_model_service.get_model(
            model_id=semantic_query_request.semantic_model_id,
            organization_id=semantic_query_request.organization_id,
        )
        semantic_model = self._load_model_payload(semantic_model_record.content_yaml)
        semantic_query = self._load_query_payload(semantic_query_request.query)

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
            joins=request.joins,
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
            joins=request.joins,
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
        joins: Iterable[Any] | None,
        metrics: Mapping[str, Any] | None,
    ) -> tuple[SemanticModel, dict[str, UUID]]:
        normalized_model_ids = self._normalize_model_ids(semantic_model_ids)
        source_models = await self._load_source_models(
            organization_id=organization_id,
            semantic_model_ids=normalized_model_ids,
        )

        joins_payload = [
            join.model_dump(by_alias=True, exclude_none=True)
            if hasattr(join, "model_dump")
            else dict(join)
            for join in joins or []
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
                source_models=source_models,
                joins=joins_payload,
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
    ) -> list[UnifiedSourceModel]:
        source_models: list[UnifiedSourceModel] = []
        for semantic_model_id in semantic_model_ids:
            model_record: SemanticModelRecordResponse = await self._semantic_model_service.get_model(
                model_id=semantic_model_id,
                organization_id=organization_id,
            )
            source_models.append(
                UnifiedSourceModel(
                    model=self._load_model_payload(model_record.content_yaml),
                    connector_id=model_record.connector_id,
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
    def _attach_full_column_paths(payload: dict[str, Any]) -> None:
        tables = payload.get("tables")
        if not isinstance(tables, dict):
            return
        for table in tables.values():
            if not isinstance(table, dict):
                continue
            catalog = str(table.get("catalog") or "").strip()
            schema = str(table.get("schema") or "").strip()
            table_name = str(table.get("name") or "").strip()
            if not table_name:
                continue
            base_parts = [part for part in [catalog, schema, table_name] if part]
            if not base_parts:
                continue
            base = ".".join(base_parts)
            for collection_key in ("dimensions", "measures"):
                items = table.get(collection_key)
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    column_name = str(item.get("name") or "").strip()
                    if not column_name:
                        continue
                    item["full_path"] = f"{base}.{column_name}"
