"""Builds runtime-backed tools for `langbridge.ai` agent runs."""
import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
import yaml

from langbridge.ai.events import AIEventEmitter
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.profiles import AnalystAgentConfig
from langbridge.ai.tools.semantic_search import SemanticSearchTool
from langbridge.ai.tools.sql.interfaces import (
    AnalyticalColumn,
    AnalyticalContext,
    AnalyticalDatasetBinding,
    AnalyticalField,
    AnalyticalMetric,
    AnalyticalQueryExecutionFailure,
    AnalyticalQueryExecutionResult,
    AnalystOutcomeStage,
    QueryResult,
    SqlQueryScope,
)
from langbridge.ai.tools.sql.tool import SqlAnalysisTool
from langbridge.ai.tools.web_search import WebSearchProvider, create_web_search_provider
from langbridge.federation.models import FederationWorkflow, VirtualDataset
from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.execution.federated_query_tool import FederatedQueryTool
from langbridge.runtime.models import DatasetMetadata, SemanticModelMetadata
from langbridge.runtime.ports import DatasetCatalogStore, DatasetColumnStore, SemanticModelStore
from langbridge.runtime.services.dataset_execution import DatasetExecutionResolver
from langbridge.runtime.services.semantic_query_execution_service import SemanticQueryExecutionService
from langbridge.runtime.services.semantic_sql_query_service import (
    SemanticSqlQueryService,
    build_semantic_sql_metadata_columns_by_source,
    resolve_semantic_sql_projection_value,
)
from langbridge.runtime.services.semantic_vector_search import SemanticVectorSearchService
from langbridge.runtime.settings import runtime_settings
from langbridge.runtime.utils.sql import enforce_preview_limit, normalize_sql_dialect
from langbridge.semantic.errors import SemanticSqlError
from langbridge.semantic.loader import load_semantic_model
from langbridge.semantic.model import Dimension, Measure, Metric, SemanticModel, Table


@dataclass(slots=True)
class RuntimeAgentTooling:
    sql_analysis_tools: dict[str, list[SqlAnalysisTool]] = field(default_factory=dict)
    semantic_search_tools: dict[str, list[SemanticSearchTool]] = field(default_factory=dict)
    web_search_providers: dict[str, WebSearchProvider] = field(default_factory=dict)


class _DatasetScopeExecutor:
    def __init__(
        self,
        *,
        federated_query_tool: FederatedQueryTool,
        workflow: FederationWorkflow,
        workspace_id: str,
    ) -> None:
        self._federated_query_tool = federated_query_tool
        self._workflow = workflow
        self._workspace_id = workspace_id

    async def execute_query(
        self,
        *,
        query: str,
        query_dialect: str,
        requested_limit: int | None = None,
    ) -> AnalyticalQueryExecutionResult:
        try:
            sqlglot.parse_one(query, read="postgres")
        except sqlglot.ParseError as exc:
            raise AnalyticalQueryExecutionFailure(
                stage=AnalystOutcomeStage.query,
                message=f"Canonical SQL failed to parse: {exc}",
                original_error=str(exc),
                recoverable=True,
            ) from exc

        executable_sql = query
        if requested_limit:
            executable_sql, _ = enforce_preview_limit(
                query,
                max_rows=requested_limit,
                dialect="postgres",
            )

        try:
            execution = await self._federated_query_tool.execute_federated_query(
                {
                    "workspace_id": self._workspace_id,
                    "query": executable_sql,
                    "dialect": query_dialect,
                    "workflow": self._workflow,
                }
            )
        except Exception as exc:
            raise AnalyticalQueryExecutionFailure(
                stage=AnalystOutcomeStage.execution,
                message=f"Execution failed: {exc}",
                original_error=str(exc),
                recoverable=self._is_transient_execution_error(str(exc)),
                metadata={"executable_query": executable_sql},
            ) from exc

        rows_payload = execution.get("rows", [])
        if not isinstance(rows_payload, list):
            raise AnalyticalQueryExecutionFailure(
                stage=AnalystOutcomeStage.execution,
                message="Federated SQL execution returned an invalid rows payload.",
                recoverable=False,
                metadata={"executable_query": executable_sql},
            )

        columns_payload = execution.get("columns", [])
        columns = [str(column) for column in columns_payload] if isinstance(columns_payload, list) else []
        if not columns and rows_payload and isinstance(rows_payload[0], dict):
            columns = [str(key) for key in rows_payload[0].keys()]

        rows: list[tuple[Any, ...]] = []
        for row in rows_payload:
            if isinstance(row, dict):
                rows.append(tuple(row.get(column) for column in columns))
            elif isinstance(row, (list, tuple)):
                rows.append(tuple(row))
            else:
                rows.append((row,))

        execution_summary = execution.get("execution", {})
        elapsed_ms = execution_summary.get("total_runtime_ms") if isinstance(execution_summary, dict) else None
        return AnalyticalQueryExecutionResult(
            executable_query=executable_sql,
            result=QueryResult(
                columns=columns,
                rows=rows,
                rowcount=len(rows),
                elapsed_ms=elapsed_ms if isinstance(elapsed_ms, int) else None,
                source_sql=executable_sql,
            ),
        )

    @staticmethod
    def _is_transient_execution_error(error_message: str) -> bool:
        normalized = str(error_message or "").lower()
        return any(
            marker in normalized
            for marker in (
                "timeout",
                "temporarily unavailable",
                "temporary",
                "connection reset",
                "connection aborted",
                "try again",
                "rate limit",
            )
        )


class _SemanticScopeExecutor:
    def __init__(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
        semantic_model_name: str,
        semantic_model: SemanticModel,
        semantic_query_service: SemanticQueryExecutionService,
        semantic_sql_service: SemanticSqlQueryService,
    ) -> None:
        self._workspace_id = workspace_id
        self._semantic_model_id = semantic_model_id
        self._semantic_model_name = semantic_model_name
        self._semantic_model = semantic_model
        self._semantic_query_service = semantic_query_service
        self._semantic_sql_service = semantic_sql_service

    async def execute_query(
        self,
        *,
        query: str,
        query_dialect: str,
        requested_limit: int | None = None,
    ) -> AnalyticalQueryExecutionResult:
        try:
            parsed_query = self._semantic_sql_service.parse_query(
                query=query,
                query_dialect=query_dialect,
            )
            if parsed_query.semantic_model_ref != self._semantic_model_name:
                raise AnalyticalQueryExecutionFailure(
                    stage=AnalystOutcomeStage.query,
                    message=f"Semantic SQL must query selected semantic model '{self._semantic_model_name}'.",
                    original_error=str(parsed_query.semantic_model_ref),
                    recoverable=False,
                )
            query_plan = self._semantic_sql_service.build_query_plan(
                parsed_query=parsed_query,
                semantic_model=self._semantic_model,
                requested_limit=requested_limit,
            )
            execution = await self._semantic_query_service.execute_standard_query(
                workspace_id=self._workspace_id,
                semantic_model_id=self._semantic_model_id,
                semantic_query=query_plan.semantic_query,
            )
        except AnalyticalQueryExecutionFailure:
            raise
        except Exception as exc:
            raise self._semantic_failure(exc) from exc

        dataset_names = set(self._semantic_model.datasets.keys())
        metadata_columns_by_source = build_semantic_sql_metadata_columns_by_source(
            execution.response.metadata
        )
        rows = [
            tuple(
                resolve_semantic_sql_projection_value(
                    row=row,
                    projection=projection,
                    metadata_columns_by_source=metadata_columns_by_source,
                    dataset_names=dataset_names,
                )
                for projection in query_plan.projections
            )
            for row in execution.response.data
            if isinstance(row, dict)
        ]
        return AnalyticalQueryExecutionResult(
            executable_query=execution.compiled_sql,
            result=QueryResult(
                columns=[projection.output_name for projection in query_plan.projections],
                rows=rows,
                rowcount=len(rows),
                elapsed_ms=None,
                source_sql=execution.compiled_sql,
            ),
            metadata={"compiled_sql": execution.compiled_sql},
        )

    @staticmethod
    def _semantic_failure(exc: Exception) -> AnalyticalQueryExecutionFailure:
        message = str(exc)
        normalized = message.lower()
        recoverable = (
            isinstance(exc, SemanticSqlError) and exc.category == "parse_error"
        ) or normalized.startswith("semantic sql parse failed")
        semantic_failure_kind = _semantic_runtime_failure_kind(exc=exc, normalized_message=normalized)
        metadata: dict[str, Any] = {}
        if semantic_failure_kind is not None:
            metadata = {
                "scope_fallback_eligible": True,
                "semantic_failure_kind": semantic_failure_kind,
            }
            stage = AnalystOutcomeStage.query
        elif _is_semantic_runtime_scope_fallback_eligible(message):
            metadata = {
                "scope_fallback_eligible": True,
                "semantic_failure_kind": "semantic_runtime_type_mismatch",
            }
            stage = AnalystOutcomeStage.execution
        else:
            stage = AnalystOutcomeStage.query if recoverable else AnalystOutcomeStage.execution
        return AnalyticalQueryExecutionFailure(
            stage=stage,
            message=message,
            original_error=message,
            recoverable=recoverable,
            metadata=metadata,
        )


def _semantic_runtime_failure_kind(*, exc: Exception, normalized_message: str) -> str | None:
    if isinstance(exc, SemanticSqlError):
        if exc.category in {
            "unsupported_construct",
            "invalid_grouping",
            "invalid_filter",
            "unsupported_expression",
            "invalid_time_bucket",
        }:
            return "unsupported_semantic_sql_shape"
        if exc.category in {"invalid_member", "ambiguous_member"}:
            return "semantic_coverage_gap"

    unsupported_shape_markers = (
        "semantic sql scope does not support",
        "semantic scope does not support",
        "semantic member columns and",
        "time buckets in select",
        "semantic sql group by",
        "semantic sql order by",
        "semantic sql like",
        "semantic sql comparisons must compare",
        "semantic sql time bucketing",
        "semantic sql where",
        "semantic sql filters only support literal values",
        "raw sql expressions are not supported in semantic filters",
        "must query the selected semantic model",
        "must match the selected semantic dimensions and time buckets",
        "can only reference semantic dimensions or time buckets",
        "selected semantic dimensions and time buckets",
    )
    if any(marker in normalized_message for marker in unsupported_shape_markers):
        return "unsupported_semantic_sql_shape"

    coverage_gap_markers = (
        "unknown semantic member",
        "semantic model not found",
        "could not resolve a selected semantic member",
        "semantic query translation failed",
    )
    if any(marker in normalized_message for marker in coverage_gap_markers):
        return "semantic_coverage_gap"
    return None


def _is_semantic_runtime_scope_fallback_eligible(error_message: str) -> bool:
    normalized = str(error_message or "").strip().lower()
    return any(
        marker in normalized
        for marker in (
            "binder error",
            "cannot compare values of type",
            "explicit cast is required",
            "type mismatch",
            "conversion error",
        )
    )


class RuntimeToolFactory:
    """Builds per-run tools from an agent profile and runtime catalog services."""

    def __init__(
        self,
        *,
        llm_provider: LLMProvider,
        analyst_configs: Sequence[AnalystAgentConfig],
        semantic_model_store: SemanticModelStore | None = None,
        dataset_repository: DatasetCatalogStore | None = None,
        dataset_column_repository: DatasetColumnStore | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
        semantic_vector_search_service: SemanticVectorSearchService | None = None,
        semantic_query_service: SemanticQueryExecutionService | None = None,
        semantic_sql_service: SemanticSqlQueryService | None = None,
        embedding_provider: EmbeddingProvider | None = None,
        event_emitter: AIEventEmitter | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._llm_provider = llm_provider
        self._analyst_configs = list(analyst_configs)
        self._semantic_model_store = semantic_model_store
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._federated_query_tool = federated_query_tool
        self._semantic_vector_search_service = semantic_vector_search_service
        self._semantic_query_service = semantic_query_service
        self._semantic_sql_service = semantic_sql_service or SemanticSqlQueryService()
        self._embedding_provider = embedding_provider
        self._event_emitter = event_emitter
        self._logger = logger or logging.getLogger(__name__)
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=dataset_repository,
        )

    async def build_tooling(self) -> RuntimeAgentTooling:
        tooling = RuntimeAgentTooling()
        for config in self._analyst_configs:
            sql_tools, semantic_tools = await self._build_scope_tools(config)
            self._register_for_scope(tooling.sql_analysis_tools, config, sql_tools)
            self._register_for_scope(tooling.semantic_search_tools, config, semantic_tools)
            provider = self._build_web_provider(config)
            if provider is not None:
                self._register_web_provider(tooling.web_search_providers, config, provider)
        return tooling

    async def _build_scope_tools(
        self,
        config: AnalystAgentConfig,
    ) -> tuple[list[SqlAnalysisTool], list[SemanticSearchTool]]:
        sql_tools: list[SqlAnalysisTool] = []
        semantic_search_tools: list[SemanticSearchTool] = []

        policy = str(config.query_policy or "semantic_preferred").strip().lower()
        allow_dataset_scope = policy != "semantic_only"
        allow_semantic_scope = policy != "dataset_only"

        if allow_dataset_scope and config.dataset_ids and self._federated_query_tool is not None:
            datasets: list[DatasetMetadata] = await self._load_datasets(config.dataset_ids)
            if datasets:
                sql_tools.append(await self._build_dataset_tool(config=config, datasets=datasets))

        semantic_model_ids = config.semantic_model_ids if allow_semantic_scope and self._semantic_model_store else []
        for semantic_model_id in semantic_model_ids:
            semantic_model_entry = await self._load_semantic_model_entry(semantic_model_id)
            if semantic_model_entry is None:
                continue
            semantic_model = load_semantic_model(semantic_model_entry.content_yaml)
            sql_tools.append(
                await self._build_semantic_model_tool(
                    config=config,
                    semantic_model_entry=semantic_model_entry,
                    semantic_model=semantic_model,
                )
            )
            semantic_search_tools.extend(
                self._build_semantic_search_tools(
                    semantic_model_entry=semantic_model_entry,
                    semantic_model=semantic_model,
                )
            )
        return sql_tools, semantic_search_tools

    async def _build_dataset_tool(
        self,
        *,
        config: AnalystAgentConfig,
        datasets: list[DatasetMetadata],
    ) -> SqlAnalysisTool:
        workflow, _dialect = await self._build_dataset_workflow(datasets)
        context = await self._build_dataset_context(config=config, datasets=datasets, workflow=workflow)
        dataset_description = "\n".join([f" - {dataset.name}: {dataset.description}" for dataset in datasets])
        return SqlAnalysisTool(
            llm_provider=self._llm_provider,
            context=context,
            semantic_model=self._build_dataset_semantic_model(context=context),
            query_executor=_DatasetScopeExecutor(
                federated_query_tool=self._require_federated_query_tool(),
                workflow=workflow,
                workspace_id=str(datasets[0].workspace_id),
            ),
            name=self._tool_name(config.name, "dataset"),
            description=f"Tool for querying datasets:\n{dataset_description}",
            logger=self._logger,
            event_emitter=self._event_emitter,
        )

    async def _build_semantic_model_tool(
        self,
        *,
        config: AnalystAgentConfig,
        semantic_model_entry: SemanticModelMetadata,
        semantic_model: SemanticModel,
    ) -> SqlAnalysisTool:
        if self._semantic_query_service is None:
            raise RuntimeError("SemanticQueryExecutionService is required for semantic model agent tools.")
        workflow, _dialect = await self._dataset_execution_resolver.build_semantic_workflow(
            workspace_id=semantic_model_entry.workspace_id,
            workflow_id=f"workflow_semantic_agent_{semantic_model_entry.id.hex[:12]}",
            dataset_name=semantic_model_entry.name or semantic_model.name or config.name,
            semantic_model=semantic_model,
            raw_datasets_payload=self._raw_semantic_datasets_payload(semantic_model_entry),
        )
        context = await self._build_semantic_model_context(
            config=config,
            semantic_model_entry=semantic_model_entry,
            semantic_model=semantic_model,
            workflow=workflow,
        )
        return SqlAnalysisTool(
            llm_provider=self._llm_provider,
            context=context,
            semantic_model=semantic_model,
            query_executor=_SemanticScopeExecutor(
                workspace_id=semantic_model_entry.workspace_id,
                semantic_model_id=semantic_model_entry.id,
                semantic_model_name=semantic_model_entry.name or semantic_model.name or config.name,
                semantic_model=semantic_model,
                semantic_query_service=self._semantic_query_service,
                semantic_sql_service=self._semantic_sql_service,
            ),
            semantic_search_tools=self._build_semantic_search_tools(
                semantic_model_entry=semantic_model_entry,
                semantic_model=semantic_model,
            ),
            name=self._tool_name(
                config.name,
                semantic_model_entry.name or semantic_model.name or semantic_model_entry.id.hex[:8],
            ),
            description=semantic_model.description,
            logger=self._logger,
            event_emitter=self._event_emitter,
        )

    async def _build_dataset_workflow(
        self,
        datasets: list[DatasetMetadata],
    ) -> tuple[FederationWorkflow, str]:
        if len(datasets) == 1:
            workflow, _default_table_key, dialect = await self._dataset_execution_resolver.build_workflow_for_dataset(
                dataset=datasets[0],
            )
            return workflow, dialect

        table_bindings: dict[str, Any] = {}
        dialects: list[str] = []
        for dataset in datasets:
            sql_alias = str(dataset.sql_alias or "").strip().lower()
            if not sql_alias:
                raise ValueError(f"Dataset '{dataset.name}' is missing sql_alias.")
            binding, dialect = self._dataset_execution_resolver._build_binding_from_dataset_record(
                dataset=dataset,
                table_key=sql_alias,
                logical_schema=None,
                logical_table_name=sql_alias,
                catalog_name=None,
            )
            table_bindings[binding.table_key] = binding
            dialects.append(dialect)

        context_id = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            "langbridge-ai-agent-datasets:" + ",".join(sorted(str(dataset.id) for dataset in datasets)),
        )
        workflow = FederationWorkflow(
            id=f"workflow_dataset_{context_id.hex[:12]}",
            workspace_id=str(datasets[0].workspace_id),
            dataset=VirtualDataset(
                id=f"dataset_context_{context_id.hex[:12]}",
                name="Dataset Analyst Context",
                workspace_id=str(datasets[0].workspace_id),
                tables=table_bindings,
                relationships=[],
            ),
            broadcast_threshold_bytes=runtime_settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
            partition_count=runtime_settings.FEDERATION_PARTITION_COUNT,
            max_stage_retries=runtime_settings.FEDERATION_STAGE_MAX_RETRIES,
            stage_parallelism=runtime_settings.FEDERATION_STAGE_PARALLELISM,
        )
        return workflow, self._choose_workflow_dialect(dialects)

    async def _build_dataset_context(
        self,
        *,
        config: AnalystAgentConfig,
        datasets: list[DatasetMetadata],
        workflow: FederationWorkflow,
    ) -> AnalyticalContext:
        bindings = await self._build_dataset_bindings(workflow=workflow, workspace_id=datasets[0].workspace_id)
        dimensions: list[AnalyticalField] = []
        measures: list[AnalyticalField] = []
        for binding in bindings:
            for column in binding.columns:
                field_name = f"{binding.sql_alias}.{column.name}"
                dimensions.append(AnalyticalField(name=field_name, expression=column.name))
                if self._is_numeric_type(column.data_type):
                    measures.append(AnalyticalField(name=field_name, expression=column.name))

        asset_id, asset_name = self._dataset_asset_identity(config=config, datasets=datasets)
        return AnalyticalContext(
            query_scope=SqlQueryScope.dataset,
            asset_type="dataset",
            asset_id=asset_id,
            asset_name=asset_name,
            description=config.description or datasets[0].description,
            tags=list(datasets[0].tags_json or []),
            execution_mode="federated",
            dialect="postgres",
            datasets=bindings,
            tables=[binding.sql_alias for binding in bindings],
            dimensions=dimensions,
            measures=measures,
            metrics=[AnalyticalMetric(name="row_count", expression="COUNT(*)", description="Count of rows")],
            relationships=[self._format_relationship(item) for item in workflow.dataset.relationships],
        )

    async def _build_semantic_model_context(
        self,
        *,
        config: AnalystAgentConfig,
        semantic_model_entry: SemanticModelMetadata,
        semantic_model: SemanticModel,
        workflow: FederationWorkflow,
    ) -> AnalyticalContext:
        bindings = await self._build_dataset_bindings(
            workflow=workflow,
            workspace_id=semantic_model_entry.workspace_id,
        )
        dimensions: list[AnalyticalField] = []
        measures: list[AnalyticalField] = []
        for table_key, table in semantic_model.tables.items():
            for dimension in table.dimensions or []:
                dimensions.append(
                    AnalyticalField(
                        name=f"{table_key}.{dimension.name}",
                        expression=str(dimension.expression or dimension.name).strip(),
                        synonyms=list(dimension.synonyms or []),
                    )
                )
            for measure in table.measures or []:
                measures.append(
                    AnalyticalField(
                        name=f"{table_key}.{measure.name}",
                        expression=str(measure.expression or measure.name).strip(),
                        synonyms=list(measure.synonyms or []),
                    )
                )
        metrics = [
            AnalyticalMetric(
                name=name,
                expression=getattr(metric, "expression", None),
                description=getattr(metric, "description", None),
            )
            for name, metric in (semantic_model.metrics or {}).items()
        ]
        return AnalyticalContext(
            query_scope=SqlQueryScope.semantic,
            asset_type="semantic_model",
            asset_id=str(semantic_model_entry.id),
            asset_name=semantic_model_entry.name or semantic_model.name or config.name,
            description=semantic_model_entry.description or semantic_model.description or config.description,
            tags=list(semantic_model.tags or []),
            execution_mode="federated",
            dialect="postgres",
            datasets=bindings,
            tables=list(semantic_model.tables.keys()),
            dimensions=dimensions,
            measures=measures,
            metrics=metrics,
            relationships=[self._format_relationship(item) for item in (semantic_model.relationships or [])],
        )

    async def _build_dataset_bindings(
        self,
        *,
        workflow: FederationWorkflow,
        workspace_id: uuid.UUID,
    ) -> list[AnalyticalDatasetBinding]:
        dataset_records = await self._load_workflow_dataset_records(
            workflow=workflow,
            workspace_id=workspace_id,
        )
        bindings: list[AnalyticalDatasetBinding] = []
        for table_binding in workflow.dataset.tables.values():
            dataset_id = self._table_binding_dataset_id(table_binding)
            dataset = dataset_records.get(dataset_id) if dataset_id is not None else None
            columns = await self._list_dataset_columns(dataset_id) if dataset_id is not None else []
            if not columns:
                columns = self._infer_dataset_columns_from_sql(
                    dataset_record=dataset,
                    table_binding=table_binding,
                )
            bindings.append(
                AnalyticalDatasetBinding(
                    dataset_id=str(dataset.id if dataset is not None else dataset_id or ""),
                    dataset_name=dataset.name if dataset is not None else str(table_binding.table),
                    sql_alias=str(table_binding.table_key),
                    description=dataset.description if dataset is not None else None,
                    source_kind=self._enum_value(
                        getattr(getattr(table_binding, "dataset_descriptor", None), "source_kind", None)
                        or (dataset.source_kind if dataset is not None else None)
                    ),
                    storage_kind=self._enum_value(
                        getattr(getattr(table_binding, "dataset_descriptor", None), "storage_kind", None)
                        or (dataset.storage_kind if dataset is not None else None)
                    ),
                    columns=columns,
                )
            )
        return bindings

    async def _load_workflow_dataset_records(
        self,
        *,
        workflow: FederationWorkflow,
        workspace_id: uuid.UUID,
    ) -> dict[uuid.UUID, DatasetMetadata]:
        if self._dataset_repository is None:
            return {}
        dataset_ids = [
            dataset_id
            for table_binding in workflow.dataset.tables.values()
            if (dataset_id := self._table_binding_dataset_id(table_binding)) is not None
        ]
        if not dataset_ids:
            return {}
        rows = await self._dataset_repository.get_by_ids_for_workspace(
            workspace_id=workspace_id,
            dataset_ids=list(dict.fromkeys(dataset_ids)),
        )
        return {row.id: row for row in rows}

    async def _load_datasets(self, dataset_ids: Sequence[str]) -> list[DatasetMetadata]:
        if self._dataset_repository is None:
            raise RuntimeError("Dataset repository is required to build dataset agent tools.")
        resolved_ids = [uuid.UUID(str(dataset_id)) for dataset_id in dataset_ids]
        rows = await self._dataset_repository.get_by_ids(resolved_ids)
        indexed = {row.id: row for row in rows}
        missing = [str(dataset_id) for dataset_id in resolved_ids if dataset_id not in indexed]
        if missing:
            raise RuntimeError(f"Dataset ids not found for agent tool: {', '.join(missing)}")
        return [indexed[dataset_id] for dataset_id in resolved_ids]

    async def _load_semantic_model_entry(self, semantic_model_id: str) -> SemanticModelMetadata | None:
        if self._semantic_model_store is None:
            raise RuntimeError("Semantic model store is required to build semantic model agent tools.")
        return await self._semantic_model_store.get_by_id(uuid.UUID(str(semantic_model_id)))

    async def _list_dataset_columns(self, dataset_id: uuid.UUID) -> list[AnalyticalColumn]:
        if self._dataset_column_repository is None:
            return []
        rows = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset_id)
        return [
            AnalyticalColumn(
                name=row.name,
                data_type=row.data_type,
                description=row.description,
            )
            for row in rows
        ]

    def _build_semantic_search_tools(
        self,
        *,
        semantic_model_entry: SemanticModelMetadata,
        semantic_model: SemanticModel,
    ) -> list[SemanticSearchTool]:
        if self._semantic_vector_search_service is None:
            return []
        tools: list[SemanticSearchTool] = []
        semantic_asset_name = semantic_model_entry.name or semantic_model.name or str(semantic_model_entry.id)
        seen: set[tuple[str, str]] = set()
        for dataset_key, dataset in semantic_model.datasets.items():
            for dimension in dataset.dimensions or []:
                if not getattr(getattr(dimension, "vector", None), "enabled", False):
                    continue
                key = (str(dataset_key), str(dimension.name))
                if key in seen:
                    continue
                seen.add(key)
                tools.append(
                    SemanticSearchTool(
                        name=f"{semantic_asset_name}:{dataset_key}.{dimension.name}",
                        llm_provider=self._llm_provider,
                        semantic_vector_search_service=self._semantic_vector_search_service,
                        semantic_vector_search_workspace_id=semantic_model_entry.workspace_id,
                        semantic_vector_search_model_id=semantic_model_entry.id,
                        semantic_vector_search_dataset_key=str(dataset_key),
                        semantic_vector_search_dimension_name=str(dimension.name),
                        embedding_provider=self._embedding_provider,
                        logger=self._logger,
                        event_emitter=self._event_emitter,
                    )
                )
        return tools

    def _build_web_provider(self, config: AnalystAgentConfig) -> WebSearchProvider | None:
        if not config.web_search_enabled:
            return None
        return create_web_search_provider(config.web_search_provider)

    @staticmethod
    def _register_for_scope(
        target: dict[str, list[Any]],
        config: AnalystAgentConfig,
        tools: list[Any],
    ) -> None:
        if not tools:
            return
        target[config.name] = list(tools)
        target[config.agent_name] = list(tools)

    @staticmethod
    def _register_web_provider(
        target: dict[str, WebSearchProvider],
        config: AnalystAgentConfig,
        provider: WebSearchProvider,
    ) -> None:
        for key in {config.name, config.agent_name, config.web_search_provider or ""}:
            if key:
                target[key] = provider

    @staticmethod
    def _build_dataset_semantic_model(*, context: AnalyticalContext) -> SemanticModel:
        tables: dict[str, Table] = {}
        for dataset in context.datasets:
            dimensions: list[Dimension] = []
            measures: list[Measure] = []
            for column in dataset.columns:
                data_type = column.data_type or "text"
                dimensions.append(
                    Dimension(
                        name=column.name,
                        type=data_type,
                        description=column.description,
                        primary_key=column.name.lower() == "id" or column.name.lower().endswith("_id"),
                    )
                )
                if RuntimeToolFactory._is_numeric_type(data_type):
                    measures.append(
                        Measure(
                            name=column.name,
                            type=data_type,
                            description=column.description,
                            aggregation="sum",
                        )
                    )
            tables[dataset.sql_alias] = Table(
                dataset_id=dataset.dataset_id,
                name=dataset.sql_alias,
                description=dataset.description,
                dimensions=dimensions,
                measures=measures,
            )
        return SemanticModel(
            version="1.0",
            name=context.asset_name,
            description=context.description,
            dialect=context.dialect,
            tags=context.tags,
            tables=tables,
            relationships=[],
            metrics={"row_count": Metric(expression="COUNT(*)", description="Count of rows")},
        )

    @staticmethod
    def _dataset_asset_identity(*, config: AnalystAgentConfig, datasets: list[DatasetMetadata]) -> tuple[str, str]:
        if len(datasets) == 1:
            return str(datasets[0].id), datasets[0].name
        return (
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    "langbridge-ai-agent-datasets:" + ",".join(sorted(str(dataset.id) for dataset in datasets)),
                )
            ),
            config.description or ", ".join(dataset.name for dataset in datasets),
        )

    @staticmethod
    def _raw_semantic_datasets_payload(semantic_model_entry: SemanticModelMetadata) -> Mapping[str, Any] | None:
        try:
            raw = yaml.safe_load(semantic_model_entry.content_yaml) or {}
        except yaml.YAMLError:
            return None
        if not isinstance(raw, Mapping):
            return None
        datasets = raw.get("datasets")
        if isinstance(datasets, Mapping):
            return datasets
        tables = raw.get("tables")
        return tables if isinstance(tables, Mapping) else None

    @staticmethod
    def _table_binding_dataset_id(table_binding: Any) -> uuid.UUID | None:
        metadata = table_binding.metadata if isinstance(table_binding.metadata, dict) else {}
        raw = metadata.get("dataset_id")
        if not raw:
            return None
        try:
            return uuid.UUID(str(raw))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _infer_dataset_columns_from_sql(*, dataset_record: Any | None, table_binding: Any) -> list[AnalyticalColumn]:
        sql_text = str(getattr(dataset_record, "sql_text", "") or "").strip()
        dialect = str(getattr(dataset_record, "dialect", "") or "tsql").strip() or "tsql"
        if not sql_text:
            metadata = table_binding.metadata if isinstance(table_binding.metadata, dict) else {}
            sql_text = str(metadata.get("physical_sql") or "").strip()
            dialect = str(metadata.get("sql_dialect") or dialect).strip() or dialect
        if not sql_text:
            return []
        try:
            expression = sqlglot.parse_one(sql_text, read=normalize_sql_dialect(dialect, default="tsql"))
        except sqlglot.ParseError:
            return []
        select_expr = expression if isinstance(expression, exp.Select) else expression.find(exp.Select)
        if not isinstance(select_expr, exp.Select):
            return []
        columns: list[AnalyticalColumn] = []
        seen: set[str] = set()
        for projection in select_expr.expressions or []:
            output_name = str(getattr(projection, "output_name", "") or "").strip()
            if not output_name or output_name == "*":
                continue
            normalized = output_name.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            columns.append(AnalyticalColumn(name=output_name))
        return columns

    @staticmethod
    def _format_relationship(value: Any) -> str:
        if isinstance(value, str):
            return value
        for attr in ("join_condition", "condition"):
            condition = getattr(value, attr, None)
            if condition:
                return str(condition)
        return str(value)

    @staticmethod
    def _is_numeric_type(data_type: str | None) -> bool:
        return bool(
            data_type
            and re.search(
                r"\b(int|integer|bigint|smallint|decimal|numeric|double|float|real|number|money)\b",
                data_type.lower(),
            )
        )

    @staticmethod
    def _choose_workflow_dialect(dialects: Sequence[str]) -> str:
        normalized = [str(item or "").strip().lower() for item in dialects if str(item or "").strip()]
        return normalized[0] if normalized else "postgres"

    @staticmethod
    def _enum_value(value: Any) -> str | None:
        if value is None:
            return None
        return str(getattr(value, "value", value))

    @staticmethod
    def _tool_name(scope_name: str, suffix: str) -> str:
        scope = re.sub(r"[^a-z0-9_]+", "_", str(scope_name or "agent_tool").strip().lower()).strip("_")
        tail = re.sub(r"[^a-z0-9_]+", "_", str(suffix or "sql").strip().lower()).strip("_")
        return "_".join(part for part in (scope, tail, "sql") if part)

    def _require_federated_query_tool(self) -> FederatedQueryTool:
        if self._federated_query_tool is None:
            raise RuntimeError("FederatedQueryTool is required to build SQL agent tools.")
        return self._federated_query_tool


__all__ = ["RuntimeAgentTooling", "RuntimeToolFactory"]
