import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import yaml

from langbridge.packages.contracts.semantic import SemanticModelRecordResponse
from langbridge.packages.common.langbridge_common.db.dataset import DatasetRecord
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.interfaces.agent_events import IAgentEventEmitter
from langbridge.packages.common.langbridge_common.interfaces.semantic_models import ISemanticModelStore
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import DatasetColumnRepository, DatasetRepository
from langbridge.packages.common.langbridge_common.utils.embedding_provider import EmbeddingProvider
from langbridge.packages.runtime.services.dataset_execution import DatasetExecutionResolver
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.analyst import AnalystAgent
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.deep_research import DeepResearchAgent
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.planner import (
    PlanningAgent,
    PlanningConstraints,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.reasoning.agent import ReasoningAgent
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.supervisor import SupervisorOrchestrator
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.visual import VisualAgent
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.web_search import WebSearchAgent
from langbridge.packages.orchestrator.langbridge_orchestrator.definitions import AgentDefinitionModel, ExecutionMode
from langbridge.packages.orchestrator.langbridge_orchestrator.definitions.model import ToolType
from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider import LLMProvider
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst import SqlAnalystTool
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.interfaces import (
    AnalyticalColumn,
    AnalyticalContext,
    AnalyticalDatasetBinding,
    AnalyticalField,
    AnalyticalMetric,
    QueryResult,
)
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.federation.models import FederationWorkflow, VirtualDataset
from langbridge.packages.semantic.langbridge_semantic.loader import load_semantic_model
from langbridge.packages.semantic.langbridge_semantic.model import Dimension, Measure, Metric, SemanticModel, Table



@dataclass(slots=True)
class AnalystBinding:
    name: str
    description: str | None = None
    dataset_ids: list[uuid.UUID] = field(default_factory=list)
    semantic_model_ids: list[uuid.UUID] = field(default_factory=list)


@dataclass(slots=True)
class AgentToolConfig:
    allow_sql: bool
    allow_web_search: bool
    allow_deep_research: bool
    allow_visualization: bool
    analyst_bindings: list[AnalystBinding] = field(default_factory=list)
    web_search_defaults: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AgentRuntime:
    supervisor: SupervisorOrchestrator
    planning_constraints: PlanningConstraints
    planning_context: Dict[str, Any] | None


class _FederatedSqlExecutor:
    def __init__(
        self,
        *,
        federated_query_tool: Any,
        workflow: FederationWorkflow,
        workspace_id: str,
    ) -> None:
        self._federated_query_tool = federated_query_tool
        self._workflow = workflow
        self._workspace_id = workspace_id

    async def execute_sql(
        self,
        *,
        sql: str,
        dialect: str,
        max_rows: int | None = None,
    ) -> QueryResult:
        execution = await self._federated_query_tool.execute_federated_query(
            {
                "workspace_id": self._workspace_id,
                "query": sql,
                "dialect": dialect,
                "workflow": self._workflow,
            }
        )
        rows_payload = execution.get("rows", [])
        if not isinstance(rows_payload, list):
            raise BusinessValidationError("Federated SQL execution returned an invalid rows payload.")

        columns_payload = execution.get("columns", [])
        if isinstance(columns_payload, list) and columns_payload:
            columns = [str(column) for column in columns_payload]
        else:
            first_row = rows_payload[0] if rows_payload else {}
            columns = [str(key) for key in first_row.keys()] if isinstance(first_row, dict) else []

        rows: list[tuple[Any, ...]] = []
        for row in rows_payload:
            if isinstance(row, dict):
                rows.append(tuple(row.get(column) for column in columns))
            elif isinstance(row, (list, tuple)):
                rows.append(tuple(row))
            else:
                rows.append((row,))

        execution_summary = execution.get("execution", {})
        elapsed_ms: int | None = None
        if isinstance(execution_summary, dict):
            raw_runtime = execution_summary.get("total_runtime_ms")
            if isinstance(raw_runtime, int):
                elapsed_ms = raw_runtime

        return QueryResult(
            columns=columns,
            rows=rows,
            rowcount=len(rows),
            elapsed_ms=elapsed_ms,
            source_sql=sql,
        )


class AgentOrchestratorFactory:
    """Build worker-side orchestrator components for dataset-first federated analysis."""

    def __init__(
        self,
        semantic_model_store: ISemanticModelStore,
        dataset_repository: DatasetRepository | None = None,
        dataset_column_repository: DatasetColumnRepository | None = None,
        federated_query_tool: Any | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._semantic_model_store = semantic_model_store
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._federated_query_tool = federated_query_tool
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=self._dataset_repository,
        )

    async def create_runtime(
        self,
        *,
        definition: AgentDefinitionModel,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[IAgentEventEmitter] = None,
    ) -> AgentRuntime:
        tool_config = self._build_agent_tool_config(definition)
        analyst_tools = await self._build_analyst_tools(
            tool_config=tool_config,
            llm_provider=llm_provider,
            embedding_provider=embedding_provider,
            event_emitter=event_emitter,
        )

        if tool_config.allow_sql and not analyst_tools:
            self._logger.warning(
                "No analytical tools could be created from the selected asset ids; disabling analyst route."
            )
            tool_config.allow_sql = False

        planning_constraints = self._build_planning_constraints(tool_config, definition)
        planning_context = self._build_planner_tool_context(
            tool_config=tool_config,
            analyst_tools=analyst_tools,
        )
        supervisor = self._build_supervisor_orchestrator(
            definition=definition,
            llm_provider=llm_provider,
            planning_constraints=planning_constraints,
            analyst_tools=analyst_tools,
            event_emitter=event_emitter,
        )

        return AgentRuntime(
            supervisor=supervisor,
            planning_constraints=planning_constraints,
            planning_context=planning_context,
        )

    def _build_agent_tool_config(self, definition: AgentDefinitionModel) -> AgentToolConfig:
        tools = list(definition.tools or [])

        sql_tools = [tool for tool in tools if tool.tool_type == ToolType.sql]
        web_search_tools = [tool for tool in tools if tool.tool_type == ToolType.web]
        doc_tools = [tool for tool in tools if tool.tool_type == ToolType.doc]

        analyst_bindings: list[AnalystBinding] = []
        for tool in sql_tools:
            try:
                config = tool.get_sql_tool_config()
            except (ValueError, TypeError) as exc:
                self._logger.warning("Invalid SQL tool config for tool '%s': %s", tool.name, exc)
                continue
            analyst_bindings.append(
                AnalystBinding(
                    name=tool.name,
                    description=tool.description,
                    dataset_ids=list(config.dataset_ids),
                    semantic_model_ids=list(config.semantic_model_ids),
                )
            )

        return AgentToolConfig(
            allow_sql=bool(analyst_bindings),
            allow_web_search=bool(web_search_tools),
            allow_deep_research=definition.features.deep_research_enabled or bool(doc_tools),
            allow_visualization=definition.features.visualization_enabled,
            analyst_bindings=analyst_bindings,
        )

    def _build_planning_constraints(
        self,
        tool_config: AgentToolConfig,
        definition: AgentDefinitionModel,
    ) -> PlanningConstraints:
        max_steps = max(1, min(int(definition.execution.max_steps_per_iteration), 10))
        if definition.execution.mode == ExecutionMode.single_step:
            max_steps = 1

        return PlanningConstraints(
            max_steps=max_steps,
            ignore_max_steps=definition.features.deep_research_enabled,
            prefer_low_latency=definition.execution.mode == ExecutionMode.single_step,
            require_viz_when_chartable=definition.features.visualization_enabled,
            allow_sql_analyst=tool_config.allow_sql,
            allow_web_search=tool_config.allow_web_search,
            allow_deep_research=tool_config.allow_deep_research,
        )

    def _build_planner_tool_context(
        self,
        *,
        tool_config: AgentToolConfig,
        analyst_tools: list[SqlAnalystTool],
    ) -> Dict[str, Any] | None:
        available_agents = [
            {
                "agent": "Analyst",
                "description": "Query datasets and governed semantic models through federated execution.",
                "enabled": tool_config.allow_sql,
                "notes": "Uses the analytical_assets list.",
            },
            {
                "agent": "Visual",
                "description": "Generate a visualization spec from analyst results.",
                "enabled": tool_config.allow_visualization,
            },
            {
                "agent": "WebSearch",
                "description": "Search the web for sources and snippets.",
                "enabled": tool_config.allow_web_search,
            },
            {
                "agent": "DocRetrieval",
                "description": "Synthesize insights from documents and sources.",
                "enabled": tool_config.allow_deep_research,
            },
            {
                "agent": "Clarify",
                "description": "Ask a clarifying question when key details are missing.",
                "enabled": True,
            },
        ]

        analytical_assets: Dict[str, Dict[str, Any]] = {}
        for tool in analyst_tools:
            analytical_assets[tool.context.asset_id] = {
                "name": tool.context.asset_name,
                "asset_type": tool.context.asset_type,
                "description": tool.context.description,
                "execution_mode": tool.context.execution_mode,
                "datasets": [
                    {
                        "name": dataset.dataset_name,
                        "sql_alias": dataset.sql_alias,
                        "source_kind": dataset.source_kind,
                        "storage_kind": dataset.storage_kind,
                    }
                    for dataset in tool.context.datasets
                ],
                "tables": list(tool.context.tables),
                "metrics": [metric.name for metric in tool.context.metrics],
                "dimensions": [field.name for field in tool.context.dimensions],
                "measures": [field.name for field in tool.context.measures],
            }

        context: Dict[str, Any] = {
            "available_agents": available_agents,
            "analytical_assets": analytical_assets,
            "analytical_assets_count": len(analytical_assets),
        }
        if tool_config.web_search_defaults:
            context.update(tool_config.web_search_defaults)

        return context or None

    async def _build_analyst_tools(
        self,
        *,
        tool_config: AgentToolConfig,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[IAgentEventEmitter],
    ) -> list[SqlAnalystTool]:
        if not tool_config.allow_sql or not tool_config.analyst_bindings:
            return []
        if self._federated_query_tool is None:
            self._logger.warning("Federated query tool is not configured; analyst route cannot be built.")
            return []

        sql_tools: list[SqlAnalystTool] = []
        for binding in tool_config.analyst_bindings:
            if binding.dataset_ids:
                datasets = await self._load_datasets(binding.dataset_ids)
                sql_tools.append(
                    await self._build_dataset_tool(
                        dataset=datasets[0],
                        selected_datasets=datasets,
                        binding=binding,
                        llm_provider=llm_provider,
                        embedding_provider=embedding_provider,
                        event_emitter=event_emitter,
                    )
                )
                continue

            for semantic_model_id in binding.semantic_model_ids:
                semantic_model_entry = await self._semantic_model_store.get_by_id(semantic_model_id)
                if semantic_model_entry is None:
                    self._logger.warning(
                        "Semantic model %s configured on analyst binding '%s' was not found.",
                        semantic_model_id,
                        binding.name,
                    )
                    continue
                sql_tools.append(
                    await self._build_semantic_model_tool(
                        semantic_model_entry=semantic_model_entry,
                        binding=binding,
                        llm_provider=llm_provider,
                        embedding_provider=embedding_provider,
                        event_emitter=event_emitter,
                    )
                )

        return sql_tools

    async def _build_dataset_tool(
        self,
        *,
        dataset: DatasetRecord,
        selected_datasets: list[DatasetRecord],
        binding: AnalystBinding,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[IAgentEventEmitter],
    ) -> SqlAnalystTool:
        workflow, _workflow_dialect = await self._build_dataset_workflow(selected_datasets)
        context = await self._build_dataset_context(
            asset_dataset=dataset,
            selected_datasets=selected_datasets,
            binding=binding,
            workflow=workflow,
        )
        federated_executor = _FederatedSqlExecutor(
            federated_query_tool=self._federated_query_tool,
            workflow=workflow,
            workspace_id=str(dataset.workspace_id),
        )
        semantic_model = self._build_dataset_semantic_model(context=context)
        return SqlAnalystTool(
            llm=llm_provider,
            context=context,
            semantic_model=semantic_model,
            federated_sql_executor=federated_executor,
            logger=self._logger,
            priority=0,
            embedder=embedding_provider,
            event_emitter=event_emitter,
        )

    async def _build_semantic_model_tool(
        self,
        *,
        semantic_model_entry: SemanticModelRecordResponse,
        binding: AnalystBinding,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[IAgentEventEmitter],
    ) -> SqlAnalystTool:
        semantic_model = load_semantic_model(semantic_model_entry.content_yaml)
        raw_model_payload = self._parse_yaml_payload(semantic_model_entry.content_yaml)
        workflow, _workflow_dialect = await self._dataset_execution_resolver.build_semantic_workflow(
            organization_id=semantic_model_entry.organization_id,
            workflow_id=f"workflow_semantic_agent_{semantic_model_entry.id.hex[:12]}",
            dataset_name=semantic_model_entry.name or f"semantic_model_{semantic_model_entry.id.hex[:8]}",
            semantic_model=semantic_model,
            raw_datasets_payload=(
                raw_model_payload.get("datasets")
                if isinstance(raw_model_payload.get("datasets"), dict)
                else (
                    raw_model_payload.get("tables")
                    if isinstance(raw_model_payload.get("tables"), dict)
                    else None
                )
            ),
        )
        context = await self._build_semantic_model_context(
            binding=binding,
            semantic_model_entry=semantic_model_entry,
            semantic_model=semantic_model,
            workflow=workflow,
        )
        federated_executor = _FederatedSqlExecutor(
            federated_query_tool=self._federated_query_tool,
            workflow=workflow,
            workspace_id=str(semantic_model_entry.organization_id),
        )
        return SqlAnalystTool(
            llm=llm_provider,
            context=context,
            semantic_model=semantic_model,
            federated_sql_executor=federated_executor,
            logger=self._logger,
            priority=0,
            embedder=embedding_provider,
            event_emitter=event_emitter,
        )

    async def _build_dataset_workflow(
        self,
        datasets: list[DatasetRecord],
    ) -> tuple[FederationWorkflow, str]:
        if len(datasets) == 1:
            workflow, _default_table_key, workflow_dialect = await self._dataset_execution_resolver.build_workflow_for_dataset(
                dataset=datasets[0],
            )
            return workflow, workflow_dialect

        table_bindings: dict[str, Any] = {}
        dialects: list[str] = []
        for dataset in datasets:
            sql_alias = str(dataset.sql_alias or "").strip().lower()
            if not sql_alias:
                raise BusinessValidationError(f"Dataset '{dataset.name}' is missing a sql_alias.")
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
            "langbridge-analyst-datasets:" + ",".join(sorted(str(dataset.id) for dataset in datasets)),
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
            broadcast_threshold_bytes=settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
            partition_count=settings.FEDERATION_PARTITION_COUNT,
            max_stage_retries=settings.FEDERATION_STAGE_MAX_RETRIES,
            stage_parallelism=settings.FEDERATION_STAGE_PARALLELISM,
        )
        return workflow, self._choose_workflow_dialect(dialects)

    def _build_dataset_semantic_model(self, *, context: AnalyticalContext) -> SemanticModel:
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
                if self._is_numeric_type(data_type):
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

        metrics = {
            "row_count": Metric(
                expression="COUNT(*)",
                description="Count of rows",
            )
        }
        return SemanticModel(
            version="1.0",
            name=context.asset_name,
            description=context.description,
            dialect=context.dialect,
            tags=context.tags,
            tables=tables,
            relationships=[],
            metrics=metrics,
        )

    async def _load_datasets(self, dataset_ids: list[uuid.UUID]) -> list[DatasetRecord]:
        if self._dataset_repository is None:
            raise BusinessValidationError("Dataset repository is required for dataset-backed analysis.")
        ordered: list[DatasetRecord] = []
        for dataset_id in dataset_ids:
            dataset = await self._dataset_repository.get_by_id(dataset_id)
            if dataset is None:
                raise BusinessValidationError(f"Dataset '{dataset_id}' was not found.")
            ordered.append(dataset)
        return ordered

    async def _build_dataset_context(
        self,
        *,
        asset_dataset: DatasetRecord,
        selected_datasets: list[DatasetRecord],
        binding: AnalystBinding,
        workflow: FederationWorkflow,
    ) -> AnalyticalContext:
        datasets = await self._build_context_dataset_bindings(
            workspace_id=asset_dataset.workspace_id,
            workflow=workflow,
        )
        dimensions: list[AnalyticalField] = []
        measures: list[AnalyticalField] = []
        for dataset_binding in datasets:
            for column in dataset_binding.columns:
                field_name = f"{dataset_binding.sql_alias}.{column.name}"
                dimensions.append(AnalyticalField(name=field_name))
                if self._is_numeric_type(column.data_type):
                    measures.append(AnalyticalField(name=field_name))

        relationships = [self._format_virtual_relationship(item) for item in workflow.dataset.relationships]
        context_name = asset_dataset.name
        if len(selected_datasets) > 1:
            context_name = binding.description or ", ".join(dataset.name for dataset in selected_datasets)
        return AnalyticalContext(
            asset_type="dataset",
            asset_id=str(asset_dataset.id) if len(selected_datasets) == 1 else str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    "langbridge-analyst-datasets:" + ",".join(sorted(str(dataset.id) for dataset in selected_datasets)),
                )
            ),
            asset_name=context_name,
            description=asset_dataset.description,
            tags=list(asset_dataset.tags_json or []),
            execution_mode="federated",
            dialect="postgres",
            datasets=datasets,
            tables=[binding.sql_alias for binding in datasets],
            dimensions=dimensions,
            measures=measures,
            metrics=[AnalyticalMetric(name="row_count", expression="COUNT(*)", description="Count of rows")],
            relationships=relationships,
        )

    async def _build_semantic_model_context(
        self,
        *,
        binding: AnalystBinding,
        semantic_model_entry: SemanticModelRecordResponse,
        semantic_model: SemanticModel,
        workflow: FederationWorkflow,
    ) -> AnalyticalContext:
        datasets = await self._build_context_dataset_bindings(
            workspace_id=semantic_model_entry.organization_id,
            workflow=workflow,
        )
        dimensions: list[AnalyticalField] = []
        measures: list[AnalyticalField] = []
        for table_key, table in semantic_model.tables.items():
            for dimension in table.dimensions or []:
                dimensions.append(
                    AnalyticalField(
                        name=f"{table_key}.{dimension.name}",
                        synonyms=list(dimension.synonyms or []),
                    )
                )
            for measure in table.measures or []:
                measures.append(
                    AnalyticalField(
                        name=f"{table_key}.{measure.name}",
                        synonyms=list(measure.synonyms or []),
                    )
                )
        metrics = [
            AnalyticalMetric(
                name=metric_name,
                expression=getattr(metric, "expression", None),
                description=getattr(metric, "description", None),
            )
            for metric_name, metric in (semantic_model.metrics or {}).items()
        ]
        relationships = [
            self._format_semantic_relationship(relationship)
            for relationship in (semantic_model.relationships or [])
        ]
        return AnalyticalContext(
            asset_type="semantic_model",
            asset_id=str(semantic_model_entry.id),
            asset_name=semantic_model_entry.name or semantic_model.name or binding.name,
            description=semantic_model_entry.description or semantic_model.description or binding.description,
            tags=list(semantic_model.tags or []),
            execution_mode="federated",
            dialect="postgres",
            datasets=datasets,
            tables=list(semantic_model.tables.keys()),
            dimensions=dimensions,
            measures=measures,
            metrics=metrics,
            relationships=relationships,
        )

    async def _build_context_dataset_bindings(
        self,
        *,
        workspace_id: uuid.UUID,
        workflow: FederationWorkflow,
    ) -> list[AnalyticalDatasetBinding]:
        dataset_records_by_id = await self._load_workflow_dataset_records(
            workspace_id=workspace_id,
            workflow=workflow,
        )
        bindings: list[AnalyticalDatasetBinding] = []
        for table_binding in workflow.dataset.tables.values():
            metadata = table_binding.metadata if isinstance(table_binding.metadata, dict) else {}
            dataset_id_raw = metadata.get("dataset_id")
            dataset_id = None
            try:
                dataset_id = uuid.UUID(str(dataset_id_raw)) if dataset_id_raw else None
            except (TypeError, ValueError):
                dataset_id = None
            dataset_record = dataset_records_by_id.get(dataset_id) if dataset_id is not None else None
            columns = await self._list_dataset_columns(dataset_id) if dataset_id is not None else []
            bindings.append(
                AnalyticalDatasetBinding(
                    dataset_id=str(dataset_record.id if dataset_record is not None else dataset_id or ""),
                    dataset_name=dataset_record.name if dataset_record is not None else str(table_binding.table),
                    sql_alias=str(table_binding.table_key),
                    description=dataset_record.description if dataset_record is not None else None,
                    source_kind=(
                        getattr(getattr(table_binding, "dataset_descriptor", None), "source_kind", None)
                        or (dataset_record.source_kind if dataset_record is not None else None)
                    ),
                    storage_kind=(
                        getattr(getattr(table_binding, "dataset_descriptor", None), "storage_kind", None)
                        or (dataset_record.storage_kind if dataset_record is not None else None)
                    ),
                    columns=columns,
                )
            )
        return bindings

    async def _load_workflow_dataset_records(
        self,
        *,
        workspace_id: uuid.UUID,
        workflow: FederationWorkflow,
    ) -> dict[uuid.UUID, DatasetRecord]:
        if self._dataset_repository is None:
            return {}

        dataset_ids: list[uuid.UUID] = []
        for table_binding in workflow.dataset.tables.values():
            metadata = table_binding.metadata if isinstance(table_binding.metadata, dict) else {}
            dataset_id_raw = metadata.get("dataset_id")
            if not dataset_id_raw:
                continue
            try:
                dataset_id = uuid.UUID(str(dataset_id_raw))
            except (TypeError, ValueError):
                continue
            if dataset_id not in dataset_ids:
                dataset_ids.append(dataset_id)
        if not dataset_ids:
            return {}

        rows = await self._dataset_repository.get_by_ids_for_workspace(
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
        )
        return {row.id: row for row in rows}

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

    @staticmethod
    def _parse_yaml_payload(content_yaml: str) -> dict[str, Any]:
        try:
            payload = yaml.safe_load(content_yaml)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _format_virtual_relationship(relationship: Any) -> str:
        join_type = str(getattr(relationship, "join_type", "inner") or "inner").upper()
        return (
            f"{join_type} join "
            f"{getattr(relationship, 'left_table', '')} -> "
            f"{getattr(relationship, 'right_table', '')} on "
            f"{getattr(relationship, 'condition', '')}"
        )

    @staticmethod
    def _format_semantic_relationship(relationship: Any) -> str:
        join_type = str(getattr(relationship, "type", "inner") or "inner").upper()
        return (
            f"{join_type} join "
            f"{getattr(relationship, 'from_', '')} -> "
            f"{getattr(relationship, 'to', '')} on "
            f"{getattr(relationship, 'join_on', '')}"
        )

    @staticmethod
    def _is_numeric_type(data_type: str | None) -> bool:
        normalized = str(data_type or "").strip().lower()
        return any(
            token in normalized
            for token in ("int", "decimal", "numeric", "number", "float", "double", "real")
        )

    @staticmethod
    def _choose_workflow_dialect(dialects: list[str]) -> str:
        normalized = [str(value or "").strip().lower() for value in dialects if str(value or "").strip()]
        if not normalized:
            return "postgres"
        if any(value == "duckdb" for value in normalized):
            return "duckdb"
        if any(value == "postgres" for value in normalized):
            return "postgres"
        return normalized[0]

    def _build_supervisor_orchestrator(
        self,
        *,
        definition: AgentDefinitionModel,
        llm_provider: LLMProvider,
        planning_constraints: PlanningConstraints,
        analyst_tools: list[SqlAnalystTool],
        event_emitter: Optional[IAgentEventEmitter],
    ) -> SupervisorOrchestrator:
        analyst_agent = None
        if analyst_tools:
            analyst_agent = AnalystAgent(
                llm=llm_provider,
                tools=analyst_tools,
                logger=self._logger,
            )

        planning_agent = PlanningAgent(llm=llm_provider, logger=self._logger)
        reasoning_agent = self._build_reasoning_agent(llm_provider, definition, planning_constraints)
        visual_agent = VisualAgent(llm=llm_provider, logger=self._logger)
        web_search_agent = WebSearchAgent(llm=llm_provider, logger=self._logger)
        deep_research_agent = DeepResearchAgent(
            llm=llm_provider,
            web_search_agent=web_search_agent,
            logger=self._logger,
            event_emitter=event_emitter,
        )

        return SupervisorOrchestrator(
            llm=llm_provider,
            analyst_agent=analyst_agent,
            visual_agent=visual_agent,
            planning_agent=planning_agent,
            reasoning_agent=reasoning_agent,
            deep_research_agent=deep_research_agent,
            web_search_agent=web_search_agent,
            logger=self._logger,
            event_emitter=event_emitter,
        )

    def _build_reasoning_agent(
        self,
        llm_provider: LLMProvider,
        definition: AgentDefinitionModel,
        planning_constraints: PlanningConstraints,
    ) -> ReasoningAgent:
        max_iterations = max(1, int(definition.execution.max_iterations))
        if definition.execution.mode == ExecutionMode.single_step:
            max_iterations = 1
        else:
            max_iterations = max(max_iterations, planning_constraints.max_steps)

        return ReasoningAgent(
            llm=llm_provider,
            max_iterations=max_iterations,
            logger=self._logger,
        )
