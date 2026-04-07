import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import sqlglot
import yaml
from sqlglot import exp

from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.orchestrator.errors import AgentError
from langbridge.runtime.events import AgentEventEmitter
from langbridge.runtime.models import DatasetMetadata, SemanticModelMetadata
from langbridge.runtime.ports import (
    DatasetCatalogStore,
    DatasetColumnStore,
    SemanticModelStore,
)
from langbridge.runtime.services.semantic_vector_search_service import (
    SemanticVectorSearchService,
)
from langbridge.runtime.services.dataset_execution import DatasetExecutionResolver
from langbridge.orchestrator.agents.analyst import AnalystAgent
from langbridge.orchestrator.agents.deep_research import DeepResearchAgent
from langbridge.orchestrator.agents.planner import (
    PlanningAgent,
    PlanningConstraints,
)
from langbridge.orchestrator.agents.reasoning.agent import ReasoningAgent
from langbridge.orchestrator.agents.supervisor.orchestrator import SupervisorOrchestrator
from langbridge.orchestrator.agents.visual import VisualAgent
from langbridge.orchestrator.agents.web_search import WebSearchAgent
from langbridge.orchestrator.definitions import AgentDefinitionModel, ExecutionMode
from langbridge.orchestrator.definitions.model import DataAccessPolicy, ToolType
from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.runtime.access_policy import (
    AnalyticalAccessScope,
    ConnectorAccessPolicyEvaluator,
)
from langbridge.orchestrator.runtime.response_formatter import ResponsePresentation
from langbridge.orchestrator.tools.sql_analyst import SqlAnalystTool
from langbridge.orchestrator.tools.sql_analyst.interfaces import (
    AnalyticalColumn,
    AnalyticalContext,
    AnalyticalDatasetBinding,
    AnalyticalField,
    AnalyticalMetric,
    QueryResult,
)
from langbridge.runtime.settings import runtime_settings
from langbridge.runtime.utils.sql import normalize_sql_dialect
from langbridge.federation.models import FederationWorkflow, VirtualDataset
from langbridge.semantic.loader import load_semantic_model
from langbridge.semantic.model import Dimension, Measure, Metric, SemanticModel, Table



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
    analytical_access_scope: AnalyticalAccessScope | None = None


@dataclass(slots=True)
class AnalystToolBuildResult:
    tools: list[SqlAnalystTool]
    access_scope: AnalyticalAccessScope


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
            raise AgentError("Federated SQL execution returned an invalid rows payload.")

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
    """Build runtime-side orchestrator components for dataset-first federated analysis."""

    def __init__(
        self,
        semantic_model_store: SemanticModelStore,
        dataset_repository: DatasetCatalogStore | None = None,
        dataset_column_repository: DatasetColumnStore | None = None,
        federated_query_tool: Any | None = None,
        semantic_vector_search_service: SemanticVectorSearchService | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._semantic_model_store = semantic_model_store
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._federated_query_tool = federated_query_tool
        self._semantic_vector_search_service = semantic_vector_search_service
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=self._dataset_repository,
        )

    async def create_runtime(
        self,
        *,
        definition: AgentDefinitionModel,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[AgentEventEmitter] = None,
    ) -> AgentRuntime:
        tool_config = self._build_agent_tool_config(definition)
        analyst_tool_build = await self._build_analyst_tools(
            tool_config=tool_config,
            access_policy=definition.access_policy,
            llm_provider=llm_provider,
            embedding_provider=embedding_provider,
            event_emitter=event_emitter,
        )
        analyst_tools = analyst_tool_build.tools
        tool_config.analytical_access_scope = analyst_tool_build.access_scope

        if tool_config.allow_sql and not analyst_tools:
            if (
                tool_config.analytical_access_scope is not None
                and tool_config.analytical_access_scope.all_configured_assets_denied
            ):
                self._logger.info(
                    "All configured analytical assets were excluded by connector access policy; "
                    "supervisor will return access_denied for analytical requests."
                )
            else:
                self._logger.warning(
                    "No analytical tools could be created from the selected asset ids; disabling analyst route."
                )
            tool_config.allow_sql = False

        planning_constraints = self._build_planning_constraints(tool_config, definition)
        planning_context = self._build_planner_tool_context(
            tool_config=tool_config,
            analyst_tools=analyst_tools,
            access_scope=tool_config.analytical_access_scope,
        )
        supervisor = self._build_supervisor_orchestrator(
            definition=definition,
            llm_provider=llm_provider,
            planning_constraints=planning_constraints,
            analyst_tools=analyst_tools,
            analytical_access_scope=tool_config.analytical_access_scope,
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
        access_scope: AnalyticalAccessScope | None,
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
        if access_scope is not None and access_scope.policy_enforced:
            context["analytical_access"] = access_scope.to_metadata()
        if tool_config.web_search_defaults:
            context.update(tool_config.web_search_defaults)

        return context or None

    async def _build_analyst_tools(
        self,
        *,
        tool_config: AgentToolConfig,
        access_policy: DataAccessPolicy,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[AgentEventEmitter],
    ) -> AnalystToolBuildResult:
        if not tool_config.allow_sql or not tool_config.analyst_bindings:
            return AnalystToolBuildResult(
                tools=[],
                access_scope=AnalyticalAccessScope(),
            )
        if self._federated_query_tool is None:
            self._logger.warning("Federated query tool is not configured; analyst route cannot be built.")
            return AnalystToolBuildResult(
                tools=[],
                access_scope=AnalyticalAccessScope(),
            )

        access_policy_evaluator = ConnectorAccessPolicyEvaluator(access_policy)
        sql_tools: list[SqlAnalystTool] = []
        denied_assets = []
        for binding in tool_config.analyst_bindings:
            if binding.dataset_ids:
                datasets = await self._load_datasets(binding.dataset_ids)
                dataset_asset_id, dataset_asset_name = self._resolve_dataset_asset_identity(
                    asset_dataset=datasets[0],
                    selected_datasets=datasets,
                    binding=binding,
                )
                access_decision = access_policy_evaluator.evaluate_asset_connectors(
                    connector_ids=[dataset.connection_id for dataset in datasets],
                )
                if not access_decision.allowed:
                    self._logger.info(
                        "Excluded dataset analytical asset '%s' from agent scope: %s",
                        dataset_asset_name,
                        access_decision.policy_rationale,
                    )
                    denied_assets.append(
                        access_policy_evaluator.build_denied_asset(
                            asset_type="dataset",
                            asset_id=dataset_asset_id,
                            asset_name=dataset_asset_name,
                            dataset_names=[dataset.name for dataset in datasets],
                            sql_aliases=[str(dataset.sql_alias or "") for dataset in datasets],
                            decision=access_decision,
                        )
                    )
                    continue
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
                semantic_model = load_semantic_model(semantic_model_entry.content_yaml)
                raw_model_payload = self._parse_yaml_payload(semantic_model_entry.content_yaml)
                workflow, _workflow_dialect = await self._dataset_execution_resolver.build_semantic_workflow(
                    workspace_id=semantic_model_entry.workspace_id,
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
                dataset_records_by_id = await self._load_workflow_dataset_records(
                    workspace_id=semantic_model_entry.workspace_id,
                    workflow=workflow,
                )
                access_decision = access_policy_evaluator.evaluate_asset_connectors(
                    connector_ids=[
                        getattr(table_binding, "connector_id", None)
                        for table_binding in workflow.dataset.tables.values()
                    ],
                )
                semantic_asset_name = semantic_model_entry.name or semantic_model.name or binding.name
                if not access_decision.allowed:
                    self._logger.info(
                        "Excluded semantic analytical asset '%s' from agent scope: %s",
                        semantic_asset_name,
                        access_decision.policy_rationale,
                    )
                    denied_assets.append(
                        access_policy_evaluator.build_denied_asset(
                            asset_type="semantic_model",
                            asset_id=str(semantic_model_entry.id),
                            asset_name=semantic_asset_name,
                            dataset_names=[
                                dataset_records_by_id.get(dataset_id).name
                                if dataset_id in dataset_records_by_id
                                else str(table_binding.table)
                                for dataset_id, table_binding in [
                                    (
                                        self._extract_dataset_id_from_table_binding(table_binding),
                                        table_binding,
                                    )
                                    for table_binding in workflow.dataset.tables.values()
                                ]
                            ],
                            sql_aliases=[
                                str(table_binding.table_key)
                                for table_binding in workflow.dataset.tables.values()
                            ],
                            decision=access_decision,
                        )
                    )
                    continue
                sql_tools.append(
                    await self._build_semantic_model_tool(
                        semantic_model_entry=semantic_model_entry,
                        semantic_model=semantic_model,
                        binding=binding,
                        workflow=workflow,
                        llm_provider=llm_provider,
                        embedding_provider=embedding_provider,
                        event_emitter=event_emitter,
                    )
                )

        return AnalystToolBuildResult(
            tools=sql_tools,
            access_scope=AnalyticalAccessScope(
                policy_enforced=access_policy_evaluator.has_restrictions,
                authorized_asset_count=len(sql_tools),
                denied_assets=tuple(denied_assets),
            ),
        )

    async def _build_dataset_tool(
        self,
        *,
        dataset: DatasetMetadata,
        selected_datasets: list[DatasetMetadata],
        binding: AnalystBinding,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[AgentEventEmitter],
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
        semantic_model_entry: SemanticModelMetadata,
        semantic_model: SemanticModel,
        binding: AnalystBinding,
        workflow: FederationWorkflow,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[AgentEventEmitter],
    ) -> SqlAnalystTool:
        context = await self._build_semantic_model_context(
            binding=binding,
            semantic_model_entry=semantic_model_entry,
            semantic_model=semantic_model,
            workflow=workflow,
        )
        federated_executor = _FederatedSqlExecutor(
            federated_query_tool=self._federated_query_tool,
            workflow=workflow,
            workspace_id=str(semantic_model_entry.workspace_id),
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
            semantic_vector_search_service=self._semantic_vector_search_service,
            semantic_vector_search_workspace_id=semantic_model_entry.workspace_id,
            semantic_vector_search_model_id=semantic_model_entry.id,
        )

    async def _build_dataset_workflow(
        self,
        datasets: list[DatasetMetadata],
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
                raise AgentError(f"Dataset '{dataset.name}' is missing a sql_alias.")
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
            broadcast_threshold_bytes=runtime_settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
            partition_count=runtime_settings.FEDERATION_PARTITION_COUNT,
            max_stage_retries=runtime_settings.FEDERATION_STAGE_MAX_RETRIES,
            stage_parallelism=runtime_settings.FEDERATION_STAGE_PARALLELISM,
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

    async def _load_datasets(self, dataset_ids: list[uuid.UUID]) -> list[DatasetMetadata]:
        if self._dataset_repository is None:
            raise AgentError("Dataset repository is required for dataset-backed analysis.")
        ordered: list[DatasetMetadata] = []
        for dataset_id in dataset_ids:
            dataset = await self._dataset_repository.get_by_id(dataset_id)
            if dataset is None:
                raise AgentError(f"Dataset '{dataset_id}' was not found.")
            ordered.append(dataset)
        return ordered

    async def _build_dataset_context(
        self,
        *,
        asset_dataset: DatasetMetadata,
        selected_datasets: list[DatasetMetadata],
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
                dimensions.append(AnalyticalField(name=field_name, expression=column.name))
                if self._is_numeric_type(column.data_type):
                    measures.append(AnalyticalField(name=field_name, expression=column.name))

        relationships = [self._format_virtual_relationship(item) for item in workflow.dataset.relationships]
        asset_id, context_name = self._resolve_dataset_asset_identity(
            asset_dataset=asset_dataset,
            selected_datasets=selected_datasets,
            binding=binding,
        )
        return AnalyticalContext(
            asset_type="dataset",
            asset_id=asset_id,
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

    @staticmethod
    def _resolve_dataset_asset_identity(
        *,
        asset_dataset: DatasetMetadata,
        selected_datasets: list[DatasetMetadata],
        binding: AnalystBinding,
    ) -> tuple[str, str]:
        context_name = asset_dataset.name
        if len(selected_datasets) > 1:
            context_name = binding.description or ", ".join(dataset.name for dataset in selected_datasets)
        if len(selected_datasets) == 1:
            return str(asset_dataset.id), context_name
        return (
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    "langbridge-analyst-datasets:"
                    + ",".join(sorted(str(dataset.id) for dataset in selected_datasets)),
                )
            ),
            context_name,
        )

    async def _build_semantic_model_context(
        self,
        *,
        binding: AnalystBinding,
        semantic_model_entry: SemanticModelMetadata,
        semantic_model: SemanticModel,
        workflow: FederationWorkflow,
    ) -> AnalyticalContext:
        datasets = await self._build_context_dataset_bindings(
            workspace_id=semantic_model_entry.workspace_id,
            workflow=workflow,
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
            if not columns:
                columns = self._infer_dataset_columns_from_sql(
                    dataset_record=dataset_record,
                    table_binding=table_binding,
                )
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
    ) -> dict[uuid.UUID, DatasetMetadata]:
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

    @staticmethod
    def _extract_dataset_id_from_table_binding(table_binding: Any) -> uuid.UUID | None:
        metadata = table_binding.metadata if isinstance(table_binding.metadata, dict) else {}
        dataset_id_raw = metadata.get("dataset_id")
        if not dataset_id_raw:
            return None
        try:
            return uuid.UUID(str(dataset_id_raw))
        except (TypeError, ValueError):
            return None

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
    def _infer_dataset_columns_from_sql(
        *,
        dataset_record: Any | None,
        table_binding: Any,
    ) -> list[AnalyticalColumn]:
        sql_text, dialect = AgentOrchestratorFactory._dataset_sql_projection_source(
            dataset_record=dataset_record,
            table_binding=table_binding,
        )
        if not sql_text:
            return []
        try:
            expression = sqlglot.parse_one(
                sql_text,
                read=normalize_sql_dialect(dialect, default="tsql"),
            )
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
    def _dataset_sql_projection_source(
        *,
        dataset_record: Any | None,
        table_binding: Any,
    ) -> tuple[str | None, str | None]:
        metadata = table_binding.metadata if isinstance(table_binding.metadata, dict) else {}
        if dataset_record is not None:
            sql_text = str(getattr(dataset_record, "sql_text", None) or "").strip()
            if sql_text:
                return sql_text, getattr(dataset_record, "dialect", None)

            source_payload = getattr(dataset_record, "source_json", None)
            if isinstance(source_payload, dict):
                source_sql = str(source_payload.get("sql") or "").strip()
                if source_sql:
                    return source_sql, getattr(dataset_record, "dialect", None)

            sync_payload = getattr(dataset_record, "sync_json", None)
            if isinstance(sync_payload, dict):
                sync_source = sync_payload.get("source")
                if isinstance(sync_source, dict):
                    sync_sql = str(sync_source.get("sql") or "").strip()
                    if sync_sql:
                        return sync_sql, getattr(dataset_record, "dialect", None)

        physical_sql = str(metadata.get("physical_sql") or "").strip()
        if physical_sql:
            return physical_sql, metadata.get("sql_dialect")
        return None, None

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
        analytical_access_scope: AnalyticalAccessScope | None,
        event_emitter: Optional[AgentEventEmitter],
    ) -> SupervisorOrchestrator:
        analyst_agent = None
        if analyst_tools:
            analyst_agent = AnalystAgent(
                llm=llm_provider,
                tools=analyst_tools,
                access_scope=analytical_access_scope,
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
            analytical_access_scope=analytical_access_scope,
            response_presentation=ResponsePresentation.from_definition(definition),
            response_mode=definition.execution.response_mode,
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
