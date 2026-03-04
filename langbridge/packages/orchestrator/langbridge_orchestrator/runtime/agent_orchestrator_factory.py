from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type

import yaml

from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorDTO
from langbridge.packages.common.langbridge_common.contracts.semantic import SemanticModelRecordResponse
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.interfaces.agent_events import IAgentEventEmitter
from langbridge.packages.common.langbridge_common.interfaces.connectors import IConnectorStore
from langbridge.packages.common.langbridge_common.interfaces.semantic_models import ISemanticModelStore
from langbridge.packages.common.langbridge_common.utils.embedding_provider import EmbeddingProvider
from langbridge.packages.connectors.langbridge_connectors.api.config import (
    BaseConnectorConfigFactory,
    ConnectorRuntimeType,
    get_connector_config_factory,
)
from langbridge.packages.connectors.langbridge_connectors.api.connector import (
    ConnectorRuntimeTypeSqlDialectMap,
    ManagedVectorDB,
    SqlConnector,
    SqlDialetcs,
    VectorDBType,
)
from langbridge.packages.connectors.langbridge_connectors.api.registry import (
    SqlConnectorFactory,
    VectorDBConnectorFactory,
)
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
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.semantic_search import SemanticSearchTool
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst import SqlAnalystTool
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.interfaces import (
    QueryResult,
)
from langbridge.packages.federation.models import (
    FederationWorkflow,
    VirtualDataset,
    VirtualRelationship,
    VirtualTableBinding,
)
from langbridge.packages.semantic.langbridge_semantic.loader import load_semantic_model
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.unified_query import (
    TenantAwareQueryContext,
    UnifiedSourceModel,
    apply_tenant_aware_context,
    build_unified_semantic_model,
)


@dataclass(slots=True)
class AgentToolConfig:
    allow_sql: bool
    allow_web_search: bool
    allow_deep_research: bool
    allow_visualization: bool
    sql_model_ids: set[uuid.UUID] = field(default_factory=set)
    web_search_defaults: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AgentRuntime:
    supervisor: SupervisorOrchestrator
    planning_constraints: PlanningConstraints
    planning_context: Dict[str, Any] | None


@dataclass(slots=True, frozen=True)
class _UnifiedSqlModelConfig:
    source_model_ids: list[uuid.UUID]
    joins: list[dict[str, Any]]
    metrics: dict[str, Any]


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
    """Builds orchestrator runtime components for worker-side agent execution."""

    def __init__(
        self,
        semantic_model_store: ISemanticModelStore,
        connector_store: IConnectorStore,
        federated_query_tool: Any | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._semantic_model_store = semantic_model_store
        self._connector_store = connector_store
        self._federated_query_tool = federated_query_tool
        self._vector_factory = VectorDBConnectorFactory()
        self._sql_connector_factory = SqlConnectorFactory()

    async def create_runtime(
        self,
        *,
        definition: AgentDefinitionModel,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[IAgentEventEmitter] = None,
    ) -> AgentRuntime:
        tool_config = self._build_agent_tool_config(definition)

        sql_tools, semantic_search_tools = await self._build_analyst_tools(
            tool_config,
            llm_provider,
            embedding_provider,
            event_emitter,
        )

        if tool_config.allow_sql and not sql_tools:
            self._logger.warning(
                "No SQL tools could be created from the selected semantic model ids; disabling analyst route."
            )
            tool_config.allow_sql = False

        planning_constraints = self._build_planning_constraints(tool_config, definition)
        planning_context = await self._build_planner_tool_context(tool_config)
        supervisor = self._build_supervisor_orchestrator(
            definition=definition,
            llm_provider=llm_provider,
            planning_constraints=planning_constraints,
            sql_tools=sql_tools,
            semantic_search_tools=semantic_search_tools,
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

        sql_semantic_model_ids: set[uuid.UUID] = set()
        for tool in sql_tools:
            try:
                sql_semantic_model_ids.add(tool.get_sql_tool_config().definition_id)
            except (ValueError, TypeError) as exc:
                self._logger.warning("Invalid SQL tool config for tool '%s': %s", tool.name, exc)

        return AgentToolConfig(
            allow_sql=bool(sql_tools),
            allow_web_search=bool(web_search_tools),
            allow_deep_research=definition.features.deep_research_enabled or bool(doc_tools),
            allow_visualization=definition.features.visualization_enabled,
            sql_model_ids=sql_semantic_model_ids,
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

    async def _build_planner_tool_context(
        self,
        tool_config: AgentToolConfig,
    ) -> Dict[str, Any] | None:
        semantic_model_entries: list[SemanticModelRecordResponse] = []
        if tool_config.sql_model_ids:
            semantic_model_entries = await self._get_semantic_model_definitions(
                list(tool_config.sql_model_ids)
            )

        available_agents = [
            {
                "agent": "Analyst",
                "description": "Query structured data via semantic models (NL to SQL).",
                "enabled": tool_config.allow_sql,
                "notes": "Uses the semantic_models list.",
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

        semantic_models: Dict[str, Dict[str, str | None]] = {}
        for entry in semantic_model_entries:
            semantic_models[str(entry.id)] = {
                "name": entry.name,
                "description": entry.description,
            }

        context: Dict[str, Any] = {
            "available_agents": available_agents,
            "semantic_models": semantic_models,
        }
        if tool_config.web_search_defaults:
            context.update(tool_config.web_search_defaults)

        return context or None

    async def _build_analyst_tools(
        self,
        agent_tool_config: AgentToolConfig,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[IAgentEventEmitter],
    ) -> tuple[list[SqlAnalystTool], list[SemanticSearchTool]]:
        if not agent_tool_config.allow_sql or not agent_tool_config.sql_model_ids:
            return [], []

        semantic_model_entries = await self._get_semantic_model_definitions(
            list(agent_tool_config.sql_model_ids)
        )

        connector_instances: Dict[uuid.UUID, SqlConnector] = {}
        sql_tools: list[SqlAnalystTool] = []
        semantic_search_tools: list[SemanticSearchTool] = []

        for entry in semantic_model_entries:
            unified_config = self._parse_unified_sql_model_config(entry)
            if unified_config is not None:
                sql_tool = await self._build_unified_sql_tool(
                    semantic_model_entry=entry,
                    unified_config=unified_config,
                    llm_provider=llm_provider,
                    embedding_provider=embedding_provider,
                    event_emitter=event_emitter,
                )
                semantic_search_tools.extend(
                    await self._build_semantic_search_tools(llm_provider, sql_tool.semantic_model)
                )
                sql_tools.append(sql_tool)
                continue

            connector = await self._get_connector(entry.connector_id)
            if connector is None:
                self._logger.warning(
                    "No connector found for semantic model %s (connector_id=%s)",
                    entry.id,
                    entry.connector_id,
                )
                continue

            sql_connector = self._get_or_create_sql_connector(
                connector=connector,
                connector_instances=connector_instances,
            )
            semantic_model: SemanticModel = load_semantic_model(entry.content_yaml)
            base_dialect = semantic_model.dialect
            dialect_str = str((base_dialect or getattr(sql_connector.DIALECT, "name", "postgres"))).lower()
            table_source_map = {
                str(table_key): str(entry.connector_id)
                for table_key in semantic_model.tables.keys()
            }

            semantic_search_tools.extend(
                await self._build_semantic_search_tools(llm_provider, semantic_model)
            )

            sql_tools.append(
                SqlAnalystTool(
                    llm=llm_provider,
                    semantic_model=semantic_model,
                    connector=sql_connector,
                    dialect=dialect_str,
                    priority=0,
                    embedder=embedding_provider,
                    event_emitter=event_emitter,
                    table_source_map=table_source_map,
                )
            )

        return sql_tools, semantic_search_tools

    @staticmethod
    def _parse_connector_config(connector: ConnectorDTO) -> Dict[str, Any]:
        if not connector.config:
            return {}
        if isinstance(connector.config.get("config"), dict):
            return connector.config["config"]
        return connector.config

    async def _get_connector(self, connector_id: uuid.UUID) -> ConnectorDTO | None:
        connectors = await self._get_connectors({connector_id})
        return connectors[0] if connectors else None

    def _get_or_create_sql_connector(
        self,
        *,
        connector: ConnectorDTO,
        connector_instances: Dict[uuid.UUID, SqlConnector],
    ) -> SqlConnector:
        connector_id = connector.id
        if connector_id is None:
            raise BusinessValidationError("Connector ID is required for SQL tool execution.")
        if connector.connector_type is None:
            raise BusinessValidationError(
                f"Connector {connector_id} has no connector_type configured."
            )

        connector_type = ConnectorRuntimeType(connector.connector_type.upper())
        if connector_id not in connector_instances:
            dialect: SqlDialetcs | None = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
            if dialect is None:
                raise BusinessValidationError(
                    f"Connector type {connector_type.value} does not support SQL operations."
                )

            config_factory: Type[BaseConnectorConfigFactory] = get_connector_config_factory(
                connector_type
            )
            connector_config_payload = self._parse_connector_config(connector)
            config_instance = config_factory.create(connector_config_payload)
            connector_instances[connector_id] = self._sql_connector_factory.create_sql_connector(
                dialect,
                config_instance,
                logger=self._logger,
            )

        return connector_instances[connector_id]

    @staticmethod
    def _parse_unified_sql_model_config(
        semantic_model_entry: SemanticModelRecordResponse,
    ) -> _UnifiedSqlModelConfig | None:
        try:
            payload = yaml.safe_load(semantic_model_entry.content_yaml)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None

        source_models_raw = payload.get("source_models") or payload.get("sourceModels")
        if not isinstance(source_models_raw, list):
            if isinstance(payload.get("semantic_models"), list):
                raise BusinessValidationError(
                    "Unified semantic model is missing source_models metadata required for SQL tool federation."
                )
            return None

        source_model_ids: list[uuid.UUID] = []
        seen: set[uuid.UUID] = set()
        for source_model in source_models_raw:
            if not isinstance(source_model, dict):
                continue
            source_id = source_model.get("id")
            if source_id is None:
                continue
            try:
                parsed_id = uuid.UUID(str(source_id))
            except (TypeError, ValueError) as exc:
                raise BusinessValidationError(
                    "Unified semantic model contains an invalid source model id."
                ) from exc
            if parsed_id in seen:
                continue
            seen.add(parsed_id)
            source_model_ids.append(parsed_id)

        if not source_model_ids:
            raise BusinessValidationError(
                "Unified semantic model is missing source model ids."
            )

        joins_raw = payload.get("relationships")
        joins = [dict(join) for join in joins_raw if isinstance(join, dict)] if isinstance(joins_raw, list) else []

        metrics_raw = payload.get("metrics")
        metrics = dict(metrics_raw) if isinstance(metrics_raw, dict) else {}

        return _UnifiedSqlModelConfig(
            source_model_ids=source_model_ids,
            joins=joins,
            metrics=metrics,
        )

    async def _build_unified_sql_tool(
        self,
        *,
        semantic_model_entry: SemanticModelRecordResponse,
        unified_config: _UnifiedSqlModelConfig,
        llm_provider: LLMProvider,
        embedding_provider: Optional[EmbeddingProvider],
        event_emitter: Optional[IAgentEventEmitter],
    ) -> SqlAnalystTool:
        if self._federated_query_tool is None:
            raise BusinessValidationError(
                "Federated query tool is required to execute unified SQL tool queries."
            )

        source_entries = await self._get_semantic_model_definitions(unified_config.source_model_ids)
        source_entry_lookup = {entry.id: entry for entry in source_entries}

        source_models: list[UnifiedSourceModel] = []
        for source_model_id in unified_config.source_model_ids:
            source_entry = source_entry_lookup.get(source_model_id)
            if source_entry is None:
                raise BusinessValidationError(
                    f"Unified source semantic model '{source_model_id}' was not found."
                )
            source_models.append(
                UnifiedSourceModel(
                    model=load_semantic_model(source_entry.content_yaml),
                    connector_id=source_entry.connector_id,
                )
            )

        unified_model, table_connector_map = build_unified_semantic_model(
            source_models=source_models,
            joins=unified_config.joins,
            metrics=unified_config.metrics or None,
            name=semantic_model_entry.name,
            description=semantic_model_entry.description,
            dialect="postgres",
        )

        execution_model = apply_tenant_aware_context(
            unified_model,
            context=TenantAwareQueryContext(
                organization_id=semantic_model_entry.organization_id,
                execution_connector_id=self._build_unified_execution_connector_id(
                    organization_id=semantic_model_entry.organization_id
                ),
            ),
            table_connector_map=table_connector_map,
        )
        if not execution_model.name:
            execution_model.name = semantic_model_entry.name or f"model_{semantic_model_entry.id}"

        workflow = self._build_unified_workflow_payload(
            organization_id=semantic_model_entry.organization_id,
            semantic_model=execution_model,
            source_semantic_model=unified_model,
            table_connector_map=table_connector_map,
            semantic_model_id=semantic_model_entry.id,
        )

        table_source_map = {
            table_key: str(connector_id)
            for table_key, connector_id in table_connector_map.items()
        }
        federated_executor = _FederatedSqlExecutor(
            federated_query_tool=self._federated_query_tool,
            workflow=workflow,
            workspace_id=str(semantic_model_entry.organization_id),
        )
        dialect_str = str(execution_model.dialect or "postgres").lower()

        return SqlAnalystTool(
            llm=llm_provider,
            semantic_model=execution_model,
            connector=None,
            dialect=dialect_str,
            priority=0,
            embedder=embedding_provider,
            event_emitter=event_emitter,
            federated_sql_executor=federated_executor,
            table_source_map=table_source_map,
            prefer_federated_execution=True,
        )

    def _build_unified_workflow_payload(
        self,
        *,
        organization_id: uuid.UUID,
        semantic_model: SemanticModel,
        source_semantic_model: SemanticModel,
        table_connector_map: dict[str, uuid.UUID],
        semantic_model_id: uuid.UUID,
    ) -> FederationWorkflow:
        workspace_id = str(organization_id)
        dataset_id = f"unified_semantic_{organization_id.hex[:12]}_{semantic_model_id.hex[:12]}"

        table_bindings: dict[str, VirtualTableBinding] = {}
        for table_key, table in semantic_model.tables.items():
            source_table = source_semantic_model.tables.get(table_key, table)
            connector_id = table_connector_map.get(table_key)
            if connector_id is None:
                raise BusinessValidationError(
                    f"Missing connector binding for unified table '{table_key}'."
                )
            source_catalog = source_table.catalog
            uses_synthetic_catalog = source_catalog is None and table.catalog is not None

            table_bindings[table_key] = VirtualTableBinding(
                table_key=table_key,
                source_id=f"source_{connector_id.hex[:12]}",
                connector_id=connector_id,
                schema=table.schema,
                table=table.name,
                catalog=table.catalog,
                metadata={
                    "physical_catalog": source_catalog,
                    "physical_schema": source_table.schema,
                    "physical_table": source_table.name,
                    "skip_catalog_in_pushdown": uses_synthetic_catalog,
                },
            )

        relationships = [
            VirtualRelationship(
                name=relationship.name,
                left_table=relationship.from_,
                right_table=relationship.to,
                join_type=relationship.type,
                condition=relationship.join_on,
            )
            for relationship in (semantic_model.relationships or [])
        ]

        return FederationWorkflow(
            id=f"workflow_{dataset_id}",
            workspace_id=workspace_id,
            dataset=VirtualDataset(
                id=dataset_id,
                name="Unified Semantic Dataset",
                workspace_id=workspace_id,
                tables=table_bindings,
                relationships=relationships,
            ),
            broadcast_threshold_bytes=settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
            partition_count=settings.FEDERATION_PARTITION_COUNT,
            max_stage_retries=settings.FEDERATION_STAGE_MAX_RETRIES,
            stage_parallelism=settings.FEDERATION_STAGE_PARALLELISM,
        )

    @staticmethod
    def _build_unified_execution_connector_id(*, organization_id: uuid.UUID) -> uuid.UUID:
        return uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"langbridge-unified-federation:{organization_id}",
        )

    def _build_supervisor_orchestrator(
        self,
        *,
        definition: AgentDefinitionModel,
        llm_provider: LLMProvider,
        planning_constraints: PlanningConstraints,
        sql_tools: list[SqlAnalystTool],
        semantic_search_tools: list[SemanticSearchTool],
        event_emitter: Optional[IAgentEventEmitter],
    ) -> SupervisorOrchestrator:
        analyst_agent = None
        if sql_tools:
            analyst_agent = AnalystAgent(
                llm=llm_provider,
                sql_tools=sql_tools,
                search_tools=semantic_search_tools,
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

    async def _get_semantic_model_definitions(
        self,
        semantic_model_ids: list[uuid.UUID],
    ) -> list[SemanticModelRecordResponse]:
        if not semantic_model_ids:
            return []
        return await self._semantic_model_store.get_by_ids(semantic_model_ids)

    async def _get_connectors(self, connector_ids: set[uuid.UUID]) -> list[ConnectorDTO]:
        if not connector_ids:
            return []
        return await self._connector_store.get_by_ids(list(connector_ids))

    def _get_vector_semantic_searches(self, semantic_model: SemanticModel) -> list[dict[str, Any]]:
        searches: list[dict[str, Any]] = []
        for table_key, table in semantic_model.tables.items():
            for dimension in table.dimensions or []:
                if not dimension.vectorized:
                    continue
                vector_index = dimension.vector_index or {}
                if not vector_index:
                    continue
                self._logger.info("Found vectorized dimension: %s.%s", table_key, dimension.name)
                vector_parameters = {
                    **vector_index,
                    "semantic_name": f"{semantic_model.name or 'semantic_model'}::{table_key}.{dimension.name}",
                }
                searches.append(
                    {
                        "metadata_filters": {},
                        "vector_parameters": vector_parameters,
                    }
                )
        return searches

    async def _build_semantic_search_tools(
        self,
        llm_provider: LLMProvider,
        semantic_model: SemanticModel,
    ) -> list[SemanticSearchTool]:
        tools: list[SemanticSearchTool] = []
        vector_searches = self._get_vector_semantic_searches(semantic_model)

        for vector_search in vector_searches:
            vector_params = vector_search.get("vector_parameters", {})
            if not vector_params:
                continue
            tool = await self._build_semantic_search_tool(
                llm_provider,
                vector_type=VectorDBType.FAISS,
                vector_params=vector_params,
            )
            tools.append(tool)
        return tools

    async def _build_semantic_search_tool(
        self,
        llm_provider: LLMProvider,
        vector_type: VectorDBType,
        vector_params: dict[str, Any],
    ) -> SemanticSearchTool:
        vector_managed_class_ref: Type[ManagedVectorDB] = (
            self._vector_factory.get_managed_vector_db_class_reference(vector_type)
        )
        vector_store: ManagedVectorDB = await vector_managed_class_ref.create_managed_instance(
            kwargs={"index_name": vector_params.get("vector_namespace")},
            logger=self._logger,
        )
        return SemanticSearchTool(
            semantic_name=vector_params.get("semantic_name", "default_search"),
            llm=llm_provider,
            embedding_model=vector_params.get("model"),
            vector_store=vector_store,
            entity_reconignition=True,
        )
