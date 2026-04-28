import logging
from collections.abc import Sequence

from langbridge.ai import AnalystAgentConfig
from langbridge.ai.llm.base import LLMProvider
from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.events import AgentEventEmitter
from langbridge.runtime.execution.federated_query_tool import FederatedQueryTool
from langbridge.runtime.ports import DatasetCatalogStore, DatasetColumnStore, SemanticModelStore
from langbridge.runtime.services.agents.agent_run_tool_factory import RuntimeToolFactory
from langbridge.runtime.services.agents.types import AgentExecutionServiceTooling
from langbridge.runtime.services.semantic_query_execution_service import SemanticQueryExecutionService
from langbridge.runtime.services.semantic_sql_query_service import SemanticSqlQueryService
from langbridge.runtime.services.semantic_vector_search import SemanticVectorSearchService


class AgentRuntimeToolingBuilder:
    def __init__(
        self,
        *,
        tooling: AgentExecutionServiceTooling,
        semantic_model_store: SemanticModelStore | None,
        dataset_repository: DatasetCatalogStore | None,
        dataset_column_repository: DatasetColumnStore | None,
        federated_query_tool: FederatedQueryTool | None,
        semantic_vector_search_service: SemanticVectorSearchService | None,
        semantic_query_service: SemanticQueryExecutionService | None,
        semantic_sql_service: SemanticSqlQueryService | None,
        embedding_provider: EmbeddingProvider | None,
        logger: logging.Logger,
    ) -> None:
        self._tooling = tooling
        self._semantic_model_store = semantic_model_store
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._federated_query_tool = federated_query_tool
        self._semantic_vector_search_service = semantic_vector_search_service
        self._semantic_query_service = semantic_query_service
        self._semantic_sql_service = semantic_sql_service
        self._embedding_provider = embedding_provider
        self._logger = logger

    async def build(
        self,
        *,
        llm_provider: LLMProvider,
        analyst_configs: Sequence[AnalystAgentConfig],
        event_emitter: AgentEventEmitter | None = None,
    ) -> AgentExecutionServiceTooling:
        generated = await RuntimeToolFactory(
            llm_provider=llm_provider,
            analyst_configs=analyst_configs,
            semantic_model_store=self._semantic_model_store,
            dataset_repository=self._dataset_repository,
            dataset_column_repository=self._dataset_column_repository,
            federated_query_tool=self._federated_query_tool,
            semantic_vector_search_service=self._semantic_vector_search_service,
            semantic_query_service=self._semantic_query_service,
            semantic_sql_service=self._semantic_sql_service,
            embedding_provider=self._embedding_provider,
            event_emitter=event_emitter,
            logger=self._logger,
        ).build_tooling()
        return AgentExecutionServiceTooling(
            sql_analysis_tools={
                **dict(generated.sql_analysis_tools),
                **dict(self._tooling.sql_analysis_tools),
            },
            semantic_search_tools={
                **dict(generated.semantic_search_tools),
                **dict(self._tooling.semantic_search_tools),
            },
            web_search_providers={
                **dict(generated.web_search_providers),
                **dict(self._tooling.web_search_providers),
            },
        )
