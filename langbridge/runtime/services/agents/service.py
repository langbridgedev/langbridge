"""Runtime agent execution service for the Langbridge AI agent flow."""
import logging
import uuid

from langbridge.ai import AnalystToolBundle, LangbridgeAIFactory
from langbridge.ai.llm import create_provider
from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.events import AgentEventEmitter, AgentEventVisibility
from langbridge.runtime.execution.federated_query_tool import FederatedQueryTool
from langbridge.runtime.models import CreateAgentJobRequest, RuntimeThread
from langbridge.runtime.ports import (
    AgentDefinitionStore,
    ConversationMemoryStore,
    DatasetCatalogStore,
    DatasetColumnStore,
    LLMConnectionStore,
    SemanticModelStore,
    ThreadMessageStore,
    ThreadStore,
)
from langbridge.runtime.services.agents.context import AgentConversationContextBuilder
from langbridge.runtime.services.agents.definitions import AgentExecutionDefinitionResolver
from langbridge.runtime.services.agents.events import AgentRunEventPublisher
from langbridge.runtime.services.agents.memory import AgentConversationMemoryWriter
from langbridge.runtime.services.agents.response import AgentRunResponseBuilder
from langbridge.runtime.services.agents.thread_state import AgentThreadStateManager
from langbridge.runtime.services.agents.tooling import AgentRuntimeToolingBuilder
from langbridge.runtime.services.agents.types import (
    AgentExecutionResult,
    AgentExecutionServiceTooling,
    LLMProviderFactory,
)
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.services.semantic_query_execution_service import SemanticQueryExecutionService
from langbridge.runtime.services.semantic_sql_query_service import SemanticSqlQueryService
from langbridge.runtime.services.semantic_vector_search import SemanticVectorSearchService


class AgentExecutionService:
    """Executes runtime agent jobs through `langbridge.ai`, not old orchestrator."""

    def __init__(
        self,
        *,
        agent_definition_repository: AgentDefinitionStore,
        llm_repository: LLMConnectionStore,
        thread_repository: ThreadStore,
        thread_message_repository: ThreadMessageStore,
        memory_repository: ConversationMemoryStore | None = None,
        tooling: AgentExecutionServiceTooling | None = None,
        semantic_model_store: SemanticModelStore | None = None,
        dataset_repository: DatasetCatalogStore | None = None,
        dataset_column_repository: DatasetColumnStore | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
        semantic_vector_search_service: SemanticVectorSearchService | None = None,
        semantic_query_service: SemanticQueryExecutionService | None = None,
        semantic_sql_service: SemanticSqlQueryService | None = None,
        embedding_provider: EmbeddingProvider | None = None,
        llm_provider_factory: LLMProviderFactory = create_provider,
    ) -> None:
        logger = logging.getLogger(__name__)
        merged_tooling = tooling or AgentExecutionServiceTooling()
        self._llm_provider_factory = llm_provider_factory
        self._definitions = AgentExecutionDefinitionResolver(
            agent_definition_repository=agent_definition_repository,
            llm_repository=llm_repository,
        )
        self._context_builder = AgentConversationContextBuilder(
            memory_repository=memory_repository,
        )
        self._memory_writer = AgentConversationMemoryWriter(
            memory_repository=memory_repository,
        )
        self._thread_state = AgentThreadStateManager(
            thread_repository=thread_repository,
            thread_message_repository=thread_message_repository,
            memory_writer=self._memory_writer,
        )
        self._tooling_builder = AgentRuntimeToolingBuilder(
            tooling=merged_tooling,
            semantic_model_store=semantic_model_store,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            federated_query_tool=federated_query_tool,
            semantic_vector_search_service=semantic_vector_search_service,
            semantic_query_service=semantic_query_service,
            semantic_sql_service=semantic_sql_service,
            embedding_provider=embedding_provider,
            logger=logger,
        )
        self._response_builder = AgentRunResponseBuilder()
        self._events = AgentRunEventPublisher()

    async def execute(
        self,
        *,
        job_id: uuid.UUID,
        request: CreateAgentJobRequest,
        event_emitter: AgentEventEmitter | None = None,
    ) -> AgentExecutionResult:
        thread, user_message, thread_messages = await self._thread_state.get_thread_and_last_user_message(
            request.thread_id
        )
        agent_definition = await self._definitions.get_agent_definition(request.agent_definition_id)
        llm_connection = await self._definitions.get_llm_connection(agent_definition.llm_connection_id)
        user_query = self._context_builder.extract_user_query(user_message)

        await self._events.emit(
            event_emitter,
            event_type="AgentRunStarted",
            message="Agent run started.",
            source="agent-execution",
            details={"job_id": str(job_id), "agent_definition_id": str(agent_definition.id)},
        )

        try:
            llm_provider = self._llm_provider_factory(llm_connection)
            analyst_configs = self._definitions.build_analyst_configs(agent_definition)
            if not analyst_configs:
                raise ExecutionValidationError(
                    f"Agent definition {agent_definition.id} did not resolve to any analyst configs."
                )
            execution = self._definitions.build_execution(agent_definition)
            tooling = await self._tooling_builder.build(
                llm_provider=llm_provider,
                analyst_configs=analyst_configs,
                event_emitter=event_emitter,
            )
            controller = LangbridgeAIFactory(
                llm_provider=llm_provider,
                event_emitter=event_emitter,
            ).create_meta_controller(
                analysts=[
                    AnalystToolBundle(
                        config=config,
                        sql_tools=tooling.sql_analysis_tools.get(config.name)
                        or tooling.sql_analysis_tools.get(config.agent_name)
                        or [],
                        semantic_search_tools=tooling.semantic_search_tools.get(config.name)
                        or tooling.semantic_search_tools.get(config.agent_name)
                        or [],
                        web_search_provider=tooling.web_search_providers.get(config.name)
                        or tooling.web_search_providers.get(config.agent_name),
                    )
                    for config in analyst_configs
                ],
                max_iterations=execution.max_iterations,
                max_replans=execution.max_replans,
                max_step_retries=execution.max_step_retries,
            )
            context = await self._context_builder.build(
                thread=thread,
                messages=thread_messages,
                user_message=user_message,
                agent_definition=agent_definition,
                agent_mode=request.agent_mode,
            )

            ai_run = await controller.handle(
                question=user_query,
                context=context,
            )
            response = self._response_builder.build_response(ai_run)
            continuation_state = self._thread_state.persist_ai_state(
                thread,
                response,
                user_query=user_query,
                ai_run=ai_run,
            )
            self._thread_state.clear_active_run_metadata(thread)

            assistant_message = self._thread_state.record_assistant_message(
                thread=thread,
                user_message=user_message,
                response=response,
                agent_id=agent_definition.id,
                ai_run=ai_run,
                continuation_state=continuation_state,
            )
            await self._thread_state.save_thread(thread)
            await self._memory_writer.write(
                thread=thread,
                user_query=user_query,
                response=response,
                ai_run=ai_run,
            )
            await self._events.emit(
                event_emitter,
                event_type="AgentRunCompleted",
                message=self._response_builder.public_completion_message(response),
                visibility=AgentEventVisibility.public,
                source="agent-execution",
                details={
                    "job_id": str(job_id),
                    "execution_mode": ai_run.execution_mode,
                    "route": ai_run.plan.route,
                    "summary": response.get("summary"),
                    "answer": response.get("answer"),
                    "clarifying_question": self._response_builder.clarifying_question(response),
                },
            )
            return AgentExecutionResult(
                response=response,
                thread=thread,
                user_message=user_message,
                assistant_message=assistant_message,
                agent_definition=agent_definition,
                ai_run=ai_run,
            )
        except Exception as exc:
            await self._events.emit(
                event_emitter,
                event_type="AgentRunFailed",
                message=str(exc),
                visibility=AgentEventVisibility.public,
                source="agent-execution",
                details={"job_id": str(job_id), "error_type": exc.__class__.__name__},
            )
            raise

    async def reset_thread_after_failure(self, *, thread_id: uuid.UUID) -> RuntimeThread | None:
        return await self._thread_state.reset_after_failure(thread_id=thread_id)
