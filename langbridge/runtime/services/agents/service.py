"""Runtime agent execution service for the Langbridge AI agent flow."""
import logging
import uuid

from langbridge.ai import AnalystToolBundle, ExecutionPlan, LangbridgeAIFactory, MetaControllerRun
from langbridge.ai.agents.presentation.contracts import (
    MARKDOWN_ARTIFACT_RESPONSE_VERSION,
    PresentationResponseContract,
)
from langbridge.ai.llm import create_provider
from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.events import AgentEventEmitter, AgentEventVisibility
from langbridge.runtime.execution.federated_query_tool import FederatedQueryTool
from langbridge.runtime.models import CreateAgentJobRequest, RuntimeAgentDefinition, RuntimeThread
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
from langbridge.runtime.services.agents.selection import (
    AgentAutoSelectionAction,
    AgentAutoSelectionDecision,
    AgentAutoSelector,
)
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
        self._auto_selector = AgentAutoSelector()
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
        router_agent_definition = await self._definitions.get_agent_definition(
            request.router_agent_definition_id or request.agent_definition_id
        )
        user_query = self._context_builder.extract_user_query(user_message)

        await self._events.emit(
            event_emitter,
            event_type="AgentRunStarted",
            message="Agent run started.",
            source="agent-execution",
            details={
                "job_id": str(job_id),
                "agent_definition_id": str(router_agent_definition.id),
                "agent_selection": request.agent_selection,
            },
        )

        try:
            agent_definition = router_agent_definition
            selection_payload: dict[str, object] | None = None
            llm_connection = await self._definitions.get_llm_connection(router_agent_definition.llm_connection_id)
            llm_provider = self._llm_provider_factory(llm_connection)
            context = await self._context_builder.build(
                thread=thread,
                messages=thread_messages,
                user_message=user_message,
                agent_definition=router_agent_definition,
                agent_mode=request.agent_mode,
            )

            if request.agent_selection == "auto":
                candidate_ids = list(request.candidate_agent_definition_ids or [])
                if not candidate_ids:
                    candidate_ids = [router_agent_definition.id]
                candidate_agent_definitions = await self._definitions.get_agent_definitions(candidate_ids)
                await self._events.emit(
                    event_emitter,
                    event_type="AgentAutoSelectionStarted",
                    message="Selecting the best available agent.",
                    source="agent-execution",
                    details={
                        "job_id": str(job_id),
                        "candidate_agent_definition_ids": [
                            str(candidate.id)
                            for candidate in candidate_agent_definitions
                        ],
                    },
                )
                decision = await self._auto_selector.select(
                    llm_provider=llm_provider,
                    question=user_query,
                    context=context,
                    candidates=candidate_agent_definitions,
                )
                selection_payload = decision.diagnostic_payload(
                    candidate_count=len(candidate_agent_definitions)
                )
                await self._events.emit(
                    event_emitter,
                    event_type="AgentAutoSelectionCompleted",
                    message=self._auto_selection_event_message(decision),
                    source="agent-execution",
                    details={"job_id": str(job_id), "agent_selection": selection_payload},
                )
                if decision.action != AgentAutoSelectionAction.select:
                    ai_run = self._terminal_auto_selection_run(
                        decision=decision,
                        selection_payload=selection_payload,
                    )
                    response = self._response_builder.build_response(ai_run)
                    self._attach_agent_selection_diagnostics(
                        response=response,
                        selection_payload=selection_payload,
                    )
                    return await self._finalize_execution(
                        job_id=job_id,
                        event_emitter=event_emitter,
                        thread=thread,
                        user_message=user_message,
                        user_query=user_query,
                        response=response,
                        ai_run=ai_run,
                        agent_definition=router_agent_definition,
                    )

                agent_definition = self._selected_agent_definition(
                    decision=decision,
                    candidates=candidate_agent_definitions,
                )
                selection_payload["selected_agent_definition_id"] = str(agent_definition.id)
                if agent_definition.id != router_agent_definition.id:
                    llm_connection = await self._definitions.get_llm_connection(agent_definition.llm_connection_id)
                    llm_provider = self._llm_provider_factory(llm_connection)
                    context = await self._context_builder.build(
                        thread=thread,
                        messages=thread_messages,
                        user_message=user_message,
                        agent_definition=agent_definition,
                        agent_mode=request.agent_mode,
                    )

            analyst_configs = self._definitions.build_analyst_configs(agent_definition)
            if not analyst_configs:
                raise ExecutionValidationError(
                    f"Agent definition {agent_definition.id} did not resolve to any analyst configs."
                )
            orchestration = self._definitions.resolve_orchestration(agent_definition)
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
                max_iterations=orchestration.max_iterations,
                max_replans=orchestration.max_replans,
                max_step_retries=orchestration.max_step_retries,
            )

            ai_run = await controller.handle(
                question=user_query,
                context=context,
            )
            if selection_payload is not None:
                self._attach_agent_selection_run_diagnostics(
                    ai_run=ai_run,
                    selection_payload=selection_payload,
                )
            response = self._response_builder.build_response(ai_run)
            if selection_payload is not None:
                self._attach_agent_selection_diagnostics(
                    response=response,
                    selection_payload=selection_payload,
                )
            return await self._finalize_execution(
                job_id=job_id,
                event_emitter=event_emitter,
                thread=thread,
                user_message=user_message,
                user_query=user_query,
                response=response,
                ai_run=ai_run,
                agent_definition=agent_definition,
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

    async def _finalize_execution(
        self,
        *,
        job_id: uuid.UUID,
        event_emitter: AgentEventEmitter | None,
        thread: RuntimeThread,
        user_message,
        user_query: str,
        response: dict[str, object],
        ai_run: MetaControllerRun,
        agent_definition: RuntimeAgentDefinition,
    ) -> AgentExecutionResult:
        continuation_state = self._thread_state.persist_ai_state(
            thread,
            response,
            user_query=user_query,
            ai_run=ai_run,
        )
        self._thread_state.clear_active_job_metadata(thread)
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
                "answer_markdown": response.get("answer_markdown"),
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

    def _selected_agent_definition(
        self,
        *,
        decision: AgentAutoSelectionDecision,
        candidates: list[RuntimeAgentDefinition],
    ) -> RuntimeAgentDefinition:
        for candidate in candidates:
            if candidate.name == decision.agent_name:
                return candidate
        raise ExecutionValidationError(f"Auto agent selector chose unknown agent '{decision.agent_name}'.")

    def _terminal_auto_selection_run(
        self,
        *,
        decision: AgentAutoSelectionDecision,
        selection_payload: dict[str, object],
    ) -> MetaControllerRun:
        answer_markdown = (
            str(decision.answer_markdown or "").strip()
            or str(decision.clarification_question or "").strip()
            or str(decision.rationale or "").strip()
            or "This request cannot be routed to an available runtime agent."
        )
        diagnostics: dict[str, object] = {
            "mode": "agent_selection",
            "agent_selection": selection_payload,
        }
        if decision.action == AgentAutoSelectionAction.clarify and decision.clarification_question:
            diagnostics["clarifying_question"] = decision.clarification_question
        final_result = PresentationResponseContract.model_validate(
            {
                "answer_markdown": answer_markdown,
                "artifacts": [],
                "diagnostics": diagnostics,
                "metadata": {
                    "contract_version": MARKDOWN_ARTIFACT_RESPONSE_VERSION,
                    "mode": "agent_selection",
                    "agent_selection_action": decision.action.value,
                },
            }
        ).model_dump(mode="json", exclude_none=True)
        return MetaControllerRun(
            execution_mode=None,
            status="clarification_needed"
            if decision.action == AgentAutoSelectionAction.clarify
            else "completed",
            plan=ExecutionPlan(
                route=f"agent_selection:{decision.action.value}",
                steps=[],
                rationale=decision.rationale,
                requires_pev=False,
                clarification_question=decision.clarification_question,
            ),
            final_result=final_result,
            presentation=final_result,
            diagnostics={
                "agent_selection": selection_payload,
                "stop_reason": f"agent_selection_{decision.action.value}",
            },
        )

    def _attach_agent_selection_run_diagnostics(
        self,
        *,
        ai_run: MetaControllerRun,
        selection_payload: dict[str, object],
    ) -> None:
        diagnostics = dict(ai_run.diagnostics or {})
        diagnostics["agent_selection"] = selection_payload
        ai_run.diagnostics = diagnostics

    def _attach_agent_selection_diagnostics(
        self,
        *,
        response: dict[str, object],
        selection_payload: dict[str, object],
    ) -> None:
        diagnostics = response.get("diagnostics")
        if not isinstance(diagnostics, dict):
            diagnostics = {}
        diagnostics["agent_selection"] = selection_payload
        execution = diagnostics.get("execution")
        if isinstance(execution, dict):
            execution["agent_selection"] = selection_payload
        response["diagnostics"] = diagnostics

    @staticmethod
    def _auto_selection_event_message(decision: AgentAutoSelectionDecision) -> str:
        if decision.action == AgentAutoSelectionAction.select and decision.agent_name:
            return f"Selected {decision.agent_name}."
        if decision.action == AgentAutoSelectionAction.clarify:
            return "Clarification needed before selecting an agent."
        if decision.action == AgentAutoSelectionAction.respond:
            return "Answered without running an analyst agent."
        return "Could not route request to an available agent."

    async def reset_thread_after_failure(self, *, thread_id: uuid.UUID) -> RuntimeThread | None:
        return await self._thread_state.reset_after_failure(thread_id=thread_id)
