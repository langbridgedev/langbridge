import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from .errors import ExecutionValidationError

from langbridge.runtime.models import (
    CreateAgentJobRequest,
    LLMConnectionSecret,
    RuntimeAgentDefinition,
    RuntimeMessageRole,
    RuntimeThread,
    RuntimeThreadMessage,
    RuntimeThreadState,
)
from langbridge.runtime.embeddings import (
    EmbeddingProvider,
    EmbeddingProviderError,
)
from langbridge.runtime.events import AgentEventEmitter
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
from langbridge.orchestrator.agents.supervisor.memory_manager import MemoryManager
from langbridge.orchestrator.agents.visual import (
    is_placeholder_visualization_title,
    suggest_visualization_title,
)
from langbridge.orchestrator.definitions import (
    AgentDefinitionModel,
)
from langbridge.orchestrator.llm.provider import (
    create_provider,
)
from langbridge.orchestrator.runtime.agent_orchestrator_factory import AgentOrchestratorFactory
from ..execution.federated_query_tool import FederatedQueryTool
from .semantic_vector_search_service import SemanticVectorSearchService


@dataclass(slots=True)
class AgentExecutionResult:
    response: dict[str, Any]
    thread: RuntimeThread
    user_message: RuntimeThreadMessage
    assistant_message: RuntimeThreadMessage
    agent_definition: RuntimeAgentDefinition


class AgentExecutionService:
    def __init__(
        self,
        *,
        agent_definition_repository: AgentDefinitionStore,
        llm_repository: LLMConnectionStore,
        semantic_model_store: SemanticModelStore,
        dataset_repository: DatasetCatalogStore,
        dataset_column_repository: DatasetColumnStore,
        thread_repository: ThreadStore,
        thread_message_repository: ThreadMessageStore,
        memory_repository: ConversationMemoryStore,
        federated_query_tool: FederatedQueryTool,
        semantic_vector_search_service: SemanticVectorSearchService | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._agent_definition_repository = agent_definition_repository
        self._llm_repository = llm_repository
        self._thread_repository = thread_repository
        self._thread_message_repository = thread_message_repository
        self._memory_repository = memory_repository
        self._agent_orchestrator_factory = AgentOrchestratorFactory(
            semantic_model_store=semantic_model_store,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            federated_query_tool=federated_query_tool,
            semantic_vector_search_service=semantic_vector_search_service,
        )

    async def execute(
        self,
        *,
        job_id: uuid.UUID,
        request: CreateAgentJobRequest,
        event_emitter: AgentEventEmitter | None = None,
    ) -> AgentExecutionResult:
        thread, user_message, thread_messages = await self._get_thread_and_last_user_message(
            request.thread_id
        )
        agent_definition, definition_model = await self._get_agent_definition(
            request.agent_definition_id
        )
        llm_connection = await self._get_llm_connection(agent_definition.llm_connection_id)  # type: ignore[arg-type]
        llm_provider = create_provider(llm_connection)
        embedding_provider = self._create_embedding_provider(job_id, llm_connection)

        runtime = await self._agent_orchestrator_factory.create_runtime(
            definition=definition_model,
            llm_provider=llm_provider,
            embedding_provider=embedding_provider,
            event_emitter=event_emitter,
        )

        user_query = self._extract_user_query(user_message)
        memory_manager = MemoryManager(
            repository=self._memory_repository,
            embedding_provider=embedding_provider,
            logger=self._logger,
        )
        memory_context = await memory_manager.retrieve_context(
            thread_id=thread.id,
            query=user_query,
            messages=thread_messages,
            top_k=5,
        )
        planning_context = self._build_planning_context(
            base_context=runtime.planning_context,
            thread=thread,
            memory_context=memory_context,
        )
        response = await runtime.supervisor.handle(
            user_query=user_query,
            planning_constraints=runtime.planning_constraints,
            planning_context=planning_context,
        )
        response = self._ensure_response_defaults(response, user_query=user_query)
        self._persist_supervisor_state(thread, response)
        self._clear_active_run_metadata(thread)

        assistant_message = self._record_assistant_message(
            thread=thread,
            user_message=user_message,
            response=response,
            agent_id=agent_definition.id,
        )
        await self._thread_repository.save(thread)
        await memory_manager.write_back(
            thread_id=thread.id,
            actor_id=thread.created_by,
            user_query=user_query,
            response=response,
        )
        return AgentExecutionResult(
            response=response,
            thread=thread,
            user_message=user_message,
            assistant_message=assistant_message,
            agent_definition=agent_definition,
        )

    async def reset_thread_after_failure(self, *, thread_id: uuid.UUID) -> RuntimeThread | None:
        thread = await self._thread_repository.get_by_id(thread_id)
        if thread is not None:
            self._clear_active_run_metadata(thread)
            self._set_thread_awaiting_user_input(thread)
            thread.updated_at = datetime.now(timezone.utc)
            await self._thread_repository.save(thread)
        return thread

    async def _get_thread_and_last_user_message(
        self,
        thread_id: uuid.UUID,
    ) -> tuple[RuntimeThread, RuntimeThreadMessage, list[RuntimeThreadMessage]]:
        thread = await self._thread_repository.get_by_id(thread_id)
        if thread is None:
            raise ExecutionValidationError(f"Thread with ID {thread_id} does not exist.")

        messages = await self._thread_message_repository.list_for_thread(thread.id)
        if not messages:
            raise ExecutionValidationError(f"Thread {thread.id} has no messages to process.")

        last_message: RuntimeThreadMessage | None = None
        if thread.last_message_id is not None:
            last_message = next((msg for msg in messages if msg.id == thread.last_message_id), None)

        if last_message is None:
            last_message = messages[-1]

        if self._role_value(last_message.role) != RuntimeMessageRole.user.value:
            user_messages = [
                msg for msg in messages if self._role_value(msg.role) == RuntimeMessageRole.user.value
            ]
            if not user_messages:
                raise ExecutionValidationError(f"Thread {thread.id} does not contain a user message.")
            last_message = user_messages[-1]

        return thread, last_message, messages

    @staticmethod
    def _role_value(role: Any) -> str:
        return str(getattr(role, "value", role))

    @staticmethod
    def _set_thread_awaiting_user_input(thread: RuntimeThread) -> None:
        thread.state = RuntimeThreadState.awaiting_user_input

    @staticmethod
    def _build_planning_context(
        *,
        base_context: Optional[dict[str, Any]],
        thread: RuntimeThread,
        memory_context: Any,
    ) -> dict[str, Any]:
        context: dict[str, Any] = dict(base_context or {})

        short_term_context = getattr(memory_context, "short_term_context", "")
        if isinstance(short_term_context, str) and short_term_context.strip():
            context["short_term_context"] = short_term_context

        retrieved_items = getattr(memory_context, "retrieved_items", [])
        if isinstance(retrieved_items, list) and retrieved_items:
            context["retrieved_memories"] = [
                item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                for item in retrieved_items
            ]

        metadata = thread.metadata_json if isinstance(thread.metadata_json, dict) else {}
        clarification_state = metadata.get("clarification_state")
        if AgentExecutionService._has_active_clarification_state(clarification_state):
            context["clarification_state"] = clarification_state

        return context

    @staticmethod
    def _persist_supervisor_state(thread: RuntimeThread, response: dict[str, Any]) -> None:
        diagnostics = response.get("diagnostics")
        metadata = dict(thread.metadata or {})
        if not isinstance(diagnostics, dict):
            metadata.pop("clarification_state", None)
            thread.metadata = metadata
            return

        clarification_state = diagnostics.get("clarification_state")
        clarifying_question = diagnostics.get("clarifying_question")
        if AgentExecutionService._has_active_clarification_state(clarification_state) or (
            isinstance(clarifying_question, str) and clarifying_question.strip()
        ):
            metadata["clarification_state"] = clarification_state
        else:
            metadata.pop("clarification_state", None)
        thread.metadata = metadata

    @staticmethod
    def _clear_active_run_metadata(thread: RuntimeThread) -> None:
        metadata = dict(thread.metadata or {})
        metadata.pop("active_run_id", None)
        metadata.pop("active_run_type", None)
        thread.metadata = metadata

    @staticmethod
    def _has_active_clarification_state(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        pending_slots = payload.get("pending_slots")
        if isinstance(pending_slots, list) and any(
            isinstance(item, str) and item.strip() for item in pending_slots
        ):
            return True
        return False

    async def _get_agent_definition(
        self,
        agent_definition_id: uuid.UUID,
    ) -> tuple[RuntimeAgentDefinition, AgentDefinitionModel]:
        agent_definition = await self._agent_definition_repository.get_by_id(agent_definition_id)
        if agent_definition is None:
            raise ExecutionValidationError(
                f"Agent definition with ID {agent_definition_id} does not exist."
            )
        return agent_definition, AgentDefinitionModel.model_validate(agent_definition.definition)

    async def _get_llm_connection(self, llm_connection_id: uuid.UUID) -> LLMConnectionSecret:
        llm_connection = await self._llm_repository.get_by_id(llm_connection_id)
        if llm_connection is None:
            raise ExecutionValidationError(
                f"LLM connection with ID {llm_connection_id} does not exist."
            )
        return llm_connection

    def _create_embedding_provider(
        self,
        job_id: uuid.UUID,
        llm_connection: LLMConnectionSecret,
    ) -> Optional[EmbeddingProvider]:
        try:
            return EmbeddingProvider.from_llm_connection(llm_connection)
        except EmbeddingProviderError as exc:
            self._logger.warning(
                "request_id=%s embedding provider unavailable; skipping vector search: %s",
                job_id,
                exc,
            )
            return None

    @staticmethod
    def _extract_user_query(message: RuntimeThreadMessage) -> str:
        content = message.content
        if isinstance(content, str):
            text = content.strip()
            if text:
                return text
        if isinstance(content, dict):
            for key in ("text", "message", "prompt", "query"):
                value = content.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        raise ExecutionValidationError(f"Thread message {message.id} does not contain user text.")

    def _record_assistant_message(
        self,
        *,
        thread: RuntimeThread,
        user_message: RuntimeThreadMessage,
        response: dict[str, Any],
        agent_id: uuid.UUID,
    ) -> RuntimeThreadMessage:
        assistant_message_id = uuid.uuid4()
        assistant_message = RuntimeThreadMessage(
            id=assistant_message_id,
            thread_id=thread.id,
            parent_message_id=user_message.id,
            role=RuntimeMessageRole.assistant,
            content={
                "summary": response.get("summary"),
                "result": response.get("result"),
                "visualization": response.get("visualization"),
                "diagnostics": response.get("diagnostics"),
            },
            model_snapshot={"agent_id": str(agent_id)},
            error=response.get("error"),
        )
        self._thread_message_repository.add(assistant_message)
        thread.last_message_id = assistant_message_id
        self._set_thread_awaiting_user_input(thread)
        thread.updated_at = datetime.now(timezone.utc)
        return assistant_message

    @staticmethod
    def _ensure_response_defaults(response: dict[str, Any], *, user_query: str) -> dict[str, Any]:
        payload = dict(response or {})
        payload = AgentExecutionService._ensure_requested_visualization(payload, user_query=user_query)
        if payload.get("summary"):
            return payload

        result = payload.get("result")
        if isinstance(result, dict) and isinstance(result.get("rows"), list):
            row_count = len(result.get("rows", []))
            columns = result.get("columns")
            col_count = len(columns) if isinstance(columns, list) else 0
            payload["summary"] = (
                f"Found {row_count} rows across {col_count} columns for '{user_query}'."
                if row_count > 0
                else "Completed, but no tabular rows were returned."
            )
            return payload

        payload["summary"] = "Completed."
        return payload

    @staticmethod
    def _ensure_requested_visualization(payload: dict[str, Any], *, user_query: str) -> dict[str, Any]:
        requested_chart = AgentExecutionService._detect_requested_chart_type(user_query)
        if not requested_chart:
            return payload

        result = payload.get("result")
        if not isinstance(result, dict):
            return payload
        columns = result.get("columns")
        rows = result.get("rows")
        if not isinstance(columns, list) or not isinstance(rows, list) or not columns or not rows:
            return payload

        visualization = payload.get("visualization")
        existing_type = None
        if isinstance(visualization, dict):
            existing_raw = visualization.get("chart_type") or visualization.get("chartType")
            if isinstance(existing_raw, str):
                existing_type = existing_raw.strip().lower()
        if existing_type == requested_chart:
            return payload

        generated = AgentExecutionService._build_chart_spec(
            chart_type=requested_chart,
            columns=[str(column) for column in columns],
            rows=rows,
            title=f"Visualization for '{user_query}'",
            question=user_query,
        )
        if generated is None:
            return payload

        payload["visualization"] = generated
        summary = payload.get("summary")
        if isinstance(summary, str) and "table visualization" in summary.lower():
            payload["summary"] = summary.replace("table visualization", f"{requested_chart} visualization")
        return payload

    @staticmethod
    def _detect_requested_chart_type(question: str) -> str | None:
        text = str(question or "").lower()
        if not text:
            return None
        if "pie chart" in text or "donut chart" in text or "doughnut chart" in text:
            return "pie"
        if " pie " in f" {text} " or "donut" in text or "doughnut" in text:
            return "pie"
        if "bar chart" in text or "bar graph" in text:
            return "bar"
        if "line chart" in text or "line graph" in text:
            return "line"
        if "scatter plot" in text or "scatter chart" in text:
            return "scatter"
        return None

    @staticmethod
    def _coerce_number(value: Any) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip().replace(",", "")
            cleaned = cleaned.replace("$", "").replace("Â£", "").replace("â‚¬", "")
            if cleaned.endswith("%"):
                cleaned = cleaned[:-1]
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    @staticmethod
    def _build_chart_spec(
        *,
        chart_type: str,
        columns: list[str],
        rows: list[Any],
        title: str,
        question: str | None = None,
    ) -> dict[str, Any] | None:
        sample_rows = rows[: min(20, len(rows))]

        def get_cell(row: Any, index: int) -> Any:
            if isinstance(row, (list, tuple)):
                return row[index] if index < len(row) else None
            if isinstance(row, dict):
                key = columns[index]
                return row.get(key)
            return None

        numeric_indexes: list[int] = []
        for idx in range(len(columns)):
            seen = 0
            numeric = 0
            for row in sample_rows:
                value = get_cell(row, idx)
                if value is None:
                    continue
                seen += 1
                if AgentExecutionService._coerce_number(value) is not None:
                    numeric += 1
            if seen > 0 and numeric / seen >= 0.6:
                numeric_indexes.append(idx)

        if not numeric_indexes:
            return None

        non_numeric_indexes = [idx for idx in range(len(columns)) if idx not in numeric_indexes]
        dimension_idx = non_numeric_indexes[0] if non_numeric_indexes else None
        measure_idx = numeric_indexes[0]

        if chart_type in {"pie", "bar", "line"}:
            if dimension_idx is None:
                return None
            resolved_title = title
            if is_placeholder_visualization_title(resolved_title, question=question):
                resolved_title = suggest_visualization_title(
                    chart_type=chart_type,
                    x=columns[dimension_idx],
                    y=columns[measure_idx],
                )
            return {
                "chart_type": chart_type,
                "x": columns[dimension_idx],
                "y": columns[measure_idx],
                "title": resolved_title,
                "options": {"row_count": len(rows)},
            }

        if chart_type == "scatter":
            if len(numeric_indexes) < 2:
                return None
            resolved_title = title
            if is_placeholder_visualization_title(resolved_title, question=question):
                resolved_title = suggest_visualization_title(
                    chart_type="scatter",
                    x=columns[numeric_indexes[0]],
                    y=columns[numeric_indexes[1]],
                )
            return {
                "chart_type": "scatter",
                "x": columns[numeric_indexes[0]],
                "y": columns[numeric_indexes[1]],
                "title": resolved_title,
                "options": {"row_count": len(rows)},
            }

        return None
