import re
import uuid
from collections.abc import Mapping
from typing import Any

from langbridge.ai.orchestration.continuation import ContinuationStateBuilder
from langbridge.runtime.models import RuntimeAgentDefinition, RuntimeMessageRole, RuntimeThread, RuntimeThreadMessage
from langbridge.runtime.ports import ConversationMemoryStore
from langbridge.runtime.services.errors import ExecutionValidationError


class AgentConversationContextBuilder:
    def __init__(self, *, memory_repository: ConversationMemoryStore | None = None) -> None:
        self._memory_repository = memory_repository

    async def build(
        self,
        *,
        thread: RuntimeThread,
        messages: list[RuntimeThreadMessage],
        user_message: RuntimeThreadMessage,
        agent_definition: RuntimeAgentDefinition,
        agent_mode: str | None = None,
    ) -> dict[str, Any]:
        context: dict[str, Any] = {
            "thread": {
                "id": str(thread.id),
                "workspace_id": str(thread.workspace_id),
            },
            "agent_definition": {
                "id": str(agent_definition.id),
                "name": agent_definition.name,
            },
            "agent_mode": agent_mode,
            "conversation_context": self.conversation_context(messages),
        }
        normalized_agent_mode = str(agent_mode or "").strip().lower()
        if normalized_agent_mode and normalized_agent_mode != "auto":
            context["requested_agent_mode"] = normalized_agent_mode

        content = user_message.content if isinstance(user_message.content, dict) else {}
        message_context = content.get("context")
        if isinstance(message_context, dict):
            context.update(message_context)
        for key in ("result", "filters", "sources", "mode", "force_web_search", "limit"):
            if key in content:
                context[key] = content[key]
        self.prune_empty_structured_context(context)

        continuation_state = self.resolve_continuation_state(
            thread=thread,
            messages=messages,
            user_message=user_message,
        )
        if continuation_state and self._is_analytical_continuation_state(continuation_state):
            self._merge_continuation_state(context, continuation_state)
        self.prune_empty_structured_context(context)

        memories = await self._memory_items(thread.id, query=self.message_text(user_message))
        if memories:
            context["retrieved_memories"] = memories
            context["memory_context"] = self.memory_context(memories)
        return context

    def _is_analytical_continuation_state(self, continuation_state: Mapping[str, Any]) -> bool:
        if not isinstance(continuation_state, Mapping):
            return False
        if isinstance(continuation_state.get("result"), Mapping):
            return True
        if isinstance(continuation_state.get("visualization"), Mapping):
            return True
        if isinstance(continuation_state.get("research"), Mapping):
            return True
        if isinstance(continuation_state.get("analysis_state"), Mapping):
            return True
        sources = continuation_state.get("sources")
        return isinstance(sources, list) and bool(sources)

    def _merge_continuation_state(
        self,
        context: dict[str, Any],
        continuation_state: dict[str, Any],
    ) -> None:
        context["continuation_state"] = dict(continuation_state)
        context["last_turn_state"] = dict(continuation_state)
        continuation_summary = str(continuation_state.get("summary") or "").strip()
        if continuation_summary:
            context["continuation_summary"] = continuation_summary
        if not isinstance(context.get("analysis_state"), dict):
            prior_analysis_state = continuation_state.get("analysis_state")
            if isinstance(prior_analysis_state, dict):
                context["analysis_state"] = dict(prior_analysis_state)
        if not isinstance(context.get("visualization_state"), dict):
            prior_visualization_state = continuation_state.get("visualization_state")
            if isinstance(prior_visualization_state, dict) and prior_visualization_state:
                context["visualization_state"] = dict(prior_visualization_state)
        if not isinstance(context.get("result"), dict):
            prior_result = continuation_state.get("result")
            if isinstance(prior_result, dict) and prior_result:
                context["result"] = dict(prior_result)
        if not isinstance(context.get("visualization"), dict):
            prior_visualization = continuation_state.get("visualization")
            if isinstance(prior_visualization, dict) and prior_visualization:
                context["visualization"] = dict(prior_visualization)
        if not isinstance(context.get("research"), dict):
            prior_research = continuation_state.get("research")
            if isinstance(prior_research, dict) and prior_research:
                context["research"] = dict(prior_research)
        if not context.get("sources"):
            prior_sources = continuation_state.get("sources")
            if isinstance(prior_sources, list):
                context["sources"] = list(prior_sources)

    async def _memory_items(self, thread_id: uuid.UUID, *, query: str) -> list[dict[str, Any]]:
        if self._memory_repository is None:
            return []
        items = await self._memory_repository.list_for_thread(thread_id, limit=250)
        if not items:
            return []
        ranked = sorted(
            items,
            key=lambda item: self.memory_score(query=query, content=item.content),
            reverse=True,
        )[:8]
        await self._memory_repository.touch_items([item.id for item in ranked if item.id is not None])
        return [
            {
                "id": str(item.id),
                "category": self.role_value(item.category),
                "content": item.content,
                "metadata": dict(item.metadata or {}),
            }
            for item in ranked
        ]

    @classmethod
    def resolve_continuation_state(
        cls,
        *,
        thread: RuntimeThread,
        messages: list[RuntimeThreadMessage],
        user_message: RuntimeThreadMessage,
    ) -> dict[str, Any] | None:
        metadata = dict(thread.metadata or {})
        persisted = ContinuationStateBuilder.coerce(metadata.get("continuation_state"))
        if persisted is not None:
            return persisted.compact_payload()
        return cls.continuation_state_from_messages(messages=messages, user_message=user_message)

    @classmethod
    def continuation_state_from_messages(
        cls,
        *,
        messages: list[RuntimeThreadMessage],
        user_message: RuntimeThreadMessage,
    ) -> dict[str, Any] | None:
        cutoff = len(messages)
        for index, message in enumerate(messages):
            if message.id == user_message.id:
                cutoff = index
                break
        for message in reversed(messages[:cutoff]):
            if message.role != RuntimeMessageRole.assistant:
                continue
            content = message.content if isinstance(message.content, dict) else {}
            persisted = ContinuationStateBuilder.coerce(content.get("continuation_state"))
            if persisted is not None:
                return persisted.compact_payload()
            derived = cls.derive_continuation_state_from_content(content)
            if derived:
                return derived
        return None

    @classmethod
    def derive_continuation_state_from_content(cls, content: Mapping[str, Any]) -> dict[str, Any] | None:
        continuation_state = ContinuationStateBuilder.from_content(content)
        return continuation_state.compact_payload() if continuation_state is not None else None

    @staticmethod
    def prune_empty_structured_context(context: dict[str, Any]) -> None:
        for key in ("result", "visualization", "research", "analysis_state", "visualization_state"):
            value = context.get(key)
            if isinstance(value, dict) and not value:
                context.pop(key, None)

    @staticmethod
    def memory_context(memories: list[dict[str, Any]]) -> str:
        lines = []
        for memory in memories:
            content = str(memory.get("content") or "").strip()
            if not content:
                continue
            category = str(memory.get("category") or "memory").strip()
            lines.append(f"- [{category}] {content}")
        return "\n".join(lines)

    @staticmethod
    def memory_score(*, query: str, content: str) -> int:
        query_tokens = set(re.findall(r"[a-z0-9]+", str(query or "").casefold()))
        content_tokens = set(re.findall(r"[a-z0-9]+", str(content or "").casefold()))
        if not query_tokens or not content_tokens:
            return 0
        return len(query_tokens & content_tokens)

    @classmethod
    def conversation_context(
        cls,
        messages: list[RuntimeThreadMessage],
        *,
        max_messages: int = 12,
    ) -> str:
        lines: list[str] = []
        for message in messages[-max_messages:]:
            text = cls.message_text(message)
            if not text:
                continue
            lines.append(f"{cls.role_value(message.role)}: {text}")
        return "\n".join(lines)

    @staticmethod
    def message_text(message: RuntimeThreadMessage) -> str:
        content = message.content
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, dict):
            for key in ("text", "message", "prompt", "query", "summary", "answer"):
                value = content.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    @classmethod
    def extract_user_query(cls, message: RuntimeThreadMessage) -> str:
        text = cls.message_text(message)
        if text:
            return text
        raise ExecutionValidationError(f"Thread message {message.id} does not contain user text.")

    @staticmethod
    def role_value(role: Any) -> str:
        return str(getattr(role, "value", role))
