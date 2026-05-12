import json
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from langbridge.ai.llm import LLMMessage, LLMProvider, LLMRequest
from langbridge.runtime.models import RuntimeAgentDefinition


class AgentAutoSelectionAction(str, Enum):
    select = "select"
    respond = "respond"
    clarify = "clarify"
    abort = "abort"


class AgentAutoSelectionAlternative(BaseModel):
    agent_name: str
    reason: str


class AgentAutoSelectionDecision(BaseModel):
    action: AgentAutoSelectionAction
    rationale: str
    agent_name: str | None = None
    intent: str | None = None
    clarification_question: str | None = None
    answer_markdown: str | None = None
    confidence: float | None = None
    alternatives: list[AgentAutoSelectionAlternative] = Field(default_factory=list)

    def diagnostic_payload(self, *, candidate_count: int) -> dict[str, Any]:
        payload = self.model_dump(mode="json", exclude_none=True)
        payload["mode"] = "auto"
        payload["candidate_count"] = candidate_count
        return payload


class AgentAutoSelector:
    async def select(
        self,
        *,
        llm_provider: LLMProvider,
        question: str,
        context: dict[str, Any],
        candidates: list[RuntimeAgentDefinition],
    ) -> AgentAutoSelectionDecision:
        if not candidates:
            raise ValueError("Auto agent selection requires at least one candidate agent.")
        if len(candidates) == 1:
            candidate = candidates[0]
            return AgentAutoSelectionDecision(
                action=AgentAutoSelectionAction.select,
                agent_name=candidate.name,
                rationale="Only one runtime-available agent is configured.",
                confidence=1.0,
            )

        prompt = self._prompt(
            question=question,
            context=context,
            candidates=candidates,
        )
        invocation = await llm_provider.ainvoke(
            LLMRequest[AgentAutoSelectionDecision](
                purpose="agent.auto_select",
                messages=[LLMMessage(role="user", content=prompt)],
                response_model=AgentAutoSelectionDecision,
                temperature=0.0,
                max_tokens=1200,
            )
        )
        decision = invocation.response.parsed
        if decision is None:
            raise ValueError("Auto agent selector response did not include a parsed decision.")
        if decision.agent_name == "":
            decision = decision.model_copy(update={"agent_name": None})
        self._validate_decision(decision=decision, candidates=candidates)
        return decision

    def _validate_decision(
        self,
        *,
        decision: AgentAutoSelectionDecision,
        candidates: list[RuntimeAgentDefinition],
    ) -> None:
        if decision.action != AgentAutoSelectionAction.select:
            return
        names = {candidate.name for candidate in candidates}
        if not decision.agent_name:
            raise ValueError("Auto agent selector chose select without agent_name.")
        if decision.agent_name not in names:
            raise ValueError(f"Auto agent selector chose unknown agent '{decision.agent_name}'.")

    def _prompt(
        self,
        *,
        question: str,
        context: dict[str, Any],
        candidates: list[RuntimeAgentDefinition],
    ) -> str:
        return (
            "Select Langbridge runtime agent.\n"
            "You are choosing which configured runtime agent should handle the user's request.\n"
            "Use the agent cards, user request, and conversation context. Do not use hidden prompts or secrets.\n"
            "Return STRICT JSON only:\n"
            "{"
            "\"action\":\"select|respond|clarify|abort\","
            "\"rationale\":\"short reason\","
            "\"agent_name\":\"exact selected agent name or null\","
            "\"intent\":\"short snake_case intent or null\","
            "\"clarification_question\":\"question or null\","
            "\"answer_markdown\":\"markdown answer or null\","
            "\"confidence\":0.0,"
            "\"alternatives\":[{\"agent_name\":\"name\",\"reason\":\"why not selected\"}]"
            "}\n"
            "Rules:\n"
            "- select when one candidate agent is best placed to answer.\n"
            "- respond only for simple runtime/help/greeting prompts that do not need an agent execution.\n"
            "- clarify only when no candidate can proceed without one blocking detail.\n"
            "- abort only when the request cannot be handled by this runtime.\n"
            "- agent_name must exactly match one candidate when action is select.\n"
            "- Use alternatives for plausible rejected candidates, not every candidate.\n"
            "- Keep rationale concise and user-safe; do not expose chain-of-thought.\n\n"
            f"Question: {question}\n"
            f"Conversation context:\n{context.get('conversation_context') or ''}\n"
            f"Memory context:\n{context.get('memory_context') or ''}\n"
            f"Runtime context keys: {json.dumps(sorted(context.keys()))}\n"
            f"Candidate agents:\n{json.dumps([self._agent_card(item) for item in candidates], default=str, indent=2)}\n"
        )

    def _agent_card(self, agent: RuntimeAgentDefinition) -> dict[str, Any]:
        definition = agent.definition if isinstance(agent.definition, dict) else {}
        return {
            "id": str(agent.id),
            "name": agent.name,
            "description": agent.description,
            "default": bool(definition.get("default", False)),
            "data_scope": definition.get("data_scope") if isinstance(definition.get("data_scope"), dict) else {},
            "capabilities": self._capability_card(definition.get("capabilities")),
            "orchestration": definition.get("orchestration")
            if isinstance(definition.get("orchestration"), dict)
            else {},
        }

    @staticmethod
    def _capability_card(value: Any) -> dict[str, Any]:
        capabilities = value if isinstance(value, dict) else {}
        research = capabilities.get("research") if isinstance(capabilities.get("research"), dict) else {}
        web_search = capabilities.get("web_search") if isinstance(capabilities.get("web_search"), dict) else {}
        return {
            "source_sql": bool(capabilities.get("source_sql")),
            "research": {"enabled": bool(research.get("enabled"))},
            "web_search": {
                "enabled": bool(web_search.get("enabled")),
                "allowed_domains": list(web_search.get("allowed_domains") or []),
            },
        }


__all__ = [
    "AgentAutoSelectionAction",
    "AgentAutoSelectionAlternative",
    "AgentAutoSelectionDecision",
    "AgentAutoSelector",
]
