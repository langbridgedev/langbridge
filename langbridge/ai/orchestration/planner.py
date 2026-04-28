"""Specification-driven planner for Langbridge AI."""
import json
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from langbridge.ai.base import (
    AgentIOContract,
    AgentResult,
    AgentResultStatus,
    AgentRoutingSpec,
    AgentSpecification,
    AgentTask,
    AgentTaskKind,
    BaseAgent,
)
from langbridge.ai.modes import analyst_output_contract_for_task_input, normalize_analyst_task_input
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.orchestration.planner_prompts import build_execution_plan_prompt

if TYPE_CHECKING:  # pragma: no cover
    from langbridge.ai.orchestration.execution import PlanExecutionState


class PlanStep(BaseModel):
    step_id: str
    agent_name: str
    task_kind: AgentTaskKind
    question: str
    input: dict[str, Any] = Field(default_factory=dict)
    expected_output: AgentIOContract = Field(default_factory=AgentIOContract)
    depends_on: list[str] = Field(default_factory=list)


class ExecutionPlan(BaseModel):
    route: str
    steps: list[PlanStep]
    rationale: str
    requires_pev: bool = True
    revision_count: int = 0
    clarification_question: str | None = None


class PlannerAgent(BaseAgent):
    """Builds an execution plan from agent specifications."""

    def __init__(self, *, llm_provider: LLMProvider) -> None:
        self._llm = llm_provider

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="planner",
            description="Builds specification-driven execution plans for the AI gateway.",
            task_kinds=[AgentTaskKind.orchestration],
            capabilities=["plan execution steps", "choose specialist agents", "prepare PEV contracts"],
            constraints=["Does not execute domain work directly."],
            routing=AgentRoutingSpec(keywords=["plan", "execute", "verify"], direct_threshold=99),
            can_execute_direct=False,
            output_contract=AgentIOContract(required_keys=["plan"]),
        )

    async def execute(self, task: AgentTask) -> AgentResult:
        raw_specifications = task.context.get("agent_specifications", [])
        specifications = [
            AgentSpecification.model_validate(item)
            if not isinstance(item, AgentSpecification)
            else item
            for item in raw_specifications
        ]
        plan = await self.build_plan(
            question=task.question,
            context=task.context,
            specifications=specifications,
        )
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"plan": plan.model_dump(mode="json")},
            diagnostics={"step_count": len(plan.steps)},
        )

    async def build_plan(
        self,
        *,
        question: str,
        context: dict[str, Any],
        specifications: list[AgentSpecification],
    ) -> ExecutionPlan:
        return await self._build_plan(
            question=question,
            context=context,
            specifications=specifications,
            revision_count=0,
        )

    async def replan(
        self,
        *,
        state: "PlanExecutionState",
        context_updates: dict[str, Any] | None = None,
        specifications: list[AgentSpecification],
    ) -> ExecutionPlan:
        context = {**state.context, **(context_updates or {})}
        failed_agents = {
            record.step.agent_name
            for record in state.failed_steps
            if record.step.agent_name != "presentation"
        }
        weak_agent = context.get("weak_result_agent")
        if isinstance(weak_agent, str) and weak_agent:
            failed_agents.add(weak_agent)
        failed_agent = context.get("failed_agent")
        if isinstance(failed_agent, str) and failed_agent:
            failed_agents.add(failed_agent)
        context["avoid_agents"] = sorted(failed_agents)
        return await self._build_plan(
            question=state.original_question,
            context=context,
            specifications=specifications,
            revision_count=state.replan_count,
        )

    async def _build_plan(
        self,
        *,
        question: str,
        context: dict[str, Any],
        specifications: list[AgentSpecification],
        revision_count: int,
    ) -> ExecutionPlan:
        runnable_specs, effective_avoid_agents = self._runnable_specifications(
            specifications,
            avoid_agents=context.get("avoid_agents") or [],
        )
        prompt_context = {
            **context,
            "avoid_agents": effective_avoid_agents,
        }
        payload = await self._complete_plan(
            question=question,
            context=prompt_context,
            specifications=runnable_specs,
            revision_count=revision_count,
        )
        steps = self._steps_from_payload(
            payload=payload,
            question=question,
            specifications=runnable_specs,
            revision_count=revision_count,
            requested_agent_mode=context.get("requested_agent_mode") or context.get("agent_mode"),
        )
        return ExecutionPlan(
            route=str(payload.get("route") or "planned"),
            steps=steps,
            rationale=str(payload.get("rationale") or "Planner selected agents from structured specifications."),
            requires_pev=True,
            revision_count=revision_count,
            clarification_question=self._optional_string(payload.get("clarification_question")),
        )

    @staticmethod
    def _runnable_specifications(
        specifications: list[AgentSpecification],
        *,
        avoid_agents: list[str],
    ) -> tuple[list[AgentSpecification], list[str]]:
        runnable = [
            specification
            for specification in specifications
            if AgentTaskKind.orchestration not in specification.task_kinds
            and AgentTaskKind.presentation not in specification.task_kinds
        ]
        avoided = set(avoid_agents)
        filtered = [specification for specification in runnable if specification.name not in avoided]
        if filtered:
            return filtered, sorted(specification.name for specification in runnable if specification.name in avoided)
        return runnable, []

    async def _complete_plan(
        self,
        *,
        question: str,
        context: dict[str, Any],
        specifications: list[AgentSpecification],
        revision_count: int,
    ) -> dict[str, Any]:
        prompt = build_execution_plan_prompt(
            question=question,
            context=context,
            requested_agent_mode=str(context.get("requested_agent_mode") or context.get("agent_mode") or ""),
            specification_payloads=[self._spec_payload(item) for item in specifications],
            revision_count=revision_count,
        )
        return self._parse_json_object(await self._llm.acomplete(prompt, temperature=0.0, max_tokens=900))

    @staticmethod
    def _steps_from_payload(
        *,
        payload: dict[str, Any],
        question: str,
        specifications: list[AgentSpecification],
        revision_count: int,
        requested_agent_mode: Any,
    ) -> list[PlanStep]:
        spec_by_name = {specification.name: specification for specification in specifications}
        raw_steps = payload.get("steps")
        if not isinstance(raw_steps, list):
            raise ValueError("Planner LLM response must include a steps list.")

        prefix = f"r{revision_count}-" if revision_count else ""
        steps: list[PlanStep] = []
        for index, raw_step in enumerate(raw_steps, start=1):
            if not isinstance(raw_step, dict):
                raise ValueError("Planner LLM step entries must be objects.")
            agent_name = str(raw_step.get("agent_name") or "").strip()
            specification = spec_by_name.get(agent_name)
            if specification is None:
                raise ValueError(f"Planner selected unknown agent: {agent_name}")
            task_kind = AgentTaskKind(str(raw_step.get("task_kind") or specification.task_kinds[0].value))
            if not specification.supports(task_kind):
                raise ValueError(f"Planner selected unsupported task kind '{task_kind.value}' for {agent_name}.")
            input_payload: dict[str, Any] = raw_step.get("input") if isinstance(raw_step.get("input"), dict) else {}
            expected_output = specification.output_contract
            if task_kind == AgentTaskKind.analyst:
                input_payload = normalize_analyst_task_input(
                    input_payload,
                    requested_mode=requested_agent_mode,
                )
                if PlannerAgent._uses_mode_aware_analyst_contract(specification):
                    expected_output = analyst_output_contract_for_task_input(
                        input_payload,
                        requested_mode=requested_agent_mode,
                    )
            depends_on = [str(item) for item in raw_step.get("depends_on") or [] if str(item).strip()]
            steps.append(
                PlanStep(
                    step_id=str(raw_step.get("step_id") or f"{prefix}step-{index}"),
                    agent_name=agent_name,
                    task_kind=task_kind,
                    question=str(raw_step.get("question") or question),
                    input=input_payload,
                    expected_output=expected_output,
                    depends_on=depends_on,
                )
            )
        return steps

    @staticmethod
    def _spec_payload(specification: AgentSpecification) -> dict[str, Any]:
        return {
            "name": specification.name,
            "description": specification.description,
            "task_kinds": [item.value for item in specification.task_kinds],
            "capabilities": list(specification.capabilities),
            "constraints": list(specification.constraints),
            "tools": [tool.model_dump(mode="json") for tool in specification.tools],
            "input_contract": specification.input_contract.model_dump(mode="json"),
            "output_contract": specification.output_contract.model_dump(mode="json"),
            "can_execute_direct": specification.can_execute_direct,
            "metadata": dict(specification.metadata or {}),
        }

    @staticmethod
    def _uses_mode_aware_analyst_contract(specification: AgentSpecification) -> bool:
        supported_modes = specification.metadata.get("supported_modes")
        return isinstance(supported_modes, list) and bool(supported_modes)

    @staticmethod
    def _optional_string(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        return text or None

    @staticmethod
    def _parse_json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("Planner LLM response did not contain a JSON object.")
        parsed = json.loads(text[start : end + 1])
        if not isinstance(parsed, dict):
            raise ValueError("Planner LLM response JSON must be an object.")
        return parsed


__all__ = ["ExecutionPlan", "PlannerAgent", "PlanStep"]
