"""LLM-guided meta-controller gateway for the Langbridge AI package."""
import json
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from langbridge.ai.agents.presentation import PresentationAgent
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
from langbridge.ai.events import AIEventEmitter, AIEventSource
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.modes import AnalystAgentMode, analyst_output_contract_for_task_input, normalize_analyst_task_input
from langbridge.ai.orchestration.continuation import (
    ContinuationState,
    ContinuationStateBuilder,
    FollowUpResolver,
)
from langbridge.ai.orchestration.execution import PlanExecutionState
from langbridge.ai.orchestration.final_review import (
    FinalReviewAction,
    FinalReviewAgent,
    FinalReviewDecision,
    FinalReviewReasonCode,
)
from langbridge.ai.orchestration.meta_controller_prompts import build_meta_controller_route_prompt
from langbridge.ai.orchestration.plan_review import (
    PlanReviewAction,
    PlanReviewAgent,
    PlanReviewDecision,
    PlanReviewReasonCode,
)
from langbridge.ai.orchestration.planner import ExecutionPlan, PlannerAgent, PlanStep
from langbridge.ai.orchestration.verification import AgentVerifier, VerificationOutcome, VerificationReasonCode
from langbridge.ai.question_intent import AnalystQuestionIntent
from langbridge.ai.registry import AgentRegistry


class MetaControllerAction(str, Enum):
    direct = "direct"
    plan = "plan"
    clarify = "clarify"
    abort = "abort"


class MetaControllerDecision(BaseModel):
    action: MetaControllerAction
    rationale: str
    agent_name: str | None = None
    task_kind: AgentTaskKind | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    clarification_question: str | None = None
    plan_guidance: str | None = None


class MetaControllerRun(BaseModel):
    execution_mode: str | None = None
    status: str = "completed"
    plan: ExecutionPlan
    step_results: list[dict[str, Any]] = Field(default_factory=list)
    verification: list[VerificationOutcome] = Field(default_factory=list)
    review_decisions: list[PlanReviewDecision] = Field(default_factory=list)
    final_review: dict[str, Any] = Field(default_factory=dict)
    final_result: dict[str, Any] = Field(default_factory=dict)
    presentation: dict[str, Any] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)


class MetaControllerAgent(AIEventSource, BaseAgent):
    """Agent gateway that asks an LLM for route guidance, then executes PEV."""

    def __init__(
        self,
        *,
        registry: AgentRegistry,
        llm_provider: LLMProvider,
        presentation_agent: PresentationAgent,
        planner: PlannerAgent | None = None,
        verifier: AgentVerifier | None = None,
        plan_review: PlanReviewAgent | None = None,
        final_review: FinalReviewAgent | None = None,
        final_review_enabled: bool = True,
        max_iterations: int = 8,
        max_replans: int = 2,
        max_step_retries: int = 1,
        event_emitter: AIEventEmitter | None = None,
    ) -> None:
        super().__init__(event_emitter=event_emitter)
        self._registry = registry
        self._llm = llm_provider
        self._planner = planner or PlannerAgent(llm_provider=llm_provider)
        self._verifier = verifier or AgentVerifier()
        self._plan_review = plan_review or PlanReviewAgent()
        self._final_review = final_review or FinalReviewAgent(llm_provider=llm_provider)
        self._final_review_enabled = bool(final_review_enabled)
        self._presentation_agent = presentation_agent
        self._max_iterations = max(1, int(max_iterations))
        self._max_replans = max(0, int(max_replans))
        self._max_step_retries = max(0, int(max_step_retries))

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="meta-controller",
            description="Gateway agent that reads agent specifications, asks an LLM for routing guidance, and executes PEV.",
            task_kinds=[AgentTaskKind.orchestration],
            capabilities=[
                "read agent specifications",
                "ask LLM for route guidance",
                "ask clarifying questions",
                "invoke planner",
                "execute PEV loop",
            ],
            constraints=["Does not perform domain analysis directly."],
            routing=AgentRoutingSpec(keywords=["route", "plan", "execute"], direct_threshold=99),
            can_execute_direct=False,
            output_contract=AgentIOContract(required_keys=["run"]),
        )

    async def execute(self, task: AgentTask) -> AgentResult:
        run = await self.handle(
            question=task.question,
            context=task.context,
            force_plan=bool(task.input.get("force_plan")),
        )
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"run": run.model_dump(mode="json")},
        )

    async def handle(
        self,
        *,
        question: str,
        context: dict[str, Any] | None = None,
        force_plan: bool = False,
    ) -> MetaControllerRun:
        runtime_context = self._augment_follow_up_context(
            question=question,
            context=dict(context or {}),
        )
        specifications = self._available_specifications()
        await self._emit_ai_event(
            event_type="MetaControllerStarted",
            message="Reading agent specifications.",
            source="meta-controller",
            details={"available_agents": [specification.name for specification in specifications]},
        )

        decision = self._resolve_follow_up_route(
            question=question,
            context=runtime_context,
            specifications=self._registry.specifications(),
            force_plan=force_plan,
        )
        if decision is None:
            decision = await self._route_with_llm(
                question=question,
                context=runtime_context,
                specifications=self._registry.specifications(),
                force_plan=force_plan,
            )
        else:
            await self._emit_ai_event(
                event_type="FollowUpResolutionApplied",
                message=decision.rationale,
                source="meta-controller",
                details={
                    "action": decision.action.value,
                    "agent_name": decision.agent_name,
                    "task_kind": decision.task_kind.value if decision.task_kind else None,
                },
            )
        await self._emit_ai_event(
            event_type="AgentRouteSelected",
            message=decision.rationale,
            source="meta-controller",
            details={
                "action": decision.action.value,
                "agent_name": decision.agent_name,
                "task_kind": decision.task_kind.value if decision.task_kind else None,
            },
        )

        if decision.action == MetaControllerAction.clarify:
            return await self._finish_before_execution(
                execution_mode=None,
                route="clarification",
                question=question,
                context={
                    **runtime_context,
                    "clarification_question": decision.clarification_question or decision.rationale,
                },
                presentation_mode="clarification",
                diagnostics={
                    "route_decision": decision.model_dump(mode="json"),
                    "available_agents": [specification.name for specification in specifications],
                    "stop_reason": "clarification",
                },
                rationale=decision.rationale,
            )

        if decision.action == MetaControllerAction.abort:
            return await self._finish_before_execution(
                execution_mode=None,
                route="abort",
                question=question,
                context={**runtime_context, "error": decision.rationale},
                presentation_mode="failure",
                diagnostics={
                    "route_decision": decision.model_dump(mode="json"),
                    "available_agents": [specification.name for specification in specifications],
                    "stop_reason": "abort",
                },
                rationale=decision.rationale,
            )

        if decision.action == MetaControllerAction.direct:
            target = self._resolve_direct_target(decision)
            input_payload = decision.input
            if (decision.task_kind or target.task_kinds[0]) == AgentTaskKind.analyst:
                input_payload = normalize_analyst_task_input(
                    input_payload,
                    requested_mode=runtime_context.get("requested_agent_mode") or runtime_context.get("agent_mode"),
                )
            execution_question = self._resolved_execution_question(
                question=question,
                context=runtime_context,
                decision=decision,
            )
            plan = self._build_direct_plan(
                question=execution_question,
                specification=target,
                task_kind=decision.task_kind,
                input_payload=input_payload,
                rationale=decision.rationale,
            )
            return await self._execute_plan(
                execution_mode="direct",
                plan=plan,
                question=question,
                context=runtime_context,
                diagnostics={
                    "selected_agent": target.name,
                    "route_decision": decision.model_dump(mode="json"),
                    "available_agents": [specification.name for specification in specifications],
                },
            )

        if decision.action != MetaControllerAction.plan:
            raise ValueError(f"Meta-controller LLM selected unsupported action: {decision.action}")

        plan_context = {
            **runtime_context,
            "plan_guidance": decision.plan_guidance or decision.rationale,
            "route_decision": decision.model_dump(mode="json"),
        }
        await self._emit_ai_event(
            event_type="PlannerStarted",
            message="Building execution plan.",
            source="planner",
        )
        plan = await self._planner.build_plan(
            question=question,
            context=plan_context,
            specifications=self._registry.specifications(),
        )
        await self._emit_ai_event(
            event_type="PlanCreated",
            message=f"Created plan with {len(plan.steps)} step(s).",
            source="planner",
            details={"route": plan.route, "step_count": len(plan.steps)},
        )
        return await self._execute_plan(
            execution_mode="planned",
            plan=plan,
            question=question,
            context=plan_context,
            diagnostics={
                "planner": self._planner.specification.name,
                "route_decision": decision.model_dump(mode="json"),
                "available_agents": [specification.name for specification in specifications],
            },
        )

    async def _route_with_llm(
        self,
        *,
        question: str,
        context: dict[str, Any],
        specifications: list[AgentSpecification],
        force_plan: bool,
    ) -> MetaControllerDecision:
        await self._emit_ai_event(
            event_type="AgentRoutingStarted",
            message="Asking for route guidance.",
            source="meta-controller",
        )
        prompt = build_meta_controller_route_prompt(
            question=question,
            context=context,
            force_plan=force_plan,
            requested_agent_mode=str(context.get("requested_agent_mode") or context.get("agent_mode") or ""),
            specification_payloads=[self._spec_payload(item) for item in specifications],
        )
        raw = await self._llm.acomplete(prompt, temperature=0.0, max_tokens=700)
        parsed = self._parse_json_object(raw)
        for key in ("agent_name", "task_kind", "clarification_question", "plan_guidance"):
            if parsed.get(key) in ("", None):
                parsed[key] = None
        decision = MetaControllerDecision.model_validate(parsed)
        decision = self._normalize_route_decision(
            decision,
            question=question,
            context=context,
            specifications=specifications,
            force_plan=force_plan,
        )
        if force_plan and decision.action == MetaControllerAction.direct:
            raise ValueError("Meta-controller LLM selected direct route despite force_plan.")
        return decision

    def _available_specifications(self) -> list[AgentSpecification]:
        return [
            self.specification,
            self._planner.specification,
            self._plan_review.specification,
            self._presentation_agent.specification,
            *self._registry.specifications(),
        ]

    def _resolve_direct_target(self, decision: MetaControllerDecision) -> AgentSpecification:
        if not decision.agent_name:
            raise ValueError("Meta-controller direct route requires agent_name.")
        specification = self._registry.get(decision.agent_name).specification
        if decision.task_kind is not None and not specification.supports(decision.task_kind):
            raise ValueError(
                f"Meta-controller selected unsupported task kind '{decision.task_kind.value}' for {specification.name}."
            )
        return specification

    def _normalize_route_decision(
        self,
        decision: MetaControllerDecision,
        *,
        question: str,
        context: dict[str, Any],
        specifications: list[AgentSpecification],
        force_plan: bool = False,
    ) -> MetaControllerDecision:
        if decision.action == MetaControllerAction.clarify and not force_plan:
            redirected = self._redirect_premature_analyst_clarification(
                decision=decision,
                question=question,
                context=context,
                specifications=specifications,
            )
            if redirected is not None:
                decision = redirected
        if decision.action != MetaControllerAction.direct:
            return decision
        if decision.task_kind != AgentTaskKind.analyst:
            return decision
        requested_mode = context.get("requested_agent_mode") or context.get("agent_mode")
        requested_mode_text = str(requested_mode or "").strip().lower()
        normalized_input = normalize_analyst_task_input(
            decision.input,
            requested_mode=requested_mode,
        )
        follow_up_input = MetaControllerAgent._follow_up_analyst_input(
            context=context,
            requested_mode=requested_mode,
        )
        if (
            normalized_input.get("agent_mode") == AnalystAgentMode.context_analysis.value
            and requested_mode_text != AnalystAgentMode.context_analysis.value
            and "agent_mode" not in follow_up_input
        ):
            normalized_input.pop("agent_mode", None)
        merged_input = {**follow_up_input, **normalized_input}
        if (
            requested_mode_text in {"", AnalystAgentMode.auto.value}
            and "agent_mode" not in merged_input
            and "agent_mode" not in follow_up_input
            and AnalystQuestionIntent.is_assumption_first_question(question)
            and self._agent_supports_analyst_mode(
                agent_name=decision.agent_name,
                specifications=specifications,
                mode=AnalystAgentMode.research,
            )
        ):
            merged_input["agent_mode"] = AnalystAgentMode.research.value
        if (
            decision.task_kind == AgentTaskKind.analyst
            and requested_mode_text in {"", AnalystAgentMode.auto.value}
            and "agent_mode" not in merged_input
        ):
            merged_input["agent_mode"] = AnalystAgentMode.auto.value
        return decision.model_copy(
            update={
                "input": merged_input
            }
        )

    def _redirect_premature_analyst_clarification(
        self,
        *,
        decision: MetaControllerDecision,
        question: str,
        context: dict[str, Any],
        specifications: list[AgentSpecification],
    ) -> MetaControllerDecision | None:
        if not self._looks_like_assumption_first_analyst_question(question):
            return None
        analyst_spec = self._single_available_analyst_spec(specifications)
        if analyst_spec is None:
            return None
        supported_modes = (
            analyst_spec.metadata.get("supported_modes")
            if isinstance(analyst_spec.metadata, dict)
            else None
        )
        input_payload: dict[str, Any] = {}
        requested_mode = str(context.get("requested_agent_mode") or context.get("agent_mode") or "").strip().lower()
        if requested_mode and requested_mode != AnalystAgentMode.auto.value:
            input_payload["agent_mode"] = requested_mode
        elif isinstance(supported_modes, list) and AnalystAgentMode.research.value in supported_modes:
            input_payload["agent_mode"] = AnalystAgentMode.research.value
        else:
            input_payload["agent_mode"] = AnalystAgentMode.auto.value
        return MetaControllerDecision(
            action=MetaControllerAction.direct,
            rationale=(
                "One analyst can inspect governed data first and answer with explicit assumptions instead of "
                f"asking an upfront clarification. Original clarification: {decision.rationale}"
            ),
            agent_name=analyst_spec.name,
            task_kind=AgentTaskKind.analyst,
            input=input_payload,
            clarification_question=None,
            plan_guidance=None,
        )

    @staticmethod
    def _single_available_analyst_spec(
        specifications: list[AgentSpecification],
    ) -> AgentSpecification | None:
        analysts = [specification for specification in specifications if specification.supports(AgentTaskKind.analyst)]
        if len(analysts) != 1:
            return None
        return analysts[0]

    @staticmethod
    def _looks_like_assumption_first_analyst_question(question: str) -> bool:
        return AnalystQuestionIntent.is_assumption_first_question(question)

    @staticmethod
    def _agent_supports_analyst_mode(
        *,
        agent_name: str | None,
        specifications: list[AgentSpecification],
        mode: AnalystAgentMode,
    ) -> bool:
        if not agent_name:
            return False
        for specification in specifications:
            if specification.name != agent_name:
                continue
            supported_modes = (
                specification.metadata.get("supported_modes")
                if isinstance(specification.metadata, dict)
                else None
            )
            return isinstance(supported_modes, list) and mode.value in supported_modes
        return False

    @classmethod
    def _augment_follow_up_context(
        cls,
        *,
        question: str,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        augmented = dict(context)
        continuation_state = ContinuationStateBuilder.from_context(context=augmented, question=question)
        if continuation_state is not None:
            augmented["continuation_state"] = continuation_state.compact_payload()
            if not isinstance(augmented.get("analysis_state"), dict) and continuation_state.analysis_state is not None:
                augmented["analysis_state"] = continuation_state.analysis_state.model_dump(mode="json")
            if (
                not isinstance(augmented.get("visualization_state"), dict)
                and continuation_state.visualization_state is not None
            ):
                augmented["visualization_state"] = continuation_state.visualization_state.model_dump(mode="json")
            if not isinstance(augmented.get("result"), dict) and isinstance(continuation_state.result, dict):
                augmented["result"] = dict(continuation_state.result)
            if not isinstance(augmented.get("visualization"), dict) and isinstance(continuation_state.visualization, dict):
                augmented["visualization"] = dict(continuation_state.visualization)
            if not isinstance(augmented.get("research"), dict) and isinstance(continuation_state.research, dict):
                augmented["research"] = dict(continuation_state.research)
            if not augmented.get("sources") and continuation_state.sources:
                augmented["sources"] = list(continuation_state.sources)
        resolution = cls._resolve_follow_up_resolution(
            question=question,
            continuation_state=continuation_state,
        )
        if resolution:
            resolution_payload = resolution.model_dump(mode="json", exclude_none=True)
            if resolution.filters:
                resolution_payload["filter"] = resolution.filters[0].single_value_payload()
            augmented["follow_up_resolution"] = resolution_payload
        return augmented

    @classmethod
    def _resolve_follow_up_resolution(
        cls,
        *,
        question: str,
        continuation_state: ContinuationState | None,
    ):
        return FollowUpResolver.resolve(
            question=question,
            continuation_state=continuation_state,
        )

    def _resolve_follow_up_route(
        self,
        *,
        question: str,
        context: dict[str, Any],
        specifications: list[AgentSpecification],
        force_plan: bool,
    ) -> MetaControllerDecision | None:
        if force_plan:
            return None
        requested_mode = str(context.get("requested_agent_mode") or context.get("agent_mode") or "").strip().lower()
        resolution = context.get("follow_up_resolution")
        if not isinstance(resolution, dict):
            return None
        resolution_kind = str(resolution.get("kind") or "").strip().lower()
        if resolution_kind == "clarify_follow_up":
            clarification_question = str(
                resolution.get("clarification_question") or resolution.get("rationale") or ""
            ).strip()
            if not clarification_question:
                return None
            return MetaControllerDecision(
                action=MetaControllerAction.clarify,
                rationale=clarification_question,
                clarification_question=clarification_question,
            )
        suggested_mode = str(resolution.get("suggested_agent_mode") or "").strip().lower()
        allowed_requested_modes = {
            "visualize_prior_result": {"", "auto", "context_analysis"},
            "analyze_prior_result": {"", "auto", "context_analysis"},
            "requery_prior_analysis": {"", "auto", "sql"},
        }.get(resolution_kind)
        if not allowed_requested_modes or requested_mode not in allowed_requested_modes:
            return None
        target = self._resolve_follow_up_target(resolution=resolution, specifications=specifications)
        if target is None:
            return None
        return MetaControllerDecision(
            action=MetaControllerAction.direct,
            rationale=str(
                resolution.get("rationale")
                or "Reuse the prior verified result for the requested chart follow-up."
            ),
            agent_name=target.name,
            task_kind=AgentTaskKind.analyst,
            input={
                "agent_mode": suggested_mode or "context_analysis",
                "reuse_last_result": bool(resolution.get("reuse_last_result")),
                "follow_up_intent": resolution_kind or "visualize_prior_result",
                "chart_request": question if resolution_kind == "visualize_prior_result" else None,
                "resolved_from_prior_question": bool(resolution.get("resolved_question")),
                **(
                    {"follow_up_chart_type": resolution.get("chart_type")}
                    if resolution.get("chart_type")
                    else {}
                ),
                **(
                    {"follow_up_focus_field": resolution.get("focus_field")}
                    if resolution.get("focus_field")
                    else {}
                ),
                **(
                    {"follow_up_dimension": resolution.get("dimension")}
                    if resolution.get("dimension")
                    else {}
                ),
                **(
                    {"follow_up_period": resolution.get("period")}
                    if resolution.get("period")
                    else {}
                ),
                **(
                    {"follow_up_filter": resolution.get("filter")}
                    if resolution.get("filter")
                    else {}
                ),
                **(
                    {"follow_up_filters": resolution.get("filters")}
                    if resolution.get("filters")
                    else {}
                ),
                **(
                    {"active_filters": resolution.get("active_filters")}
                    if resolution.get("active_filters")
                    else {}
                ),
            },
        )

    @staticmethod
    def _resolve_follow_up_target(
        *,
        resolution: dict[str, Any],
        specifications: list[AgentSpecification],
    ) -> AgentSpecification | None:
        preferred_name = str(resolution.get("selected_agent") or "").strip()
        analyst_specs = [
            specification
            for specification in specifications
            if specification.supports(AgentTaskKind.analyst)
        ]
        if preferred_name:
            for specification in analyst_specs:
                if specification.name == preferred_name:
                    return specification
        if len(analyst_specs) == 1:
            return analyst_specs[0]
        return None

    @classmethod
    def _follow_up_analyst_input(
        cls,
        *,
        context: dict[str, Any],
        requested_mode: Any,
    ) -> dict[str, Any]:
        resolution = context.get("follow_up_resolution")
        if not isinstance(resolution, dict):
            return {}
        resolution_kind = str(resolution.get("kind") or "").strip().lower()
        suggested_mode = str(resolution.get("suggested_agent_mode") or "").strip().lower()
        if resolution_kind not in {"visualize_prior_result", "analyze_prior_result", "requery_prior_analysis"}:
            return {}
        normalized_requested = str(requested_mode or "").strip().lower()
        payload: dict[str, Any] = {
            "reuse_last_result": bool(resolution.get("reuse_last_result")),
            "follow_up_intent": resolution_kind,
        }
        if resolution.get("chart_type"):
            payload["follow_up_chart_type"] = resolution.get("chart_type")
        if resolution.get("focus_field"):
            payload["follow_up_focus_field"] = resolution.get("focus_field")
        if resolution.get("dimension"):
            payload["follow_up_dimension"] = resolution.get("dimension")
        if resolution.get("period"):
            payload["follow_up_period"] = resolution.get("period")
        if resolution.get("filter"):
            payload["follow_up_filter"] = resolution.get("filter")
        if resolution.get("filters"):
            payload["follow_up_filters"] = resolution.get("filters")
        if resolution.get("active_filters"):
            payload["active_filters"] = resolution.get("active_filters")
        if normalized_requested in {"", "auto", suggested_mode} and suggested_mode:
            payload["agent_mode"] = suggested_mode
        return payload

    @staticmethod
    def _resolved_execution_question(
        *,
        question: str,
        context: dict[str, Any],
        decision: MetaControllerDecision,
    ) -> str:
        if decision.action != MetaControllerAction.direct:
            return question
        resolution = context.get("follow_up_resolution")
        if not isinstance(resolution, dict):
            return question
        resolved_question = str(resolution.get("resolved_question") or "").strip()
        return resolved_question or question

    @staticmethod
    def _build_direct_plan(
        *,
        question: str,
        specification: AgentSpecification,
        task_kind: AgentTaskKind | None = None,
        input_payload: dict[str, Any] | None = None,
        rationale: str | None = None,
    ) -> ExecutionPlan:
        resolved_task_kind = task_kind or specification.task_kinds[0]
        expected_output = specification.output_contract
        if (
            resolved_task_kind == AgentTaskKind.analyst
            and MetaControllerAgent._uses_mode_aware_analyst_contract(specification)
        ):
            expected_output = analyst_output_contract_for_task_input(input_payload or {})
        return ExecutionPlan(
            route=f"direct:{specification.name}",
            steps=[
                PlanStep(
                    step_id="step-1",
                    agent_name=specification.name,
                    task_kind=resolved_task_kind,
                    question=question,
                    input=input_payload or {},
                    expected_output=expected_output,
                )
            ],
            rationale=rationale or "Meta-controller LLM selected one direct agent.",
            requires_pev=True,
        )

    @staticmethod
    def _build_terminal_plan(*, route: str, rationale: str) -> ExecutionPlan:
        return ExecutionPlan(route=route, steps=[], rationale=rationale, requires_pev=False)

    @staticmethod
    def _build_revision_plan(
        *,
        step: PlanStep,
        revision_count: int,
        rationale: str,
    ) -> ExecutionPlan:
        revised_step = step.model_copy(
            update={"step_id": f"r{revision_count}-answer-revision"}
        )
        return ExecutionPlan(
            route="planned:answer_revision",
            steps=[revised_step],
            rationale=rationale or "Final review requested a revised answer.",
            requires_pev=True,
            revision_count=revision_count,
        )

    async def _finish_before_execution(
        self,
        *,
        execution_mode: str | None,
        route: str,
        question: str,
        context: dict[str, Any],
        presentation_mode: str,
        diagnostics: dict[str, Any],
        rationale: str,
    ) -> MetaControllerRun:
        final = await self._present(question=question, context=context, mode=presentation_mode)
        plan = self._build_terminal_plan(route=route, rationale=rationale)
        return MetaControllerRun(
            execution_mode=execution_mode,
            status=self._status_for_presentation_mode(presentation_mode),
            plan=plan,
            final_result=final,
            presentation=final,
            diagnostics=diagnostics,
        )

    async def _execute_plan(
        self,
        *,
        execution_mode: str,
        plan: ExecutionPlan | None = None,
        state: PlanExecutionState | None = None,
        question: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> MetaControllerRun:
        active_plan = state.current_plan if state is not None else plan
        if active_plan is None:
            raise ValueError("Execution requires a plan or existing plan state.")

        if not active_plan.steps:
            if active_plan.clarification_question:
                final = await self._present(
                    question=question,
                    context={**context, "clarification_question": active_plan.clarification_question},
                    mode="clarification",
                )
                return MetaControllerRun(
                    execution_mode=self._execution_mode_from_plan(plan=active_plan, requested_mode=execution_mode),
                    status="clarification_needed",
                    plan=active_plan,
                    final_result=final,
                    presentation=final,
                    diagnostics={
                        **diagnostics,
                        "stop_reason": "clarification",
                        "clarification_source": "planner",
                    },
                )
            await self._emit_ai_event(
                event_type="PlanFailed",
                message="Planner returned no executable steps.",
                source="meta-controller",
            )
            failed = VerificationOutcome(
                passed=False,
                step_id="plan",
                agent_name="planner",
                message="Planner returned no executable steps.",
                reason_code=VerificationReasonCode.planner_no_steps,
            )
            final = await self._present(
                question=question,
                context={**context, "error": failed.message},
                mode="failure",
            )
            return MetaControllerRun(
                execution_mode=self._execution_mode_from_plan(plan=active_plan, requested_mode=execution_mode),
                status="failed",
                plan=active_plan,
                verification=[failed],
                final_result=final,
                presentation=final,
                diagnostics=diagnostics,
            )

        if state is None:
            state = PlanExecutionState(
                original_question=question,
                current_plan=active_plan,
                context=dict(context),
                max_iterations=self._max_iterations,
                max_replans=self._max_replans,
                max_step_retries=self._max_step_retries,
            )
        else:
            state.current_plan = active_plan

        while state.iteration < state.max_iterations:
            step = state.next_pending_step()
            if step is None:
                if state.has_pending_steps():
                    blocked_steps = [item.step_id for item in state.pending_steps]
                    return await self._finish_with_mode(
                        execution_mode=execution_mode,
                        state=state,
                        question=question,
                        context={
                            "error": (
                                "Execution plan has pending steps blocked by unresolved dependencies: "
                                + ", ".join(blocked_steps)
                            )
                        },
                        presentation_mode="failure",
                        diagnostics={
                            **diagnostics,
                            "stop_reason": "blocked_dependencies",
                            "blocked_step_ids": blocked_steps,
                        },
                    )
                return await self._finalize(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context={},
                    diagnostics={**diagnostics, "stop_reason": "plan_completed"},
                )

            agent = self._registry.get(step.agent_name)
            await self._emit_ai_event(
                event_type="PlanStepStarted",
                message=f"Running {step.agent_name}.",
                source=step.agent_name,
                details={
                    "step_id": step.step_id,
                    "agent_name": step.agent_name,
                    "task_kind": step.task_kind.value,
                },
            )
            task = AgentTask(
                task_id=step.step_id,
                task_kind=step.task_kind,
                question=step.question or question,
                input=step.input,
                context={**state.context, "step_results": state.step_results_payload()},
                expected_output=step.expected_output,
            )
            result = await agent.execute(task)
            await self._emit_ai_event(
                event_type="PlanStepCompleted",
                message=f"{step.agent_name} returned {result.status.value}.",
                source=step.agent_name,
                details={
                    "step_id": step.step_id,
                    "agent_name": step.agent_name,
                    "status": result.status.value,
                },
            )
            verification = self._verifier.verify(step=step, result=result)
            await self._emit_ai_event(
                event_type="VerificationCompleted",
                message=verification.message,
                source="verifier",
                details={
                    "step_id": verification.step_id,
                    "agent_name": verification.agent_name,
                    "passed": verification.passed,
                    "reason_code": verification.reason_code.value,
                    "missing_output_keys": list(verification.missing_output_keys),
                },
            )
            state.record(step=step, result=result, verification=verification)

            decision = self._plan_review.review(state)
            state.record_review(decision)
            if decision.updated_context:
                state.context = {**state.context, **decision.updated_context}
            await self._emit_ai_event(
                event_type="PlanReviewDecision",
                message=decision.rationale,
                source="plan-review",
                details={
                    "action": decision.action.value,
                    "reason_code": decision.reason_code.value,
                    "retry_step_id": decision.retry_step_id,
                },
            )

            if decision.action == PlanReviewAction.continue_plan:
                if state.iteration >= state.max_iterations:
                    return await self._finish_iteration_exhausted(
                        execution_mode=execution_mode,
                        state=state,
                        question=question,
                        context={},
                        diagnostics=diagnostics,
                        decision=decision,
                    )
                continue
            if decision.action == PlanReviewAction.retry_step:
                if state.iteration >= state.max_iterations:
                    return await self._finish_iteration_exhausted(
                        execution_mode=execution_mode,
                        state=state,
                        question=question,
                        context={},
                        diagnostics=diagnostics,
                        decision=decision,
                    )
                state.increment_retry(decision.retry_step_id or step.step_id)
                await self._emit_ai_event(
                    event_type="PlanRetryScheduled",
                    message=f"Retrying {decision.retry_step_id or step.step_id}.",
                    source="plan-review",
                    details={"step_id": decision.retry_step_id or step.step_id},
                )
                continue
            if decision.action == PlanReviewAction.revise_plan:
                if state.iteration >= state.max_iterations:
                    return await self._finish_iteration_exhausted(
                        execution_mode=execution_mode,
                        state=state,
                        question=question,
                        context={},
                        diagnostics=diagnostics,
                        decision=decision,
                    )
                state.replan_count += 1
                await self._emit_ai_event(
                    event_type="PlanReplanStarted",
                    message="Revising execution plan.",
                    source="planner",
                    details={"replan_count": state.replan_count},
                )
                state.current_plan = await self._planner.replan(
                    state=state,
                    context_updates=decision.updated_context,
                    specifications=self._registry.specifications(),
                )
                await self._emit_ai_event(
                    event_type="PlanReplanCreated",
                    message=f"Revised plan has {len(state.current_plan.steps)} step(s).",
                    source="planner",
                    details={"step_count": len(state.current_plan.steps)},
                )
                continue
            if decision.action == PlanReviewAction.ask_clarification:
                return await self._finish_with_mode(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context={"clarification_question": decision.clarification_question or decision.rationale},
                    presentation_mode="clarification",
                    diagnostics={**diagnostics, "stop_reason": "clarification"},
                )
            if decision.action == PlanReviewAction.abort:
                return await self._finish_with_mode(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context={"error": decision.rationale},
                    presentation_mode="failure",
                    diagnostics={**diagnostics, "stop_reason": "abort"},
                )
            if decision.action == PlanReviewAction.finalize:
                return await self._finalize(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context={},
                    diagnostics={**diagnostics, "stop_reason": "finalize"},
                )

        return await self._finish_iteration_exhausted(
            execution_mode=execution_mode,
            state=state,
            question=question,
            context={},
            diagnostics=diagnostics,
            decision=None,
        )

    async def _finalize(
        self,
        *,
        execution_mode: str,
        state: PlanExecutionState,
        question: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> MetaControllerRun:
        answer_package = self._build_answer_package(state=state, context=context)
        final_review_decision = await self._review_final_answer(
            question=question,
            state=state,
            answer_package=answer_package,
        )
        if final_review_decision is not None:
            if final_review_decision.updated_context:
                state.context = {**state.context, **final_review_decision.updated_context}
            final_review_payload = final_review_decision.model_dump(mode="json")

            if final_review_decision.action == FinalReviewAction.ask_clarification:
                clarification_question = (
                    final_review_decision.clarification_question
                    or final_review_decision.rationale
                    or "Clarification needed."
                )
                return await self._finish_with_mode(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context={**context, "clarification_question": clarification_question},
                    presentation_mode="clarification",
                    diagnostics={
                        **diagnostics,
                        "stop_reason": "final_review_clarification",
                        "final_review": final_review_payload,
                    },
                    final_review=final_review_decision,
                )

            if final_review_decision.action == FinalReviewAction.abort:
                return await self._finish_with_mode(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context={**context, "error": final_review_decision.rationale},
                    presentation_mode="failure",
                    diagnostics={
                        **diagnostics,
                        "stop_reason": "final_review_abort",
                        "final_review": final_review_payload,
                    },
                    final_review=final_review_decision,
                )

            if final_review_decision.action == FinalReviewAction.revise_answer:
                revised_in_presentation = await self._attempt_presentation_revision_after_final_review(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context=context,
                    diagnostics=diagnostics,
                    final_review=final_review_decision,
                )
                if revised_in_presentation is not None:
                    return revised_in_presentation

            if final_review_decision.action in {FinalReviewAction.revise_answer, FinalReviewAction.replan}:
                if state.replan_count >= state.max_replans:
                    return await self._finish_with_mode(
                        execution_mode=execution_mode,
                        state=state,
                        question=question,
                        context={
                            **context,
                            "error": (
                                "Final review requested another analytical pass, "
                                "but the replan budget is exhausted."
                            ),
                        },
                        presentation_mode="failure",
                        diagnostics={
                            **diagnostics,
                            "stop_reason": "final_review_replan_budget_exhausted",
                            "final_review": final_review_payload,
                        },
                        final_review=final_review_decision,
                    )
                state.replan_count += 1
                replan_updates = {
                    "final_review_action": final_review_decision.action.value,
                    "final_review_rationale": final_review_decision.rationale,
                    "final_review_issues": list(final_review_decision.issues),
                    "reviewed_answer_package": answer_package,
                    **final_review_decision.updated_context,
                }
                state.context = {**state.context, **replan_updates}

                if final_review_decision.action == FinalReviewAction.revise_answer:
                    latest = state.latest_record
                    if latest is not None:
                        await self._emit_ai_event(
                            event_type="PlanReplanStarted",
                            message="Revising the latest answer after final review.",
                            source=latest.step.agent_name,
                            details={
                                "replan_count": state.replan_count,
                                "trigger": final_review_decision.action.value,
                            },
                        )
                        state.current_plan = self._build_revision_plan(
                            step=latest.step,
                            revision_count=state.replan_count,
                            rationale=final_review_decision.rationale,
                        )
                        await self._emit_ai_event(
                            event_type="PlanReplanCreated",
                            message="Prepared a direct answer-revision step.",
                            source=latest.step.agent_name,
                            details={"step_count": len(state.current_plan.steps)},
                        )
                        return await self._execute_plan(
                            execution_mode=execution_mode,
                            state=state,
                            question=question,
                            context=context,
                            diagnostics={**diagnostics, "final_review": final_review_payload},
                        )

                await self._emit_ai_event(
                    event_type="PlanReplanStarted",
                    message="Replanning after final review.",
                    source="planner",
                    details={"replan_count": state.replan_count, "trigger": final_review_decision.action.value},
                )
                state.current_plan = await self._planner.replan(
                    state=state,
                    context_updates=replan_updates,
                    specifications=self._registry.specifications(),
                )
                await self._emit_ai_event(
                    event_type="PlanReplanCreated",
                    message=f"Revised plan has {len(state.current_plan.steps)} step(s).",
                    source="planner",
                    details={"step_count": len(state.current_plan.steps)},
                )
                return await self._execute_plan(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context=context,
                    diagnostics={**diagnostics, "final_review": final_review_payload},
                )

        return await self._finish_final_with_presented_review(
            execution_mode=execution_mode,
            state=state,
            question=question,
            context=context,
            diagnostics=diagnostics,
            initial_final_review=final_review_decision,
        )

    async def _finish_final_with_presented_review(
        self,
        *,
        execution_mode: str,
        state: PlanExecutionState,
        question: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
        initial_final_review: FinalReviewDecision | None,
    ) -> MetaControllerRun:
        final = await self._present(
            question=question,
            context={
                **state.context,
                **context,
                "step_results": state.step_results_payload(),
                "plan": state.current_plan.model_dump(mode="json"),
            },
            mode="final",
        )
        presented_answer_package = self._build_presented_answer_package(
            state=state,
            presented_final=final,
        )
        presented_review = await self._review_final_answer(
            question=question,
            state=state,
            answer_package=presented_answer_package,
        )
        if presented_review is None or presented_review.action == FinalReviewAction.approve:
            return self._build_run(
                execution_mode=execution_mode,
                state=state,
                final=final,
                diagnostics=diagnostics,
                status="completed",
                final_review=presented_review or initial_final_review,
            )

        if presented_review.updated_context:
            state.context = {**state.context, **presented_review.updated_context}
        final_review_payload = presented_review.model_dump(mode="json")

        if presented_review.action == FinalReviewAction.ask_clarification:
            clarification_question = (
                presented_review.clarification_question
                or presented_review.rationale
                or "Clarification needed."
            )
            return await self._finish_with_mode(
                execution_mode=execution_mode,
                state=state,
                question=question,
                context={**context, "clarification_question": clarification_question},
                presentation_mode="clarification",
                diagnostics={
                    **diagnostics,
                    "stop_reason": "final_review_clarification_after_presentation",
                    "final_review": final_review_payload,
                },
                final_review=presented_review,
            )

        if presented_review.action == FinalReviewAction.abort:
            return await self._finish_with_mode(
                execution_mode=execution_mode,
                state=state,
                question=question,
                context={**context, "error": presented_review.rationale},
                presentation_mode="failure",
                diagnostics={
                    **diagnostics,
                    "stop_reason": "final_review_abort_after_presentation",
                    "final_review": final_review_payload,
                },
                final_review=presented_review,
            )

        if presented_review.action == FinalReviewAction.revise_answer:
            revised_in_presentation = await self._attempt_presentation_revision_after_final_review(
                execution_mode=execution_mode,
                state=state,
                question=question,
                context=context,
                diagnostics=diagnostics,
                final_review=presented_review,
            )
            if revised_in_presentation is not None:
                return revised_in_presentation

        if presented_review.action in {FinalReviewAction.revise_answer, FinalReviewAction.replan}:
            if state.replan_count >= state.max_replans:
                return await self._finish_with_mode(
                    execution_mode=execution_mode,
                    state=state,
                    question=question,
                    context={
                        **context,
                        "error": (
                            "Final review requested another analytical pass, "
                            "but the replan budget is exhausted."
                        ),
                    },
                    presentation_mode="failure",
                    diagnostics={
                        **diagnostics,
                        "stop_reason": "final_review_replan_budget_exhausted",
                        "final_review": final_review_payload,
                    },
                    final_review=presented_review,
                )
            state.replan_count += 1
            replan_updates = {
                "final_review_action": presented_review.action.value,
                "final_review_rationale": presented_review.rationale,
                "final_review_issues": list(presented_review.issues),
                "reviewed_answer_package": presented_answer_package,
                **presented_review.updated_context,
            }
            state.context = {**state.context, **replan_updates}
            state.current_plan = await self._planner.replan(
                state=state,
                context_updates=replan_updates,
                specifications=self._registry.specifications(),
            )
            return await self._execute_plan(
                execution_mode=execution_mode,
                state=state,
                question=question,
                context=context,
                diagnostics={**diagnostics, "final_review": final_review_payload},
            )

        return self._build_run(
            execution_mode=execution_mode,
            state=state,
            final=final,
            diagnostics={**diagnostics, "final_review": final_review_payload},
            status="completed",
            final_review=presented_review,
        )

    async def _finish_with_mode(
        self,
        *,
        execution_mode: str,
        state: PlanExecutionState,
        question: str,
        context: dict[str, Any],
        presentation_mode: str,
        diagnostics: dict[str, Any],
        final_review: FinalReviewDecision | None = None,
    ) -> MetaControllerRun:
        final = await self._present(
            question=question,
            context={
                **state.context,
                **context,
                "step_results": state.step_results_payload(),
                "plan": state.current_plan.model_dump(mode="json"),
            },
            mode=presentation_mode,
        )
        return self._build_run(
            execution_mode=execution_mode,
            state=state,
            final=final,
            diagnostics=diagnostics,
            status=self._status_for_presentation_mode(presentation_mode),
            final_review=final_review,
        )

    async def _attempt_presentation_revision_after_final_review(
        self,
        *,
        execution_mode: str,
        state: PlanExecutionState,
        question: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
        final_review: FinalReviewDecision,
    ) -> MetaControllerRun | None:
        revision_context = self._presentation_revision_context(final_review)
        revised_final = await self._present(
            question=question,
            context={
                **state.context,
                **context,
                **revision_context,
                "step_results": state.step_results_payload(),
                "plan": state.current_plan.model_dump(mode="json"),
            },
            mode="final",
        )
        revised_answer_package = self._build_presented_answer_package(
            state=state,
            presented_final=revised_final,
        )
        revised_review = await self._review_final_answer(
            question=question,
            state=state,
            answer_package=revised_answer_package,
        )
        if revised_review is None or revised_review.action == FinalReviewAction.approve:
            return self._build_run(
                execution_mode=execution_mode,
                state=state,
                final=revised_final,
                diagnostics={
                    **diagnostics,
                    "stop_reason": "final_review_presentation_revision",
                },
                status="completed",
                final_review=revised_review or final_review,
            )
        if revised_review.action == FinalReviewAction.ask_clarification:
            clarification_question = str(revised_review.clarification_question or "").strip() or revised_review.rationale
            return await self._finish_with_mode(
                execution_mode=execution_mode,
                state=state,
                question=question,
                context={**context, "clarification_question": clarification_question},
                presentation_mode="clarification",
                diagnostics={
                    **diagnostics,
                    "stop_reason": "final_review_clarification_after_presentation_revision",
                    "final_review": revised_review.model_dump(mode="json"),
                },
                final_review=revised_review,
            )
        if revised_review.action == FinalReviewAction.abort:
            return await self._finish_with_mode(
                execution_mode=execution_mode,
                state=state,
                question=question,
                context={**context, "error": revised_review.rationale},
                presentation_mode="failure",
                diagnostics={
                    **diagnostics,
                    "stop_reason": "final_review_abort_after_presentation_revision",
                    "final_review": revised_review.model_dump(mode="json"),
                },
                final_review=revised_review,
            )
        return None

    async def _finish_iteration_exhausted(
        self,
        *,
        execution_mode: str,
        state: PlanExecutionState,
        question: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
        decision: PlanReviewDecision | None,
    ) -> MetaControllerRun:
        terminal_error = self._iteration_exhausted_error_message(state=state, decision=decision)
        return await self._finish_with_mode(
            execution_mode=execution_mode,
            state=state,
            question=question,
            context={**context, "error": terminal_error},
            presentation_mode="failure",
            diagnostics={
                **diagnostics,
                "stop_reason": "max_iterations",
                "terminal_error": terminal_error,
            },
        )

    async def _present(
        self,
        *,
        question: str,
        context: dict[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        presentation_context = self._context_with_presentation_guidance(context)
        task = AgentTask(
            task_id="presentation",
            task_kind=AgentTaskKind.presentation,
            question=question,
            input={"mode": mode},
            context=presentation_context,
            expected_output=self._presentation_agent.specification.output_contract,
        )
        await self._emit_ai_event(
            event_type="PresentationStarted",
            message="Preparing final response.",
            source="presentation",
            details={"mode": mode},
        )
        result = await self._presentation_agent.execute(task)
        await self._emit_ai_event(
            event_type="PresentationCompleted",
            message="Final response prepared.",
            source="presentation",
            details={"status": result.status.value},
        )
        response = result.output.get("response")
        return response if isinstance(response, dict) else result.output

    def _context_with_presentation_guidance(self, context: dict[str, Any]) -> dict[str, Any]:
        if isinstance(context.get("presentation_guidance"), dict):
            return context
        agent_name = self._presentation_guidance_agent_name(context)
        if not agent_name:
            return context
        try:
            agent = self._registry.get(agent_name)
        except KeyError:
            return context
        guidance = getattr(agent, "presentation_guidance", None)
        if callable(guidance):
            guidance = guidance()
        if guidance is None:
            return context
        if hasattr(guidance, "to_dict"):
            payload = guidance.to_dict()
        elif isinstance(guidance, dict):
            payload = guidance
        else:
            return context
        return {**context, "presentation_guidance": payload} if isinstance(payload, dict) and payload else context

    @staticmethod
    def _presentation_guidance_agent_name(context: dict[str, Any]) -> str | None:
        step_results = context.get("step_results")
        if isinstance(step_results, list):
            for item in reversed(step_results):
                if isinstance(item, dict):
                    agent_name = str(item.get("agent_name") or "").strip()
                    if agent_name:
                        return agent_name
        plan = context.get("plan")
        if isinstance(plan, dict):
            steps = plan.get("steps")
            if isinstance(steps, list):
                for step in reversed(steps):
                    if isinstance(step, dict):
                        agent_name = str(step.get("agent_name") or "").strip()
                        if agent_name:
                            return agent_name
        for key in ("selected_agent", "agent_name"):
            agent_name = str(context.get(key) or "").strip()
            if agent_name:
                return agent_name
        return None

    @staticmethod
    def _build_run(
        *,
        execution_mode: str,
        state: PlanExecutionState,
        final: dict[str, Any],
        diagnostics: dict[str, Any],
        status: str,
        final_review: FinalReviewDecision | None = None,
    ) -> MetaControllerRun:
        resolved_execution_mode = MetaControllerAgent._execution_mode_from_plan(
            plan=state.current_plan,
            requested_mode=execution_mode,
        )
        return MetaControllerRun(
            execution_mode=resolved_execution_mode,
            status=status,
            plan=state.current_plan,
            step_results=state.step_results_payload(),
            verification=list(state.verifier_outcomes),
            review_decisions=[
                PlanReviewDecision.model_validate(item) for item in state.review_decisions
            ],
            final_review=final_review.model_dump(mode="json") if final_review is not None else {},
            final_result=final,
            presentation=final,
            diagnostics={
                **diagnostics,
                "iterations": state.iteration,
                "replan_count": state.replan_count,
            },
        )

    async def _review_final_answer(
        self,
        *,
        question: str,
        state: PlanExecutionState,
        answer_package: dict[str, Any],
    ) -> FinalReviewDecision | None:
        if not self._final_review_enabled:
            return None
        if not answer_package:
            return None
        evidence = answer_package.get("evidence")
        result = answer_package.get("result")
        research = (
            dict(answer_package.get("research"))
            if isinstance(answer_package.get("research"), dict)
            else {
                key: value
                for key, value in answer_package.items()
                if key in {"sources", "findings", "follow_ups", "synthesis"}
            }
        )
        task = AgentTask(
            task_id="final-review",
            task_kind=AgentTaskKind.orchestration,
            question=question,
            context={
                "answer_package": answer_package,
                "evidence": evidence if isinstance(evidence, dict) else {},
                "result": result if isinstance(result, dict) else {},
                "research": research,
                "step_results": state.step_results_payload(),
            },
            expected_output=self._final_review.specification.output_contract,
        )
        await self._emit_ai_event(
            event_type="FinalReviewStarted",
            message="Reviewing final analytical answer.",
            source="final-review",
        )
        result_payload = await self._final_review.execute(task)
        await self._emit_ai_event(
            event_type="FinalReviewCompleted",
            message=result_payload.diagnostics.get("action") or result_payload.status.value,
            source="final-review",
            details={"status": result_payload.status.value},
        )
        if result_payload.status != AgentResultStatus.succeeded:
            return FinalReviewDecision(
                action=FinalReviewAction.abort,
                reason_code=FinalReviewReasonCode.review_error,
                rationale=result_payload.error or "Final review failed.",
            )
        decision = result_payload.output.get("decision")
        if not isinstance(decision, dict):
            return FinalReviewDecision(
                action=FinalReviewAction.abort,
                reason_code=FinalReviewReasonCode.review_error,
                rationale="Final review returned an invalid decision payload.",
            )
        return FinalReviewDecision.model_validate(decision)

    @staticmethod
    def _build_answer_package(
        *,
        state: PlanExecutionState,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        latest_output = state.latest_record.result.output if state.latest_record is not None else {}
        if not isinstance(latest_output, dict):
            latest_output = {}
        answer = (
            latest_output.get("answer")
            or latest_output.get("analysis")
            or context.get("answer")
            or context.get("analysis")
            or ""
        )
        package = {
            **latest_output,
            "answer": answer,
        }
        return {key: value for key, value in package.items() if value is not None}

    @staticmethod
    def _build_presented_answer_package(
        *,
        state: PlanExecutionState,
        presented_final: dict[str, Any],
    ) -> dict[str, Any]:
        latest_output = state.latest_record.result.output if state.latest_record is not None else {}
        if not isinstance(latest_output, dict):
            latest_output = {}
        package = dict(latest_output)
        for key, value in presented_final.items():
            if value is not None:
                package[key] = value
        answer = package.get("answer") or package.get("analysis") or package.get("summary") or ""
        package["answer"] = answer
        return {key: value for key, value in package.items() if value is not None}

    @staticmethod
    def _presentation_revision_context(final_review: FinalReviewDecision) -> dict[str, Any]:
        revision_request = {
            "reason_code": final_review.reason_code.value,
            "rationale": final_review.rationale,
            "issues": list(final_review.issues),
            **final_review.updated_context,
        }
        return {
            "presentation_revision_request": revision_request,
            "final_review_rationale": final_review.rationale,
            "final_review_issues": list(final_review.issues),
            **final_review.updated_context,
        }

    @staticmethod
    def _execution_mode_from_plan(*, plan: ExecutionPlan, requested_mode: str) -> str | None:
        route = str(plan.route or "").strip().lower()
        if route.startswith("direct:"):
            return "direct"
        if route.startswith("planned"):
            return "planned"
        if plan.steps:
            return "planned"
        if requested_mode in {"direct", "planned"}:
            return requested_mode
        return None

    @staticmethod
    def _status_for_presentation_mode(presentation_mode: str) -> str:
        normalized = str(presentation_mode or "final").strip().lower()
        if normalized == "clarification":
            return "clarification_needed"
        if normalized == "failure":
            return "failed"
        return "completed"

    @staticmethod
    def _iteration_exhausted_error_message(
        *,
        state: PlanExecutionState,
        decision: PlanReviewDecision | None,
    ) -> str:
        latest = state.latest_record
        if latest is not None:
            if latest.verification.message:
                return latest.verification.message
            if latest.result.error:
                return latest.result.error
        if decision is not None and decision.rationale:
            return decision.rationale
        return "Meta-controller reached max iterations before finalizing."

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
    def _parse_json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("Meta-controller LLM response did not contain a JSON object.")
        parsed = json.loads(text[start : end + 1])
        if not isinstance(parsed, dict):
            raise ValueError("Meta-controller LLM response JSON must be an object.")
        return parsed


__all__ = ["MetaControllerAction", "MetaControllerAgent", "MetaControllerDecision", "MetaControllerRun"]
