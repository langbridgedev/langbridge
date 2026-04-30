"""Specification-driven analyst agent for Langbridge AI."""

import json
from dataclasses import dataclass
from typing import Any, Sequence

from langbridge.ai.base import (
    AgentCostLevel,
    AgentIOContract,
    AgentResult,
    AgentResultStatus,
    AgentRoutingSpec,
    AgentSpecification,
    AgentTask,
    AgentTaskKind,
    AgentToolSpecification,
    BaseAgent,
)
from langbridge.ai.events import AIEventEmitter, AIEventSource
from langbridge.ai.agents.analyst.prompts import (
    ANALYST_CONTEXT_ANALYSIS_PROMPT,
    ANALYST_DEEP_RESEARCH_PROMPT,
    ANALYST_EVIDENCE_PLAN_PROMPT,
    ANALYST_MODE_SELECTION_PROMPT,
    ANALYST_RESEARCH_STEP_PROMPT,
    ANALYST_SQL_EVIDENCE_REVIEW_PROMPT,
    ANALYST_SQL_RESPONSE_PROMPT,
    ANALYST_SQL_SYNTHESIS_PROMPT,
    ANALYST_SQL_TOOL_SELECTION_PROMPT,
)
from langbridge.ai.agents.analyst.contracts import AnalystEvidencePlan, AnalystEvidencePlanStep
from langbridge.ai.agents.analyst.research_workflow import (
    EvidenceBundle,
    ResearchDecisionAction,
    ResearchStepDecision,
    ResearchWorkflowState,
)
from langbridge.ai.agents.presentation.guidance import PresentationGuidance
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.modes import (
    AnalystAgentMode,
    analyst_output_contract_for_task_input,
    normalize_analyst_mode,
    normalize_analyst_mode_decision,
)
from langbridge.ai.profiles import AnalystAgentConfig
from langbridge.ai.question_intent import AnalystQuestionIntent
from langbridge.ai.tools.semantic_search import SemanticSearchTool
from langbridge.ai.tools.sql import SqlAnalysisTool
from langbridge.ai.tools.sql.interfaces import (
    AnalystOutcomeStatus,
    AnalystQueryRequest,
    AnalystQueryResponse,
    AnalystRecoveryAction,
    SqlQueryScope,
)
from langbridge.ai.tools.web_search import WebSearchResult, WebSearchTool


_SEMANTIC_FALLBACK_MARKERS = (
    "semantic sql scope does not support",
    "semantic scope does not support",
    "semantic query translation failed",
    "unknown semantic member",
    "could not resolve a selected semantic member",
    "semantic model not found",
    "semantic coverage gap",
    "unsupported semantic sql shape",
    "semantic sql filters only support literal values",
    "raw sql expressions are not supported in semantic filters",
    "semantic sql group by",
    "semantic sql order by",
    "semantic sql where",
    "semantic sql time bucketing",
    "must query the selected semantic model",
    "must match the selected semantic dimensions and time buckets",
)


@dataclass(frozen=True, slots=True)
class SqlFailureTaxonomy:
    kind: str
    status: str | None
    stage: str | None
    message: str | None
    fallback_eligible: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "status": self.status,
            "stage": self.stage,
            "message": self.message,
            "fallback_eligible": self.fallback_eligible,
        }


@dataclass(frozen=True, slots=True)
class SqlEvidenceReviewDecision:
    decision: str
    reason: str
    sufficiency: str
    clarification_question: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reason": self.reason,
            "sufficiency": self.sufficiency,
            "clarification_question": self.clarification_question,
        }


@dataclass(frozen=True, slots=True)
class SqlGovernedAttempt:
    round_index: int
    tool_name: str
    query_scope: str | None
    status: str | None
    attempted_tools: tuple[str, ...]
    fallback_details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "round_index": self.round_index,
            "tool_name": self.tool_name,
            "query_scope": self.query_scope,
            "status": self.status,
            "attempted_tools": list(self.attempted_tools),
            "fallback": self.fallback_details,
        }


class AnalystAgent(AIEventSource, BaseAgent):
    """Performs governed analytical work and research for one analyst profile."""

    def __init__(
        self,
        *,
        llm_provider: LLMProvider,
        config: AnalystAgentConfig,
        sql_analysis_tools: Sequence[SqlAnalysisTool] | None = None,
        semantic_search_tools: Sequence[SemanticSearchTool] | None = None,
        web_search_tool: WebSearchTool | None = None,
        event_emitter: AIEventEmitter | None = None,
    ) -> None:
        super().__init__(event_emitter=event_emitter)
        self._llm = llm_provider
        self._config = config
        self._sql_tools = list(sql_analysis_tools or [])
        self._semantic_search_tools = list(semantic_search_tools or [])
        self._web_search_tool = web_search_tool

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name=self._config.agent_name,
            description=self._config.description
            or "Answers governed analytical questions and source-backed research requests.",
            task_kinds=[AgentTaskKind.analyst],
            capabilities=self._capabilities(),
            constraints=self._constraints(),
            routing=AgentRoutingSpec(
                keywords=["analyze", "metric", "trend", "research", "compare", "summarize"],
                phrases=["what is", "what are", "show", "compare", "break down"],
                direct_threshold=2,
                planner_threshold=4,
            ),
            input_contract=AgentIOContract(optional_keys=["agent_mode", "force_web_search"]),
            output_contract=analyst_output_contract_for_task_input({}),
            tools=self._tool_specifications(),
            metadata={
                "scope": {
                    "semantic_models": self._config.semantic_model_ids,
                    "datasets": self._config.dataset_ids,
                    "query_policy": self._config.query_policy,
                    "allow_source_scope": self._config.allow_source_scope,
                    "research_enabled": self._config.supports_research,
                    "extended_thinking_enabled": self._config.supports_extended_thinking,
                    "web_search_enabled": self._config.web_search_enabled,
                    "web_search_allowed_domains": self._config.web_search_allowed_domains,
                    "allowed_connectors": list(self._config.access.allowed_connectors),
                    "denied_connectors": list(self._config.access.denied_connectors),
                },
                "supported_modes": [mode.value for mode in self._supported_modes()],
            },
        )

    @property
    def presentation_guidance(self) -> PresentationGuidance | None:
        return PresentationGuidance.from_prompt(
            profile_name=self._config.name,
            agent_name=self._config.agent_name,
            prompt=self._config.prompts.presentation_prompt,
        )

    async def execute(self, task: AgentTask) -> AgentResult:
        if task.task_kind != AgentTaskKind.analyst:
            return self.build_result(
                task=task,
                status=AgentResultStatus.blocked,
                error=f"Analyst agent only supports '{AgentTaskKind.analyst.value}' tasks.",
            )
        if not task.question.strip():
            return self.build_result(
                task=task,
                status=AgentResultStatus.needs_clarification,
                error="Question is required for analyst work.",
            )

        try:
            requested_mode = self._requested_mode(task)
        except ValueError as exc:
            return self.build_result(
                task=task,
                status=AgentResultStatus.blocked,
                error=str(exc),
            )
        if requested_mode == AnalystAgentMode.auto:
            await self._emit_ai_event(
                event_type="AnalystModeSelectionStarted",
                message="Choosing analyst execution mode.",
                source=self.specification.name,
            )
            decision = await self._select_execution_mode(task)
            decision = self._defer_premature_mode_clarification(task=task, decision=decision)
            selected_mode = str(decision.get("agent_mode") or "").strip().lower()
            if selected_mode == "clarify":
                return self.build_result(
                    task=task,
                    status=AgentResultStatus.needs_clarification,
                    error=str(
                        decision.get("clarification_question") or decision.get("reason") or "Clarification needed."
                    ),
                    diagnostics={"mode_decision": decision},
                )
            normalized_mode = normalize_analyst_mode(selected_mode, default=AnalystAgentMode.sql)
            if normalized_mode is None:
                raise ValueError("Analyst mode selection returned no mode.")
            mode = normalized_mode
            await self._emit_ai_event(
                event_type="AnalystModeSelected",
                message=f"Selected {mode.value} mode.",
                source=self.specification.name,
                details={"agent_mode": mode.value, "reason": decision.get("reason")},
            )
        else:
            decision = {"agent_mode": requested_mode.value, "reason": "Mode forced by task input."}
            mode = requested_mode

        if mode == AnalystAgentMode.research:
            return await self._execute_research(task, mode_decision=decision)
        if mode == AnalystAgentMode.sql:
            return await self._execute_sql(task, mode_decision=decision)
        if mode == AnalystAgentMode.context_analysis:
            return await self._execute_llm_analysis(task, mode_decision=decision)

        raise ValueError(f"Unsupported analyst execution mode: {mode.value}")

    async def _execute_sql(
        self,
        task: AgentTask,
        *,
        mode_decision: dict[str, Any],
        allow_web_augmentation: bool = True,
    ) -> AgentResult:
        candidate_tools = self._initial_sql_tools()
        if not candidate_tools:
            return self.build_result(
                task=task,
                status=AgentResultStatus.blocked,
                error="No SQL analysis tools are configured for this analyst profile.",
                diagnostics={"agent_mode": AnalystAgentMode.sql.value, "mode_decision": mode_decision},
            )

        request = self._sql_request(task)
        response: AnalystQueryResponse | None = None
        taxonomy: SqlFailureTaxonomy | None = None
        final_tool: SqlAnalysisTool | None = None
        fallback_details: dict[str, Any] | None = None
        sources: list[dict[str, Any]] = []
        findings: list[dict[str, Any]] = []
        follow_ups: list[str] = []
        web_result: WebSearchResult | None = None
        review: SqlEvidenceReviewDecision | None = None
        governed_attempts: list[SqlGovernedAttempt] = []
        tried_tool_names: set[str] = set()
        governed_round_limit = self._governed_round_limit()

        for round_index in range(1, governed_round_limit + 1):
            available_tools = [tool for tool in candidate_tools if tool.name not in tried_tool_names]
            if not available_tools:
                break

            final_tool, response, taxonomy, fallback_details, attempted_tools = await self._execute_governed_sql_round(
                task=task,
                tools=available_tools,
                request=request,
            )
            tried_tool_names.update(attempted_tools)
            governed_attempts.append(
                SqlGovernedAttempt(
                    round_index=round_index,
                    tool_name=final_tool.name,
                    query_scope=final_tool.query_scope.value,
                    status=response.outcome.status.value if response.outcome is not None else None,
                    attempted_tools=tuple(attempted_tools),
                    fallback_details=fallback_details,
                )
            )

            if not response.has_error:
                await self._emit_ai_event(
                    event_type="AnalystEvidenceReviewStarted",
                    message="Reviewing governed evidence sufficiency.",
                    source=self.specification.name,
                    details={"tool": final_tool.name, "round": round_index},
                )
                review = await self._review_sql_evidence(
                    task=task,
                    response=response,
                    memory_context=self._memory_context(task.context),
                )
                await self._emit_ai_event(
                    event_type="AnalystEvidenceReviewCompleted",
                    message=review.reason,
                    source=self.specification.name,
                    details={**review.to_dict(), "round": round_index},
                )
                if review.decision == "clarify":
                    if self._should_retry_governed_sql(
                        task=task,
                        response=response,
                        review=review,
                        candidate_tools=candidate_tools,
                        tried_tool_names=tried_tool_names,
                        round_index=round_index,
                        round_limit=governed_round_limit,
                    ):
                        request = self._next_sql_request_for_governed_retry(
                            request=request,
                            response=response,
                            taxonomy=taxonomy,
                            review=review,
                        )
                        continue

                    clarification_question = (
                        review.clarification_question
                        or review.reason
                        or "Which filters, time period, or entity should I use to answer this question?"
                    )
                    output = self._build_sql_output(
                        summary="",
                        response=response,
                        taxonomy=taxonomy,
                        sources=sources,
                        findings=findings,
                        follow_ups=follow_ups,
                        review=review,
                        governed_attempts=governed_attempts,
                    )
                    diagnostics = {
                        "agent_mode": AnalystAgentMode.sql.value,
                        "mode_decision": mode_decision,
                        "selected_tool": final_tool.name,
                        "selected_query_scope": final_tool.query_scope.value,
                        "error_taxonomy": taxonomy.to_dict(),
                        "web_search": None,
                        "weak_evidence": True,
                        "evidence_review": review.to_dict(),
                        "governed_attempt_count": len(governed_attempts),
                        "governed_attempts": [attempt.to_dict() for attempt in governed_attempts],
                        "governed_tools_tried": self._governed_tools_tried(governed_attempts),
                    }
                    if fallback_details is not None:
                        diagnostics["fallback"] = fallback_details
                    return self.build_result(
                        task=task,
                        status=AgentResultStatus.needs_clarification,
                        output=output,
                        artifacts=output.get("artifacts") if isinstance(output.get("artifacts"), dict) else {},
                        diagnostics=diagnostics,
                        error=clarification_question,
                    )
            elif self._should_retry_governed_sql(
                task=task,
                response=response,
                review=None,
                candidate_tools=candidate_tools,
                tried_tool_names=tried_tool_names,
                round_index=round_index,
                round_limit=governed_round_limit,
            ):
                request = self._next_sql_request_for_governed_retry(
                    request=request,
                    response=response,
                    taxonomy=taxonomy,
                    review=review,
                )
                continue

            break

        if response is None or taxonomy is None or final_tool is None:
            return self.build_result(
                task=task,
                status=AgentResultStatus.failed,
                error="No governed SQL attempt could be completed for this analyst task.",
                diagnostics={"agent_mode": AnalystAgentMode.sql.value, "mode_decision": mode_decision},
            )

        await self._emit_ai_event(
            event_type="AnalystSummaryStarted",
            message="Summarizing SQL analysis result.",
            source=self.specification.name,
            details={"tool": final_tool.name},
        )
        summary = await self._summarize_sql_response(
            question=task.question,
            response=response,
            memory_context=self._memory_context(task.context),
        )
        if allow_web_augmentation and self._should_augment_sql_with_web(task=task, response=response, review=review):
            web_result = await self._run_web_search_if_needed(task=task, existing_sources=[])
            if web_result is not None and web_result.results:
                sources = [item.to_dict() for item in web_result.results][: self._max_external_augmentations()]
                synthesis = await self._synthesize_sql_with_sources(
                    question=task.question,
                    analysis=summary,
                    response=response,
                    sources=sources,
                    memory_context=self._memory_context(task.context),
                )
                summary = synthesis["analysis"]
                findings = synthesis.get("findings", [])
                follow_ups = synthesis.get("follow_ups", [])
        output = self._build_sql_output(
            summary=summary,
            response=response,
            taxonomy=taxonomy,
            sources=sources,
            findings=findings,
            follow_ups=follow_ups,
            review=review,
            governed_attempts=governed_attempts,
        )
        status = self._result_status_for_sql(response)
        weak_evidence = bool(
            (review is not None and review.sufficiency != "sufficient" and not sources)
            or (response.is_empty_result and not sources)
        )
        diagnostics = {
            "agent_mode": AnalystAgentMode.sql.value,
            "mode_decision": mode_decision,
            "selected_tool": final_tool.name,
            "selected_query_scope": final_tool.query_scope.value,
            "error_taxonomy": taxonomy.to_dict(),
            "web_search": web_result.to_dict() if web_result else None,
            "weak_evidence": weak_evidence,
            "evidence_review": review.to_dict() if review is not None else None,
            "governed_attempt_count": len(governed_attempts),
            "governed_attempts": [attempt.to_dict() for attempt in governed_attempts],
            "governed_tools_tried": self._governed_tools_tried(governed_attempts),
        }
        if fallback_details is not None:
            diagnostics["fallback"] = fallback_details
        return self.build_result(
            task=task,
            status=status,
            output=output,
            artifacts=output.get("artifacts") if isinstance(output.get("artifacts"), dict) else {},
            diagnostics=diagnostics,
            error=response.error if status != AgentResultStatus.succeeded else None,
        )

    async def _execute_research(
        self,
        task: AgentTask,
        *,
        mode_decision: dict[str, Any],
    ) -> AgentResult:
        await self._emit_ai_event(
            event_type="DeepResearchStarted",
            message="Gathering evidence for research answer.",
            source=self.specification.name,
        )
        if not self._config.supports_research:
            return self.build_result(
                task=task,
                status=AgentResultStatus.blocked,
                error="Research mode is not enabled for this analyst profile.",
                diagnostics={"agent_mode": AnalystAgentMode.research.value, "mode_decision": mode_decision},
            )
        sources = self._dedupe_sources(self._collect_sources(task.context), limit=self._config.max_sources)
        evidence_plan = await self._create_research_evidence_plan(task=task, sources=sources)
        workflow_state = ResearchWorkflowState(original_question=task.question, evidence_plan=evidence_plan)
        if sources:
            workflow_state.add_sources(query=None, sources=sources)

        latest_governed_result: AgentResult | None = None
        latest_governed_output: dict[str, Any] = {}
        latest_governed_diagnostics: dict[str, Any] = {}
        best_governed_output: dict[str, Any] = {}
        best_governed_diagnostics: dict[str, Any] = {}
        web_result: WebSearchResult | None = None
        research_steps: list[dict[str, Any]] = []
        total_step_limit = max(1, self._governed_round_limit() + self._max_external_augmentations() + 1)

        for _ in range(total_step_limit):
            decision = await self._decide_research_step(
                task=task,
                workflow_state=workflow_state,
                sources=sources,
            )
            research_steps.append(decision.model_dump(mode="json", exclude_none=True))

            if decision.action == ResearchDecisionAction.clarify:
                clarification_question = (
                    decision.clarification_question
                    or decision.rationale
                    or "Which metric, entity, or time period should I use for this analysis?"
                )
                return self.build_result(
                    task=task,
                    status=AgentResultStatus.needs_clarification,
                    error=clarification_question,
                    diagnostics={
                        "agent_mode": AnalystAgentMode.research.value,
                        "mode_decision": mode_decision,
                        "research_phase": "clarify",
                        "research_steps": research_steps,
                    },
                )

            if decision.action == ResearchDecisionAction.query_governed:
                if self._remaining_governed_rounds(workflow_state=workflow_state) <= 0:
                    workflow_state.add_note("No governed rounds remain in the research budget.")
                    continue
                if not self._initial_sql_tools():
                    workflow_state.add_note("No governed SQL tools are configured for this research request.")
                    continue

                governed_question = self._optional_string(decision.governed_question) or task.question
                governed_task = task.model_copy(
                    update={
                        "question": governed_question,
                        "input": {
                            **dict(task.input),
                            "agent_mode": AnalystAgentMode.sql.value,
                        },
                    }
                )
                latest_governed_result = await self._execute_sql(
                    governed_task,
                    mode_decision={
                        "agent_mode": AnalystAgentMode.sql.value,
                        "reason": decision.rationale or "Research workflow requested a governed evidence round.",
                    },
                    allow_web_augmentation=False,
                )
                latest_governed_output = (
                    latest_governed_result.output if isinstance(latest_governed_result.output, dict) else {}
                )
                latest_governed_diagnostics = (
                    latest_governed_result.diagnostics
                    if isinstance(latest_governed_result.diagnostics, dict)
                    else {}
                )
                if latest_governed_result.status == AgentResultStatus.needs_clarification:
                    return self._retag_result_for_research(
                        result=latest_governed_result,
                        mode_decision=mode_decision,
                        research_phase="governed_round_clarification",
                    )
                if latest_governed_result.status == AgentResultStatus.succeeded and latest_governed_output:
                    best_governed_output = latest_governed_output
                    best_governed_diagnostics = latest_governed_diagnostics
                self._record_research_governed_round(
                    workflow_state=workflow_state,
                    question=governed_question,
                    result=latest_governed_result,
                )
                continue

            if decision.action == ResearchDecisionAction.augment_with_web:
                if self._remaining_web_augmentations(workflow_state=workflow_state) <= 0:
                    workflow_state.add_note("No web augmentation rounds remain in the research budget.")
                    continue
                if not self._web_augmentation_available(task=task):
                    workflow_state.add_note("Web augmentation is not available for this research request.")
                    continue

                search_query = self._optional_string(decision.search_query) or task.question
                web_result = await self._run_web_search_if_needed(
                    task=task,
                    existing_sources=sources,
                    query=search_query,
                    allow_existing_sources=True,
                )
                if web_result is None:
                    workflow_state.add_note("Web augmentation did not return additional evidence.")
                    continue
                sources = self._dedupe_sources(
                    [*sources, *[item.to_dict() for item in web_result.results]],
                    limit=self._config.max_sources,
                )
                workflow_state.add_sources(query=search_query, sources=sources)
                continue

            if decision.action == ResearchDecisionAction.synthesize:
                if not self._research_ready_to_synthesize(workflow_state=workflow_state, sources=sources):
                    workflow_state.add_note("Synthesis was deferred because no usable evidence has been collected yet.")
                    continue
                break

        evidence_bundle = workflow_state.evidence_bundle or EvidenceBundle(
            original_question=task.question,
            evidence_plan=evidence_plan,
        )
        governed_output = evidence_bundle.best_governed_output() or best_governed_output or latest_governed_output
        governed_diagnostics = (
            evidence_bundle.best_governed_diagnostics() or best_governed_diagnostics or latest_governed_diagnostics
        )
        if self._config.require_sources and not sources and not governed_output:
            return self.build_result(
                task=task,
                status=AgentResultStatus.blocked,
                output={
                    "analysis": "",
                    "result": governed_output.get("result") or {},
                    "synthesis": "",
                    "findings": [],
                    "sources": [],
                    "evidence_plan": evidence_plan.model_dump(mode="json", exclude_none=True)
                    if evidence_plan is not None
                    else None,
                    "evidence_bundle": evidence_bundle.to_dict(),
                },
                error="Research mode requires sources, but no evidence was available.",
                diagnostics={
                    "agent_mode": AnalystAgentMode.research.value,
                    "mode_decision": mode_decision,
                    "governed_seeded": bool(governed_output),
                    "research_steps": research_steps,
                },
            )
        if latest_governed_result is not None and latest_governed_result.status == AgentResultStatus.failed and not sources:
            return self._retag_result_for_research(
                result=latest_governed_result,
                mode_decision=mode_decision,
                research_phase="governed_round_failed",
            )
        if not evidence_bundle.has_evidence and not sources and not governed_output:
            return self.build_result(
                task=task,
                status=AgentResultStatus.blocked,
                output={
                    "analysis": "",
                    "result": {},
                    "synthesis": "",
                    "findings": [],
                    "sources": [],
                    "evidence_plan": evidence_plan.model_dump(mode="json", exclude_none=True)
                    if evidence_plan is not None
                    else None,
                    "evidence_bundle": evidence_bundle.to_dict(),
                },
                error="Research mode could not gather governed or external evidence for this request.",
                diagnostics={
                    "agent_mode": AnalystAgentMode.research.value,
                    "mode_decision": mode_decision,
                    "research_phase": "evidence_unavailable",
                    "research_steps": research_steps,
                    "research_state": workflow_state.compact_payload(),
                },
            )
        visualization_decision = research_steps[-1] if research_steps else {}
        plan_visualization = (
            evidence_plan.visualization_recommendation.model_dump(mode="json", exclude_none=True)
            if evidence_plan is not None and evidence_plan.visualization_recommendation is not None
            else {}
        )
        research = await self._synthesize_research(
            question=task.question,
            sources=sources,
            memory_context=self._memory_context(task.context),
            governed_analysis=str(governed_output.get("analysis") or ""),
            governed_result=governed_output.get("result") if isinstance(governed_output.get("result"), dict) else {},
            governed_outcome=governed_output.get("outcome") if isinstance(governed_output.get("outcome"), dict) else {},
            governed_rounds=workflow_state.compact_payload().get("governed_rounds", []),
            evidence_bundle=evidence_bundle.to_prompt_dict(),
        )
        visualization_payload = self._build_visualization_recommendation_payload(
            recommendation=visualization_decision.get("visualization_recommendation")
            or plan_visualization.get("recommendation"),
            recommended_chart_type=visualization_decision.get("recommended_chart_type") or plan_visualization.get("chart_type"),
            rationale=visualization_decision.get("rationale") or plan_visualization.get("rationale"),
        )
        research_artifacts = (
            governed_output.get("artifacts")
            if isinstance(governed_output.get("artifacts"), dict)
            else {}
        )
        await self._emit_ai_event(
            event_type="DeepResearchCompleted",
            message=(
                f"Synthesized research from {len(sources)} external source(s)"
                + (" and governed evidence." if governed_output else ".")
            ),
            source=self.specification.name,
            details={"source_count": len(sources), "governed_seeded": bool(governed_output)},
        )
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={
                "analysis": research["synthesis"],
                "result": governed_output.get("result") if isinstance(governed_output.get("result"), dict) else {},
                "synthesis": research["synthesis"],
                "findings": research.get("findings", []),
                "sources": sources,
                "follow_ups": research.get("follow_ups", []),
                "artifacts": research_artifacts,
                "selected_datasets": governed_output.get("selected_datasets") or self._config.dataset_ids,
                "selected_semantic_models": governed_output.get("selected_semantic_models") or self._config.semantic_model_ids,
                "query_scope": governed_output.get("query_scope") or self._config.query_policy,
                "outcome": governed_output.get("outcome"),
                "verdict": research.get("verdict"),
                "key_comparisons": research.get("key_comparisons", []),
                "limitations": research.get("limitations", []),
                "visualization_recommendation": visualization_payload,
                "recommended_chart_type": visualization_decision.get("recommended_chart_type")
                or plan_visualization.get("chart_type"),
                "evidence_plan": evidence_plan.model_dump(mode="json", exclude_none=True)
                if evidence_plan is not None
                else None,
                "evidence_bundle": evidence_bundle.to_dict(),
                "evidence": self._build_research_evidence(
                    sources=sources,
                    governed_output=governed_output,
                    governed_diagnostics=governed_diagnostics,
                    workflow_state=workflow_state,
                ),
                "review_hints": self._build_research_review_hints(
                    sources=sources,
                    governed_output=governed_output,
                    governed_diagnostics=governed_diagnostics,
                    workflow_state=workflow_state,
                    visualization_recommendation=visualization_payload,
                    recommended_chart_type=visualization_decision.get("recommended_chart_type")
                    or plan_visualization.get("chart_type"),
                ),
            },
            artifacts=research_artifacts,
            diagnostics={
                "agent_mode": AnalystAgentMode.research.value,
                "mode_decision": mode_decision,
                "web_search": web_result.to_dict() if web_result else None,
                "weak_evidence": not bool(sources) and not bool(governed_output),
                "governed_seeded": bool(governed_output),
                "governed_attempt_count": governed_diagnostics.get("governed_attempt_count", 0),
                "governed_tools_tried": governed_diagnostics.get("governed_tools_tried", []),
                "research_steps": research_steps,
                "evidence_plan": evidence_plan.model_dump(mode="json", exclude_none=True)
                if evidence_plan is not None
                else None,
                "evidence_bundle_assessment": evidence_bundle.assessment(),
                "research_state": workflow_state.compact_payload(),
            },
        )

    async def _execute_llm_analysis(
        self,
        task: AgentTask,
        *,
        mode_decision: dict[str, Any],
    ) -> AgentResult:
        await self._emit_ai_event(
            event_type="AnalystContextAnalysisStarted",
            message="Analyzing provided result context.",
            source=self.specification.name,
        )
        context_result = task.context.get("result")
        if not isinstance(context_result, dict):
            return self.build_result(
                task=task,
                status=AgentResultStatus.blocked,
                error="No structured result context is available for context analysis mode.",
                diagnostics={"agent_mode": AnalystAgentMode.context_analysis.value, "mode_decision": mode_decision},
            )
        prompt = self._prompt(
            ANALYST_CONTEXT_ANALYSIS_PROMPT.format(
                question=task.question,
                memory_context=self._memory_context(task.context),
                detail_expectation=self._detail_expectation(task.question),
                result=json.dumps(context_result, default=str),
            )
        )
        parsed = self._parse_json_object(
            await self._llm.acomplete(
                prompt,
                temperature=0.0,
                max_tokens=self._detail_token_limit(task.question, standard=800, detailed=1200),
            )
        )
        result = parsed.get("result")
        if not isinstance(result, dict):
            raise ValueError("Analyst LLM response missing object field: result.")
        artifacts = self._build_context_result_artifacts(result=result)
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={
                "analysis": str(parsed.get("analysis") or ""),
                "result": result,
                "artifacts": artifacts,
                "evidence": self._build_context_analysis_evidence(result=result),
                "review_hints": self._build_context_analysis_review_hints(result=result),
            },
            artifacts=artifacts,
            diagnostics={"agent_mode": AnalystAgentMode.context_analysis.value, "mode_decision": mode_decision},
        )

    async def _select_sql_tool(
        self,
        *,
        question: str,
        tools: Sequence[SqlAnalysisTool],
        memory_context: str = "",
    ) -> SqlAnalysisTool:
        if len(tools) == 1:
            return tools[0]
        prompt = self._prompt(
            ANALYST_SQL_TOOL_SELECTION_PROMPT.format(
                question=question,
                memory_context=memory_context,
                filters="{}",
                tools=json.dumps([tool.describe() for tool in tools], default=str, indent=2),
            )
        )
        parsed = self._parse_json_object(await self._llm.acomplete(prompt, temperature=0.0, max_tokens=400))
        selected_name = str(parsed.get("tool_name") or "").strip()
        for tool in tools:
            if tool.name == selected_name:
                return tool
        raise ValueError(f"LLM selected unknown SQL analysis tool: {selected_name}")

    async def _select_fallback_tool(
        self,
        *,
        question: str,
        current_tool: SqlAnalysisTool,
        memory_context: str = "",
    ) -> SqlAnalysisTool | None:
        tools = self._fallback_sql_tools(current_tool)
        if not tools:
            return None
        return await self._select_sql_tool(question=question, tools=tools, memory_context=memory_context)

    async def _execute_governed_sql_round(
        self,
        *,
        task: AgentTask,
        tools: Sequence[SqlAnalysisTool],
        request: AnalystQueryRequest,
    ) -> tuple[SqlAnalysisTool, AnalystQueryResponse, SqlFailureTaxonomy, dict[str, Any] | None, list[str]]:
        await self._emit_ai_event(
            event_type="AgentToolStarted",
            message="Selecting SQL analysis tool.",
            source=self.specification.name,
        )
        selected_tool = await self._select_sql_tool(
            question=task.question,
            tools=tools,
            memory_context=self._memory_context(task.context),
        )
        await self._emit_ai_event(
            event_type="AgentToolSelected",
            message=f"Selected SQL tool {selected_tool.name}.",
            source=self.specification.name,
            details={
                "tool": selected_tool.name,
                "asset_type": selected_tool.asset_type,
                "query_scope": selected_tool.query_scope.value,
            },
        )

        response = await selected_tool.arun(request)
        taxonomy = self._classify_sql_failure(response=response, tool=selected_tool)
        final_tool = selected_tool
        fallback_details: dict[str, Any] | None = None
        attempted_tools = [selected_tool.name]

        if taxonomy.fallback_eligible:
            fallback_tools = [
                tool
                for tool in self._fallback_sql_tools(selected_tool)
                if tool.name not in attempted_tools
            ]
            if fallback_tools:
                fallback_tool = await self._select_sql_tool(
                    question=task.question,
                    tools=fallback_tools,
                    memory_context=self._memory_context(task.context),
                )
                await self._emit_ai_event(
                    event_type="AnalystScopeFallbackStarted",
                    message=(
                        f"Falling back from {selected_tool.query_scope.value} scope to "
                        f"{fallback_tool.query_scope.value} scope."
                    ),
                    source=self.specification.name,
                    details={
                        "from_tool": selected_tool.name,
                        "to_tool": fallback_tool.name,
                        "reason": taxonomy.message,
                        "error_kind": taxonomy.kind,
                    },
                )
                fallback_request = request.model_copy(
                    update={
                        "error_history": [*request.error_history, taxonomy.message or response.error or ""],
                        "error_retries": request.error_retries + 1,
                    }
                )
                fallback_response = await fallback_tool.arun(fallback_request)
                response = self._apply_scope_fallback(
                    fallback_response=fallback_response,
                    original_response=response,
                    from_tool=selected_tool,
                    to_tool=fallback_tool,
                    taxonomy=taxonomy,
                )
                final_tool = fallback_tool
                attempted_tools.append(fallback_tool.name)
                fallback_details = {
                    "from_tool": selected_tool.name,
                    "to_tool": fallback_tool.name,
                    "from_scope": selected_tool.query_scope.value,
                    "to_scope": fallback_tool.query_scope.value,
                    "reason": taxonomy.message,
                    "error_kind": taxonomy.kind,
                }
                await self._emit_ai_event(
                    event_type="AnalystScopeFallbackCompleted",
                    message=f"Retrying with dataset-native scope via {fallback_tool.name}.",
                    source=self.specification.name,
                    details=fallback_details,
                )
        return final_tool, response, taxonomy, fallback_details, attempted_tools

    async def _summarize_sql_response(
        self,
        *,
        question: str,
        response: AnalystQueryResponse,
        memory_context: str = "",
    ) -> str:
        prompt = self._prompt(
            ANALYST_SQL_RESPONSE_PROMPT.format(
                question=question,
                memory_context=memory_context,
                detail_expectation=self._detail_expectation(question),
                sql=response.sql_canonical,
                result=json.dumps(response.result.model_dump(mode="json") if response.result else {}, default=str),
                outcome=json.dumps(response.outcome.model_dump(mode="json") if response.outcome else {}, default=str),
            )
        )
        parsed = self._parse_json_object(
            await self._llm.acomplete(
                prompt,
                temperature=0.0,
                max_tokens=self._detail_token_limit(question, standard=700, detailed=1100),
            )
        )
        analysis = str(parsed.get("analysis") or "").strip()
        if not analysis:
            raise ValueError("Analyst SQL summary response missing analysis.")
        return analysis

    async def _review_sql_evidence(
        self,
        *,
        task: AgentTask,
        response: AnalystQueryResponse,
        memory_context: str = "",
    ) -> SqlEvidenceReviewDecision:
        prompt = self._prompt(
            ANALYST_SQL_EVIDENCE_REVIEW_PROMPT.format(
                question=task.question,
                memory_context=memory_context,
                web_augmentation_available=self._web_augmentation_available(task=task),
                sql=response.sql_canonical,
                result=json.dumps(response.result.model_dump(mode="json") if response.result else {}, default=str),
                outcome=json.dumps(response.outcome.model_dump(mode="json") if response.outcome else {}, default=str),
            )
        )
        try:
            parsed = self._parse_json_object(await self._llm.acomplete(prompt, temperature=0.0, max_tokens=500))
        except Exception:
            return self._default_sql_evidence_review(task=task, response=response)

        decision = str(parsed.get("decision") or "").strip().lower()
        sufficiency = str(parsed.get("sufficiency") or "").strip().lower()
        if decision not in {"answer", "augment_with_web", "clarify"}:
            return self._default_sql_evidence_review(task=task, response=response)
        if sufficiency not in {"sufficient", "partial", "insufficient"}:
            return self._default_sql_evidence_review(task=task, response=response)
        return SqlEvidenceReviewDecision(
            decision=decision,
            reason=str(parsed.get("reason") or "").strip() or "Reviewed governed SQL evidence.",
            sufficiency=sufficiency,
            clarification_question=self._optional_string(parsed.get("clarification_question")),
        )

    async def _synthesize_research(
        self,
        *,
        question: str,
        sources: list[dict[str, Any]],
        memory_context: str = "",
        governed_analysis: str = "",
        governed_result: dict[str, Any] | None = None,
        governed_outcome: dict[str, Any] | None = None,
        governed_rounds: list[dict[str, Any]] | None = None,
        evidence_bundle: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        prompt = self._prompt(
            ANALYST_DEEP_RESEARCH_PROMPT.format(
                question=question,
                memory_context=memory_context,
                detail_expectation=self._detail_expectation(question),
                governed_analysis=governed_analysis,
                governed_result=json.dumps(governed_result or {}, default=str),
                governed_outcome=json.dumps(governed_outcome or {}, default=str),
                governed_rounds=json.dumps(governed_rounds or [], default=str),
                evidence_bundle=json.dumps(evidence_bundle or {}, default=str),
                sources=json.dumps(sources, default=str),
            )
        )
        parsed = self._parse_json_object(
            await self._llm.acomplete(
                prompt,
                temperature=0.0,
                max_tokens=self._detail_token_limit(question, standard=1200, detailed=1600),
            )
        )
        if not isinstance(parsed.get("synthesis"), str):
            raise ValueError("Research LLM response missing synthesis.")
        findings = parsed.get("findings")
        if findings is not None and not isinstance(findings, list):
            raise ValueError("Research LLM response findings must be a list.")
        follow_ups = parsed.get("follow_ups")
        if follow_ups is not None and not isinstance(follow_ups, list):
            raise ValueError("Research LLM response follow_ups must be a list.")
        key_comparisons = parsed.get("key_comparisons")
        if key_comparisons is not None and not isinstance(key_comparisons, list):
            raise ValueError("Research LLM response key_comparisons must be a list.")
        limitations = parsed.get("limitations")
        if limitations is not None and not isinstance(limitations, list):
            raise ValueError("Research LLM response limitations must be a list.")
        return parsed

    async def _synthesize_sql_with_sources(
        self,
        *,
        question: str,
        analysis: str,
        response: AnalystQueryResponse,
        sources: list[dict[str, Any]],
        memory_context: str = "",
    ) -> dict[str, Any]:
        prompt = self._prompt(
            ANALYST_SQL_SYNTHESIS_PROMPT.format(
                question=question,
                memory_context=memory_context,
                detail_expectation=self._detail_expectation(question),
                analysis=analysis,
                result=json.dumps(response.result.model_dump(mode="json") if response.result else {}, default=str),
                outcome=json.dumps(response.outcome.model_dump(mode="json") if response.outcome else {}, default=str),
                sources=json.dumps(sources, default=str),
            )
        )
        parsed = self._parse_json_object(
            await self._llm.acomplete(
                prompt,
                temperature=0.0,
                max_tokens=self._detail_token_limit(question, standard=1200, detailed=1600),
            )
        )
        if not isinstance(parsed.get("analysis"), str):
            raise ValueError("Analyst SQL synthesis response missing analysis.")
        findings = parsed.get("findings")
        if findings is not None and not isinstance(findings, list):
            raise ValueError("Analyst SQL synthesis response findings must be a list.")
        follow_ups = parsed.get("follow_ups")
        if follow_ups is not None and not isinstance(follow_ups, list):
            raise ValueError("Analyst SQL synthesis response follow_ups must be a list.")
        return parsed

    async def _create_research_evidence_plan(
        self,
        *,
        task: AgentTask,
        sources: list[dict[str, Any]],
    ) -> AnalystEvidencePlan:
        prompt = self._prompt(
            ANALYST_EVIDENCE_PLAN_PROMPT.format(
                question=task.question,
                memory_context=self._combined_conversation_context(task.context),
                sql_tools=json.dumps([tool.describe() for tool in self._initial_sql_tools()], default=str, indent=2),
                web_search_available=self._web_augmentation_available(task=task),
                governed_round_limit=self._governed_round_limit(),
                web_augmentation_limit=self._max_external_augmentations(),
                sources=json.dumps(sources, default=str, indent=2),
            )
        )
        try:
            parsed = self._parse_json_object(await self._llm.acomplete(prompt, temperature=0.0, max_tokens=1000))
            return AnalystEvidencePlan.model_validate(parsed)
        except Exception:
            return self._fallback_research_evidence_plan(task=task)

    def _fallback_research_evidence_plan(self, *, task: AgentTask) -> AnalystEvidencePlan:
        steps: list[AnalystEvidencePlanStep] = []
        if self._initial_sql_tools():
            steps.append(
                AnalystEvidencePlanStep(
                    step_id="e1",
                    action="query_governed",
                    question=task.question,
                    evidence_goal="Gather governed evidence that can answer or constrain the analytical question.",
                    expected_signal="A governed result with rows, metric definitions, or a clear execution limitation.",
                    success_criteria="The governed result contains usable rows or a concrete limitation for synthesis.",
                )
            )
        elif self._web_augmentation_available(task=task):
            steps.append(
                AnalystEvidencePlanStep(
                    step_id="e1",
                    action="augment_with_web",
                    search_query=task.question,
                    evidence_goal="Gather source evidence because no governed SQL tool is available.",
                    expected_signal="Relevant source-backed facts that help answer the question.",
                    success_criteria="At least one relevant source is returned.",
                )
            )
        steps.append(
            AnalystEvidencePlanStep(
                step_id=f"e{len(steps) + 1}",
                action="synthesize",
                evidence_goal="Produce the final answer from the evidence collected in the bounded workflow.",
                success_criteria="The answer gives a direct verdict with evidence-backed caveats.",
                depends_on=[step.step_id for step in steps],
            )
        )
        return AnalystEvidencePlan(
            objective=task.question,
            question_type="research",
            steps=steps,
            synthesis_requirements=[
                "Answer the user's question directly.",
                "Ground claims in governed or source evidence.",
                "State material limitations.",
            ],
        )

    @staticmethod
    def _research_ready_to_synthesize(
        *,
        workflow_state: ResearchWorkflowState,
        sources: list[dict[str, Any]],
    ) -> bool:
        evidence_bundle = workflow_state.evidence_bundle
        return bool(sources or (evidence_bundle is not None and evidence_bundle.has_usable_evidence))

    async def _decide_research_step(
        self,
        *,
        task: AgentTask,
        workflow_state: ResearchWorkflowState,
        sources: list[dict[str, Any]],
    ) -> ResearchStepDecision:
        prompt = self._prompt(
            ANALYST_RESEARCH_STEP_PROMPT.format(
                question=task.question,
                memory_context=self._combined_conversation_context(task.context),
                sql_tools=json.dumps([tool.describe() for tool in self._initial_sql_tools()], default=str, indent=2),
                web_search_available=self._web_augmentation_available(task=task),
                remaining_governed_rounds=self._remaining_governed_rounds(workflow_state=workflow_state),
                remaining_web_augmentations=self._remaining_web_augmentations(workflow_state=workflow_state),
                research_state=json.dumps(workflow_state.compact_payload(), default=str, indent=2),
            )
        )
        try:
            parsed = self._parse_json_object(await self._llm.acomplete(prompt, temperature=0.0, max_tokens=700))
            decision = ResearchStepDecision.model_validate(parsed)
            return self._defer_premature_research_clarification(
                task=task,
                workflow_state=workflow_state,
                decision=decision,
            )
        except Exception:
            return self._fallback_research_step_decision(
                task=task,
                workflow_state=workflow_state,
                sources=sources,
            )

    def _defer_premature_research_clarification(
        self,
        *,
        task: AgentTask,
        workflow_state: ResearchWorkflowState,
        decision: ResearchStepDecision,
    ) -> ResearchStepDecision:
        if decision.action != ResearchDecisionAction.clarify:
            return decision
        if workflow_state.governed_round_count > 0:
            return decision
        if self._remaining_governed_rounds(workflow_state=workflow_state) <= 0:
            return decision
        plan_step = self._first_governed_evidence_plan_step(workflow_state=workflow_state)
        if plan_step is None:
            return decision
        return ResearchStepDecision(
            action=ResearchDecisionAction.query_governed,
            rationale=(
                "Deferring clarification until governed evidence is inspected; "
                f"original clarification rationale: {decision.rationale}"
            ),
            governed_question=plan_step.question or task.question,
            plan_step_id=plan_step.step_id,
            evidence_goal=plan_step.evidence_goal,
            expected_signal=plan_step.expected_signal,
            success_criteria=plan_step.success_criteria,
            gaps_addressed=decision.gaps_addressed,
        )

    @staticmethod
    def _first_governed_evidence_plan_step(
        *,
        workflow_state: ResearchWorkflowState,
    ) -> AnalystEvidencePlanStep | None:
        plan = workflow_state.evidence_plan
        if plan is None:
            return None
        for step in plan.steps:
            if step.action == "query_governed":
                return step
        return None

    def _fallback_research_step_decision(
        self,
        *,
        task: AgentTask,
        workflow_state: ResearchWorkflowState,
        sources: list[dict[str, Any]],
    ) -> ResearchStepDecision:
        remaining_governed_rounds = self._remaining_governed_rounds(workflow_state=workflow_state)
        remaining_web_augmentations = self._remaining_web_augmentations(workflow_state=workflow_state)
        if remaining_governed_rounds > 0 and self._initial_sql_tools() and not workflow_state.answered_by_governed:
            return ResearchStepDecision(
                action=ResearchDecisionAction.query_governed,
                rationale="Research should gather governed evidence before synthesis.",
                governed_question=task.question,
            )
        if (
            remaining_web_augmentations > 0
            and self._web_augmentation_available(task=task)
            and (workflow_state.answered_by_governed or not self._initial_sql_tools())
            and not sources
        ):
            return ResearchStepDecision(
                action=ResearchDecisionAction.augment_with_web,
                rationale="Governed evidence exists, but external context may still help complete the answer.",
                search_query=task.question,
            )
        return ResearchStepDecision(
            action=ResearchDecisionAction.synthesize,
            rationale="The bounded workflow should synthesize from the evidence collected so far.",
        )

    async def _run_web_search_if_needed(
        self,
        *,
        task: AgentTask,
        existing_sources: list[dict[str, Any]],
        query: str | None = None,
        allow_existing_sources: bool = False,
    ) -> WebSearchResult | None:
        if existing_sources and not allow_existing_sources and not task.input.get("force_web_search"):
            return None
        if not self._config.web_search_enabled:
            return None
        if self._web_search_tool is None:
            if task.input.get("force_web_search"):
                raise RuntimeError("Web search was forced, but no WebSearchTool is configured.")
            return None
        search_query = self._optional_string(query) or task.question
        if not allow_existing_sources and not self._question_requests_web(search_query) and not task.input.get("force_web_search"):
            return None
        return await self._web_search_tool.search(search_query)

    def _collect_sources(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        explicit = context.get("sources")
        if isinstance(explicit, list):
            return [item for item in explicit if isinstance(item, dict)]

        sources: list[dict[str, Any]] = []
        for result in context.get("step_results", []):
            if not isinstance(result, dict):
                continue
            output = result.get("output")
            if not isinstance(output, dict):
                continue
            for key in ("sources", "results"):
                raw_items = output.get(key)
                if isinstance(raw_items, list):
                    sources.extend(item for item in raw_items if isinstance(item, dict))
        return sources

    def _requested_mode(self, task: AgentTask) -> AnalystAgentMode:
        return normalize_analyst_mode(
            task.input.get("agent_mode") or task.input.get("mode"),
            default=AnalystAgentMode.auto,
        ) or AnalystAgentMode.auto

    async def _select_execution_mode(self, task: AgentTask) -> dict[str, Any]:
        prompt = self._prompt(
            ANALYST_MODE_SELECTION_PROMPT.format(
                question=task.question,
                task_kind=task.task_kind.value,
                input_mode=str(task.input.get("agent_mode") or task.input.get("mode") or ""),
                scope=json.dumps(self.specification.metadata.get("scope") or {}, default=str, indent=2),
                sql_tools=json.dumps([tool.describe() for tool in self._initial_sql_tools()], default=str, indent=2),
                web_search_configured=self._web_search_tool is not None,
                has_result_context=isinstance(task.context.get("result"), dict),
                has_sources=bool(task.context.get("sources") or task.context.get("step_results")),
                memory_context=self._memory_context(task.context),
            )
        )
        parsed = normalize_analyst_mode_decision(
            self._parse_json_object(await self._llm.acomplete(prompt, temperature=0.0, max_tokens=500)),
            default_mode=AnalystAgentMode.sql,
        )
        if parsed is None:
            raise ValueError("Analyst mode selection returned no decision.")
        if parsed.agent_mode == "clarify":
            return parsed.model_dump(mode="json", exclude_none=True)
        mode = normalize_analyst_mode(parsed.agent_mode)
        if mode is None or mode.value not in {item.value for item in self._supported_modes()}:
            raise ValueError(f"Analyst mode selection returned invalid mode: {mode}")
        return parsed.model_dump(mode="json", exclude_none=True)

    def _defer_premature_mode_clarification(
        self,
        *,
        task: AgentTask,
        decision: dict[str, Any],
    ) -> dict[str, Any]:
        selected_mode = str(decision.get("agent_mode") or "").strip().lower()
        if selected_mode != "clarify":
            return decision
        if not self._initial_sql_tools():
            return decision
        clarification = str(
            decision.get("clarification_question") or decision.get("reason") or ""
        ).strip()
        if not AnalystQuestionIntent.should_inspect_evidence_before_clarifying(
            question=task.question,
            clarification=clarification,
        ):
            return decision

        mode = AnalystAgentMode.research if self._config.supports_research else AnalystAgentMode.sql
        return {
            "agent_mode": mode.value,
            "reason": (
                "Deferring clarification until governed evidence is inspected; "
                f"original clarification: {clarification or 'Clarification needed.'}"
            ),
            "deferred_clarification_question": clarification or None,
        }

    def _supported_modes(self) -> list[AnalystAgentMode]:
        modes = [AnalystAgentMode.sql]
        if self._config.supports_research:
            modes.append(AnalystAgentMode.research)
        modes.append(AnalystAgentMode.context_analysis)
        return modes

    def _capabilities(self) -> list[str]:
        capabilities = ["governed analytics", "dataset-native SQL fallback"]
        if self._config.semantic_model_ids:
            capabilities.append("semantic-model analysis")
        if self._config.supports_research:
            capabilities.append("source-backed research")
        if self._config.web_search_enabled:
            capabilities.append("web search")
        if self._semantic_search_tools:
            capabilities.append("semantic grounding")
        capabilities.append(f"agent_modes: {', '.join(mode.value for mode in self._supported_modes())}")
        return capabilities

    def _constraints(self) -> list[str]:
        return [
            "Read-only analytical work only.",
            "No connector sync or mutation side effects.",
            f"semantic models: {', '.join(self._config.semantic_model_ids) or 'none'}",
            f"datasets: {', '.join(self._config.dataset_ids) or 'none'}",
            f"query policy: {self._config.query_policy}",
            f"web search: {'enabled' if self._config.web_search_enabled else 'disabled'}",
            f"web allowed domains: {', '.join(self._config.web_search_allowed_domains) or 'any'}",
        ]

    def _initial_sql_tools(self) -> list[SqlAnalysisTool]:
        semantic_tools = [tool for tool in self._sql_tools if tool.query_scope == SqlQueryScope.semantic]
        dataset_tools = [
            tool
            for tool in self._sql_tools
            if tool.query_scope == SqlQueryScope.dataset
            or (self._config.allow_source_scope and tool.query_scope == SqlQueryScope.source)
        ]
        if self._config.query_policy == "semantic_only":
            return semantic_tools
        if self._config.query_policy == "dataset_only":
            return dataset_tools
        if self._config.query_policy == "dataset_preferred":
            return [*dataset_tools, *[tool for tool in semantic_tools if tool not in dataset_tools]]
        return [*semantic_tools, *[tool for tool in dataset_tools if tool not in semantic_tools]]

    def _fallback_sql_tools(self, current_tool: SqlAnalysisTool) -> list[SqlAnalysisTool]:
        if current_tool.query_scope != SqlQueryScope.semantic:
            return []
        if self._config.query_policy == "semantic_only":
            return []
        return [
            tool
            for tool in self._sql_tools
            if tool is not current_tool
            and (
                tool.query_scope == SqlQueryScope.dataset
                or (self._config.allow_source_scope and tool.query_scope == SqlQueryScope.source)
            )
        ]

    def _sql_request(self, task: AgentTask) -> AnalystQueryRequest:
        return AnalystQueryRequest(
            question=task.question,
            conversation_context=self._combined_conversation_context(task.context),
            filters=task.context.get("filters"),
            limit=task.context.get("limit", 1000),
            error_retries=0,
            error_history=[],
        )

    def _classify_sql_failure(self, *, response: AnalystQueryResponse, tool: SqlAnalysisTool) -> SqlFailureTaxonomy:
        outcome = response.outcome
        if outcome is None:
            return SqlFailureTaxonomy(
                kind="unknown",
                status=None,
                stage=None,
                message=response.error,
                fallback_eligible=False,
            )
        message = response.error or outcome.message or outcome.original_error
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        failure_kind = str(metadata.get("semantic_failure_kind") or "").strip()
        fallback_eligible = self._is_semantic_scope_fallback_eligible(response=response, tool=tool)
        if fallback_eligible and failure_kind:
            kind = failure_kind
        elif fallback_eligible:
            kind = "semantic_scope_limit"
        elif outcome.status == AnalystOutcomeStatus.access_denied:
            kind = "access_denied"
        elif outcome.status == AnalystOutcomeStatus.invalid_request:
            kind = "invalid_request"
        elif outcome.status == AnalystOutcomeStatus.needs_clarification:
            kind = "needs_clarification"
        elif outcome.status == AnalystOutcomeStatus.empty_result:
            kind = "empty_result"
        elif outcome.status == AnalystOutcomeStatus.query_error:
            kind = "query_error"
        elif outcome.status == AnalystOutcomeStatus.execution_error:
            kind = "execution_error"
        else:
            kind = outcome.status.value
        return SqlFailureTaxonomy(
            kind=kind,
            status=outcome.status.value,
            stage=outcome.stage.value if outcome.stage is not None else None,
            message=message,
            fallback_eligible=fallback_eligible,
        )

    def _is_semantic_scope_fallback_eligible(self, *, response: AnalystQueryResponse, tool: SqlAnalysisTool) -> bool:
        outcome = response.outcome
        if outcome is None or tool.query_scope != SqlQueryScope.semantic:
            return False
        if outcome.status not in {AnalystOutcomeStatus.query_error, AnalystOutcomeStatus.execution_error}:
            return False
        if not self._fallback_sql_tools(tool):
            return False
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        if metadata.get("scope_fallback_eligible") is True:
            return True
        error_text = " ".join(
            part
            for part in (outcome.message, outcome.original_error, response.error)
            if isinstance(part, str) and part.strip()
        ).casefold()
        return any(marker in error_text for marker in _SEMANTIC_FALLBACK_MARKERS)

    def _apply_scope_fallback(
        self,
        *,
        fallback_response: AnalystQueryResponse,
        original_response: AnalystQueryResponse,
        from_tool: SqlAnalysisTool,
        to_tool: SqlAnalysisTool,
        taxonomy: SqlFailureTaxonomy,
    ) -> AnalystQueryResponse:
        original_outcome = original_response.outcome
        fallback_outcome = fallback_response.outcome
        if fallback_outcome is None:
            return fallback_response
        recovery_actions = list(fallback_outcome.recovery_actions)
        recovery_actions.append(
            AnalystRecoveryAction(
                action="fallback_query_scope",
                rationale=(
                    f"Fell back from {from_tool.query_scope.value} scope to {to_tool.query_scope.value} "
                    "after semantic execution feedback."
                ),
                details={
                    "from_scope": from_tool.query_scope.value,
                    "to_scope": to_tool.query_scope.value,
                    "from_tool": from_tool.name,
                    "to_tool": to_tool.name,
                    "reason": taxonomy.message,
                    "semantic_failure_kind": taxonomy.kind,
                },
            )
        )
        metadata = dict(fallback_outcome.metadata or {})
        metadata["semantic_failure"] = taxonomy.to_dict()
        updated_outcome = fallback_outcome.model_copy(
            update={
                "attempted_query_scope": from_tool.query_scope,
                "final_query_scope": to_tool.query_scope,
                "fallback_from_query_scope": from_tool.query_scope,
                "fallback_to_query_scope": to_tool.query_scope,
                "fallback_reason": taxonomy.message,
                "selected_semantic_model_id": (
                    original_outcome.selected_semantic_model_id
                    if original_outcome and original_outcome.selected_semantic_model_id
                    else fallback_outcome.selected_semantic_model_id
                ),
                "recovery_actions": recovery_actions,
                "metadata": metadata,
            }
        )
        return fallback_response.model_copy(update={"outcome": updated_outcome})

    def _build_sql_output(
        self,
        *,
        summary: str,
        response: AnalystQueryResponse,
        taxonomy: SqlFailureTaxonomy,
        sources: list[dict[str, Any]] | None = None,
        findings: list[dict[str, Any]] | None = None,
        follow_ups: list[str] | None = None,
        review: SqlEvidenceReviewDecision | None = None,
        governed_attempts: Sequence[SqlGovernedAttempt] | None = None,
    ) -> dict[str, Any]:
        source_items = list(sources or [])
        finding_items = list(findings or [])
        follow_up_items = list(follow_ups or [])
        artifacts = self._build_sql_artifacts(
            response=response,
            taxonomy=taxonomy,
            governed_attempts=governed_attempts or [],
        )
        return {
            "analysis": summary,
            "result": response.result.model_dump(mode="json") if response.result else {},
            "artifacts": artifacts,
            "analysis_path": response.analysis_path,
            "sql_canonical": response.sql_canonical,
            "sql_executable": response.sql_executable,
            "selected_datasets": [dataset.dataset_id for dataset in response.selected_datasets],
            "selected_semantic_models": (
                [response.selected_semantic_model_id] if response.selected_semantic_model_id else []
            ),
            "query_scope": response.query_scope.value if response.query_scope else None,
            "outcome": response.outcome.model_dump(mode="json") if response.outcome else None,
            "error_taxonomy": taxonomy.to_dict(),
            "sources": source_items,
            "findings": finding_items,
            "follow_ups": follow_up_items,
            "evidence": self._build_sql_evidence(
                response=response,
                sources=source_items,
                review=review,
                governed_attempts=governed_attempts or [],
            ),
            "review_hints": self._build_sql_review_hints(
                response=response,
                sources=source_items,
                review=review,
                governed_attempts=governed_attempts or [],
            ),
        }

    def _build_sql_artifacts(
        self,
        *,
        response: AnalystQueryResponse,
        taxonomy: SqlFailureTaxonomy,
        governed_attempts: Sequence[SqlGovernedAttempt],
    ) -> dict[str, Any]:
        artifacts: dict[str, Any] = {}
        result_payload = response.result.model_dump(mode="json") if response.result else {}
        result_artifact = self._build_result_table_artifact(
            artifact_id="primary_result",
            title="Verified analyst result",
            result=result_payload,
            provenance={
                "source": "analyst",
                "analysis_path": response.analysis_path,
                "query_scope": response.query_scope.value if response.query_scope else None,
                "selected_datasets": [dataset.dataset_id for dataset in response.selected_datasets],
                "selected_semantic_models": (
                    [response.selected_semantic_model_id] if response.selected_semantic_model_id else []
                ),
            },
        )
        if result_artifact is not None:
            artifacts[result_artifact["id"]] = result_artifact

        sql_payload = {
            "sql_canonical": response.sql_canonical,
            "sql_executable": response.sql_executable,
        }
        if response.sql_canonical or response.sql_executable:
            artifacts["primary_sql"] = {
                "id": "primary_sql",
                "type": "sql",
                "role": "diagnostic",
                "title": "Generated SQL",
                "payload": sql_payload,
                "provenance": {
                    "source": "analyst",
                    "analysis_path": response.analysis_path,
                    "query_scope": response.query_scope.value if response.query_scope else None,
                    "status": response.outcome.status.value if response.outcome is not None else None,
                    "error_taxonomy": taxonomy.to_dict(),
                },
                "visible_by_default": False,
            }

        if governed_attempts:
            artifacts["governed_attempts"] = {
                "id": "governed_attempts",
                "type": "diagnostics",
                "role": "diagnostic",
                "title": "Governed query attempts",
                "payload": [attempt.to_dict() for attempt in governed_attempts],
                "provenance": {
                    "source": "analyst",
                    "attempt_count": len(governed_attempts),
                },
                "visible_by_default": False,
            }
        return artifacts

    @staticmethod
    def _build_context_result_artifacts(result: dict[str, Any]) -> dict[str, Any]:
        artifact = AnalystAgent._build_result_table_artifact(
            artifact_id="primary_result",
            title="Verified context result",
            result=result,
            provenance={"source": "context_analysis"},
        )
        return {artifact["id"]: artifact} if artifact is not None else {}

    @staticmethod
    def _build_result_table_artifact(
        *,
        artifact_id: str,
        title: str,
        result: dict[str, Any],
        provenance: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not isinstance(result, dict) or not isinstance(result.get("rows"), list):
            return None
        row_count = result.get("rowcount")
        if row_count is None:
            row_count = result.get("rowCount")
        if row_count is None:
            row_count = len(result.get("rows") or [])
        return {
            "id": artifact_id,
            "type": "table",
            "role": "primary_result",
            "title": title,
            "payload": result,
            "data_ref": "result",
            "provenance": {
                **provenance,
                "row_count": row_count,
                "columns": list(result.get("columns") or []),
            },
        }

    def _build_sql_evidence(
        self,
        *,
        response: AnalystQueryResponse,
        sources: list[dict[str, Any]],
        review: SqlEvidenceReviewDecision | None,
        governed_attempts: Sequence[SqlGovernedAttempt],
    ) -> dict[str, Any]:
        return {
            "governed": {
                "attempted": True,
                "answered_question": response.has_rows,
                "query_scope": response.query_scope.value if response.query_scope else None,
                "status": response.outcome.status.value if response.outcome is not None else None,
                "used_fallback": bool(
                    response.outcome
                    and response.outcome.fallback_to_query_scope is not None
                    and response.outcome.fallback_from_query_scope is not None
                ),
                "attempt_count": len(governed_attempts),
                "tools_tried": self._governed_tools_tried(governed_attempts),
            },
            "assessment": review.to_dict() if review is not None else None,
            "external": {
                "used": bool(sources),
                "source_count": len(sources),
            },
            "limitations": self._sql_limitations(response=response, sources=sources),
        }

    def _build_research_evidence(
        self,
        *,
        sources: list[dict[str, Any]],
        governed_output: dict[str, Any],
        governed_diagnostics: dict[str, Any],
        workflow_state: ResearchWorkflowState | None = None,
    ) -> dict[str, Any]:
        governed_evidence = governed_output.get("evidence") if isinstance(governed_output.get("evidence"), dict) else {}
        governed_section = governed_evidence.get("governed") if isinstance(governed_evidence.get("governed"), dict) else {}
        limitations = list(governed_evidence.get("limitations") or []) if isinstance(governed_evidence, dict) else []
        if governed_output and not governed_section:
            outcome = governed_output.get("outcome") if isinstance(governed_output.get("outcome"), dict) else {}
            governed_section = {
                "attempted": True,
                "answered_question": bool(governed_output.get("result")),
                "query_scope": governed_output.get("query_scope"),
                "status": outcome.get("status"),
                "used_fallback": False,
                "attempt_count": governed_diagnostics.get("governed_attempt_count", 0),
                "tools_tried": governed_diagnostics.get("governed_tools_tried", []),
            }
        if workflow_state is not None and workflow_state.governed_rounds:
            governed_section = {
                **governed_section,
                "attempted": True,
                "answered_question": governed_section.get("answered_question") or workflow_state.answered_by_governed,
                "attempt_count": max(
                    int(governed_section.get("attempt_count") or 0),
                    workflow_state.governed_round_count,
                ),
                "questions": [round_.question for round_ in workflow_state.governed_rounds],
                "rounds": [round_.to_prompt_dict() for round_ in workflow_state.governed_rounds],
            }
            for note in workflow_state.notes:
                if note not in limitations:
                    limitations.append(note)
        if sources:
            limitations.append("External sources were used to supplement governed or contextual evidence.")
        if not governed_output and not sources:
            limitations.append("No governed or external evidence was available.")
        evidence_bundle = workflow_state.evidence_bundle if workflow_state is not None else None
        return {
            "governed": governed_section
            or {
                "attempted": False,
                "answered_question": False,
                "query_scope": None,
                "status": None,
                "used_fallback": False,
            },
            "external": {
                "used": bool(sources),
                "source_count": len(sources),
            },
            "plan": (
                workflow_state.evidence_plan.model_dump(mode="json", exclude_none=True)
                if workflow_state is not None and workflow_state.evidence_plan is not None
                else None
            ),
            "bundle": evidence_bundle.to_dict() if evidence_bundle is not None else None,
            "assessment": evidence_bundle.assessment() if evidence_bundle is not None else None,
            "limitations": limitations or ["Evidence is source-backed only; governed result data was not produced."],
        }

    @staticmethod
    def _build_context_analysis_evidence(*, result: dict[str, Any]) -> dict[str, Any]:
        rows = result.get("rows")
        return {
            "governed": {
                "attempted": False,
                "answered_question": isinstance(rows, list) and bool(rows),
                "query_scope": "context_result",
                "status": "provided_result",
                "used_fallback": False,
            },
            "external": {
                "used": False,
                "source_count": 0,
            },
            "limitations": [] if isinstance(rows, list) and rows else ["Provided result context is empty."],
        }

    def _build_sql_review_hints(
        self,
        *,
        response: AnalystQueryResponse,
        sources: list[dict[str, Any]],
        review: SqlEvidenceReviewDecision | None,
        governed_attempts: Sequence[SqlGovernedAttempt],
    ) -> dict[str, Any]:
        return {
            "requires_source_review": bool(sources),
            "governed_empty_result": response.is_empty_result,
            "governed_error": response.has_error,
            "external_augmentation_used": bool(sources),
            "evidence_review_decision": review.decision if review is not None else None,
            "governed_attempt_count": len(governed_attempts),
        }

    @staticmethod
    def _build_research_review_hints(
        *,
        sources: list[dict[str, Any]],
        governed_output: dict[str, Any],
        governed_diagnostics: dict[str, Any],
        workflow_state: ResearchWorkflowState | None = None,
        visualization_recommendation: dict[str, Any] | None = None,
        recommended_chart_type: str | None = None,
    ) -> dict[str, Any]:
        governed_hints = (
            governed_output.get("review_hints")
            if isinstance(governed_output.get("review_hints"), dict)
            else {}
        )
        evidence_bundle = workflow_state.evidence_bundle if workflow_state is not None else None
        return {
            "requires_source_review": bool(sources),
            "governed_empty_result": bool(governed_hints.get("governed_empty_result")),
            "governed_error": bool(governed_hints.get("governed_error")),
            "external_augmentation_used": bool(sources),
            "governed_attempt_count": max(
                int(governed_diagnostics.get("governed_attempt_count", 0)),
                workflow_state.governed_round_count if workflow_state is not None else 0,
            ),
            "evidence_review_decision": governed_hints.get("evidence_review_decision"),
            "visualization_recommendation": visualization_recommendation,
            "recommended_chart_type": recommended_chart_type,
            "evidence_plan_step_count": (
                len(workflow_state.evidence_plan.steps)
                if workflow_state is not None and workflow_state.evidence_plan is not None
                else 0
            ),
            "evidence_bundle_assessment": evidence_bundle.assessment() if evidence_bundle is not None else None,
        }

    @staticmethod
    def _build_visualization_recommendation_payload(
        *,
        recommendation: Any,
        recommended_chart_type: Any,
        rationale: Any,
    ) -> dict[str, Any] | None:
        recommendation_text = str(recommendation or "").strip().lower()
        chart_type = str(recommended_chart_type or "").strip().lower() or None
        rationale_text = str(rationale or "").strip() or None
        if recommendation_text not in {"none", "helpful", "required"} and not chart_type and not rationale_text:
            return None
        payload: dict[str, Any] = {
            "recommendation": recommendation_text if recommendation_text in {"none", "helpful", "required"} else "none",
        }
        if chart_type:
            payload["chart_type"] = chart_type
        if rationale_text:
            payload["rationale"] = rationale_text
        return payload

    @staticmethod
    def _build_context_analysis_review_hints(*, result: dict[str, Any]) -> dict[str, Any]:
        rows = result.get("rows")
        return {
            "requires_source_review": False,
            "governed_empty_result": isinstance(rows, list) and len(rows) == 0,
            "governed_error": False,
            "external_augmentation_used": False,
        }

    def _sql_limitations(self, *, response: AnalystQueryResponse, sources: list[dict[str, Any]]) -> list[str]:
        limitations: list[str] = []
        if response.is_empty_result:
            limitations.append("Governed SQL returned no matching rows.")
        if response.has_error:
            limitations.append(response.error or "Governed SQL execution did not succeed.")
        if sources:
            limitations.append("External sources were used to supplement governed evidence.")
        return limitations

    def _should_retry_governed_sql(
        self,
        *,
        task: AgentTask,
        response: AnalystQueryResponse,
        review: SqlEvidenceReviewDecision | None,
        candidate_tools: Sequence[SqlAnalysisTool],
        tried_tool_names: set[str],
        round_index: int,
        round_limit: int,
    ) -> bool:
        if round_index >= round_limit:
            return False
        if not any(tool.name not in tried_tool_names for tool in candidate_tools):
            return False
        if response.has_error:
            return True
        if review is None:
            return False
        if review.decision == "augment_with_web" and self._should_augment_sql_with_web(
            task=task,
            response=response,
            review=review,
        ):
            return False
        return review.decision != "answer"

    @staticmethod
    def _next_sql_request_for_governed_retry(
        *,
        request: AnalystQueryRequest,
        response: AnalystQueryResponse,
        taxonomy: SqlFailureTaxonomy,
        review: SqlEvidenceReviewDecision | None,
    ) -> AnalystQueryRequest:
        error_history = list(request.error_history)
        for item in (
            taxonomy.message,
            response.error,
            review.reason if review is not None else None,
        ):
            text = str(item or "").strip()
            if text:
                error_history.append(text)
        return request.model_copy(
            update={
                "error_retries": request.error_retries + 1,
                "error_history": error_history,
            }
        )

    @staticmethod
    def _governed_tools_tried(governed_attempts: Sequence[SqlGovernedAttempt]) -> list[str]:
        seen: list[str] = []
        for attempt in governed_attempts:
            for tool_name in attempt.attempted_tools:
                if tool_name not in seen:
                    seen.append(tool_name)
        return seen

    def _default_sql_evidence_review(
        self,
        *,
        task: AgentTask,
        response: AnalystQueryResponse,
    ) -> SqlEvidenceReviewDecision:
        if response.has_error:
            return SqlEvidenceReviewDecision(
                decision="answer",
                reason="Governed SQL returned an execution failure that should not trigger web augmentation.",
                sufficiency="insufficient",
            )
        if task.input.get("force_web_search") and self._web_augmentation_available(task=task):
            return SqlEvidenceReviewDecision(
                decision="augment_with_web",
                reason="Web search was forced by task input.",
                sufficiency="partial" if response.has_rows else "insufficient",
            )
        if self._question_requests_web(task.question) and self._web_augmentation_available(task=task):
            return SqlEvidenceReviewDecision(
                decision="augment_with_web",
                reason="Question asks for current or external context beyond governed SQL.",
                sufficiency="partial" if response.has_rows else "insufficient",
            )
        if response.is_empty_result:
            return SqlEvidenceReviewDecision(
                decision="clarify",
                reason="Governed SQL returned no matching rows for the current request.",
                sufficiency="insufficient",
                clarification_question="Which filters, entity, or time period should I use to refine the analysis?",
            )
        return SqlEvidenceReviewDecision(
            decision="answer",
            reason="Governed SQL evidence is sufficient to answer directly.",
            sufficiency="sufficient",
        )

    def _retag_result_for_research(
        self,
        *,
        result: AgentResult,
        mode_decision: dict[str, Any],
        research_phase: str,
    ) -> AgentResult:
        diagnostics = result.diagnostics if isinstance(result.diagnostics, dict) else {}
        return result.model_copy(
            update={
                "diagnostics": {
                    **diagnostics,
                    "agent_mode": AnalystAgentMode.research.value,
                    "mode_decision": mode_decision,
                    "research_phase": research_phase,
                }
            }
        )

    def _should_augment_sql_with_web(
        self,
        *,
        task: AgentTask,
        response: AnalystQueryResponse,
        review: SqlEvidenceReviewDecision | None,
    ) -> bool:
        if response.has_error:
            return False
        if review is not None:
            return review.decision == "augment_with_web" and self._web_augmentation_available(task=task)
        if task.input.get("force_web_search"):
            return True
        if self._question_requests_web(task.question) and self._web_augmentation_available(task=task):
            return True
        return response.is_empty_result and self._web_augmentation_available(task=task)

    def _web_augmentation_available(self, *, task: AgentTask) -> bool:
        if self._web_search_tool is None:
            return False
        if not self._config.web_search_enabled:
            return False
        if not self._config.supports_research and not task.input.get("force_web_search"):
            return False
        return self._max_external_augmentations() > 0

    def _remaining_governed_rounds(self, *, workflow_state: ResearchWorkflowState) -> int:
        if not self._initial_sql_tools():
            return 0
        return max(0, self._governed_round_limit() - workflow_state.governed_round_count)

    def _remaining_web_augmentations(self, *, workflow_state: ResearchWorkflowState) -> int:
        return max(0, self._max_external_augmentations() - len(workflow_state.web_search_queries))

    def _record_research_governed_round(
        self,
        *,
        workflow_state: ResearchWorkflowState,
        question: str,
        result: AgentResult,
    ) -> None:
        output = result.output if isinstance(result.output, dict) else {}
        diagnostics = result.diagnostics if isinstance(result.diagnostics, dict) else {}
        result_payload = output.get("result") if isinstance(output.get("result"), dict) else {}
        rows = result_payload.get("rows")
        rowcount = len(rows) if isinstance(rows, list) else None
        evidence = output.get("evidence") if isinstance(output.get("evidence"), dict) else {}
        governed = evidence.get("governed") if isinstance(evidence.get("governed"), dict) else {}
        limitations = evidence.get("limitations") if isinstance(evidence.get("limitations"), list) else []
        workflow_state.add_governed_round(
            question=question,
            status=result.status.value,
            query_scope=str(output.get("query_scope") or governed.get("query_scope") or ""),
            rowcount=rowcount,
            answered_question=bool(governed.get("answered_question")) or (isinstance(rows, list) and bool(rows)),
            weak_evidence=bool(diagnostics.get("weak_evidence")),
            analysis=str(output.get("analysis") or result.error or "").strip(),
            limitations=[str(item).strip() for item in limitations if isinstance(item, str) and str(item).strip()],
            follow_ups=[str(item).strip() for item in (output.get("follow_ups") or []) if isinstance(item, str)],
            output=output,
            diagnostics=diagnostics,
        )

    def _governed_round_limit(self) -> int:
        return max(1, min(self._max_evidence_rounds(), self._max_governed_attempts()))

    def _max_evidence_rounds(self) -> int:
        try:
            return max(1, int(self._config.max_evidence_rounds))
        except (AttributeError, TypeError, ValueError):
            return 2

    def _max_governed_attempts(self) -> int:
        try:
            return max(1, int(self._config.max_governed_attempts))
        except (AttributeError, TypeError, ValueError):
            return 2

    def _max_external_augmentations(self) -> int:
        try:
            return max(0, int(self._config.max_external_augmentations))
        except (AttributeError, TypeError, ValueError):
            return 3

    @staticmethod
    def _detail_expectation(question: str) -> str:
        text = question.casefold()
        detail_cues = (
            "detail",
            "detailed",
            "breakdown",
            "why",
            "how",
            "explain",
            "evidence",
            "compare",
            "comparison",
            "versus",
            " vs ",
            "relationship",
            "relationships",
            "associate",
            "associated",
            "association",
            "correlate",
            "correlation",
            "driver",
            "drivers",
            "trend",
            "rank",
            "ranking",
            "underperform",
            "outperform",
            "walk me through",
            "show your work",
            "step by step",
            "cite",
            "source",
            "sources",
        )
        return "detailed" if any(cue in text for cue in detail_cues) else "standard"

    def _detail_token_limit(self, question: str, *, standard: int, detailed: int) -> int:
        return detailed if self._detail_expectation(question) == "detailed" else standard

    @staticmethod
    def _dedupe_sources(sources: Sequence[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for item in sources:
            if not isinstance(item, dict):
                continue
            key = (
                str(item.get("url") or "").strip(),
                str(item.get("title") or item.get("source") or "").strip(),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
            if len(deduped) >= limit:
                break
        return deduped

    @staticmethod
    def _optional_string(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        return text or None

    @staticmethod
    def _result_status_for_sql(response: AnalystQueryResponse) -> AgentResultStatus:
        outcome = response.outcome
        if outcome is not None and outcome.status == AnalystOutcomeStatus.needs_clarification:
            return AgentResultStatus.needs_clarification
        if response.has_error:
            return AgentResultStatus.failed
        return AgentResultStatus.succeeded

    def _tool_specifications(self) -> list[AgentToolSpecification]:
        tools = [
            AgentToolSpecification(
                name=tool.name,
                description=tool.description or "Executes governed analytical SQL.",
                output_contract=AgentIOContract(required_keys=["result"]),
            )
            for tool in self._sql_tools
        ]
        if self._web_search_tool is not None:
            tools.append(
                AgentToolSpecification(
                    name="web-search",
                    description="Retrieves external evidence under analyst policy.",
                    output_contract=AgentIOContract(required_keys=["results"]),
                )
            )
        return tools

    def _prompt(self, base_prompt: str) -> str:
        sections = [base_prompt.strip()]
        if self._config.prompts.system_prompt:
            sections.append(f"Analyst system guidance:\n{self._config.prompts.system_prompt.strip()}")
        if self._config.prompts.user_prompt:
            sections.append(f"Analyst execution guidance:\n{self._config.prompts.user_prompt.strip()}")
        if self._config.prompts.response_format_prompt:
            sections.append(f"Analyst response format guidance:\n{self._config.prompts.response_format_prompt.strip()}")
        return "\n\n".join(section for section in sections if section)

    @staticmethod
    def _memory_context(context: dict[str, Any]) -> str:
        value = context.get("memory_context")
        return value if isinstance(value, str) else ""

    @staticmethod
    def _combined_conversation_context(context: dict[str, Any]) -> str:
        parts = []
        conversation = context.get("conversation_context")
        if isinstance(conversation, str) and conversation.strip():
            parts.append(conversation.strip())
        memory = context.get("memory_context")
        if isinstance(memory, str) and memory.strip():
            parts.append("Memory:\n" + memory.strip())
        final_review_rationale = context.get("final_review_rationale")
        final_review_issues = context.get("final_review_issues")
        review_lines: list[str] = []
        if isinstance(final_review_rationale, str) and final_review_rationale.strip():
            review_lines.append(final_review_rationale.strip())
        if isinstance(final_review_issues, list):
            review_lines.extend(
                str(item).strip()
                for item in final_review_issues
                if isinstance(item, str) and str(item).strip()
            )
        if review_lines:
            parts.append("Final review guidance:\n" + "\n".join(f"- {line}" for line in review_lines))
        return "\n\n".join(parts)

    @staticmethod
    def _question_requests_web(question: str) -> bool:
        text = question.casefold()
        return any(
            cue in text
            for cue in ("search", "web", "latest", "current", "news", "source", "sources", "look up", "research")
        )

    @staticmethod
    def _parse_json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("LLM response did not contain a JSON object.")
        parsed = json.loads(text[start : end + 1])
        if not isinstance(parsed, dict):
            raise ValueError("LLM response JSON must be an object.")
        return parsed


SemanticAnalystAgent = AnalystAgent

__all__ = ["AnalystAgent", "SemanticAnalystAgent"]
