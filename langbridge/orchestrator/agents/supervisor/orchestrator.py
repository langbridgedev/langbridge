"""
Supervisor orchestrator that coordinates planner, analyst, research, and visual agents.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence

from langbridge.runtime.events import (
    AgentEventVisibility,
    AgentEventEmitter,
)
from langbridge.orchestrator.definitions import ResponseMode
from langbridge.orchestrator.agents.models import PlanExecutionArtifacts
from langbridge.orchestrator.agents.reasoning.agent import ReasoningAgent, ReasoningDecision
from langbridge.orchestrator.agents.analyst import AnalystAgent
from langbridge.orchestrator.agents.bi_copilot import BICopilotAgent
from langbridge.orchestrator.agents.deep_research import DeepResearchAgent, DeepResearchResult
from langbridge.orchestrator.agents.planner import (
    AgentName,
    Plan,
    PlanStep,
    PlannerRequest,
    PlanningAgent,
    PlanningConstraints,
    RouteName,
)
from langbridge.orchestrator.agents.supervisor.clarification_manager import (
    ClarificationManager,
)
from langbridge.orchestrator.agents.supervisor.entity_resolver import (
    EntityResolver,
)
from langbridge.orchestrator.agents.supervisor.question_classifier import (
    QuestionClassifier,
)
from langbridge.orchestrator.agents.supervisor.schemas import (
    ClarificationDecision,
    ClarificationState,
    ClassifiedQuestion,
    ResolvedEntities,
)
from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.agents.visual import VisualAgent
from langbridge.orchestrator.agents.web_search import WebSearchAgent, WebSearchResult
from langbridge.orchestrator.tools.semantic_query_builder import (
    QueryBuilderCopilotRequest,
    QueryBuilderCopilotResponse,
)
from langbridge.orchestrator.runtime.analysis_grounding import (
    build_analyst_grounding,
    compose_analyst_summary,
)
from langbridge.orchestrator.tools.sql_analyst.interfaces import AnalystQueryResponse


@dataclass
class OrchestrationContext:
    """Context passed into the supervisor to capture routing metadata."""

    analyst_tools: Sequence[Any]  # Retained for backwards compatibility / auditing
    trace_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SupervisorPreflight:
    classification: ClassifiedQuestion
    resolved_entities: ResolvedEntities
    clarification_decision: ClarificationDecision
    context_updates: Dict[str, Any] = field(default_factory=dict)


class SupervisorOrchestrator:
    """High-level orchestrator routing between planner, analyst, research, and visual agents."""

    def __init__(
        self,
        *,
        llm: LLMProvider,
        visual_agent: Optional[VisualAgent] = None,
        analyst_agent: Optional[AnalystAgent] = None,
        logger: Optional[logging.Logger] = None,
        planning_agent: Optional[PlanningAgent] = None,
        deep_research_agent: Optional[DeepResearchAgent] = None,
        web_search_agent: Optional[WebSearchAgent] = None,
        reasoning_agent: Optional[ReasoningAgent] = None,
        bi_copilot_agent: Optional[BICopilotAgent] = None,
        event_emitter: Optional[AgentEventEmitter] = None,
        question_classifier: Optional[QuestionClassifier] = None,
        entity_resolver: Optional[EntityResolver] = None,
        clarification_manager: Optional[ClarificationManager] = None,
        response_mode: ResponseMode = ResponseMode.analyst,
    ) -> None:
        self.analyst_agent = analyst_agent
        self.visual_agent = visual_agent
        self.logger = logger or logging.getLogger(__name__)
        self.planning_agent = planning_agent
        self.deep_research_agent = deep_research_agent
        self.web_search_agent = web_search_agent
        self.reasoning_agent = reasoning_agent
        self.bi_copilot_agent = bi_copilot_agent
        self.event_emitter = event_emitter
        self.question_classifier = question_classifier or QuestionClassifier(llm=llm, logger=self.logger)
        self.entity_resolver = entity_resolver or EntityResolver(llm=llm, logger=self.logger)
        self.clarification_manager = clarification_manager or ClarificationManager(default_max_turns=2)
        self.response_mode = response_mode

    async def run_copilot(
        self,
        request: QueryBuilderCopilotRequest,
    ) -> QueryBuilderCopilotResponse:
        """Delegate query builder requests to the BI copilot agent."""

        if not self.bi_copilot_agent:
            raise RuntimeError("BI Copilot agent is not configured on this orchestrator instance.")
        return await self.bi_copilot_agent.assist_async(request)

    async def handle(
        self,
        user_query: str,
        *,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        title: Optional[str] = None,
        planning_constraints: Optional[PlanningConstraints] = None,
        planning_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute planner-driven workflows for a single user query."""

        await self._emit_event(
            event_type="AgentRunStarted",
            message="Working on your request.",
            visibility=AgentEventVisibility.public,
            source="supervisor",
        )
        start = time.perf_counter()

        plan: Optional[Plan] = None
        artifacts: Optional[PlanExecutionArtifacts] = None
        combined_artifacts = PlanExecutionArtifacts()
        final_decision: Optional[ReasoningDecision] = None
        extra_context: Dict[str, Any] = dict(planning_context or {})
        preflight = await self._run_preflight(user_query=user_query, planning_context=extra_context)
        classification = preflight.classification
        resolved_entities = preflight.resolved_entities
        clarification_state = preflight.clarification_decision.updated_state
        assumptions_applied = list(preflight.clarification_decision.assumptions)
        extra_context = self._merge_context(extra_context, preflight.context_updates)

        if preflight.clarification_decision.requires_clarification:
            clarifying_question = preflight.clarification_decision.clarifying_question or (
                "Could you share the missing details to continue?"
            )
            await self._emit_event(
                event_type="ClarificationRequested",
                message="More detail is needed to continue.",
                visibility=AgentEventVisibility.public,
                source="agent:Clarify",
                details={
                    "question": clarifying_question,
                    "missing_slots": preflight.clarification_decision.missing_blocking_slots,
                },
            )
            return {
                "sql_canonical": "",
                "sql_executable": "",
                "dialect": "n/a",
                "asset": "",
                "asset_type": None,
                "analysis_path": None,
                "execution_mode": None,
                "result": {},
                "visualization": None,
                "summary": f"I need one clarification before continuing: {clarifying_question}",
                "diagnostics": {
                    "clarifying_question": clarifying_question,
                    "classification": classification.model_dump(),
                    "resolved_entities": resolved_entities.model_dump(),
                    "clarification_state": clarification_state.model_dump(),
                    "assumptions_applied": assumptions_applied,
                    "plan": None,
                    "iterations_diagnostics": [],
                },
                "tool_calls": [],
            }

        if not self.planning_agent or not self.reasoning_agent:
            raise RuntimeError("Planning and reasoning agents must be configured for supervisor execution.")

        iteration_diagnostics: Dict[str, Any] = {}
        iteration_diagnostics_history: list[Dict[str, Any]] = []
        iterations_completed = 0

        for iteration in range(self.reasoning_agent.max_iterations):
            await self._emit_event(
                event_type="PlanningIterationStarted",
                message=f"Planning iteration {iteration + 1} started.",
                visibility=AgentEventVisibility.internal,
                source="planner",
                details={"iteration": iteration},
            )
            planner_request = self._build_planner_request(
                user_query,
                filters=filters,
                limit=limit,
                title=title,
                extra_context=extra_context,
                constraints=planning_constraints,
            )
            plan = await asyncio.to_thread(self.planning_agent.plan, planner_request)
            await self._emit_event(
                event_type="PlanCreated",
                message=f"Planned route: {plan.route}.",
                visibility=AgentEventVisibility.public,
                source="planner",
                details={"iteration": iteration, "route": plan.route, "step_count": len(plan.steps)},
            )
            await self._emit_event(
                event_type="PlanCreatedInternal",
                message="Detailed plan generated.",
                visibility=AgentEventVisibility.internal,
                source="planner",
                details={"iteration": iteration, "plan": plan.model_dump()},
            )
            artifacts = await self._execute_plan(
                plan,
                user_query=user_query,
                filters=filters,
                limit=limit,
                title=title,
            )
            self._merge_artifacts(combined_artifacts, artifacts)

            iteration_diagnostics = {
                "iteration": iteration,
                "plan_route": plan.route,
                "extra_context": extra_context,
            }
            if artifacts.clarifying_question:
                iteration_diagnostics["clarifying_question"] = artifacts.clarifying_question
            iteration_diagnostics_history.append(iteration_diagnostics)
            
            final_decision = await asyncio.to_thread(
                self.reasoning_agent.evaluate,
                iteration=iteration,
                plan=plan,
                artifacts=artifacts,
                diagnostics=iteration_diagnostics,
                user_query=user_query,
            )
            iterations_completed = iteration + 1

            await self._emit_event(
                event_type="ReasoningDecision",
                message="Reasoning pass completed.",
                visibility=AgentEventVisibility.internal,
                source="reasoning",
                details={
                    "iteration": iteration,
                    "continue_planning": final_decision.continue_planning if final_decision else False,
                    "rationale": final_decision.rationale if final_decision else None,
                },
            )

            if not final_decision.continue_planning:
                break

            extra_context = self._merge_context(extra_context, final_decision.updated_context or {})
        else:
            # Loop exhausted without final decision; treat last pass as final.
            self.logger.warning("Reasoning agent exhausted max iterations without convergence.")
            await self._emit_event(
                event_type="ReasoningMaxIterationsReached",
                message="Reached max reasoning iterations.",
                visibility=AgentEventVisibility.public,
                source="reasoning",
            )
            if not final_decision or final_decision.continue_planning:
                final_decision = ReasoningDecision(
                    continue_planning=False,
                    rationale="Max iterations reached without convergence.",
                )

        if plan is None or artifacts is None:
            raise RuntimeError("Planner did not produce a plan or artifacts.")

        analyst_result = artifacts.analyst_result or self._build_empty_analyst_response(
            error_message=artifacts.clarifying_question
            or "Planner route completed without invoking analyst agent.",
        )
        data_payload = artifacts.data_payload
        if not data_payload and artifacts.research_result:
            data_payload = self._coerce_research_payload(artifacts.research_result)
        visualization = artifacts.visualization
        if not visualization and data_payload and self.visual_agent:
            try:
                visualization = await asyncio.to_thread(
                    self.visual_agent.run,
                    data_payload,
                    title=f"Visualization for '{user_query}'",
                    question=user_query,
                )
                await self._emit_event(
                    event_type="VisualizationAutoGenerated",
                    message="Generated a visualization from result data.",
                    visibility=AgentEventVisibility.public,
                    source="agent:Visual",
                )
            except Exception as exc:  # pragma: no cover - visualization is best-effort
                self.logger.warning("Auto visualization failed: %s", exc)
                await self._emit_event(
                    event_type="VisualizationAutoGenerationFailed",
                    message="Could not auto-generate a visualization.",
                    visibility=AgentEventVisibility.internal,
                    source="agent:Visual",
                    details={"error": str(exc)},
                )
        visualization = self._ensure_requested_visualization(
            user_query=user_query,
            data_payload=data_payload,
            visualization=visualization,
        )
        analyst_grounding = build_analyst_grounding(user_query, data_payload)

        elapsed_ms = int((time.perf_counter() - start) * 1000)

        diagnostics: Dict[str, Any] = {
            "execution_time_ms": analyst_result.execution_time_ms,
            "total_elapsed_ms": elapsed_ms,
            "sql_executable": analyst_result.sql_executable,
            "sql_canonical": analyst_result.sql_canonical,
            "error": analyst_result.error,
            "dialect": analyst_result.dialect,
            "analysis_path": analyst_result.analysis_path,
            "execution_mode": analyst_result.execution_mode,
            "asset_type": analyst_result.asset_type,
            "asset_name": analyst_result.asset_name,
            "selected_datasets": [dataset.model_dump(mode="json") for dataset in analyst_result.selected_datasets],
            "iterations_diagnostics": iteration_diagnostics_history,
            "plan": plan.model_dump(),
            "classification": classification.model_dump(),
            "resolved_entities": resolved_entities.model_dump(),
            "clarification_state": clarification_state.model_dump(),
            "assumptions_applied": assumptions_applied,
            "response_mode": getattr(self.response_mode, "value", None) or str(self.response_mode or "analyst"),
            "analyst_grounding": analyst_grounding,
        }
        if artifacts.research_result:
            diagnostics["research"] = artifacts.research_result.to_dict()
        web_search_result = artifacts.web_search_result or combined_artifacts.web_search_result
        if web_search_result:
            diagnostics["web_search"] = web_search_result.to_dict()
        if artifacts.clarifying_question:
            diagnostics["clarifying_question"] = artifacts.clarifying_question
        diagnostics["reasoning"] = {
            "iterations": iterations_completed,
            "final_rationale": final_decision.rationale if final_decision else None,
        }
        summary = self._build_response_summary(
            user_query=user_query,
            data_payload=data_payload,
            visualization=visualization,
            analyst_result=analyst_result,
            clarifying_question=artifacts.clarifying_question,
            web_search_result=web_search_result,
            research_result=artifacts.research_result or combined_artifacts.research_result,
            assumptions_applied=assumptions_applied,
            response_mode=self.response_mode,
            analyst_grounding=analyst_grounding,
        )

        self.logger.info(
            "Planner route '%s' completed in %sms for query '%s'",
            plan.route,
            elapsed_ms,
            user_query,
        )
        await self._emit_event(
            event_type="AgentRunCompleted",
            message="Response is ready.",
            visibility=AgentEventVisibility.public,
            source="supervisor",
            details={"elapsed_ms": elapsed_ms, "route": plan.route},
        )

        return {
            "sql_canonical": analyst_result.sql_canonical,
            "sql_executable": analyst_result.sql_executable,
            "dialect": analyst_result.dialect,
            "asset": analyst_result.asset_name,
            "asset_type": analyst_result.asset_type,
            "analysis_path": analyst_result.analysis_path,
            "execution_mode": analyst_result.execution_mode,
            "result": data_payload,
            "visualization": visualization,
            "summary": summary,
            "diagnostics": diagnostics,
            "tool_calls": combined_artifacts.tool_calls,
        }

    def _build_planner_request(
        self,
        user_query: str,
        *,
        filters: Optional[Dict[str, Any]],
        limit: Optional[int],
        title: Optional[str],
        extra_context: Optional[Dict[str, Any]] = None,
        constraints: Optional[PlanningConstraints] = None,
    ) -> PlannerRequest:
        context: Dict[str, Any] = {}
        if filters is not None:
            context["filters"] = filters
        if limit is not None:
            context["limit"] = limit
        if title:
            context["title"] = title
        if extra_context:
            context.update(extra_context)
        return PlannerRequest(
            question=user_query,
            context=context or None,
            constraints=constraints or PlanningConstraints(),
        )

    async def _run_preflight(
        self,
        *,
        user_query: str,
        planning_context: Dict[str, Any],
    ) -> SupervisorPreflight:
        classification = await self.question_classifier.classify_async(
            user_query,
            context=planning_context,
        )
        resolved_entities = await self.entity_resolver.resolve_async(
            user_query,
            classification=classification,
            context=planning_context,
        )
        prior_state = self._coerce_clarification_state(planning_context.get("clarification_state"))
        clarification_decision = self.clarification_manager.decide(
            classification=classification,
            prior_state=prior_state,
        )

        context_updates: Dict[str, Any] = {
            "classification": classification.model_dump(),
            "resolved_entities": resolved_entities.model_dump(),
            "clarification_state": clarification_decision.updated_state.model_dump(),
        }

        routing_context: Dict[str, Any] = {}
        route_hint = classification.route_hint
        if (
            route_hint
            and route_hint in {route.value for route in RouteName}
            and route_hint != RouteName.CLARIFY.value
            and classification.confidence >= 0.55
        ):
            routing_context["force_route"] = route_hint

        if clarification_decision.assumptions:
            routing_context["avoid_routes"] = [RouteName.CLARIFY.value]
            context_updates["assumptions"] = clarification_decision.assumptions

        if routing_context:
            context_updates["routing"] = routing_context

        memory_context = self._build_memory_context(planning_context)
        if memory_context:
            existing_context = planning_context.get("conversation_context")
            context_updates["conversation_context"] = self._merge_conversation_context(
                existing_context if isinstance(existing_context, str) else None,
                memory_context,
            )

        return SupervisorPreflight(
            classification=classification,
            resolved_entities=resolved_entities,
            clarification_decision=clarification_decision,
            context_updates=context_updates,
        )

    @staticmethod
    def _coerce_clarification_state(payload: Any) -> ClarificationState:
        if isinstance(payload, ClarificationState):
            return payload
        if isinstance(payload, dict):
            try:
                return ClarificationState.model_validate(payload)
            except Exception:
                return ClarificationState()
        return ClarificationState()

    @staticmethod
    def _build_memory_context(planning_context: Dict[str, Any]) -> str:
        sections: list[str] = []
        short_term = planning_context.get("short_term_context")
        if isinstance(short_term, str) and short_term.strip():
            sections.append(f"Short-term conversation context:\n{short_term.strip()}")

        retrieved = planning_context.get("retrieved_memories")
        if isinstance(retrieved, list) and retrieved:
            memory_lines: list[str] = []
            for item in retrieved[:8]:
                if not isinstance(item, dict):
                    continue
                category = str(item.get("category") or "memory")
                content = str(item.get("content") or "").strip()
                if not content:
                    continue
                memory_lines.append(f"[{category}] {content}")
            if memory_lines:
                sections.append("Relevant long-term memories:\n" + "\n".join(memory_lines))

        return "\n\n".join(sections).strip()

    async def _execute_plan(
        self,
        plan: Plan,
        *,
        user_query: str,
        filters: Optional[Dict[str, Any]],
        limit: Optional[int],
        title: Optional[str],
    ) -> PlanExecutionArtifacts:
        artifacts = PlanExecutionArtifacts()
        step_outputs: Dict[str, Dict[str, Any]] = {}

        for step in plan.steps:
            agent_name = step.agent
            await self._emit_event(
                event_type="AgentStepStarted",
                message=f"{agent_name} is running.",
                visibility=AgentEventVisibility.public,
                source=f"agent:{agent_name}",
                details={"step_id": step.id},
            )
            await self._emit_event(
                event_type="AgentStepStartedInternal",
                message="Agent step started with inputs.",
                visibility=AgentEventVisibility.internal,
                source=f"agent:{agent_name}",
                details={"step_id": step.id, "input": step.input},
            )

            if agent_name == AgentName.ANALYST.value:
                step_start = time.perf_counter()
                tool_args: Dict[str, Any] = {"step_id": step.id, "input": step.input}
                try:
                    analyst_result, data_payload, tool_args = await self._run_analyst_step(
                        step,
                        user_query=user_query,
                        default_filters=filters,
                        default_limit=limit,
                        step_outputs=step_outputs,
                    )
                except Exception as exc:
                    duration_ms = int((time.perf_counter() - step_start) * 1000)
                    artifacts.tool_calls.append(
                        {
                            "tool_name": AgentName.ANALYST.value,
                            "arguments": tool_args,
                            "result": None,
                            "duration_ms": duration_ms,
                            "error": {"message": str(exc), "type": exc.__class__.__name__},
                        }
                    )
                    raise
                duration_ms = int((time.perf_counter() - step_start) * 1000)
                artifacts.tool_calls.append(
                    {
                        "tool_name": AgentName.ANALYST.value,
                        "arguments": tool_args,
                        "result": self._summarize_analyst_result(
                            analyst_result,
                            data_payload,
                            question=str(tool_args.get("query") or user_query),
                        ),
                        "duration_ms": duration_ms,
                        "error": self._coerce_tool_error(analyst_result.error),
                    }
                )
                artifacts.analyst_result = analyst_result
                if data_payload:
                    artifacts.data_payload = data_payload
                analyst_grounding = build_analyst_grounding(
                    str(tool_args.get("query") or user_query),
                    data_payload,
                )
                step_outputs[step.id] = {
                    "agent": AgentName.ANALYST.value,
                    "analyst_result": analyst_result,
                    "data_payload": data_payload,
                    "analyst_grounding": analyst_grounding,
                }
                await self._emit_event(
                    event_type="AgentStepCompleted",
                    message="Analyst step completed.",
                    visibility=AgentEventVisibility.public,
                    source="agent:Analyst",
                    details={"step_id": step.id},
                )
                continue

            if agent_name == AgentName.VISUAL.value:
                step_start = time.perf_counter()
                tool_args = {"step_id": step.id, "input": step.input}
                try:
                    visualization, tool_args = await self._run_visual_step(
                        step,
                        user_query=user_query,
                        title=title,
                        fallback_payload=artifacts.data_payload,
                        step_outputs=step_outputs,
                    )
                except Exception as exc:
                    duration_ms = int((time.perf_counter() - step_start) * 1000)
                    artifacts.tool_calls.append(
                        {
                            "tool_name": AgentName.VISUAL.value,
                            "arguments": tool_args,
                            "result": None,
                            "duration_ms": duration_ms,
                            "error": {"message": str(exc), "type": exc.__class__.__name__},
                        }
                    )
                    raise
                duration_ms = int((time.perf_counter() - step_start) * 1000)
                artifacts.tool_calls.append(
                    {
                        "tool_name": AgentName.VISUAL.value,
                        "arguments": tool_args,
                        "result": visualization,
                        "duration_ms": duration_ms,
                        "error": None,
                    }
                )
                artifacts.visualization = visualization
                step_outputs[step.id] = {
                    "agent": AgentName.VISUAL.value,
                    "visualization": visualization,
                }
                await self._emit_event(
                    event_type="AgentStepCompleted",
                    message="Visualization step completed.",
                    visibility=AgentEventVisibility.public,
                    source="agent:Visual",
                    details={"step_id": step.id},
                )
                continue

            if agent_name == AgentName.DOC_RETRIEVAL.value:
                step_start = time.perf_counter()
                tool_args = {"step_id": step.id, "input": step.input}
                try:
                    research_result, tool_args = await self._run_doc_retrieval_step(
                        step, user_query, step_outputs
                    )
                except Exception as exc:
                    duration_ms = int((time.perf_counter() - step_start) * 1000)
                    artifacts.tool_calls.append(
                        {
                            "tool_name": AgentName.DOC_RETRIEVAL.value,
                            "arguments": tool_args,
                            "result": None,
                            "duration_ms": duration_ms,
                            "error": {"message": str(exc), "type": exc.__class__.__name__},
                        }
                    )
                    raise
                duration_ms = int((time.perf_counter() - step_start) * 1000)
                artifacts.tool_calls.append(
                    {
                        "tool_name": AgentName.DOC_RETRIEVAL.value,
                        "arguments": tool_args,
                        "result": research_result.to_dict() if research_result else None,
                        "duration_ms": duration_ms,
                        "error": None,
                    }
                )
                artifacts.research_result = research_result
                step_outputs[step.id] = {
                    "agent": AgentName.DOC_RETRIEVAL.value,
                    "research_result": research_result,
                }
                if not artifacts.data_payload:
                    artifacts.data_payload = research_result.to_tabular()
                await self._emit_event(
                    event_type="AgentStepCompleted",
                    message="Research step completed.",
                    visibility=AgentEventVisibility.public,
                    source="agent:DocRetrieval",
                    details={"step_id": step.id},
                )
                continue

            if agent_name == AgentName.WEB_SEARCH.value:
                step_start = time.perf_counter()
                tool_args = {"step_id": step.id, "input": step.input}
                try:
                    web_search_result, tool_args = await self._run_web_search_step(step, user_query)
                except Exception as exc:
                    duration_ms = int((time.perf_counter() - step_start) * 1000)
                    artifacts.tool_calls.append(
                        {
                            "tool_name": AgentName.WEB_SEARCH.value,
                            "arguments": tool_args,
                            "result": None,
                            "duration_ms": duration_ms,
                            "error": {"message": str(exc), "type": exc.__class__.__name__},
                        }
                    )
                    raise
                duration_ms = int((time.perf_counter() - step_start) * 1000)
                artifacts.tool_calls.append(
                    {
                        "tool_name": AgentName.WEB_SEARCH.value,
                        "arguments": tool_args,
                        "result": web_search_result.to_dict() if web_search_result else None,
                        "duration_ms": duration_ms,
                        "error": None,
                    }
                )
                artifacts.web_search_result = web_search_result
                step_outputs[step.id] = {
                    "agent": AgentName.WEB_SEARCH.value,
                    "web_search_result": web_search_result,
                    "documents": web_search_result.to_documents(),
                }
                if not artifacts.data_payload:
                    artifacts.data_payload = web_search_result.to_tabular()
                await self._emit_event(
                    event_type="AgentStepCompleted",
                    message="Web search step completed.",
                    visibility=AgentEventVisibility.public,
                    source="agent:WebSearch",
                    details={"step_id": step.id},
                )
                continue

            if agent_name == AgentName.CLARIFY.value:
                artifacts.clarifying_question = step.input.get("clarifying_question")
                self.logger.info("Planner requested clarification: %s", artifacts.clarifying_question)
                await self._emit_event(
                    event_type="ClarificationRequested",
                    message="More detail is needed to continue.",
                    visibility=AgentEventVisibility.public,
                    source="agent:Clarify",
                    details={"step_id": step.id, "question": artifacts.clarifying_question},
                )
                break

            self.logger.warning("Unsupported agent '%s' in plan; skipping step.", agent_name)

        return artifacts

    async def _emit_event(
        self,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility,
        source: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self.event_emitter:
            return
        try:
            await self.event_emitter.emit(
                event_type=event_type,
                message=message,
                visibility=visibility,
                source=source,
                details=details,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.warning("Failed to emit event %s: %s", event_type, exc)

    async def _run_analyst_step(
        self,
        step: PlanStep,
        *,
        user_query: str,
        default_filters: Optional[Dict[str, Any]],
        default_limit: Optional[int],
        step_outputs: Dict[str, Dict[str, Any]],
    ) -> tuple[AnalystQueryResponse, Dict[str, Any], Dict[str, Any]]:
        if not self.analyst_agent:
            raise RuntimeError("AnalystAgent is not configured but planner requested SQL analysis.")
        base_question = step.input.get("question") or user_query
        question = base_question
        context_overrides = step.input.get("context") or {}
        step_filters = context_overrides.get("filters", default_filters)
        step_limit = context_overrides.get("limit", default_limit)
        conversation_context = context_overrides.get("conversation_context")
        source_step_ref = step.input.get("source_step_ref")
        tool_context = self._build_step_context_summary(source_step_ref, step_outputs)
        if tool_context:
            conversation_context = self._merge_conversation_context(conversation_context, tool_context)

        if tool_context:
            rewritten = await asyncio.to_thread(
                self._rewrite_question_with_llm,
                question=question,
                tool_context=tool_context,
                original_question=step.input.get("original_question") or user_query,
            )
            if rewritten:
                question = rewritten

        follow_up = step.input.get("follow_up")
        if isinstance(follow_up, str) and follow_up.strip():
            if follow_up.strip().lower() not in question.lower():
                question = f"{question}\nFollow-up: {follow_up.strip()}"

        analyst_result: AnalystQueryResponse = await self.analyst_agent.answer_async(
            question,
            conversation_context=conversation_context,
            filters=step_filters,
            limit=step_limit,
        )
        data_payload = self._extract_data_payload(analyst_result)
        tool_args: Dict[str, Any] = {
            "step_id": step.id,
            "input": step.input,
            "question": question,
            "filters": step_filters,
            "limit": step_limit,
        }
        if base_question and base_question != question:
            tool_args["original_question"] = base_question
        if conversation_context:
            tool_args["conversation_context"] = conversation_context
        if source_step_ref:
            tool_args["source_step_ref"] = source_step_ref
        return analyst_result, data_payload, tool_args

    async def _run_visual_step(
        self,
        step: PlanStep,
        *,
        user_query: str,
        title: Optional[str],
        fallback_payload: Dict[str, Any],
        step_outputs: Dict[str, Dict[str, Any]],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        reference_id = step.input.get("rows_ref")
        referenced_payload = self._resolve_rows_reference(reference_id, step_outputs)
        data = referenced_payload or fallback_payload or {"columns": [], "rows": []}
        viz_title = title or f"Visualization for '{user_query}'"
        user_intent = step.input.get("user_intent")
        visualization = await asyncio.to_thread(
            self.visual_agent.run,
            data,
            title=viz_title,
            question=user_query,
            user_intent=user_intent,
        )
        tool_args: Dict[str, Any] = {
            "step_id": step.id,
            "input": step.input,
            "question": user_query,
            "title": viz_title,
            "data_summary": self._summarize_tabular_payload(data),
        }
        if reference_id:
            tool_args["rows_ref"] = reference_id
        if user_intent:
            tool_args["user_intent"] = user_intent
        return visualization, tool_args

    async def _run_doc_retrieval_step(
        self,
        step: PlanStep,
        user_query: str,
        step_outputs: Dict[str, Dict[str, Any]],
    ) -> tuple[DeepResearchResult, Dict[str, Any]]:
        if not self.deep_research_agent:
            raise RuntimeError("DeepResearchAgent is not configured but planner requested DocRetrieval.")
        context = step.input.get("context") or {}
        source_step_ref = step.input.get("source_step_ref")
        if source_step_ref:
            documents = self._resolve_documents_reference(source_step_ref, step_outputs)
            if documents:
                context = self._merge_document_context(context, documents)
        timebox = int(step.input.get("timebox_seconds", 30))
        question = step.input.get("question") or user_query
        result = await self.deep_research_agent.research_async(
            question=question,
            context=context,
            timebox_seconds=timebox,
        )
        await self._emit_event(
            event_type="ResearchTraceAvailable",
            message="Deep research trace captured.",
            visibility=AgentEventVisibility.internal,
            source="agent:DocRetrieval",
            details={
                "step_id": step.id,
                "question": question,
                "plan": result.plan.model_dump(mode="json") if result.plan else None,
                "state": result.state.model_dump(mode="json") if result.state else None,
                "sources": [
                    {
                        "id": item.id,
                        "source_type": item.source_type,
                        "source": item.source,
                        "source_ref": item.source_ref,
                        "domain": item.domain,
                        "score": item.score,
                    }
                    for item in result.evidence[:12]
                ],
            },
        )
        tool_args: Dict[str, Any] = {
            "step_id": step.id,
            "input": step.input,
            "question": question,
            "context": context,
            "timebox_seconds": timebox,
        }
        if source_step_ref:
            tool_args["source_step_ref"] = source_step_ref
        return result, tool_args

    async def _run_web_search_step(
        self,
        step: PlanStep,
        user_query: str,
    ) -> tuple[WebSearchResult, Dict[str, Any]]:
        if not self.web_search_agent:
            raise RuntimeError("WebSearchAgent is not configured but planner requested WebSearch.")
        query = step.input.get("query") or user_query
        context = step.input.get("context") or {}
        max_results = step.input.get("max_results", 6)
        region = step.input.get("region") or context.get("region")
        safe_search = step.input.get("safe_search") or context.get("safe_search")
        timebox = int(step.input.get("timebox_seconds", 10))
        try:
            max_results_value = int(max_results)
        except (TypeError, ValueError):
            max_results_value = 6

        result = await self.web_search_agent.search_async(
            query,
            max_results=max_results_value,
            region=region,
            safe_search=safe_search,
            timebox_seconds=timebox,
        )
        tool_args: Dict[str, Any] = {
            "step_id": step.id,
            "input": step.input,
            "query": query,
            "max_results": max_results_value,
            "region": region,
            "safe_search": safe_search,
            "timebox_seconds": timebox,
        }
        return result, tool_args

    @staticmethod
    def _coerce_tool_error(error: Any) -> Optional[Dict[str, Any]]:
        if not error:
            return None
        if isinstance(error, dict):
            return error
        return {"message": str(error)}

    @staticmethod
    def _summarize_tabular_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        columns = payload.get("columns")
        if isinstance(columns, list):
            summary["columns"] = columns
        rows = payload.get("rows")
        if isinstance(rows, list):
            summary["row_count"] = len(rows)
        return summary

    @staticmethod
    def _summarize_analyst_result(
        analyst_result: AnalystQueryResponse,
        data_payload: Dict[str, Any],
        *,
        question: str | None = None,
    ) -> Dict[str, Any]:
        summary: Dict[str, Any] = {
            "analysis_path": analyst_result.analysis_path,
            "execution_mode": analyst_result.execution_mode,
            "asset_type": analyst_result.asset_type,
            "asset_name": analyst_result.asset_name,
            "sql_canonical": analyst_result.sql_canonical,
            "sql_executable": analyst_result.sql_executable,
            "dialect": analyst_result.dialect,
            "execution_time_ms": analyst_result.execution_time_ms,
            "selected_datasets": [dataset.model_dump(mode="json") for dataset in analyst_result.selected_datasets],
        }
        query_result = analyst_result.result
        if query_result:
            row_count = query_result.rowcount
            if row_count is None and isinstance(query_result.rows, list):
                row_count = len(query_result.rows)
            if row_count is not None:
                summary["row_count"] = row_count
            if query_result.elapsed_ms is not None:
                summary["elapsed_ms"] = query_result.elapsed_ms
            if query_result.columns:
                summary["columns"] = list(query_result.columns)
            if query_result.source_sql:
                summary["source_sql"] = query_result.source_sql
        elif data_payload:
            summary.update(SupervisorOrchestrator._summarize_tabular_payload(data_payload))
        if not analyst_result.error and data_payload:
            summary["analyst_grounding"] = build_analyst_grounding(str(question or ""), data_payload)
        return summary

    @staticmethod
    def _build_response_summary(
        *,
        user_query: str,
        data_payload: Dict[str, Any],
        visualization: Dict[str, Any] | None,
        analyst_result: AnalystQueryResponse,
        clarifying_question: str | None,
        web_search_result: WebSearchResult | None = None,
        research_result: DeepResearchResult | None = None,
        assumptions_applied: Sequence[str] | None = None,
        response_mode: ResponseMode = ResponseMode.analyst,
        analyst_grounding: Dict[str, Any] | None = None,
    ) -> str:
        if clarifying_question:
            return f"I need one clarification before continuing: {clarifying_question}"

        assumptions = [item for item in (assumptions_applied or []) if isinstance(item, str) and item.strip()]

        if research_result:
            summary = SupervisorOrchestrator._format_research_summary(research_result, user_query=user_query)
            if assumptions:
                summary = summary + "\n\nAssumptions: " + "; ".join(assumptions)
            return summary

        if web_search_result and not data_payload:
            if web_search_result.answer:
                summary = web_search_result.answer
            elif web_search_result.results:
                summary = f"Found {len(web_search_result.results)} web sources for '{user_query}'."
            else:
                follow_up = web_search_result.follow_up_question or "Could you narrow the topic or provide a target source?"
                summary = (
                    "I could not find strong enough external sources to answer confidently. "
                    f"{follow_up}"
                )
            if assumptions:
                summary = summary + " Assumptions: " + "; ".join(assumptions)
            return summary

        if analyst_result.error:
            return f"I could not complete that request: {analyst_result.error}"

        rows = data_payload.get("rows") if isinstance(data_payload, dict) else None
        columns = data_payload.get("columns") if isinstance(data_payload, dict) else None
        row_count = len(rows) if isinstance(rows, list) else 0
        col_count = len(columns) if isinstance(columns, list) else 0

        if response_mode == ResponseMode.analyst:
            grounding = analyst_grounding if isinstance(analyst_grounding, dict) else build_analyst_grounding(
                user_query,
                data_payload,
            )
            return compose_analyst_summary(
                grounding,
                assumptions=assumptions,
                extra_note=SupervisorOrchestrator._build_visualization_note(
                    user_query=user_query,
                    visualization=visualization,
                ),
            )

        if row_count == 0:
            summary = "Completed, but no tabular rows were returned."
            if assumptions:
                summary = summary + " Assumptions: " + "; ".join(assumptions)
            return summary

        requested_chart = SupervisorOrchestrator._detect_requested_chart_type(user_query)
        if visualization and isinstance(visualization, dict):
            chart_type_raw = visualization.get("chart_type") or visualization.get("chartType")
            chart_type = chart_type_raw.lower() if isinstance(chart_type_raw, str) else None
            options = visualization.get("options") if isinstance(visualization.get("options"), dict) else {}
            warning = options.get("visualization_warning") if isinstance(options, dict) else None
            if isinstance(warning, str) and warning.strip():
                summary = (
                    f"Found {row_count} rows across {col_count} columns for '{user_query}'. "
                    f"{warning.strip()}"
                )
                if assumptions:
                    summary = summary + " Assumptions: " + "; ".join(assumptions)
                return summary
            if requested_chart and chart_type and chart_type != requested_chart:
                summary = (
                    f"Found {row_count} rows across {col_count} columns for '{user_query}'. "
                    f"I could not prepare the requested {requested_chart} chart from this dataset."
                )
                if assumptions:
                    summary = summary + " Assumptions: " + "; ".join(assumptions)
                return summary
            if chart_type and chart_type != "table":
                summary = (
                    f"Found {row_count} rows across {col_count} columns for '{user_query}'. "
                    f"I also prepared a {chart_type} visualization."
                )
                if assumptions:
                    summary = summary + " Assumptions: " + "; ".join(assumptions)
                return summary
            if requested_chart and chart_type == "table":
                summary = (
                    f"Found {row_count} rows across {col_count} columns for '{user_query}'. "
                    f"I could not prepare the requested {requested_chart} chart from this dataset."
                )
                if assumptions:
                    summary = summary + " Assumptions: " + "; ".join(assumptions)
                return summary

        if requested_chart:
            summary = (
                f"Found {row_count} rows across {col_count} columns for '{user_query}'. "
                f"I could not prepare the requested {requested_chart} chart from this dataset."
            )
            if assumptions:
                summary = summary + " Assumptions: " + "; ".join(assumptions)
            return summary

        summary = f"Found {row_count} rows across {col_count} columns for '{user_query}'."
        if assumptions:
            summary = summary + " Assumptions: " + "; ".join(assumptions)
        return summary

    @staticmethod
    def _build_visualization_note(
        *,
        user_query: str,
        visualization: Dict[str, Any] | None,
    ) -> str | None:
        requested_chart = SupervisorOrchestrator._detect_requested_chart_type(user_query)
        if not isinstance(visualization, dict) or not visualization:
            if requested_chart:
                return f"I could not prepare the requested {requested_chart} chart from this dataset."
            return None

        chart_type_raw = visualization.get("chart_type") or visualization.get("chartType")
        chart_type = chart_type_raw.lower() if isinstance(chart_type_raw, str) else None
        options = visualization.get("options") if isinstance(visualization.get("options"), dict) else {}
        warning = options.get("visualization_warning") if isinstance(options, dict) else None
        if isinstance(warning, str) and warning.strip():
            return warning.strip()
        if requested_chart and chart_type and chart_type != requested_chart:
            return f"I could not prepare the requested {requested_chart} chart from this dataset."
        if requested_chart and chart_type == "table":
            return f"I could not prepare the requested {requested_chart} chart from this dataset."
        if chart_type and chart_type != "table":
            return f"I also prepared a {chart_type} visualization."
        return None

    @staticmethod
    def _format_research_summary(result: DeepResearchResult, *, user_query: str) -> str:
        report = result.report
        if not report:
            return result.synthesis or f"Completed deep research for '{user_query}'."

        lines: list[str] = []
        lines.append("Executive summary:")
        lines.append(report.executive_summary.strip() or f"Completed deep research for '{user_query}'.")

        lines.append("")
        lines.append("Key findings:")
        if report.key_findings:
            for finding in report.key_findings[:5]:
                citations = ", ".join(finding.citations) if finding.citations else "no citation"
                lines.append(f"- {finding.claim} [{finding.confidence}] ({citations})")
        else:
            lines.append("- No high-confidence findings could be supported by current evidence.")

        lines.append("")
        lines.append("Supporting evidence:")
        if report.supporting_evidence:
            for finding_id, evidence_ids in list(report.supporting_evidence.items())[:8]:
                evidence_text = ", ".join(evidence_ids) if evidence_ids else "none"
                lines.append(f"- {finding_id}: {evidence_text}")
        else:
            lines.append("- No explicit evidence mapping was produced.")

        lines.append("")
        lines.append("Risks/uncertainties:")
        if report.risks_uncertainties:
            for risk in report.risks_uncertainties[:4]:
                lines.append(f"- {risk}")
        else:
            lines.append("- None noted.")

        lines.append("")
        lines.append("What I'd do next:")
        if report.next_steps:
            for step in report.next_steps[:4]:
                lines.append(f"- {step}")
        else:
            lines.append("- Gather additional primary sources for unresolved subquestions.")
        if report.follow_up_question:
            lines.append(f"- Follow-up needed: {report.follow_up_question}")

        return "\n".join(lines).strip()

    @staticmethod
    def _detect_requested_chart_type(question: str) -> str | None:
        text = str(question or "").lower()
        if not text:
            return None
        if "pie chart" in text or "pie " in text or "donut" in text or "doughnut" in text:
            return "pie"
        if "bar chart" in text or "bar graph" in text:
            return "bar"
        if "line chart" in text or "line graph" in text:
            return "line"
        if "scatter plot" in text or "scatter chart" in text:
            return "scatter"
        return None

    def _ensure_requested_visualization(
        self,
        *,
        user_query: str,
        data_payload: Dict[str, Any],
        visualization: Dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        requested_chart = self._detect_requested_chart_type(user_query)
        if not requested_chart:
            return visualization

        existing_type_raw = None
        if isinstance(visualization, dict):
            existing_type_raw = visualization.get("chart_type") or visualization.get("chartType")
        existing_type = str(existing_type_raw).lower() if isinstance(existing_type_raw, str) else None
        if existing_type == requested_chart:
            return visualization

        requested_spec = self._build_requested_visualization_spec(requested_chart, data_payload)
        if requested_spec is None:
            return visualization
        return requested_spec

    @staticmethod
    def _build_requested_visualization_spec(
        chart_type: str,
        data_payload: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        if not isinstance(data_payload, dict):
            return None

        columns_raw = data_payload.get("columns")
        rows_raw = data_payload.get("rows")
        if not isinstance(columns_raw, list) or not isinstance(rows_raw, list) or not columns_raw or not rows_raw:
            return None

        columns = [str(column) for column in columns_raw]
        sample_rows = rows_raw[: min(20, len(rows_raw))]

        def get_cell(row: Any, index: int) -> Any:
            if isinstance(row, (list, tuple)):
                return row[index] if index < len(row) else None
            if isinstance(row, dict):
                key = columns[index]
                return row.get(key)
            return None

        def is_numeric_column(index: int) -> bool:
            seen = 0
            numeric = 0
            for row in sample_rows:
                value = get_cell(row, index)
                if value is None:
                    continue
                seen += 1
                if SupervisorOrchestrator._coerce_numeric(value) is not None:
                    numeric += 1
            if seen == 0:
                return False
            return numeric / seen >= 0.6

        numeric_indexes = [idx for idx in range(len(columns)) if is_numeric_column(idx)]
        if not numeric_indexes:
            return None

        measure_index = numeric_indexes[0]
        non_numeric_indexes = [idx for idx in range(len(columns)) if idx not in numeric_indexes]
        dimension_index = non_numeric_indexes[0] if non_numeric_indexes else None

        if chart_type in {"pie", "bar", "line"}:
            if dimension_index is None:
                return None
            return {
                "chart_type": chart_type,
                "x": columns[dimension_index],
                "y": columns[measure_index],
                "title": "Requested chart",
            }

        if chart_type == "scatter":
            if len(numeric_indexes) < 2:
                return None
            return {
                "chart_type": "scatter",
                "x": columns[numeric_indexes[0]],
                "y": columns[numeric_indexes[1]],
                "title": "Requested chart",
            }

        return None

    @staticmethod
    def _coerce_numeric(value: Any) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip().replace(",", "")
            cleaned = cleaned.replace("$", "").replace("£", "").replace("€", "")
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
    def _extract_data_payload(analyst_result: AnalystQueryResponse) -> Dict[str, Any]:
        if analyst_result.result:
            return {
                "columns": analyst_result.result.columns,
                "rows": analyst_result.result.rows,
            }
        return {}

    @staticmethod
    def _trim_text(value: str, limit: int = 280) -> str:
        cleaned = str(value or "").strip()
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[:limit].rstrip() + "..."

    @staticmethod
    def _merge_conversation_context(base: Optional[str], extra: str) -> str:
        base_text = str(base or "").strip()
        extra_text = str(extra or "").strip()
        if not extra_text:
            return base_text
        if base_text:
            return f"{base_text}\n\n{extra_text}"
        return extra_text

    def _build_step_context_summary(
        self,
        reference_id: Optional[str],
        step_outputs: Dict[str, Dict[str, Any]],
    ) -> Optional[str]:
        if not reference_id:
            return None
        referenced = step_outputs.get(reference_id)
        if not referenced:
            return None

        parts: list[str] = []
        research_result = referenced.get("research_result")
        if isinstance(research_result, DeepResearchResult):
            if research_result.synthesis:
                parts.append(
                    f"Research synthesis: {self._trim_text(research_result.synthesis, 360)}"
                )
            if research_result.findings:
                insights = "; ".join(
                    self._trim_text(finding.insight, 160) for finding in research_result.findings[:3]
                )
                parts.append(f"Research findings: {insights}")

        web_search_result = referenced.get("web_search_result")
        if isinstance(web_search_result, WebSearchResult) and web_search_result.results:
            sources = "; ".join(
                f"{self._trim_text(item.title, 100)} ({item.url})"
                for item in web_search_result.results[:3]
            )
            parts.append(f"Web sources: {sources}")

        data_payload = referenced.get("data_payload")
        if isinstance(data_payload, dict):
            columns = data_payload.get("columns")
            rows = data_payload.get("rows")
            if isinstance(columns, list) and columns:
                column_list = ", ".join(str(col) for col in columns[:8])
                parts.append(f"Data columns: {self._trim_text(column_list, 180)}")
            if isinstance(rows, list):
                parts.append(f"Row count: {len(rows)}")
                if isinstance(columns, list):
                    samples = self._extract_sample_values(columns, rows)
                    if samples:
                        parts.extend(samples)

        analyst_grounding = referenced.get("analyst_grounding")
        if isinstance(analyst_grounding, dict):
            observed_facts = analyst_grounding.get("observed_facts")
            if isinstance(observed_facts, list) and observed_facts:
                facts = "; ".join(self._trim_text(str(item), 180) for item in observed_facts[:2])
                parts.append(f"Analyst findings: {facts}")
            interpretations = analyst_grounding.get("interpretations")
            if isinstance(interpretations, list) and interpretations:
                insights = "; ".join(self._trim_text(str(item), 180) for item in interpretations[:2])
                parts.append(f"Analyst interpretation: {insights}")
            caveats = analyst_grounding.get("caveats")
            if isinstance(caveats, list) and caveats:
                notes = "; ".join(self._trim_text(str(item), 160) for item in caveats[:1])
                parts.append(f"Analyst caveats: {notes}")

        if not parts:
            return None
        return "\n".join(parts)

    @staticmethod
    def _extract_json_blob(text: str) -> Optional[str]:
        if not text:
            return None
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        for index in range(start, len(text)):
            char = text[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start : index + 1]
        return None

    @classmethod
    def _parse_llm_payload(cls, response: str) -> Optional[Dict[str, Any]]:
        blob = cls._extract_json_blob(response)
        if not blob:
            return None
        try:
            parsed = json.loads(blob)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        return parsed

    def _rewrite_question_with_llm(
        self,
        *,
        question: str,
        tool_context: str,
        original_question: str,
    ) -> Optional[str]:
        llm = self.reasoning_agent.llm if self.reasoning_agent else None
        if not llm:
            return None

        prompt_sections = [
            "You rewrite analyst questions to align with known entity names.",
            "Return ONLY JSON with key: rewritten_question.",
            f"Original question: {original_question}",
            f"Current question: {question}",
            f"Tool context: {tool_context}",
        ]
        prompt = "\n".join(prompt_sections)
        try:
            response = llm.complete(prompt, temperature=0.0, max_tokens=160)
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.warning("LLM question rewrite failed: %s", exc)
            return None

        payload = self._parse_llm_payload(str(response))
        if not payload:
            return None
        rewritten = payload.get("rewritten_question") or payload.get("question")
        if not isinstance(rewritten, str):
            return None
        rewritten = rewritten.strip()
        if not rewritten:
            return None
        return rewritten

    @staticmethod
    def _extract_sample_values(columns: Sequence[Any], rows: Sequence[Any]) -> list[str]:
        if not rows or not columns:
            return []
        sample_lines: list[str] = []
        max_columns = 4
        max_rows = 6
        max_values = 4

        for col_index, col in enumerate(columns[:max_columns]):
            seen: list[str] = []
            for row in rows[:max_rows]:
                if not isinstance(row, (list, tuple)) or col_index >= len(row):
                    continue
                value = row[col_index]
                if value is None:
                    continue
                text = str(value).strip()
                if not text or len(text) > 80:
                    continue
                if text not in seen:
                    seen.append(text)
                if len(seen) >= max_values:
                    break
            if seen:
                sample_values = ", ".join(seen)
                sample_lines.append(f"Sample values for {col}: {sample_values}")

        return sample_lines

    @staticmethod
    def _merge_artifacts(base: PlanExecutionArtifacts, updates: PlanExecutionArtifacts) -> None:
        if updates.analyst_result:
            base.analyst_result = updates.analyst_result
        if updates.data_payload:
            base.data_payload = updates.data_payload
        if updates.visualization:
            base.visualization = updates.visualization
        if updates.research_result:
            base.research_result = updates.research_result
        if updates.web_search_result:
            base.web_search_result = updates.web_search_result
        if updates.clarifying_question:
            base.clarifying_question = updates.clarifying_question
        if updates.tool_calls:
            base.tool_calls.extend(updates.tool_calls)

    @staticmethod
    def _merge_context(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
        if not updates:
            return dict(base)
        merged = dict(base)
        for key, value in updates.items():
            if key == "reasoning" and isinstance(value, dict):
                existing = merged.get("reasoning")
                if isinstance(existing, dict):
                    merged["reasoning"] = {**existing, **value}
                else:
                    merged["reasoning"] = dict(value)
                continue
            if key == "documents" and isinstance(value, list):
                existing = merged.get("documents")
                if isinstance(existing, list):
                    merged["documents"] = existing + [doc for doc in value if doc not in existing]
                elif isinstance(existing, dict):
                    merged["documents"] = [existing] + list(value)
                else:
                    merged["documents"] = list(value)
                continue
            merged[key] = value
        return merged

    @staticmethod
    def _coerce_research_payload(result: DeepResearchResult) -> Dict[str, Any]:
        return result.to_tabular()

    @staticmethod
    def _merge_document_context(
        context: Dict[str, Any],
        documents: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        merged = dict(context)
        cleaned_docs = [doc for doc in documents if isinstance(doc, dict)]
        if not cleaned_docs:
            return merged
        existing = merged.get("documents")
        if isinstance(existing, list):
            merged["documents"] = existing + [doc for doc in cleaned_docs if doc not in existing]
        elif isinstance(existing, dict):
            merged["documents"] = [existing] + cleaned_docs
        else:
            merged["documents"] = cleaned_docs
        return merged

    @staticmethod
    def _resolve_rows_reference(
        reference_id: Optional[str],
        step_outputs: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not reference_id:
            return None
        referenced = step_outputs.get(reference_id)
        if not referenced:
            return None
        if "data_payload" in referenced:
            return referenced["data_payload"]
        if "research_result" in referenced:
            research_result: DeepResearchResult = referenced["research_result"]
            return research_result.to_tabular()
        return None

    @staticmethod
    def _resolve_documents_reference(
        reference_id: Optional[str],
        step_outputs: Dict[str, Dict[str, Any]],
    ) -> Optional[Sequence[Dict[str, Any]]]:
        if not reference_id:
            return None
        referenced = step_outputs.get(reference_id)
        if not referenced:
            return None
        documents = referenced.get("documents")
        if isinstance(documents, list):
            return [doc for doc in documents if isinstance(doc, dict)]
        web_search_result = referenced.get("web_search_result")
        if isinstance(web_search_result, WebSearchResult):
            return web_search_result.to_documents()
        return None

    @staticmethod
    def _build_empty_analyst_response(*, error_message: str) -> AnalystQueryResponse:
        return AnalystQueryResponse(
            analysis_path="dataset",
            execution_mode="federated",
            asset_type="dataset",
            asset_id="",
            asset_name="",
            sql_canonical="",
            sql_executable="",
            dialect="n/a",
            selected_datasets=[],
            result=None,
            error=error_message,
            execution_time_ms=None,
        )


__all__ = ["OrchestrationContext", "SupervisorOrchestrator"]
