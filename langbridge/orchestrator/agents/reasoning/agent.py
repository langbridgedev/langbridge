import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from langbridge.orchestrator.agents.models import PlanExecutionArtifacts
from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.agents.deep_research import DeepResearchResult
from langbridge.orchestrator.agents.planner import (
    AgentName,
    Plan,
    RouteName,
)
from langbridge.orchestrator.agents.planner.router import _extract_signals
from langbridge.orchestrator.tools.sql_analyst.interfaces import AnalystOutcomeStatus

@dataclass
class ReasoningDecision:
    """Outcome returned by the reasoning agent after each execution pass."""

    continue_planning: bool
    updated_context: Optional[Dict[str, Any]] = None
    rationale: Optional[str] = None


class ReasoningAgent:
    """Simple reasoning layer that decides whether additional planning is required."""

    _ENTITY_ALIAS_MAP: dict[str, tuple[str, ...]] = {
        "store": ("store", "shop", "outlet", "branch", "location"),
        "client": ("client", "customer", "account"),
        "product": ("product", "sku", "item"),
        "region": ("region", "territory", "area", "country"),
        "fund": ("fund", "portfolio", "strategy"),
        "team": ("team", "desk"),
        "sector": ("sector", "industry"),
        "channel": ("channel", "source"),
        "segment": ("segment",),
        "asset": ("asset",),
    }
    _MAX_ENTITY_RESOLUTION_ATTEMPTS = 1

    def __init__(
        self,
        *,
        max_iterations: int = 2,
        llm: Optional[LLMProvider] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if max_iterations < 1:
            raise ValueError("ReasoningAgent requires at least one iteration.")
        self.max_iterations = max_iterations
        self.logger = logger or logging.getLogger(__name__)
        self.llm = llm

    @staticmethod
    def _has_structured_data(artifacts: PlanExecutionArtifacts) -> bool:
        return bool(artifacts.analyst_result and artifacts.analyst_result.result)

    @staticmethod
    def _analyst_outcome_status(artifacts: PlanExecutionArtifacts) -> str | None:
        analyst_result = artifacts.analyst_result
        if not analyst_result or not analyst_result.outcome:
            return None
        return analyst_result.outcome.status.value

    @staticmethod
    def _analyst_error(artifacts: PlanExecutionArtifacts) -> str | None:
        analyst_result = artifacts.analyst_result
        if not analyst_result:
            return None
        outcome = analyst_result.outcome
        if outcome and not outcome.is_error:
            return None
        return (outcome.message if outcome else None) or analyst_result.error

    @staticmethod
    def _has_web_results(artifacts: PlanExecutionArtifacts) -> bool:
        return bool(artifacts.web_search_result and artifacts.web_search_result.results)

    @staticmethod
    def _has_research_results(artifacts: PlanExecutionArtifacts) -> bool:
        if not artifacts.research_result:
            return False
        return bool(artifacts.research_result.findings or artifacts.research_result.synthesis)

    @staticmethod
    def _is_low_signal_research(result: DeepResearchResult) -> bool:
        if not result.findings:
            return True
        if all(finding.source == "knowledge_base" for finding in result.findings):
            return True
        synthesis = (result.synthesis or "").lower()
        if "no documents provided" in synthesis or "reviewed 0 document" in synthesis:
            return True
        return False

    @staticmethod
    def _pick_fallback_route(current_route: Optional[str]) -> RouteName:
        if current_route == RouteName.WEB_SEARCH.value:
            return RouteName.DEEP_RESEARCH
        return RouteName.WEB_SEARCH

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

    def _parse_llm_payload(self, response: str) -> Optional[Dict[str, Any]]:
        blob = self._extract_json_blob(response)
        if not blob:
            return None
        try:
            parsed = json.loads(blob)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        return parsed

    @staticmethod
    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in {"true", "yes", "1"}:
                return True
            if cleaned in {"false", "no", "0"}:
                return False
        return None

    @staticmethod
    def _normalize_route_name(value: Any) -> Optional[RouteName]:
        if isinstance(value, RouteName):
            return value
        if value is None:
            return None
        cleaned = str(value).strip().lower()
        if not cleaned:
            return None
        for route in RouteName:
            if cleaned == route.value.lower() or cleaned == route.name.lower():
                return route
        alias_map = {
            "analyst": RouteName.SIMPLE_ANALYST,
            "simpleanalyst": RouteName.SIMPLE_ANALYST,
            "visual": RouteName.ANALYST_THEN_VISUAL,
            "chart": RouteName.ANALYST_THEN_VISUAL,
            "websearch": RouteName.WEB_SEARCH,
            "web": RouteName.WEB_SEARCH,
            "research": RouteName.DEEP_RESEARCH,
            "deepresearch": RouteName.DEEP_RESEARCH,
            "clarify": RouteName.CLARIFY,
        }
        return alias_map.get(cleaned)

    def _normalize_route_list(self, value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, (list, tuple, set)):
            items = value
        else:
            items = [value]
        routes: list[str] = []
        for item in items:
            route = self._normalize_route_name(item)
            if route and route.value not in routes:
                routes.append(route.value)
        return routes

    @staticmethod
    def _normalize_agent_name(value: Any) -> Optional[AgentName]:
        if isinstance(value, AgentName):
            return value
        if value is None:
            return None
        cleaned = str(value).strip().lower()
        if not cleaned:
            return None
        for agent in AgentName:
            if cleaned == agent.value.lower() or cleaned == agent.name.lower():
                return agent
        alias_map = {
            "analysis": AgentName.ANALYST,
            "sql": AgentName.ANALYST,
            "visualization": AgentName.VISUAL,
            "visual": AgentName.VISUAL,
            "websearch": AgentName.WEB_SEARCH,
            "web": AgentName.WEB_SEARCH,
            "docretrieval": AgentName.DOC_RETRIEVAL,
            "doc_retrieval": AgentName.DOC_RETRIEVAL,
            "research": AgentName.DOC_RETRIEVAL,
            "clarify": AgentName.CLARIFY,
        }
        return alias_map.get(cleaned)

    def _coerce_tool_rewrites(self, value: Any) -> list[dict[str, Any]]:
        if not value:
            return []
        items = value if isinstance(value, list) else [value]
        rewrites: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            agent = self._normalize_agent_name(
                item.get("agent") or item.get("tool") or item.get("target")
            )
            if not agent:
                continue
            entry: dict[str, Any] = {"agent": agent.value}
            step_id = item.get("step_id") or item.get("step") or item.get("id")
            if isinstance(step_id, str) and step_id.strip():
                entry["step_id"] = step_id.strip()
            source_step_ref = item.get("source_step_ref") or item.get("source_step")
            if isinstance(source_step_ref, str) and source_step_ref.strip():
                entry["source_step_ref"] = source_step_ref.strip()
            follow_up = item.get("follow_up") or item.get("instruction")
            if isinstance(follow_up, str) and follow_up.strip():
                entry["follow_up"] = follow_up.strip()

            question = item.get("question") or item.get("query") or item.get("rewritten_question")
            if isinstance(question, str) and question.strip():
                if agent == AgentName.WEB_SEARCH:
                    entry["query"] = question.strip()
                else:
                    entry["question"] = question.strip()

            if len(entry) > 1:
                rewrites.append(entry)
        return rewrites

    @staticmethod
    def _coerce_entity_resolution(value: Any) -> Optional[dict[str, Any]]:
        if not isinstance(value, dict):
            return None
        entity_type = str(value.get("entity_type") or "").strip()
        entity_phrase = str(value.get("entity_phrase") or value.get("entity") or "").strip()
        probe_question = str(value.get("probe_question") or "").strip()
        follow_up = str(value.get("follow_up") or "").strip()
        original_question = str(value.get("original_question") or "").strip()
        payload: dict[str, Any] = {}
        if entity_type:
            payload["entity_type"] = entity_type
        if entity_phrase:
            payload["entity_phrase"] = entity_phrase
        if probe_question:
            payload["probe_question"] = probe_question
        if follow_up:
            payload["follow_up"] = follow_up
        if original_question:
            payload["original_question"] = original_question
        return payload or None

    def _summarize_artifacts(self, artifacts: PlanExecutionArtifacts) -> Dict[str, Any]:
        columns: list[Any] = []
        rows: list[Any] = []
        if artifacts.data_payload:
            columns = list(artifacts.data_payload.get("columns") or [])
            rows = list(artifacts.data_payload.get("rows") or [])
        elif artifacts.analyst_result and artifacts.analyst_result.result:
            columns = list(artifacts.analyst_result.result.columns or [])
            rows = list(artifacts.analyst_result.result.rows or [])

        analyst_error = self._analyst_error(artifacts)
        analyst_outcome = self._analyst_outcome_status(artifacts)
        analyst_terminal = bool(
            artifacts.analyst_result and artifacts.analyst_result.outcome and artifacts.analyst_result.outcome.terminal
        )
        web_count = len(artifacts.web_search_result.results) if artifacts.web_search_result else 0
        research_findings = (
            len(artifacts.research_result.findings) if artifacts.research_result else 0
        )
        research_synthesis = (
            (artifacts.research_result.synthesis or "") if artifacts.research_result else ""
        )
        chart_type = None
        if isinstance(artifacts.visualization, dict):
            chart_type = artifacts.visualization.get("chart_type") or artifacts.visualization.get(
                "chartType"
            )

        sample_values = self._sample_column_values(columns, rows)

        return {
            "row_count": len(rows),
            "columns": columns,
            "sample_values": sample_values,
            "analyst_error": analyst_error,
            "analyst_outcome": analyst_outcome,
            "analyst_terminal": analyst_terminal,
            "web_results_count": web_count,
            "research_findings_count": research_findings,
            "research_synthesis": research_synthesis[:240],
            "visualization_chart_type": chart_type,
        }

    @staticmethod
    def _sample_column_values(
        columns: Sequence[Any],
        rows: Sequence[Any],
        *,
        max_columns: int = 4,
        max_rows: int = 6,
        max_values: int = 4,
    ) -> Dict[str, list[str]]:
        if not columns or not rows:
            return {}
        samples: Dict[str, list[str]] = {}
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
                samples[str(col)] = seen
        return samples

    @staticmethod
    def _structured_row_count(artifacts: PlanExecutionArtifacts) -> Optional[int]:
        if artifacts.data_payload:
            rows = artifacts.data_payload.get("rows")
            if isinstance(rows, list):
                return len(rows)
        if artifacts.analyst_result and artifacts.analyst_result.result:
            rows = artifacts.analyst_result.result.rows
            if isinstance(rows, list):
                return len(rows)
        return None

    @staticmethod
    def _pluralize_label(label: str) -> str:
        cleaned = str(label or "").strip()
        if not cleaned:
            return "items"
        lower = cleaned.lower()
        if lower.endswith("y") and len(lower) > 1:
            return f"{cleaned[:-1]}ies"
        if lower.endswith("s"):
            return cleaned
        return f"{cleaned}s"

    def _extract_entity_target(self, question: str) -> Optional[Dict[str, str]]:
        if not question:
            return None
        for entity_type, aliases in self._ENTITY_ALIAS_MAP.items():
            for alias in aliases:
                pattern = rf"\b{re.escape(alias)}s?\b\s+([A-Za-z0-9&.'\-]+(?:\s+[A-Za-z0-9&.'\-]+){{0,2}})"
                match = re.search(pattern, question, flags=re.IGNORECASE)
                if match:
                    phrase = match.group(0).strip()
                    return {
                        "entity_type": entity_type,
                        "entity_phrase": phrase,
                    }
        return None

    @staticmethod
    def _extract_entity_resolution_context(diagnostics: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        extra_context = diagnostics.get("extra_context")
        if not isinstance(extra_context, dict):
            return None
        reasoning = extra_context.get("reasoning")
        if not isinstance(reasoning, dict):
            return None
        resolution = reasoning.get("entity_resolution")
        if not isinstance(resolution, dict):
            return None
        return resolution

    def _build_entity_resolution(
        self,
        *,
        user_query: str,
        diagnostics: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        target = self._extract_entity_target(user_query)
        if not target:
            return None

        existing = self._extract_entity_resolution_context(diagnostics) or {}
        attempts = int(existing.get("attempts") or 0)
        if attempts >= self._MAX_ENTITY_RESOLUTION_ATTEMPTS:
            return None

        entity_type = target["entity_type"]
        entity_phrase = target["entity_phrase"]
        plural = self._pluralize_label(entity_type)
        probe_question = f"List all {plural} names."

        return {
            "entity_type": entity_type,
            "entity_phrase": entity_phrase,
            "original_question": user_query,
            "probe_question": probe_question,
            "attempts": attempts + 1,
        }

    def _build_llm_prompt(
        self,
        *,
        iteration: int,
        plan: Plan,
        artifacts: PlanExecutionArtifacts,
        diagnostics: Dict[str, Any],
        user_query: Optional[str],
    ) -> str:
        summary = self._summarize_artifacts(artifacts)
        prompt_sections = [
            "You are an orchestration evaluator. Decide if more planning is needed.",
            "Return ONLY JSON with keys: continue_planning (boolean), rationale (string).",
            "Optional keys: force_route, prefer_routes, avoid_routes, require_web_search,",
            "require_deep_research, require_visual, require_sql, retry_due_to_error,",
            "retry_due_to_empty, retry_due_to_low_sources, tool_rewrites, entity_resolution.",
            "tool_rewrites should be a list of objects with keys: agent, question/query,",
            "optional step_id, source_step_ref, follow_up. Use it to rewrite tool inputs.",
            "entity_resolution should be an object with keys: entity_type, entity_phrase,",
            "probe_question, follow_up, original_question. Use it when SQL results are empty",
            "and you need to resolve entity naming mismatches (e.g. Store A vs Shop A).",
            "If results are empty or errors occurred, set continue_planning=true and provide",
            "tool_rewrites or entity_resolution to improve the next tool call.",
            "Routes: SimpleAnalyst, AnalystThenVisual, WebSearch, DeepResearch, Clarify.",
            f"User query: {user_query or ''}",
            f"Current route: {plan.route}",
            f"Iteration: {iteration + 1} of {self.max_iterations}",
            f"Execution summary (JSON): {json.dumps(summary, default=str, ensure_ascii=True)}",
            f"Diagnostics (JSON): {json.dumps(diagnostics or {}, default=str, ensure_ascii=True)}",
        ]
        return "\n".join(prompt_sections)

    def _evaluate_with_llm(
        self,
        *,
        iteration: int,
        plan: Plan,
        artifacts: PlanExecutionArtifacts,
        diagnostics: Dict[str, Any],
        user_query: Optional[str],
    ) -> Optional[ReasoningDecision]:
        if not self.llm:
            return None
        prompt = self._build_llm_prompt(
            iteration=iteration,
            plan=plan,
            artifacts=artifacts,
            diagnostics=diagnostics,
            user_query=user_query,
        )
        try:
            response = self.llm.complete(prompt, temperature=0.0, max_tokens=350)
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.warning("ReasoningAgent LLM evaluation failed: %s", exc)
            return None

        payload = self._parse_llm_payload(str(response))
        if not payload:
            return None

        continue_value = self._coerce_bool(payload.get("continue_planning"))
        if continue_value is None:
            return None

        rationale = str(payload.get("rationale") or "").strip() or "LLM evaluation completed."
        if not continue_value:
            return ReasoningDecision(continue_planning=False, rationale=rationale)

        reasoning_payload: Dict[str, Any] = {"previous_route": plan.route}
        force_route = self._normalize_route_name(payload.get("force_route") or payload.get("force_tool"))
        if force_route:
            reasoning_payload["force_route"] = force_route.value

        prefer_routes = self._normalize_route_list(payload.get("prefer_routes") or payload.get("preferred_routes"))
        if prefer_routes:
            reasoning_payload["prefer_routes"] = prefer_routes
        avoid_routes = self._normalize_route_list(payload.get("avoid_routes"))
        if avoid_routes:
            reasoning_payload["avoid_routes"] = avoid_routes

        require_web_search = self._coerce_bool(payload.get("require_web_search"))
        if require_web_search:
            reasoning_payload["require_web_search"] = True
        require_deep = self._coerce_bool(payload.get("require_deep_research"))
        if require_deep:
            reasoning_payload["require_deep_research"] = True
        require_visual = self._coerce_bool(payload.get("require_visual"))
        if require_visual:
            reasoning_payload["require_visual"] = True
        require_sql = self._coerce_bool(payload.get("require_sql"))
        if require_sql:
            reasoning_payload["require_sql"] = True

        tool_rewrites = self._coerce_tool_rewrites(payload.get("tool_rewrites") or payload.get("rewrites"))
        if tool_rewrites:
            reasoning_payload["tool_rewrites"] = tool_rewrites

        entity_resolution = self._coerce_entity_resolution(payload.get("entity_resolution"))
        if entity_resolution:
            reasoning_payload["entity_resolution"] = entity_resolution

        for flag in ("retry_due_to_error", "retry_due_to_empty", "retry_due_to_low_sources"):
            flag_value = payload.get(flag)
            if flag_value is not None:
                reasoning_payload[flag] = flag_value

        return ReasoningDecision(
            continue_planning=True,
            updated_context={"reasoning": reasoning_payload},
            rationale=rationale,
        )

    def _apply_llm_safeguards(
        self,
        decision: ReasoningDecision,
        *,
        plan: Plan,
        artifacts: PlanExecutionArtifacts,
        user_query: Optional[str],
    ) -> ReasoningDecision:
        has_structured_data = self._has_structured_data(artifacts)
        has_web_results = self._has_web_results(artifacts)
        has_research = self._has_research_results(artifacts)
        has_data = has_structured_data or has_web_results or has_research

        analyst_error = self._analyst_error(artifacts)
        analyst_outcome = artifacts.analyst_result.outcome if artifacts.analyst_result else None
        if (
            analyst_outcome
            and analyst_outcome.terminal
            and analyst_outcome.status in {
                AnalystOutcomeStatus.access_denied,
                AnalystOutcomeStatus.invalid_request,
                AnalystOutcomeStatus.selection_error,
                AnalystOutcomeStatus.query_error,
                AnalystOutcomeStatus.execution_error,
                AnalystOutcomeStatus.needs_clarification,
            }
        ):
            return ReasoningDecision(
                continue_planning=False,
                rationale=f"Terminal analyst outcome ({analyst_outcome.status.value}); returning final response.",
            )

        if not decision.continue_planning and analyst_error and not (has_web_results or has_research):
            force_route = self._pick_fallback_route(plan.route)
            rationale = "Retrying due to analyst error."
            self.logger.debug("%s Error: %s", rationale, analyst_error)
            return self._build_retry_decision(
                plan=plan,
                rationale=rationale,
                force_route=force_route,
                retry_flag="retry_due_to_error",
                detail=str(analyst_error),
            )

        if not decision.continue_planning and not has_data:
            force_route = self._pick_fallback_route(plan.route)
            rationale = "No structured or research data produced; requesting replanning."
            return self._build_retry_decision(
                plan=plan,
                rationale=rationale,
                force_route=force_route,
                retry_flag="retry_due_to_empty",
            )

        if not decision.continue_planning:
            return decision

        updated_context = decision.updated_context or {}
        reasoning_payload = updated_context.get("reasoning")
        if not isinstance(reasoning_payload, dict):
            reasoning_payload = {"previous_route": plan.route}
        else:
            reasoning_payload.setdefault("previous_route", plan.route)

        if not any(
            flag in reasoning_payload for flag in ("retry_due_to_error", "retry_due_to_empty", "retry_due_to_low_sources")
        ):
            if analyst_error and not (has_web_results or has_research):
                reasoning_payload["retry_due_to_error"] = str(analyst_error)
            elif not has_data:
                reasoning_payload["retry_due_to_empty"] = True
            elif artifacts.research_result and self._is_low_signal_research(artifacts.research_result):
                reasoning_payload["retry_due_to_low_sources"] = True

        updated_context["reasoning"] = reasoning_payload
        return ReasoningDecision(
            continue_planning=True,
            updated_context=updated_context,
            rationale=decision.rationale,
        )

    def _build_retry_decision(
        self,
        *,
        plan: Plan,
        rationale: str,
        force_route: RouteName,
        retry_flag: str,
        detail: Optional[str] = None,
    ) -> ReasoningDecision:
        self.logger.debug(rationale)
        reasoning_payload: Dict[str, Any] = {
            "force_route": force_route.value,
            "previous_route": plan.route,
        }
        reasoning_payload[retry_flag] = detail if detail is not None else True
        return ReasoningDecision(
            continue_planning=True,
            updated_context={
                "reasoning": reasoning_payload
            },
            rationale=rationale,
        )

    @staticmethod
    def _normalize_error_signature(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip().lower()[:240]

    def _is_repeated_analyst_error(
        self,
        *,
        diagnostics: Dict[str, Any],
        analyst_error: Any,
    ) -> bool:
        if not analyst_error:
            return False
        extra_context = diagnostics.get("extra_context")
        if not isinstance(extra_context, dict):
            return False
        reasoning = extra_context.get("reasoning")
        if not isinstance(reasoning, dict):
            return False
        previous_error = reasoning.get("retry_due_to_error")
        if not isinstance(previous_error, str) or not previous_error.strip():
            return False

        current_signature = self._normalize_error_signature(str(analyst_error))
        previous_signature = self._normalize_error_signature(previous_error)
        if not current_signature or not previous_signature:
            return False
        return (
            current_signature == previous_signature
            or current_signature in previous_signature
            or previous_signature in current_signature
        )

    def evaluate(
        self,
        *,
        iteration: int,
        plan: Plan,
        artifacts: PlanExecutionArtifacts,
        diagnostics: Dict[str, Any],
        user_query: Optional[str] = None,
    ) -> ReasoningDecision:
        if artifacts.clarifying_question:
            rationale = "Clarification needed from user; stopping further planning."
            self.logger.debug(rationale)
            return ReasoningDecision(continue_planning=False, rationale=rationale)

        analyst_error = self._analyst_error(artifacts)
        analyst_outcome = artifacts.analyst_result.outcome if artifacts.analyst_result else None

        if iteration + 1 >= self.max_iterations:
            rationale = "Max reasoning iterations reached; finalising current response."
            self.logger.debug(rationale)
            return ReasoningDecision(continue_planning=False, rationale=rationale)

        if self._is_repeated_analyst_error(diagnostics=diagnostics, analyst_error=analyst_error):
            rationale = "Repeated analyst error detected; stopping retries."
            self.logger.debug("%s Error: %s", rationale, analyst_error)
            return ReasoningDecision(continue_planning=False, rationale=rationale)

        if (
            analyst_outcome
            and analyst_outcome.terminal
            and analyst_outcome.status in {
                AnalystOutcomeStatus.access_denied,
                AnalystOutcomeStatus.invalid_request,
                AnalystOutcomeStatus.selection_error,
                AnalystOutcomeStatus.query_error,
                AnalystOutcomeStatus.execution_error,
                AnalystOutcomeStatus.needs_clarification,
            }
        ):
            rationale = f"Analyst returned terminal outcome {analyst_outcome.status.value}; stopping further planning."
            self.logger.debug(rationale)
            return ReasoningDecision(continue_planning=False, rationale=rationale)

        llm_decision = self._evaluate_with_llm(
            iteration=iteration,
            plan=plan,
            artifacts=artifacts,
            diagnostics=diagnostics,
            user_query=user_query,
        )
        if llm_decision:
            return self._apply_llm_safeguards(
                llm_decision,
                plan=plan,
                artifacts=artifacts,
                user_query=user_query,
            )

        has_structured_data = self._has_structured_data(artifacts)
        has_web_results = self._has_web_results(artifacts)
        has_research = self._has_research_results(artifacts)
        has_data = has_structured_data or has_web_results or has_research
        row_count = self._structured_row_count(artifacts)

        if (
            row_count == 0
            and has_structured_data
            and not analyst_error
            and not has_web_results
            and not has_research
            and user_query
            and plan.route in (RouteName.SIMPLE_ANALYST.value, RouteName.ANALYST_THEN_VISUAL.value)
        ):
            entity_resolution = self._build_entity_resolution(
                user_query=user_query,
                diagnostics=diagnostics,
            )
            if entity_resolution:
                rationale = "No rows returned; probing entity names to resolve mismatches."
                self.logger.debug(rationale)
                return ReasoningDecision(
                    continue_planning=True,
                    updated_context={
                        "reasoning": {
                            "previous_route": plan.route,
                            "entity_resolution": entity_resolution,
                        }
                    },
                    rationale=rationale,
                )

        if analyst_error and not (has_web_results or has_research):
            force_route = self._pick_fallback_route(plan.route)
            rationale = "Retrying due to analyst error."
            self.logger.debug("%s Error: %s", rationale, analyst_error)
            return self._build_retry_decision(
                plan=plan,
                rationale=rationale,
                force_route=force_route,
                retry_flag="retry_due_to_error",
                detail=str(analyst_error),
            )

        if not has_data:
            force_route = self._pick_fallback_route(plan.route)
            rationale = "No structured or research data produced; requesting replanning."
            return self._build_retry_decision(
                plan=plan,
                rationale=rationale,
                force_route=force_route,
                retry_flag="retry_due_to_empty",
            )

        signals = _extract_signals(user_query) if user_query else None

        if has_web_results and not has_research:
            has_sources = bool(artifacts.web_search_result and artifacts.web_search_result.results)
            if has_sources and (signals is None or signals.has_research_signals):
                rationale = "Web search produced sources; synthesizing with deep research."
                self.logger.debug(rationale)
                web_docs = artifacts.web_search_result.to_documents() if artifacts.web_search_result else []
                return ReasoningDecision(
                    continue_planning=True,
                    updated_context={
                        "documents": web_docs,
                        "reasoning": {
                            "force_route": RouteName.DEEP_RESEARCH.value,
                            "previous_route": plan.route,
                            "promoted_from_web_search": True,
                        },
                    },
                    rationale=rationale,
                )

        if has_research and not has_web_results:
            if artifacts.research_result and self._is_low_signal_research(artifacts.research_result):
                rationale = "Research lacked source material; broadening with web search."
                self.logger.debug(rationale)
                return ReasoningDecision(
                    continue_planning=True,
                    updated_context={
                        "reasoning": {
                            "force_route": RouteName.WEB_SEARCH.value,
                            "previous_route": plan.route,
                            "retry_due_to_low_sources": True,
                        }
                    },
                    rationale=rationale,
                )

        self.logger.debug("Reasoning agent determined results look sufficient.")
        return ReasoningDecision(continue_planning=False, rationale="Results look sufficient.")
