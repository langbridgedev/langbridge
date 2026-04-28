"""Shared continuation-state and follow-up resolution helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from langbridge.ai.orchestration.timeframes import (
    TimeframeState,
    extract_timeframe_state,
    resolve_requested_timeframe,
    rewrite_prior_question_for_requested_timeframe,
    same_timeframe,
)

PeriodState = TimeframeState
extract_period_state = extract_timeframe_state


class FilterClause(BaseModel):
    model_config = ConfigDict(extra="ignore")

    field: str
    operator: Literal["include", "exclude"]
    values: list[str] = Field(default_factory=list)

    @classmethod
    def normalize_payload(cls, payload: Any) -> list["FilterClause"]:
        if isinstance(payload, Mapping):
            clause = cls._from_payload(payload)
            return [clause] if clause is not None else []
        if isinstance(payload, list):
            clauses: list[FilterClause] = []
            for item in payload:
                if not isinstance(item, Mapping):
                    continue
                clause = cls._from_payload(item)
                if clause is not None:
                    clauses.append(clause)
            return clauses
        return []

    @classmethod
    def _from_payload(cls, payload: Mapping[str, Any]) -> "FilterClause" | None:
        field_name = str(payload.get("field") or "").strip()
        operator = str(payload.get("operator") or "").strip().lower()
        values_payload = payload.get("values")
        raw_value = payload.get("value")
        values: list[str] = []
        if isinstance(values_payload, list):
            values = [str(item).strip() for item in values_payload if str(item).strip()]
        elif isinstance(raw_value, str) and raw_value.strip():
            values = [raw_value.strip()]
        if not field_name or operator not in {"include", "exclude"} or not values:
            return None
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            normalized = normalize_field_name(value)
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(value)
        if not deduped:
            return None
        return cls(field=field_name, operator=operator, values=deduped)

    def single_value_payload(self) -> dict[str, Any] | None:
        if not self.values:
            return None
        return {
            "field": self.field,
            "operator": self.operator,
            "value": self.values[0],
        }


class AnalysisState(BaseModel):
    model_config = ConfigDict(extra="ignore")

    available_fields: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    primary_dimension: str | None = None
    period: TimeframeState | None = None
    dimension_value_samples: dict[str, list[str]] = Field(default_factory=dict)
    active_filters: list[FilterClause] = Field(default_factory=list)


class VisualizationState(BaseModel):
    model_config = ConfigDict(extra="ignore")

    chart_type: str | None = None
    title: str | None = None
    x: str | None = None
    y: str | None = None
    series: str | None = None
    encoding: dict[str, Any] = Field(default_factory=dict)


class ContinuationState(BaseModel):
    model_config = ConfigDict(extra="ignore")

    question: str | None = None
    resolved_question: str | None = None
    summary: str | None = None
    answer: str | None = None
    result: dict[str, Any] | None = None
    visualization: dict[str, Any] | None = None
    visualization_state: VisualizationState | None = None
    research: dict[str, Any] | None = None
    sources: list[dict[str, Any]] = Field(default_factory=list)
    chartable: bool | None = None
    status: str | None = None
    selected_agent: str | None = None
    analysis_state: AnalysisState | None = None

    def compact_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class FollowUpResolution(BaseModel):
    model_config = ConfigDict(extra="ignore")

    kind: Literal[
        "visualize_prior_result",
        "analyze_prior_result",
        "requery_prior_analysis",
        "clarify_follow_up",
    ]
    rationale: str
    selected_agent: str | None = None
    reuse_last_result: bool = False
    suggested_agent_mode: str | None = None
    question_type: str | None = None
    resolved_question: str | None = None
    chart_type: str | None = None
    focus_field: str | None = None
    dimension: str | None = None
    period: TimeframeState | None = None
    filters: list[FilterClause] = Field(default_factory=list)
    active_filters: list[FilterClause] = Field(default_factory=list)
    clarification_question: str | None = None


class _ExecutedPlanStep(BaseModel):
    model_config = ConfigDict(extra="ignore")

    step_id: str | None = None
    agent_name: str | None = None
    question: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)


class _ParsedFilterRequest(BaseModel):
    operator: Literal["include", "exclude"]
    raw_value: str


class _ResolvedFilterRefinement(BaseModel):
    filters: list[FilterClause] = Field(default_factory=list)
    active_filters: list[FilterClause] = Field(default_factory=list)
    ambiguous_fields: list[str] = Field(default_factory=list)
    ambiguous_value: str | None = None


def normalize_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").casefold()).strip("_")


def humanize_field_name(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[_-]+", " ", text)).strip().lower()


def is_tabular_result(result: Any) -> bool:
    return isinstance(result, Mapping) and {"columns", "rows"}.issubset(result)


def resolve_candidate_name(
    requested: str,
    *,
    preferred_candidates: Sequence[str] | None = None,
    fallback_candidates: Sequence[str] | None = None,
) -> str | None:
    requested_value = str(requested or "").strip()
    normalized_requested = normalize_field_name(requested_value)
    requested_tokens = {token for token in normalized_requested.split("_") if token}
    if not normalized_requested:
        return None
    candidates: list[str] = []
    seen: set[str] = set()
    for group in (preferred_candidates or [], fallback_candidates or []):
        for candidate in group:
            candidate_text = str(candidate or "").strip()
            if not candidate_text:
                continue
            normalized_candidate = normalize_field_name(candidate_text)
            if normalized_candidate in seen:
                continue
            seen.add(normalized_candidate)
            candidates.append(candidate_text)
    best_match: str | None = None
    best_score = -1
    for candidate in candidates:
        normalized_candidate = normalize_field_name(candidate)
        candidate_tokens = {token for token in normalized_candidate.split("_") if token}
        score = 0
        if normalized_candidate == normalized_requested:
            score = 100
        elif normalized_requested in normalized_candidate or normalized_candidate in normalized_requested:
            score = 90
        elif requested_tokens and candidate_tokens:
            overlap = len(requested_tokens & candidate_tokens)
            if overlap:
                score = 70 + overlap
        if score > best_score:
            best_match = candidate
            best_score = score
    return best_match if best_score >= 70 else None


def join_values(values: Sequence[str]) -> str:
    normalized_values = [str(value).strip() for value in values if str(value).strip()]
    if not normalized_values:
        return ""
    if len(normalized_values) == 1:
        return normalized_values[0]
    if len(normalized_values) == 2:
        return f"{normalized_values[0]} and {normalized_values[1]}"
    return f"{', '.join(normalized_values[:-1])}, and {normalized_values[-1]}"


def merge_filter_clauses(
    *,
    active_filters: Sequence[FilterClause],
    refinements: Sequence[FilterClause],
) -> list[FilterClause]:
    merged = [FilterClause.model_validate(item.model_dump(mode="json")) for item in active_filters]
    for refinement in refinements:
        field_key = normalize_field_name(refinement.field)
        values = list(refinement.values)
        if refinement.operator == "include":
            merged = [item for item in merged if normalize_field_name(item.field) != field_key]
            merged.append(refinement)
            continue
        updated = False
        for item in merged:
            if normalize_field_name(item.field) != field_key:
                continue
            if item.operator == "exclude":
                item.values = _merge_filter_values(item.values, values)
                updated = True
            elif item.operator == "include":
                item.values = _subtract_filter_values(item.values, values)
                updated = True
        merged = [item for item in merged if item.values]
        if not updated:
            merged.append(refinement)
    return merged


def filters_instruction(filters: Sequence[FilterClause]) -> str:
    instructions: list[str] = []
    for filter_clause in filters:
        joined_values = join_values(filter_clause.values)
        if filter_clause.operator == "include":
            instructions.append(f"Filter the analysis to only {joined_values} for {filter_clause.field}.")
        else:
            instructions.append(f"Exclude {joined_values} from {filter_clause.field}.")
    return " ".join(item for item in instructions if item).strip()


class ContinuationStateBuilder:
    @classmethod
    def from_context(
        cls,
        *,
        context: Mapping[str, Any],
        question: str | None = None,
    ) -> ContinuationState | None:
        base = cls.coerce(context.get("continuation_state"))
        state = base.model_copy(deep=True) if base is not None else ContinuationState()
        changed = base is not None

        analysis_state = cls._coerce_analysis_state(context.get("analysis_state"))
        if analysis_state is not None:
            state.analysis_state = analysis_state
            changed = True
        visualization_state = cls._coerce_visualization_state(context.get("visualization_state"))
        if visualization_state is not None:
            state.visualization_state = visualization_state
            changed = True
        result = context.get("result")
        if isinstance(result, Mapping) and result:
            state.result = dict(result)
            state.chartable = is_tabular_result(result)
            changed = True
        visualization = context.get("visualization")
        if isinstance(visualization, Mapping) and visualization:
            state.visualization = dict(visualization)
            state.visualization_state = state.visualization_state or cls.build_visualization_state(visualization)
            changed = True
        research = context.get("research")
        if isinstance(research, Mapping) and research:
            state.research = dict(research)
            changed = True
        sources = context.get("sources")
        if isinstance(sources, list):
            state.sources = [dict(item) for item in sources if isinstance(item, Mapping)]
            changed = True
        selected_agent = cls.selected_agent_from_context(context)
        if selected_agent:
            state.selected_agent = selected_agent
            changed = True
        if base is None:
            if not state.resolved_question and question:
                state.resolved_question = question
            if not state.question and question:
                state.question = question
        if state.analysis_state is None and is_tabular_result(state.result):
            state.analysis_state = cls.build_analysis_state(
                question=state.resolved_question or state.question,
                result=state.result,
                active_filters=[],
            )
        if not changed and not state.result and not state.visualization and not state.research:
            return None
        return state

    @classmethod
    def from_content(cls, content: Mapping[str, Any]) -> ContinuationState | None:
        if not isinstance(content, Mapping):
            return None
        diagnostics = content.get("diagnostics")
        return cls._from_parts(
            user_query=None,
            result=content.get("result"),
            visualization=content.get("visualization"),
            research=content.get("research"),
            summary=content.get("summary"),
            answer=content.get("answer"),
            diagnostics=diagnostics if isinstance(diagnostics, Mapping) else {},
            ai_run=None,
        )

    @classmethod
    def from_response(
        cls,
        *,
        response: Mapping[str, Any],
        user_query: str,
        ai_run: Any | None = None,
    ) -> ContinuationState | None:
        diagnostics = response.get("diagnostics") if isinstance(response, Mapping) else None
        return cls._from_parts(
            user_query=user_query,
            result=response.get("result") if isinstance(response, Mapping) else None,
            visualization=response.get("visualization") if isinstance(response, Mapping) else None,
            research=response.get("research") if isinstance(response, Mapping) else None,
            summary=response.get("summary") if isinstance(response, Mapping) else None,
            answer=response.get("answer") if isinstance(response, Mapping) else None,
            diagnostics=diagnostics if isinstance(diagnostics, Mapping) else {},
            ai_run=ai_run,
        )

    @classmethod
    def coerce(cls, payload: Any) -> ContinuationState | None:
        if not isinstance(payload, Mapping):
            return None
        try:
            state = ContinuationState.model_validate(payload)
        except Exception:
            return None
        return cls._sanitize_state(state)

    @classmethod
    def build_visualization_state(cls, visualization: Any) -> VisualizationState | None:
        if not isinstance(visualization, Mapping):
            return None
        state: dict[str, Any] = {}
        for key in ("chart_type", "title", "x", "y", "series"):
            value = visualization.get(key)
            if isinstance(value, str) and value.strip():
                state[key] = value.strip()
        encoding = visualization.get("encoding")
        if isinstance(encoding, Mapping) and encoding:
            state["encoding"] = dict(encoding)
        return VisualizationState.model_validate(state) if state else None

    @classmethod
    def build_analysis_state(
        cls,
        *,
        question: str | None,
        result: Any,
        active_filters: Sequence[FilterClause],
    ) -> AnalysisState | None:
        if not is_tabular_result(result):
            return None
        available_fields = cls.available_fields_from_result(result)
        if not available_fields:
            return None
        metrics = cls.resolve_metric_fields(available_fields)
        primary_dimension = cls.resolve_primary_dimension(
            question=question,
            available_fields=available_fields,
            metric_fields=metrics,
        )
        dimensions = [field_name for field_name in available_fields if field_name not in metrics]
        if primary_dimension and primary_dimension not in dimensions:
            dimensions.insert(0, primary_dimension)
        return AnalysisState(
            available_fields=available_fields,
            metrics=metrics,
            dimensions=dimensions,
            primary_dimension=primary_dimension,
            period=extract_timeframe_state(question),
            dimension_value_samples=cls.dimension_value_samples_from_result(
                result=result,
                dimensions=dimensions,
                available_fields=available_fields,
            ),
            active_filters=list(active_filters),
        )

    @classmethod
    def available_fields_from_result(cls, result: Any) -> list[str]:
        if not is_tabular_result(result):
            return []
        columns = result.get("columns")
        if not isinstance(columns, list):
            return []
        fields: list[str] = []
        seen: set[str] = set()
        for column in columns:
            field_name = humanize_field_name(str(column))
            if not field_name:
                continue
            normalized = normalize_field_name(field_name)
            if normalized in seen:
                continue
            seen.add(normalized)
            fields.append(field_name)
        return fields

    @classmethod
    def resolve_metric_fields(cls, available_fields: Sequence[str]) -> list[str]:
        metrics = [field_name for field_name in available_fields if cls.field_looks_metric(field_name)]
        if not metrics and len(available_fields) > 1:
            metrics = list(available_fields[1:])
        return metrics

    @classmethod
    def resolve_primary_dimension(
        cls,
        *,
        question: str | None,
        available_fields: Sequence[str],
        metric_fields: Sequence[str],
    ) -> str | None:
        requested_dimension = cls.extract_grouping_from_question(question)
        if requested_dimension:
            resolved = resolve_candidate_name(
                requested_dimension,
                preferred_candidates=available_fields,
                fallback_candidates=available_fields,
            )
            if resolved:
                return resolved
        metric_normalized = {normalize_field_name(field_name) for field_name in metric_fields}
        for field_name in available_fields:
            if normalize_field_name(field_name) not in metric_normalized:
                return field_name
        return available_fields[0] if available_fields else None

    @classmethod
    def field_looks_metric(cls, field_name: str) -> bool:
        tokens = {token for token in normalize_field_name(field_name).split("_") if token}
        if not tokens:
            return False
        metric_tokens = {
            "amount",
            "average",
            "avg",
            "cost",
            "count",
            "gmv",
            "margin",
            "orders",
            "percent",
            "percentage",
            "price",
            "profit",
            "quantity",
            "rate",
            "ratio",
            "revenue",
            "sales",
            "score",
            "sum",
            "total",
            "units",
            "value",
            "volume",
        }
        return any(token in metric_tokens for token in tokens)

    @staticmethod
    def extract_grouping_from_question(question: str | None) -> str | None:
        text = str(question or "").strip()
        patterns = (
            r"\bby (?P<dimension>[a-z0-9 _-]+)",
            r"\bwhich (?P<dimension>[a-z0-9 _-]+?) drove\b",
            r"\bwhich (?P<dimension>[a-z0-9 _-]+?) had\b",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            dimension = str(match.group("dimension") or "").strip(" ?.,")
            if dimension:
                return dimension
        return None

    @classmethod
    def dimension_value_samples_from_result(
        cls,
        *,
        result: Any,
        dimensions: Sequence[str],
        available_fields: Sequence[str],
    ) -> dict[str, list[str]]:
        if not is_tabular_result(result):
            return {}
        columns = result.get("columns")
        rows = result.get("rows")
        if not isinstance(columns, list) or not isinstance(rows, list):
            return {}
        index_by_field: dict[str, int] = {}
        for index, column in enumerate(columns):
            field_name = humanize_field_name(str(column))
            if field_name:
                index_by_field[field_name] = index
        sample_fields = list(dimensions) or [
            field_name
            for field_name in available_fields
            if not cls.field_looks_metric(field_name)
        ]
        samples: dict[str, list[str]] = {}
        for field_name in sample_fields[:6]:
            index = index_by_field.get(field_name)
            if index is None:
                continue
            values: list[str] = []
            seen: set[str] = set()
            for row in rows:
                if not isinstance(row, (list, tuple)) or index >= len(row):
                    continue
                raw_value = row[index]
                if raw_value is None:
                    continue
                value = str(raw_value).strip()
                if not value:
                    continue
                normalized = normalize_field_name(value)
                if normalized in seen:
                    continue
                seen.add(normalized)
                values.append(value)
                if len(values) >= 12:
                    break
            if values:
                samples[field_name] = values
        return samples

    @classmethod
    def selected_agent_from_context(cls, context: Mapping[str, Any]) -> str | None:
        continuation_state = cls.coerce(context.get("continuation_state"))
        if continuation_state and continuation_state.selected_agent:
            return continuation_state.selected_agent
        route_decision = context.get("route_decision")
        if isinstance(route_decision, Mapping):
            selected_agent = str(route_decision.get("agent_name") or "").strip()
            if selected_agent:
                return selected_agent
        return None

    @classmethod
    def _from_parts(
        cls,
        *,
        user_query: str | None,
        result: Any,
        visualization: Any,
        research: Any,
        summary: Any,
        answer: Any,
        diagnostics: Mapping[str, Any],
        ai_run: Any | None,
    ) -> ContinuationState | None:
        run_payload = cls._ai_run_payload(ai_run=ai_run, diagnostics=diagnostics)
        run_status = str(run_payload.get("status") or "").strip().lower() if run_payload else ""
        if run_status and run_status != "completed":
            return None
        result_payload = cls._compact_mapping(result)
        visualization_payload = cls._compact_mapping(visualization)
        research_payload = cls._compact_mapping(research)
        summary_text = str(summary or "").strip()
        answer_text = answer if isinstance(answer, str) and answer.strip() else None
        if not (
            result_payload
            or visualization_payload
            or research_payload
            or summary_text
            or answer_text
        ):
            return None
        executed_step = cls.executed_plan_step(run_payload)
        resolved_question = str(executed_step.question or "").strip() or cls.resolved_question_from_run(run_payload)
        active_filters = cls.active_filters_from_run(run_payload)
        state = ContinuationState(
            question=user_query,
            resolved_question=resolved_question or None,
            summary=summary_text or None,
            answer=answer_text,
            result=result_payload,
            visualization=visualization_payload,
            visualization_state=cls.build_visualization_state(visualization_payload),
            research=research_payload,
            sources=cls.extract_sources(research_payload or {}),
            chartable=is_tabular_result(result_payload),
            status=run_status or None,
            selected_agent=str(
                executed_step.agent_name or cls.selected_agent_from_diagnostics(diagnostics) or ""
            ).strip()
            or None,
            analysis_state=cls.build_analysis_state(
                question=resolved_question or user_query,
                result=result_payload,
                active_filters=active_filters,
            ),
        )
        return cls._sanitize_state(state)

    @staticmethod
    def _compact_mapping(payload: Any) -> dict[str, Any] | None:
        return dict(payload) if isinstance(payload, Mapping) and payload else None

    @classmethod
    def _sanitize_state(cls, state: ContinuationState) -> ContinuationState | None:
        if isinstance(state.result, Mapping) and not state.result:
            state.result = None
        if isinstance(state.visualization, Mapping) and not state.visualization:
            state.visualization = None
        if isinstance(state.research, Mapping) and not state.research:
            state.research = None
        if state.result is not None:
            state.chartable = is_tabular_result(state.result)
        if state.analysis_state is None and is_tabular_result(state.result):
            state.analysis_state = cls.build_analysis_state(
                question=state.resolved_question or state.question,
                result=state.result,
                active_filters=[],
            )
        return state if state.compact_payload() else None

    @staticmethod
    def _coerce_analysis_state(payload: Any) -> AnalysisState | None:
        if not isinstance(payload, Mapping):
            return None
        try:
            return AnalysisState.model_validate(payload)
        except Exception:
            return None

    @staticmethod
    def _coerce_visualization_state(payload: Any) -> VisualizationState | None:
        if not isinstance(payload, Mapping):
            return None
        try:
            return VisualizationState.model_validate(payload)
        except Exception:
            return None

    @staticmethod
    def _ai_run_payload(*, ai_run: Any | None, diagnostics: Mapping[str, Any]) -> Mapping[str, Any]:
        if ai_run is not None:
            if hasattr(ai_run, "model_dump"):
                return ai_run.model_dump(mode="json")
            if isinstance(ai_run, Mapping):
                return ai_run
        payload = diagnostics.get("ai_run")
        return payload if isinstance(payload, Mapping) else {}

    @staticmethod
    def extract_sources(research: Mapping[str, Any]) -> list[dict[str, Any]]:
        sources = research.get("sources") if isinstance(research, Mapping) else None
        return [dict(item) for item in sources if isinstance(item, Mapping)] if isinstance(sources, list) else []

    @staticmethod
    def selected_agent_from_diagnostics(diagnostics: Mapping[str, Any]) -> str | None:
        ai_run = diagnostics.get("ai_run")
        if isinstance(ai_run, Mapping):
            run_diagnostics = ai_run.get("diagnostics")
            if isinstance(run_diagnostics, Mapping):
                selected_agent = str(run_diagnostics.get("selected_agent") or "").strip()
                if selected_agent:
                    return selected_agent
            route = str(ai_run.get("route") or "").strip()
            if route.startswith("direct:"):
                direct_agent = route.partition(":")[2].strip()
                if direct_agent:
                    return direct_agent
        selected_agent = str(diagnostics.get("selected_agent") or "").strip()
        return selected_agent or None

    @classmethod
    def resolved_question_from_run(cls, run_payload: Mapping[str, Any]) -> str | None:
        step = cls.executed_plan_step(run_payload)
        if step.question:
            return str(step.question).strip() or None
        plan = run_payload.get("plan")
        if not isinstance(plan, Mapping):
            return None
        steps = plan.get("steps")
        if not isinstance(steps, list):
            return None
        for step_payload in reversed(steps):
            if not isinstance(step_payload, Mapping):
                continue
            question = str(step_payload.get("question") or "").strip()
            if question:
                return question
        return None

    @classmethod
    def active_filters_from_run(cls, run_payload: Mapping[str, Any]) -> list[FilterClause]:
        step = cls.executed_plan_step(run_payload)
        if step:
            filters = cls._active_filters_from_step_input(step.input)
            if filters:
                return filters
        return []

    @classmethod
    def executed_plan_step(cls, run_payload: Mapping[str, Any]) -> _ExecutedPlanStep | None:
        if not isinstance(run_payload, Mapping):
            return None
        plan = run_payload.get("plan")
        if not isinstance(plan, Mapping):
            return None
        steps = plan.get("steps")
        if not isinstance(steps, list) or not steps:
            return None
        step_map: dict[str, Mapping[str, Any]] = {}
        ordered_steps: list[Mapping[str, Any]] = []
        for step_payload in steps:
            if not isinstance(step_payload, Mapping):
                continue
            ordered_steps.append(step_payload)
            step_id = str(step_payload.get("step_id") or "").strip()
            if step_id:
                step_map[step_id] = step_payload
        verification = run_payload.get("verification")
        if isinstance(verification, list):
            for outcome in reversed(verification):
                if not isinstance(outcome, Mapping) or not bool(outcome.get("passed")):
                    continue
                step_id = str(outcome.get("step_id") or "").strip()
                if not step_id:
                    continue
                step_payload = step_map.get(step_id)
                if step_payload is not None:
                    return _ExecutedPlanStep.model_validate(step_payload)
        return _ExecutedPlanStep.model_validate(ordered_steps[-1]) if ordered_steps else None

    @classmethod
    def _active_filters_from_step_input(cls, input_payload: Any) -> list[FilterClause]:
        if not isinstance(input_payload, Mapping):
            return []
        for key in ("active_filters", "follow_up_filters", "follow_up_filter"):
            filters = FilterClause.normalize_payload(input_payload.get(key))
            if filters:
                return filters
        return []


class FollowUpResolver:
    @classmethod
    def resolve(
        cls,
        *,
        question: str,
        continuation_state: ContinuationState | None,
    ) -> FollowUpResolution | None:
        if continuation_state is None:
            return None
        result = continuation_state.result if is_tabular_result(continuation_state.result) else None
        analysis_state = continuation_state.analysis_state
        prior_question = str(continuation_state.resolved_question or continuation_state.question or "").strip()
        available_fields = list(analysis_state.available_fields) if analysis_state else []
        metric_fields = list(analysis_state.metrics) if analysis_state else []
        dimension_fields = list(analysis_state.dimensions) if analysis_state else []
        dimension_value_samples = dict(analysis_state.dimension_value_samples) if analysis_state else {}
        active_filters = list(analysis_state.active_filters) if analysis_state else []
        prior_period = analysis_state.period if analysis_state else None
        selected_agent = continuation_state.selected_agent
        chartable = bool(continuation_state.chartable) or is_tabular_result(result)
        chart_requested = cls.question_requests_chart(question)
        chart_type = cls.resolved_chart_type(
            question=question,
            continuation_state=continuation_state,
        ) if chart_requested else None

        metric_focus = cls.extract_metric_follow_up(question)
        if metric_focus and (result or prior_question):
            resolved_metric_focus = (
                resolve_candidate_name(
                    metric_focus,
                    preferred_candidates=metric_fields,
                    fallback_candidates=available_fields,
                )
                or metric_focus
            )
            if cls.result_has_field(result, resolved_metric_focus):
                return FollowUpResolution(
                    kind="analyze_prior_result",
                    rationale=f"Reuse the prior verified result and focus on {resolved_metric_focus}.",
                    selected_agent=selected_agent,
                    reuse_last_result=True,
                    question_type="metric_follow_up",
                    focus_field=resolved_metric_focus,
                    resolved_question=cls.resolved_context_question(
                        prior_question=prior_question,
                        instruction=f"answer the same analysis but focus on {resolved_metric_focus}",
                        fallback_question=question,
                    ),
                    suggested_agent_mode="context_analysis",
                )
            if prior_question:
                return FollowUpResolution(
                    kind="requery_prior_analysis",
                    rationale=f"Run the same analysis again but focus on {resolved_metric_focus}.",
                    selected_agent=selected_agent,
                    reuse_last_result=False,
                    question_type="metric_follow_up",
                    focus_field=resolved_metric_focus,
                    resolved_question=cls.append_follow_up_instruction(
                        prior_question,
                        f"Answer the same analysis but focus on {resolved_metric_focus}.",
                    ),
                    suggested_agent_mode="sql",
                )

        breakdown_dimension = cls.extract_breakdown_dimension(question)
        if breakdown_dimension and prior_question:
            resolved_dimension = (
                resolve_candidate_name(
                    breakdown_dimension,
                    preferred_candidates=dimension_fields,
                    fallback_candidates=available_fields,
                )
                or breakdown_dimension
            )
            if result and cls.result_has_field(result, resolved_dimension):
                return FollowUpResolution(
                    kind="analyze_prior_result",
                    rationale=f"Reuse the prior verified result and break the answer down by {resolved_dimension}.",
                    selected_agent=selected_agent,
                    reuse_last_result=True,
                    question_type="breakdown_follow_up",
                    dimension=resolved_dimension,
                    resolved_question=cls.resolved_context_question(
                        prior_question=prior_question,
                        instruction=f"break the answer down by {resolved_dimension}",
                        fallback_question=question,
                    ),
                    suggested_agent_mode="context_analysis",
                )
            return FollowUpResolution(
                kind="requery_prior_analysis",
                rationale=f"Refine the prior analysis by breaking it down by {resolved_dimension}.",
                selected_agent=selected_agent,
                reuse_last_result=False,
                question_type="breakdown_follow_up",
                dimension=resolved_dimension,
                resolved_question=cls.append_follow_up_instruction(
                    prior_question,
                    f"Break the analysis down by {resolved_dimension}.",
                ),
                suggested_agent_mode="sql",
            )

        filter_refinement = cls.resolve_filter_refinement(
            question=question,
            dimension_fields=dimension_fields,
            dimension_value_samples=dimension_value_samples,
            active_filters=active_filters,
        )
        if filter_refinement and filter_refinement.ambiguous_fields:
            labels = join_values(filter_refinement.ambiguous_fields)
            value_label = str(filter_refinement.ambiguous_value or "that value").strip()
            clarification_question = (
                f"I found '{value_label}' in multiple fields: {labels}. Which field should I use?"
            )
            return FollowUpResolution(
                kind="clarify_follow_up",
                rationale=clarification_question,
                clarification_question=clarification_question,
            )
        if filter_refinement and prior_question:
            instruction = filters_instruction(filter_refinement.filters)
            if chart_requested and chart_type:
                instruction = f"{instruction} Present the result as a {chart_type} chart."
            return FollowUpResolution(
                kind="requery_prior_analysis",
                rationale=f"Refine the prior analysis with {instruction.casefold()}",
                selected_agent=selected_agent,
                reuse_last_result=False,
                question_type="filter_follow_up",
                chart_type=chart_type,
                filters=list(filter_refinement.filters),
                active_filters=list(filter_refinement.active_filters),
                resolved_question=cls.append_follow_up_instruction(prior_question, instruction),
                suggested_agent_mode="sql",
            )

        if result and chart_requested and chartable:
            return FollowUpResolution(
                kind="visualize_prior_result",
                rationale="Reuse the prior verified tabular result for the requested chart follow-up.",
                selected_agent=selected_agent,
                reuse_last_result=True,
                question_type="chart_follow_up",
                chart_type=chart_type,
                resolved_question=question,
                suggested_agent_mode="context_analysis",
            )

        rewritten_period_question = cls.rewrite_prior_question_for_requested_period(
            question=question,
            prior_question=prior_question,
            prior_period=prior_period,
        )
        if rewritten_period_question:
            return FollowUpResolution(
                kind="requery_prior_analysis",
                rationale="Rerun the prior analysis with the updated requested time period.",
                selected_agent=selected_agent,
                reuse_last_result=False,
                question_type="period_follow_up",
                period=cls.resolved_requested_period(
                    question=question,
                    prior_period=prior_period,
                ),
                resolved_question=rewritten_period_question,
                suggested_agent_mode="sql",
            )
        return None

    @staticmethod
    def result_has_field(result: Mapping[str, Any] | None, field_name: str) -> bool:
        available_fields = ContinuationStateBuilder.available_fields_from_result(result)
        return resolve_candidate_name(
            field_name,
            preferred_candidates=available_fields,
            fallback_candidates=available_fields,
        ) is not None

    @staticmethod
    def extract_metric_follow_up(question: str) -> str | None:
        text = str(question or "").strip()
        patterns = (
            r"\bsame but (?P<metric>[a-z0-9 _%-]+)",
            r"\binstead use (?P<metric>[a-z0-9 _%-]+)",
            r"\bfocus on (?P<metric>[a-z0-9 _%-]+)",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            metric = str(match.group("metric") or "").strip(" ?.,")
            if metric:
                return metric
        return None

    @staticmethod
    def extract_breakdown_dimension(question: str) -> str | None:
        text = str(question or "").strip()
        patterns = (
            r"\bbreak (?:that|it|this) down by (?P<dimension>[a-z0-9 _-]+)",
            r"\bshow (?:that|it|this) by (?P<dimension>[a-z0-9 _-]+)",
            r"\bsplit (?:that|it|this) by (?P<dimension>[a-z0-9 _-]+)",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            dimension = str(match.group("dimension") or "").strip(" ?.,")
            if dimension:
                return dimension
        return None

    @staticmethod
    def extract_requested_chart_type(question: str) -> str | None:
        text = str(question or "")
        for chart_type, patterns in (
            ("bar", (r"\bbar chart\b", r"\bbar graph\b", r"\bbar\b")),
            ("line", (r"\bline chart\b", r"\bline graph\b", r"\bline\b")),
            ("pie", (r"\bpie chart\b", r"\bpie\b")),
            ("donut", (r"\bdonut chart\b", r"\bdoughnut chart\b", r"\bdonut\b", r"\bdoughnut\b")),
            ("area", (r"\barea chart\b", r"\barea\b")),
        ):
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
                return chart_type
        return None

    @classmethod
    def resolved_chart_type(
        cls,
        *,
        question: str,
        continuation_state: ContinuationState,
    ) -> str | None:
        requested_chart_type = cls.extract_requested_chart_type(question)
        if requested_chart_type:
            return requested_chart_type
        if continuation_state.visualization_state and continuation_state.visualization_state.chart_type:
            return continuation_state.visualization_state.chart_type.strip().lower()
        return None

    @staticmethod
    def question_requests_chart(question: str) -> bool:
        tokens = set(re.findall(r"[a-z0-9]+", str(question or "").casefold()))
        chart_tokens = {"chart", "graph", "plot", "visual", "bar", "line", "pie", "donut"}
        return bool(tokens & chart_tokens)

    @classmethod
    def resolved_context_question(
        cls,
        *,
        prior_question: str,
        instruction: str,
        fallback_question: str,
    ) -> str:
        if prior_question:
            return (
                f"Using the prior verified result for '{prior_question.rstrip('?').strip()}', "
                f"{instruction.strip().rstrip('.')}."
            )
        return fallback_question

    @staticmethod
    def append_follow_up_instruction(prior_question: str, instruction: str) -> str:
        base = str(prior_question or "").strip()
        if not base:
            return instruction.strip()
        trimmed_base = base.rstrip()
        if trimmed_base.endswith("?"):
            return f"{trimmed_base.rstrip('?').rstrip()}. {instruction.strip()}"
        if trimmed_base.endswith((".", "!")):
            return f"{trimmed_base} {instruction.strip()}"
        return f"{trimmed_base}. {instruction.strip()}"

    @staticmethod
    def same_period(left: TimeframeState | None, right: TimeframeState | None) -> bool:
        return same_timeframe(left, right)

    @staticmethod
    def resolved_requested_period(
        *,
        question: str,
        prior_period: TimeframeState | None = None,
    ) -> TimeframeState | None:
        return resolve_requested_timeframe(
            question=question,
            prior_timeframe=prior_period,
        )

    @classmethod
    def rewrite_prior_question_for_requested_period(
        cls,
        *,
        question: str,
        prior_question: str,
        prior_period: TimeframeState | None = None,
    ) -> str | None:
        return rewrite_prior_question_for_requested_timeframe(
            question=question,
            prior_question=prior_question,
            prior_timeframe=prior_period,
        )

    @classmethod
    def resolve_filter_refinement(
        cls,
        *,
        question: str,
        dimension_fields: Sequence[str],
        dimension_value_samples: Mapping[str, Sequence[str]],
        active_filters: Sequence[FilterClause],
    ) -> _ResolvedFilterRefinement | None:
        parsed = cls.extract_filter_request(question)
        if parsed is None:
            return None
        grouped_filters: dict[tuple[str, str], list[str]] = {}
        ambiguous_fields: list[str] = []
        ambiguous_value: str | None = None

        for segment in cls._split_filter_segments(parsed.raw_value):
            segment_matches: list[tuple[str, list[tuple[str, str]]]] = []
            if cls._segment_has_connector(segment):
                split_matches = cls._resolve_connected_filter_segments(
                    segment=segment,
                    dimension_fields=dimension_fields,
                    dimension_value_samples=dimension_value_samples,
                )
                if split_matches:
                    segment_matches.extend(split_matches)
                else:
                    matches = cls._resolve_filter_segment(
                        segment=segment,
                        dimension_fields=dimension_fields,
                        dimension_value_samples=dimension_value_samples,
                    )
                    if matches:
                        segment_matches.append((segment, matches))
            else:
                matches = cls._resolve_filter_segment(
                    segment=segment,
                    dimension_fields=dimension_fields,
                    dimension_value_samples=dimension_value_samples,
                )
                if matches:
                    segment_matches.append((segment, matches))
            if not segment_matches:
                continue
            for current_segment, current_matches in segment_matches:
                field_names = {field_name for field_name, _ in current_matches}
                if len(field_names) > 1:
                    ambiguous_fields = sorted(field_names)
                    ambiguous_value = current_segment.strip()
                    break
                field_name = current_matches[0][0]
                key = (field_name, parsed.operator)
                values = grouped_filters.setdefault(key, [])
                seen = {normalize_field_name(item) for item in values}
                for _, resolved_value in current_matches:
                    normalized_value = normalize_field_name(resolved_value)
                    if normalized_value in seen:
                        continue
                    seen.add(normalized_value)
                    values.append(resolved_value)
            if ambiguous_fields:
                break

        if ambiguous_fields:
            return _ResolvedFilterRefinement(
                ambiguous_fields=ambiguous_fields,
                ambiguous_value=ambiguous_value,
            )

        filters = [
            FilterClause(field=field_name, operator=operator_name, values=values)
            for (field_name, operator_name), values in grouped_filters.items()
            if values
        ]
        if not filters:
            return None
        merged = merge_filter_clauses(active_filters=active_filters, refinements=filters)
        return _ResolvedFilterRefinement(filters=filters, active_filters=merged)

    @staticmethod
    def extract_filter_request(question: str) -> _ParsedFilterRequest | None:
        text = str(question or "").strip()
        patterns = (
            ("exclude", r"\bexclude (?P<value>.+)$"),
            ("exclude", r"\bexcluding (?P<value>.+)$"),
            ("exclude", r"\bwithout (?P<value>.+)$"),
            ("include", r"\bonly (?P<value>.+)$"),
            ("include", r"\bjust (?P<value>.+)$"),
        )
        for operator, pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            raw_value = str(match.group("value") or "").strip(" ?.,")
            if raw_value:
                return _ParsedFilterRequest(operator=operator, raw_value=raw_value)
        return None

    @classmethod
    def _resolve_filter_segment(
        cls,
        *,
        segment: str,
        dimension_fields: Sequence[str],
        dimension_value_samples: Mapping[str, Sequence[str]],
        allow_single_dimension_fallback: bool = True,
    ) -> list[tuple[str, str]]:
        requested = str(segment or "").strip(" ?.,")
        if not requested:
            return []
        matches: list[tuple[str, str]] = []
        for field_name, candidates in dimension_value_samples.items():
            matched_value = resolve_candidate_name(
                requested,
                preferred_candidates=[str(candidate) for candidate in candidates],
                fallback_candidates=[str(candidate) for candidate in candidates],
            )
            if matched_value:
                matches.append((str(field_name).strip(), matched_value))
        if matches:
            return matches
        if allow_single_dimension_fallback and len(dimension_fields) == 1:
            return [(str(dimension_fields[0]).strip(), requested)]
        return []

    @staticmethod
    def _split_filter_segments(raw_value: str) -> list[str]:
        values = [segment.strip(" ?.,") for segment in raw_value.split(",") if segment.strip(" ?.,")]
        return values or [str(raw_value or "").strip(" ?.,")]

    @staticmethod
    def _segment_has_connector(segment: str) -> bool:
        return bool(re.search(r"\s(?:and|&)\s", str(segment or ""), flags=re.IGNORECASE))

    @classmethod
    def _resolve_connected_filter_segments(
        cls,
        *,
        segment: str,
        dimension_fields: Sequence[str],
        dimension_value_samples: Mapping[str, Sequence[str]],
    ) -> list[tuple[str, list[tuple[str, str]]]]:
        sub_segments = [item.strip() for item in re.split(r"\s*(?:and|&)\s*", segment) if item.strip()]
        if len(sub_segments) < 2:
            return []
        resolved: list[tuple[str, list[tuple[str, str]]]] = []
        for sub_segment in sub_segments:
            matches = cls._resolve_filter_segment(
                segment=sub_segment,
                dimension_fields=dimension_fields,
                dimension_value_samples=dimension_value_samples,
                allow_single_dimension_fallback=False,
            )
            if not matches:
                return []
            resolved.append((sub_segment, matches))
        return resolved


def _merge_filter_values(existing: Sequence[str], additions: Sequence[str]) -> list[str]:
    values = [str(item).strip() for item in existing if str(item).strip()]
    seen = {normalize_field_name(item) for item in values}
    for value in additions:
        normalized = normalize_field_name(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        values.append(value)
    return values


def _subtract_filter_values(existing: Sequence[str], removals: Sequence[str]) -> list[str]:
    values = [str(item).strip() for item in existing if str(item).strip()]
    removal_keys = {normalize_field_name(value) for value in removals}
    return [value for value in values if normalize_field_name(value) not in removal_keys]


__all__ = [
    "AnalysisState",
    "ContinuationState",
    "ContinuationStateBuilder",
    "FilterClause",
    "FollowUpResolution",
    "FollowUpResolver",
    "PeriodState",
    "TimeframeState",
    "VisualizationState",
    "extract_period_state",
    "extract_timeframe_state",
    "filters_instruction",
    "humanize_field_name",
    "is_tabular_result",
    "join_values",
    "merge_filter_clauses",
    "normalize_field_name",
    "resolve_candidate_name",
]
