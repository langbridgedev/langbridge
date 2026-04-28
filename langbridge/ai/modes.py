"""Shared mode contracts for Langbridge AI."""

from __future__ import annotations

from enum import Enum
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from langbridge.ai.base import AgentIOContract


class AnalystAgentMode(str, Enum):
    auto = "auto"
    sql = "sql"
    context_analysis = "context_analysis"
    research = "research"


_ANALYST_MODE_ALIASES = {
    "answer": AnalystAgentMode.context_analysis.value,
    "analysis": AnalystAgentMode.context_analysis.value,
    "context": AnalystAgentMode.context_analysis.value,
    "deep_research": AnalystAgentMode.research.value,
    "deep-research": AnalystAgentMode.research.value,
    "web_research": AnalystAgentMode.research.value,
    "web-research": AnalystAgentMode.research.value,
}

_ANALYST_OUTPUT_OPTIONAL_KEYS = [
    "analysis_path",
    "sql_canonical",
    "sql_executable",
    "selected_datasets",
    "selected_semantic_models",
    "query_scope",
    "outcome",
    "error_taxonomy",
    "evidence",
    "synthesis",
    "findings",
    "sources",
    "follow_ups",
    "review_hints",
    "verdict",
    "key_comparisons",
    "limitations",
    "visualization_recommendation",
    "recommended_chart_type",
    "artifacts",
    "evidence_plan",
    "evidence_bundle",
    "evidence_gaps",
    "next_question",
]


def normalize_analyst_mode(
    value: Any,
    *,
    default: AnalystAgentMode | None = None,
) -> AnalystAgentMode | None:
    text = str(value or "").strip().lower()
    if not text:
        return default
    normalized = _ANALYST_MODE_ALIASES.get(text, text)
    try:
        return AnalystAgentMode(normalized)
    except ValueError as exc:
        raise ValueError(f"Unsupported analyst mode '{text}'.") from exc


def normalize_analyst_task_input(
    input_payload: dict[str, Any] | None,
    *,
    requested_mode: Any = None,
) -> dict[str, Any]:
    normalized = dict(input_payload or {})
    raw_mode = normalized.get("agent_mode")
    if raw_mode in (None, ""):
        raw_mode = normalized.get("mode")
    if raw_mode in (None, ""):
        raw_mode = requested_mode
    mode = normalize_analyst_mode(raw_mode, default=AnalystAgentMode.auto)
    normalized.pop("mode", None)
    if mode is None or mode == AnalystAgentMode.auto:
        normalized.pop("agent_mode", None)
        return normalized
    normalized["agent_mode"] = mode.value
    return normalized


def analyst_output_contract_for_task_input(
    input_payload: dict[str, Any] | None,
    *,
    requested_mode: Any = None,
) -> AgentIOContract:
    normalized = normalize_analyst_task_input(input_payload, requested_mode=requested_mode)
    mode = normalize_analyst_mode(normalized.get("agent_mode"), default=AnalystAgentMode.auto)
    if mode == AnalystAgentMode.research:
        return AgentIOContract(
            required_keys=["analysis", "result", "synthesis", "sources", "findings"],
            optional_keys=list(_ANALYST_OUTPUT_OPTIONAL_KEYS),
        )
    if mode == AnalystAgentMode.sql:
        return AgentIOContract(
            required_keys=["analysis", "result", "outcome", "evidence", "review_hints"],
            optional_keys=list(_ANALYST_OUTPUT_OPTIONAL_KEYS),
        )
    if mode == AnalystAgentMode.context_analysis:
        return AgentIOContract(
            required_keys=["analysis", "result", "evidence", "review_hints"],
            optional_keys=list(_ANALYST_OUTPUT_OPTIONAL_KEYS),
        )
    return AgentIOContract(
        required_keys=["analysis", "result", "evidence", "review_hints"],
        optional_keys=list(_ANALYST_OUTPUT_OPTIONAL_KEYS),
    )


def normalize_analyst_mode_decision(
    value: Any,
    *,
    default_mode: AnalystAgentMode | None = None,
) -> AnalystModeDecision | None:
    from langbridge.ai.agents.analyst.contracts import AnalystModeDecision

    if isinstance(value, AnalystModeDecision):
        return value
    if value is None or value == "":
        return None

    payload: dict[str, Any]
    if isinstance(value, BaseModel):
        payload = value.model_dump(mode="json")
    elif isinstance(value, Mapping):
        payload = dict(value)
    else:
        payload = {"agent_mode": value}

    if "mode" in payload and "agent_mode" not in payload:
        payload["agent_mode"] = payload.pop("mode")

    raw_mode = payload.get("agent_mode")
    raw_mode_text = str(getattr(raw_mode, "value", raw_mode) or "").strip().lower()
    if raw_mode_text:
        payload["agent_mode"] = {
            "answer": "context_analysis",
            "analysis": "context_analysis",
            "context": "context_analysis",
            "deep_research": "research",
            "deep-research": "research",
            "web_research": "research",
            "web-research": "research",
        }.get(raw_mode_text, raw_mode_text)

    if default_mode is not None and default_mode != AnalystAgentMode.auto and not payload.get("agent_mode"):
        payload["agent_mode"] = default_mode.value

    return AnalystModeDecision.model_validate(payload)


def normalize_visualization_recommendation(value: Any) -> VisualizationRecommendation | None:
    from langbridge.ai.agents.analyst.contracts import VisualizationRecommendation

    if isinstance(value, VisualizationRecommendation):
        return value
    if value is None or value == "":
        return None
    if isinstance(value, BaseModel):
        return VisualizationRecommendation.model_validate(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return VisualizationRecommendation.model_validate(dict(value))
    return VisualizationRecommendation.model_validate(value)


__all__ = [
    "AnalystAgentMode",
    "analyst_output_contract_for_task_input",
    "normalize_analyst_mode",
    "normalize_analyst_mode_decision",
    "normalize_analyst_task_input",
    "normalize_visualization_recommendation",
]
