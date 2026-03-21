
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class RouteName(str, Enum):
    """Supported planner routes."""

    SIMPLE_ANALYST = "SimpleAnalyst"
    ANALYST_THEN_VISUAL = "AnalystThenVisual"
    WEB_SEARCH = "WebSearch"
    DEEP_RESEARCH = "DeepResearch"
    CLARIFY = "Clarify"


class AgentName(str, Enum):
    """Planner-recognised agent identifiers."""

    ANALYST = "Analyst"
    VISUAL = "Visual"
    WEB_SEARCH = "WebSearch"
    DOC_RETRIEVAL = "DocRetrieval"
    CLARIFY = "Clarify"


class PlanningConstraints(BaseModel):
    """User or system supplied execution constraints for the planner."""

    max_steps: int = Field(default=4, ge=1, le=10)
    ignore_max_steps: bool = False
    prefer_low_latency: bool = True
    cost_sensitivity: str = Field(default="medium")
    require_viz_when_chartable: bool = True
    allow_sql_analyst: bool = True
    allow_web_search: bool = True
    allow_deep_research: bool = True
    timebox_seconds: int = Field(default=30, ge=5, le=600)

    @field_validator("cost_sensitivity")
    @classmethod
    def _validate_cost_sensitivity(cls, value: str) -> str:
        allowed = {"low", "medium", "high"}
        lowered = value.lower()
        if lowered not in allowed:
            raise ValueError(
                f"Unsupported cost_sensitivity '{value}'. Expected one of {sorted(allowed)}."
            )
        return lowered


class PlannerRequest(BaseModel):
    """Top-level input to the planning agent."""

    actor_id: Optional[str] = None
    question: str
    context: Optional[Dict[str, Any]] = None
    constraints: PlanningConstraints = Field(default_factory=PlanningConstraints)

    @field_validator("question")
    @classmethod
    def _validate_question(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("question must be a non-empty string.")
        return cleaned


class PlanStep(BaseModel):
    """Single executable step in the plan."""

    id: str
    agent: str
    input: Dict[str, Any] = Field(default_factory=dict)
    expected_output: Dict[str, Any] = Field(default_factory=dict)


class Plan(BaseModel):
    """Planner output consumed by the supervisor/orchestrator."""

    route: str
    steps: List[PlanStep]
    justification: str
    user_summary: str
    assumptions: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)


@dataclass(slots=True)
class RouteSignals:
    """Feature flags extracted from the user question."""

    has_sql_signals: bool = False
    has_visual_cues: bool = False
    has_research_signals: bool = False
    has_web_search_signals: bool = False
    requires_clarification: bool = False
    chartable: bool = False
    has_time_reference: bool = False
    has_entity_reference: bool = False


@dataclass(slots=True)
class RouteDecision:
    """Routing outcome returned by the router."""

    route: RouteName
    justification: str
    signals: RouteSignals
    assumptions: List[str] = field(default_factory=list)
