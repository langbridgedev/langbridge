"""Typed decision contracts for the Langbridge analyst stack."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


_ALLOWED_ANALYST_DECISION_MODES = {"sql", "context_analysis", "research", "clarify"}


class AnalystModeDecision(BaseModel):
    agent_mode: str
    reason: str
    clarification_question: str | None = None

    @model_validator(mode="after")
    def _validate_mode(self) -> "AnalystModeDecision":
        mode = str(self.agent_mode or "").strip().lower()
        if mode not in _ALLOWED_ANALYST_DECISION_MODES:
            raise ValueError(f"Unsupported analyst decision mode '{self.agent_mode}'.")
        self.agent_mode = mode
        if self.clarification_question is not None:
            text = str(self.clarification_question).strip()
            self.clarification_question = text or None
        self.reason = str(self.reason or "").strip()
        return self


class VisualizationRecommendation(BaseModel):
    recommendation: Literal["none", "helpful", "required"] = "none"
    chart_type: str | None = None
    rationale: str | None = None

    @model_validator(mode="after")
    def _normalize_payload(self) -> "VisualizationRecommendation":
        if self.chart_type is not None:
            chart_type = str(self.chart_type).strip().lower()
            self.chart_type = chart_type or None
        if self.rationale is not None:
            rationale = str(self.rationale).strip()
            self.rationale = rationale or None
        return self


class AnalystEvidencePlanStep(BaseModel):
    """One internal evidence-building step owned by the analyst agent."""

    step_id: str
    action: Literal["query_governed", "augment_with_web", "synthesize", "clarify"] = "query_governed"
    question: str | None = None
    search_query: str | None = None
    evidence_goal: str
    expected_signal: str | None = None
    success_criteria: str | None = None
    depends_on: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalize_payload(self) -> "AnalystEvidencePlanStep":
        self.step_id = str(self.step_id or "").strip()
        if not self.step_id:
            raise ValueError("Evidence plan step_id is required.")
        self.evidence_goal = str(self.evidence_goal or "").strip()
        if not self.evidence_goal:
            raise ValueError("Evidence plan evidence_goal is required.")
        for attr in ("question", "search_query", "expected_signal", "success_criteria"):
            value = getattr(self, attr)
            if value is not None:
                text = str(value).strip()
                setattr(self, attr, text or None)
        self.depends_on = [str(item).strip() for item in self.depends_on if str(item).strip()]
        return self


class AnalystEvidencePlan(BaseModel):
    """Internal analyst-owned plan for evidence retrieval and synthesis."""

    objective: str
    question_type: str | None = None
    timeframe: str | None = None
    required_metrics: list[str] = Field(default_factory=list)
    required_dimensions: list[str] = Field(default_factory=list)
    steps: list[AnalystEvidencePlanStep] = Field(default_factory=list)
    synthesis_requirements: list[str] = Field(default_factory=list)
    external_context_needed: bool = False
    visualization_recommendation: VisualizationRecommendation | None = None

    @model_validator(mode="after")
    def _normalize_payload(self) -> "AnalystEvidencePlan":
        self.objective = str(self.objective or "").strip()
        if not self.objective:
            raise ValueError("Evidence plan objective is required.")
        for attr in ("question_type", "timeframe"):
            value = getattr(self, attr)
            if value is not None:
                text = str(value).strip()
                setattr(self, attr, text or None)
        self.required_metrics = [str(item).strip() for item in self.required_metrics if str(item).strip()]
        self.required_dimensions = [str(item).strip() for item in self.required_dimensions if str(item).strip()]
        self.synthesis_requirements = [
            str(item).strip() for item in self.synthesis_requirements if str(item).strip()
        ]
        return self


__all__ = [
    "AnalystEvidencePlan",
    "AnalystEvidencePlanStep",
    "AnalystModeDecision",
    "VisualizationRecommendation",
]
