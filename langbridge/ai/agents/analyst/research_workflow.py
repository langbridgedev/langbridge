"""Bounded research workflow contracts for the Langbridge analyst."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from langbridge.ai.agents.analyst.contracts import AnalystEvidencePlan


class ResearchDecisionAction(str, Enum):
    query_governed = "query_governed"
    augment_with_web = "augment_with_web"
    synthesize = "synthesize"
    clarify = "clarify"


class VisualizationRecommendation(str, Enum):
    none = "none"
    helpful = "helpful"
    required = "required"


class ResearchStepDecision(BaseModel):
    action: ResearchDecisionAction
    rationale: str
    governed_question: str | None = None
    search_query: str | None = None
    clarification_question: str | None = None
    visualization_recommendation: VisualizationRecommendation = VisualizationRecommendation.none
    recommended_chart_type: str | None = None
    plan_step_id: str | None = None
    evidence_goal: str | None = None
    expected_signal: str | None = None
    success_criteria: str | None = None
    gaps_addressed: list[str] = Field(default_factory=list)
    depends_on_rounds: list[int] = Field(default_factory=list)
    synthesis_readiness: str | None = None


@dataclass(slots=True)
class ResearchGovernedRound:
    question: str
    status: str
    query_scope: str | None
    rowcount: int | None
    answered_question: bool
    weak_evidence: bool
    analysis: str
    limitations: list[str] = field(default_factory=list)
    follow_ups: list[str] = field(default_factory=list)
    output: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_prompt_dict(self) -> dict[str, Any]:
        result = self.output.get("result") if isinstance(self.output.get("result"), dict) else {}
        return {
            "question": self.question,
            "status": self.status,
            "query_scope": self.query_scope,
            "rowcount": self.rowcount,
            "answered_question": self.answered_question,
            "weak_evidence": self.weak_evidence,
            "analysis": self.analysis,
            "sql_canonical": self.output.get("sql_canonical"),
            "selected_datasets": self.output.get("selected_datasets") or [],
            "selected_semantic_models": self.output.get("selected_semantic_models") or [],
            "result": _compact_result_payload(result),
            "limitations": list(self.limitations),
            "follow_ups": list(self.follow_ups),
        }

    def to_dict(self) -> dict[str, Any]:
        payload = self.to_prompt_dict()
        payload["output"] = self.output
        payload["diagnostics"] = self.diagnostics
        return payload


@dataclass(slots=True)
class EvidenceBundle:
    """Durable evidence collected by an analyst research workflow."""

    original_question: str
    evidence_plan: AnalystEvidencePlan | None = None
    governed_rounds: list[ResearchGovernedRound] = field(default_factory=list)
    sources: list[dict[str, Any]] = field(default_factory=list)
    web_search_queries: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def add_governed_round(self, round_: ResearchGovernedRound) -> None:
        self.governed_rounds.append(round_)

    def add_sources(self, *, query: str | None, sources: list[dict[str, Any]]) -> None:
        if query:
            self.web_search_queries.append(query)
        self.sources = list(sources)

    def add_note(self, note: str | None) -> None:
        text = str(note or "").strip()
        if text:
            self.notes.append(text)

    @property
    def governed_round_count(self) -> int:
        return len(self.governed_rounds)

    @property
    def answered_by_governed(self) -> bool:
        return any(round_.answered_question for round_ in self.governed_rounds)

    @property
    def has_evidence(self) -> bool:
        return bool(self.sources or self.governed_rounds)

    @property
    def has_usable_evidence(self) -> bool:
        return bool(self.sources or self.answered_by_governed)

    def latest_governed_output(self) -> dict[str, Any]:
        for round_ in reversed(self.governed_rounds):
            if round_.output:
                return round_.output
        return {}

    def best_governed_output(self) -> dict[str, Any]:
        for round_ in reversed(self.governed_rounds):
            if round_.answered_question and round_.output:
                return round_.output
        return self.latest_governed_output()

    def best_governed_diagnostics(self) -> dict[str, Any]:
        for round_ in reversed(self.governed_rounds):
            if round_.answered_question and round_.diagnostics:
                return round_.diagnostics
        for round_ in reversed(self.governed_rounds):
            if round_.diagnostics:
                return round_.diagnostics
        return {}

    def assessment(self) -> dict[str, Any]:
        weak_rounds = [round_ for round_ in self.governed_rounds if round_.weak_evidence]
        failed_rounds = [round_ for round_ in self.governed_rounds if round_.status not in {"succeeded", "success"}]
        return {
            "has_evidence": self.has_evidence,
            "has_usable_evidence": self.has_usable_evidence,
            "governed_round_count": self.governed_round_count,
            "answered_by_governed": self.answered_by_governed,
            "source_count": len(self.sources),
            "weak_governed_round_count": len(weak_rounds),
            "failed_governed_round_count": len(failed_rounds),
            "notes": list(self.notes),
        }

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "original_question": self.original_question,
            "evidence_plan": self._plan_payload(),
            "assessment": self.assessment(),
            "governed_rounds": [round_.to_prompt_dict() for round_ in self.governed_rounds],
            "source_count": len(self.sources),
            "source_urls": [str(item.get("url") or item.get("source") or "") for item in self.sources[:6]],
            "web_search_queries": list(self.web_search_queries),
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "original_question": self.original_question,
            "evidence_plan": self._plan_payload(),
            "assessment": self.assessment(),
            "governed_rounds": [round_.to_dict() for round_ in self.governed_rounds],
            "sources": list(self.sources),
            "web_search_queries": list(self.web_search_queries),
            "notes": list(self.notes),
        }

    def _plan_payload(self) -> dict[str, Any] | None:
        if self.evidence_plan is None:
            return None
        return self.evidence_plan.model_dump(mode="json", exclude_none=True)


@dataclass(slots=True)
class ResearchWorkflowState:
    original_question: str
    evidence_plan: AnalystEvidencePlan | None = None
    governed_rounds: list[ResearchGovernedRound] = field(default_factory=list)
    sources: list[dict[str, Any]] = field(default_factory=list)
    web_search_queries: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    evidence_bundle: EvidenceBundle | None = None

    def __post_init__(self) -> None:
        if self.evidence_bundle is None:
            self.evidence_bundle = EvidenceBundle(
                original_question=self.original_question,
                evidence_plan=self.evidence_plan,
            )

    def add_governed_round(
        self,
        *,
        question: str,
        status: str,
        query_scope: str | None,
        rowcount: int | None,
        answered_question: bool,
        weak_evidence: bool,
        analysis: str,
        limitations: list[str] | None = None,
        follow_ups: list[str] | None = None,
        output: dict[str, Any] | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        round_ = ResearchGovernedRound(
            question=question,
            status=status,
            query_scope=query_scope,
            rowcount=rowcount,
            answered_question=answered_question,
            weak_evidence=weak_evidence,
            analysis=analysis,
            limitations=list(limitations or []),
            follow_ups=list(follow_ups or []),
            output=dict(output or {}),
            diagnostics=dict(diagnostics or {}),
        )
        self.governed_rounds.append(round_)
        self.evidence_bundle.add_governed_round(round_)

    def add_sources(self, *, query: str | None, sources: list[dict[str, Any]]) -> None:
        if query:
            self.web_search_queries.append(query)
        self.sources = list(sources)
        self.evidence_bundle.add_sources(query=query, sources=self.sources)

    def add_note(self, note: str | None) -> None:
        text = str(note or "").strip()
        if text:
            self.notes.append(text)
            self.evidence_bundle.add_note(text)

    @property
    def governed_round_count(self) -> int:
        return len(self.governed_rounds)

    @property
    def answered_by_governed(self) -> bool:
        return any(round_.answered_question for round_ in self.governed_rounds)

    def compact_payload(self) -> dict[str, Any]:
        return {
            "original_question": self.original_question,
            "evidence_plan": (
                self.evidence_plan.model_dump(mode="json", exclude_none=True)
                if self.evidence_plan is not None
                else None
            ),
            "governed_round_count": self.governed_round_count,
            "answered_by_governed": self.answered_by_governed,
            "governed_rounds": [round_.to_prompt_dict() for round_ in self.governed_rounds],
            "evidence_bundle": self.evidence_bundle.to_prompt_dict(),
            "source_count": len(self.sources),
            "source_urls": [str(item.get("url") or item.get("source") or "") for item in self.sources[:6]],
            "web_search_queries": list(self.web_search_queries),
            "notes": list(self.notes),
        }


def _compact_result_payload(result: dict[str, Any], *, max_rows: int = 25) -> dict[str, Any]:
    if not result:
        return {}
    rows = result.get("rows")
    row_items = list(rows[:max_rows]) if isinstance(rows, list) else []
    return {
        "columns": result.get("columns") or [],
        "rows": row_items,
        "rowcount": result.get("rowcount"),
        "truncated": isinstance(rows, list) and len(rows) > max_rows,
    }


__all__ = [
    "EvidenceBundle",
    "ResearchDecisionAction",
    "ResearchGovernedRound",
    "ResearchStepDecision",
    "ResearchWorkflowState",
    "VisualizationRecommendation",
]
