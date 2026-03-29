
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ResearchPlan(BaseModel):
    question: str
    subquestions: List[str] = Field(default_factory=list)
    hypotheses: List[str] = Field(default_factory=list)
    tool_strategy: List[str] = Field(default_factory=list)
    source_strategy: str = "prefer_diverse_sources"
    max_steps: int = Field(default=4, ge=1, le=10)
    target_coverage: float = Field(default=0.75, ge=0.0, le=1.0)


class EvidenceItem(BaseModel):
    id: str
    source_type: str
    source: str
    source_ref: Optional[str] = None
    domain: Optional[str] = None
    snippet: str
    relevance: float = Field(default=0.0, ge=0.0, le=1.0)
    quality: float = Field(default=0.0, ge=0.0, le=1.0)
    score: float = Field(default=0.0, ge=0.0, le=1.0)
    subquestion: Optional[str] = None
    metadata: Dict[str, str] = Field(default_factory=dict)


class ResearchState(BaseModel):
    started_at: datetime
    finished_at: Optional[datetime] = None
    steps_taken: int = 0
    max_steps: int = 4
    elapsed_ms: int = 0
    open_questions: List[str] = Field(default_factory=list)
    answered_questions: List[str] = Field(default_factory=list)
    attempted_tools: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    coverage_score: float = Field(default=0.0, ge=0.0, le=1.0)
    source_domains: List[str] = Field(default_factory=list)
    source_diversity: int = 0
    newly_added_evidence: int = 0
    diminishing_returns_count: int = 0
    stop_reason: Optional[str] = None
    step_trace: List[Dict[str, Any]] = Field(default_factory=list)


class ResearchFinding(BaseModel):
    id: str
    claim: str
    evidence_ids: List[str] = Field(default_factory=list)
    citations: List[str] = Field(default_factory=list)
    confidence: str = "medium"


class ResearchReport(BaseModel):
    question: str
    executive_summary: str
    key_findings: List[ResearchFinding] = Field(default_factory=list)
    supporting_evidence: Dict[str, List[str]] = Field(default_factory=dict)
    risks_uncertainties: List[str] = Field(default_factory=list)
    next_steps: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    weak_evidence: bool = False
    follow_up_question: Optional[str] = None


__all__ = [
    "ResearchPlan",
    "EvidenceItem",
    "ResearchState",
    "ResearchFinding",
    "ResearchReport",
]
