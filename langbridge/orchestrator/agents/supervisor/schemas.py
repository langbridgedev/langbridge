
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ClassifiedQuestion(BaseModel):
    """Structured supervisor classification output."""

    intent: str = "analytical"
    route_hint: Optional[str] = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    requires_clarification: bool = False
    clarifying_question: Optional[str] = None
    required_context: List[str] = Field(default_factory=list)
    extracted_entities: Dict[str, str] = Field(default_factory=dict)
    rationale: Optional[str] = None


class ResolvedEntities(BaseModel):
    """Canonical entity slots extracted from user input."""

    fund: Optional[str] = None
    region: Optional[str] = None
    time_period: Optional[str] = None
    metric: Optional[str] = None
    currency: Optional[str] = None
    benchmark: Optional[str] = None
    raw_entities: Dict[str, Any] = Field(default_factory=dict)

    def slot_values(self) -> Dict[str, str]:
        slots = {
            "fund": self.fund,
            "region": self.region,
            "time_period": self.time_period,
            "metric": self.metric,
            "currency": self.currency,
            "benchmark": self.benchmark,
        }
        return {
            key: value.strip()
            for key, value in slots.items()
            if isinstance(value, str) and value.strip()
        }


class ClarificationState(BaseModel):
    """State machine snapshot for clarification management."""

    turn_count: int = Field(default=0, ge=0)
    max_turns: int = Field(default=2, ge=1)
    asked_slots: List[str] = Field(default_factory=list)
    answered_slots: Dict[str, str] = Field(default_factory=dict)
    asked_questions: List[str] = Field(default_factory=list)
    last_question_hash: Optional[str] = None
    pending_slots: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)


class ClarificationDecision(BaseModel):
    """Supervisor clarification outcome."""

    requires_clarification: bool = False
    clarifying_question: Optional[str] = None
    missing_blocking_slots: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    updated_state: ClarificationState = Field(default_factory=ClarificationState)


class MemoryItem(BaseModel):
    """Memory item retrieved from long-term memory storage."""

    id: Optional[str] = None
    thread_id: Optional[str] = None
    actor_id: Optional[str] = None
    category: str
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    last_accessed_at: Optional[datetime] = None
    score: Optional[float] = None


class MemoryRetrievalResult(BaseModel):
    """Composite short-term + long-term memory retrieval payload."""

    short_term_context: str = ""
    retrieved_items: List[MemoryItem] = Field(default_factory=list)


__all__ = [
    "ClassifiedQuestion",
    "ResolvedEntities",
    "ClarificationState",
    "ClarificationDecision",
    "MemoryItem",
    "MemoryRetrievalResult",
]
