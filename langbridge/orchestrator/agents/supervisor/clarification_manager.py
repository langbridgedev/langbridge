
import hashlib
from typing import Optional

from .schemas import ClarificationDecision, ClarificationState, ClassifiedQuestion


class ClarificationManager:
    """Clarification dedupe/max-turn guard driven by classifier output."""

    def __init__(self, *, default_max_turns: int = 2) -> None:
        self._default_max_turns = max(1, int(default_max_turns))

    def decide(
        self,
        *,
        classification: ClassifiedQuestion,
        prior_state: Optional[ClarificationState],
    ) -> ClarificationDecision:
        state = prior_state.model_copy(deep=True) if prior_state else ClarificationState()
        if state.max_turns < 1:
            state.max_turns = self._default_max_turns

        if not classification.requires_clarification:
            state.pending_slots = []
            return ClarificationDecision(updated_state=state)

        missing = [item for item in classification.required_context if isinstance(item, str) and item.strip()]
        state.pending_slots = list(missing)
        clarifying_question = (
            classification.clarifying_question
            or "Could you provide the missing context so I can proceed accurately?"
        )
        question_hash = self._question_hash(clarifying_question)

        if question_hash == state.last_question_hash or clarifying_question in state.asked_questions:
            return self._best_effort_decision(state=state, missing=missing)
        if state.turn_count >= state.max_turns:
            return self._best_effort_decision(state=state, missing=missing)

        state.turn_count += 1
        state.asked_questions.append(clarifying_question)
        state.last_question_hash = question_hash
        state.asked_slots.extend([item for item in missing if item not in state.asked_slots])
        return ClarificationDecision(
            requires_clarification=True,
            clarifying_question=clarifying_question,
            missing_blocking_slots=missing,
            updated_state=state,
        )

    @staticmethod
    def _question_hash(value: str) -> str:
        return hashlib.sha1(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _best_effort_decision(*, state: ClarificationState, missing: list[str]) -> ClarificationDecision:
        assumptions = [
            "Proceeding with best-effort assumptions because clarification budget was exhausted."
        ]
        if missing:
            assumptions.append("Unresolved context: " + ", ".join(missing[:5]) + ".")
        state.assumptions.extend([item for item in assumptions if item not in state.assumptions])
        return ClarificationDecision(
            requires_clarification=False,
            missing_blocking_slots=missing,
            assumptions=assumptions,
            updated_state=state,
        )


__all__ = ["ClarificationManager"]
