"""Execution state for specification-driven Langbridge AI plans."""
from typing import Any

from pydantic import BaseModel, Field

from langbridge.ai.base import AgentResult
from langbridge.ai.orchestration.planner import ExecutionPlan, PlanStep
from langbridge.ai.orchestration.verification import VerificationOutcome


class PlanStepRecord(BaseModel):
    step: PlanStep
    result: AgentResult
    verification: VerificationOutcome
    retry_count: int = 0


class PlanExecutionState(BaseModel):
    """Mutable execution snapshot passed between planner, verifier, and review."""

    original_question: str
    current_plan: ExecutionPlan
    records: list[PlanStepRecord] = Field(default_factory=list)
    completed_steps: list[PlanStepRecord] = Field(default_factory=list)
    failed_steps: list[PlanStepRecord] = Field(default_factory=list)
    observations: list[str] = Field(default_factory=list)
    verifier_outcomes: list[VerificationOutcome] = Field(default_factory=list)
    review_decisions: list[dict[str, Any]] = Field(default_factory=list)
    iteration: int = 0
    replan_count: int = 0
    max_iterations: int = 8
    max_replans: int = 2
    max_step_retries: int = 1
    step_retry_counts: dict[str, int] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)

    def record(
        self,
        *,
        step: PlanStep,
        result: AgentResult,
        verification: VerificationOutcome,
    ) -> PlanStepRecord:
        retry_count = self.step_retry_counts.get(step.step_id, 0)
        record = PlanStepRecord(
            step=step,
            result=result,
            verification=verification,
            retry_count=retry_count,
        )
        self.records.append(record)
        if verification.passed:
            self.completed_steps.append(record)
        else:
            self.failed_steps.append(record)
        self.verifier_outcomes.append(verification)
        self.iteration += 1
        return record

    def record_review(self, decision: Any) -> None:
        payload = decision.model_dump(mode="json") if hasattr(decision, "model_dump") else dict(decision)
        self.review_decisions.append(payload)

    def increment_retry(self, step_id: str) -> int:
        retry_count = self.step_retry_counts.get(step_id, 0) + 1
        self.step_retry_counts[step_id] = retry_count
        return retry_count

    @property
    def latest_record(self) -> PlanStepRecord | None:
        return self.records[-1] if self.records else None

    @property
    def completed_step_ids(self) -> set[str]:
        return {record.step.step_id for record in self.completed_steps}

    @property
    def pending_steps(self) -> list[PlanStep]:
        completed = self.completed_step_ids
        return [
            step
            for step in self.current_plan.steps
            if step.step_id not in completed
        ]

    def has_pending_steps(self) -> bool:
        return bool(self.pending_steps)

    def next_pending_step(self) -> PlanStep | None:
        completed = self.completed_step_ids
        for step in self.current_plan.steps:
            if step.step_id in completed:
                continue
            if all(dependency in completed for dependency in step.depends_on):
                return step
        return None

    def step_results_payload(self) -> list[dict[str, Any]]:
        return [record.result.model_dump(mode="json") for record in self.completed_steps]


__all__ = ["PlanExecutionState", "PlanStepRecord"]
