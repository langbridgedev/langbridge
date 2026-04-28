"""Plan review decisions for Langbridge AI PEV execution."""
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from langbridge.ai.base import (
    AgentIOContract,
    AgentResult,
    AgentResultStatus,
    AgentRoutingSpec,
    AgentSpecification,
    AgentTask,
    AgentTaskKind,
    BaseAgent,
)
from langbridge.ai.orchestration.execution import PlanExecutionState


class PlanReviewAction(str, Enum):
    continue_plan = "continue"
    revise_plan = "revise_plan"
    retry_step = "retry_step"
    ask_clarification = "ask_clarification"
    abort = "abort"
    finalize = "finalize"


class PlanReviewReasonCode(str, Enum):
    no_executed_step = "no_executed_step"
    agent_needs_clarification = "agent_needs_clarification"
    retryable_step_failure = "retryable_step_failure"
    deterministic_verification_failed = "deterministic_verification_failed"
    verification_failed_after_replans = "verification_failed_after_replans"
    weak_evidence = "weak_evidence"
    all_steps_completed = "all_steps_completed"
    pending_steps_remaining = "pending_steps_remaining"


class PlanReviewDecision(BaseModel):
    action: PlanReviewAction
    reason_code: PlanReviewReasonCode
    rationale: str
    updated_context: dict[str, Any] = Field(default_factory=dict)
    retry_step_id: str | None = None
    clarification_question: str | None = None


class PlanReviewAgent(BaseAgent):
    """Reviews verifier outcomes and decides whether the plan still fits."""

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="plan-review",
            description="Reviews plan progress after each verified step and selects continue, retry, replan, clarify, abort, or finalize.",
            task_kinds=[AgentTaskKind.orchestration],
            capabilities=["review PEV outcomes", "decide replanning", "detect weak results"],
            constraints=["Does not execute domain work."],
            routing=AgentRoutingSpec(keywords=["review", "replan", "retry"], direct_threshold=99),
            can_execute_direct=False,
            output_contract=AgentIOContract(required_keys=["decision"]),
        )

    async def execute(self, task: AgentTask) -> AgentResult:
        state_payload = task.context.get("plan_execution_state")
        if not isinstance(state_payload, PlanExecutionState):
            return self.build_result(
                task=task,
                status=AgentResultStatus.failed,
                error="PlanReviewAgent requires PlanExecutionState in context.",
            )
        decision = self.review(state_payload)
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"decision": decision.model_dump(mode="json")},
        )

    def review(self, state: PlanExecutionState) -> PlanReviewDecision:
        latest = state.latest_record
        if latest is None:
            return PlanReviewDecision(
                action=PlanReviewAction.abort,
                reason_code=PlanReviewReasonCode.no_executed_step,
                rationale="No executed step is available to review.",
            )

        result = latest.result
        verification = latest.verification
        if result.status == AgentResultStatus.needs_clarification:
            return PlanReviewDecision(
                action=PlanReviewAction.ask_clarification,
                reason_code=PlanReviewReasonCode.agent_needs_clarification,
                rationale=result.error or "Agent needs clarification.",
                clarification_question=result.error,
            )

        if not verification.passed:
            retry_count = state.step_retry_counts.get(latest.step.step_id, 0)
            if (
                result.status == AgentResultStatus.failed
                and retry_count < state.max_step_retries
                and self._is_retryable_failure(result)
            ):
                return PlanReviewDecision(
                    action=PlanReviewAction.retry_step,
                    reason_code=PlanReviewReasonCode.retryable_step_failure,
                    rationale="Agent failed with a retryable execution error.",
                    retry_step_id=latest.step.step_id,
                    updated_context={"last_error": result.error},
                )
            if state.replan_count < state.max_replans:
                return PlanReviewDecision(
                    action=PlanReviewAction.revise_plan,
                    reason_code=PlanReviewReasonCode.deterministic_verification_failed,
                    rationale=verification.message,
                    updated_context={
                        "verification_failure": verification.model_dump(mode="json"),
                        "failed_agent": latest.step.agent_name,
                    },
                )
            return PlanReviewDecision(
                action=PlanReviewAction.abort,
                reason_code=PlanReviewReasonCode.verification_failed_after_replans,
                rationale=f"Verification failed after replanning: {verification.message}",
            )

        if self._is_weak_result(result) and state.replan_count < state.max_replans:
            return PlanReviewDecision(
                action=PlanReviewAction.revise_plan,
                reason_code=PlanReviewReasonCode.weak_evidence,
                rationale="Latest step returned weak or empty evidence.",
                updated_context={"weak_result_agent": latest.step.agent_name},
            )

        if state.next_pending_step() is None:
            return PlanReviewDecision(
                action=PlanReviewAction.finalize,
                reason_code=PlanReviewReasonCode.all_steps_completed,
                rationale="All plan steps passed verification.",
            )

        return PlanReviewDecision(
            action=PlanReviewAction.continue_plan,
            reason_code=PlanReviewReasonCode.pending_steps_remaining,
            rationale="Latest step passed verification and plan still has pending work.",
        )

    @staticmethod
    def _is_weak_result(result: AgentResult) -> bool:
        output = result.output
        evidence_lists = [
            value
            for key in ("rows", "results", "findings", "sources")
            if isinstance((value := output.get(key)), list)
        ]
        if any(evidence_lists):
            return False
        nested_result = output.get("result")
        if isinstance(nested_result, dict):
            nested_rows = nested_result.get("rows")
            if isinstance(nested_rows, list) and nested_rows:
                return False
        if result.diagnostics.get("weak_results") is True:
            return True
        if result.diagnostics.get("weak_evidence") is True:
            return True
        outcome = output.get("outcome")
        if isinstance(outcome, dict) and outcome.get("status") == "empty_result":
            return True
        for key in ("rows", "results", "findings", "sources"):
            value = output.get(key)
            if isinstance(value, list) and len(value) == 0:
                return True
        return False

    @staticmethod
    def _is_retryable_failure(result: AgentResult) -> bool:
        diagnostics = result.diagnostics if isinstance(result.diagnostics, dict) else {}
        if diagnostics.get("retryable") is True or diagnostics.get("transient_error") is True:
            return True

        outcome = result.output.get("outcome")
        if isinstance(outcome, dict) and outcome.get("recoverable") is True:
            return True

        error_text = str(result.error or "").strip().lower()
        return any(
            marker in error_text
            for marker in (
                "timeout",
                "temporarily unavailable",
                "temporary",
                "connection reset",
                "connection aborted",
                "try again",
                "rate limit",
            )
        )


__all__ = ["PlanReviewAction", "PlanReviewAgent", "PlanReviewDecision", "PlanReviewReasonCode"]
