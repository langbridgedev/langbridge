"""Deterministic PEV verification for Langbridge AI."""
from enum import Enum

from pydantic import BaseModel, Field

from langbridge.ai.base import AgentResult, AgentResultStatus
from langbridge.ai.orchestration.planner import PlanStep


class VerificationReasonCode(str, Enum):
    passed = "passed"
    agent_mismatch = "agent_mismatch"
    non_succeeded_status = "non_succeeded_status"
    missing_output_keys = "missing_output_keys"
    planner_no_steps = "planner_no_steps"


class VerificationOutcome(BaseModel):
    passed: bool
    step_id: str
    agent_name: str
    message: str
    reason_code: VerificationReasonCode
    missing_output_keys: list[str] = Field(default_factory=list)


class AgentVerifier:
    """Verifies one executed plan step against deterministic contracts."""

    def verify(self, *, step: PlanStep, result: AgentResult) -> VerificationOutcome:
        if result.agent_name != step.agent_name:
            return VerificationOutcome(
                passed=False,
                step_id=step.step_id,
                agent_name=step.agent_name,
                message="Agent result came from a different agent.",
                reason_code=VerificationReasonCode.agent_mismatch,
            )
        if result.status != AgentResultStatus.succeeded:
            return VerificationOutcome(
                passed=False,
                step_id=step.step_id,
                agent_name=step.agent_name,
                message=result.error or f"Agent returned status {result.status.value}.",
                reason_code=VerificationReasonCode.non_succeeded_status,
            )

        missing_keys = [
            key for key in step.expected_output.required_keys if key not in result.output
        ]
        if missing_keys:
            return VerificationOutcome(
                passed=False,
                step_id=step.step_id,
                agent_name=step.agent_name,
                message="Agent output missed required contract keys.",
                reason_code=VerificationReasonCode.missing_output_keys,
                missing_output_keys=missing_keys,
            )

        return VerificationOutcome(
            passed=True,
            step_id=step.step_id,
            agent_name=step.agent_name,
            message="Step output passed deterministic verification.",
            reason_code=VerificationReasonCode.passed,
        )


__all__ = ["AgentVerifier", "VerificationOutcome", "VerificationReasonCode"]
