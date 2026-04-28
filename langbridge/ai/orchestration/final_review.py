"""Semantic final review scaffold for Langbridge AI."""
import json
import re
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
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.orchestration.final_review_prompts import build_final_review_prompt


class FinalReviewAction(str, Enum):
    approve = "approve"
    revise_answer = "revise_answer"
    replan = "replan"
    ask_clarification = "ask_clarification"
    abort = "abort"


class FinalReviewReasonCode(str, Enum):
    grounded_complete = "grounded_complete"
    missing_caveat_or_framing = "missing_caveat_or_framing"
    artifact_contract_mismatch = "artifact_contract_mismatch"
    insufficient_evidence_or_workflow = "insufficient_evidence_or_workflow"
    ambiguous_question = "ambiguous_question"
    unsafe_to_finalize = "unsafe_to_finalize"
    review_error = "review_error"


class FinalReviewDecision(BaseModel):
    action: FinalReviewAction
    reason_code: FinalReviewReasonCode
    rationale: str
    issues: list[str] = Field(default_factory=list)
    updated_context: dict[str, Any] = Field(default_factory=dict)
    clarification_question: str | None = None


class FinalReviewAgent(BaseAgent):
    """Reviews the current answer package before presentation."""

    _ARTIFACT_REF_RE = re.compile(r"\{\{\s*artifact:([A-Za-z0-9_.:-]+)\s*\}\}")
    _MALFORMED_ARTIFACT_REF_RE = re.compile(
        r"\{\{\s*(?:ARTIFACT_|artifact_)[^}]+\}\}|\{ARTIFACT_[^}]+\}",
        re.IGNORECASE,
    )

    def __init__(self, *, llm_provider: LLMProvider) -> None:
        self._llm = llm_provider

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="final-review",
            description="Reviews the current analytical answer package for grounding, completeness, and final-answer readiness.",
            task_kinds=[AgentTaskKind.orchestration],
            capabilities=["review final analytical answer", "detect unsupported claims", "request revise, replan, or clarification"],
            constraints=["Does not execute domain work directly.", "Does not format presentation output."],
            routing=AgentRoutingSpec(keywords=["review", "final review"], direct_threshold=99),
            can_execute_direct=False,
            output_contract=AgentIOContract(required_keys=["decision"]),
        )

    async def execute(self, task: AgentTask) -> AgentResult:
        answer_package = self._extract_mapping(task, "answer_package")
        if answer_package is None:
            return self.build_result(
                task=task,
                status=AgentResultStatus.failed,
                error="FinalReviewAgent requires answer_package in task context or input.",
            )

        decision = await self.review(
            question=task.question,
            answer_package=answer_package,
            evidence=self._extract_mapping(task, "evidence"),
            result=self._extract_mapping(task, "result"),
            research=self._extract_mapping(task, "research"),
            step_results=self._extract_sequence(task, "step_results"),
        )
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"decision": decision.model_dump(mode="json")},
            diagnostics={
                "action": decision.action.value,
                "reason_code": decision.reason_code.value,
                "issue_count": len(decision.issues),
            },
        )

    async def review(
        self,
        *,
        question: str,
        answer_package: dict[str, Any],
        evidence: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        research: dict[str, Any] | None = None,
        step_results: list[dict[str, Any]] | None = None,
    ) -> FinalReviewDecision:
        answer_contract = self._review_answer_contract(answer_package)
        if answer_contract["issues"]:
            return FinalReviewDecision(
                action=FinalReviewAction.revise_answer,
                reason_code=FinalReviewReasonCode.artifact_contract_mismatch,
                rationale="Final answer markdown references unavailable or invalid artifacts.",
                issues=list(answer_contract["issues"]),
                updated_context={"answer_contract": answer_contract},
            )
        prompt = build_final_review_prompt(
            question=question,
            answer_package=answer_package,
            evidence=evidence,
            result=result,
            research=research,
            step_results=step_results,
            answer_contract=answer_contract,
            reason_codes=[item.value for item in FinalReviewReasonCode],
        )
        payload = self._parse_json_object(await self._llm.acomplete(prompt, temperature=0.0, max_tokens=700))
        self._ensure_reason_code(payload)
        return FinalReviewDecision.model_validate(payload)

    @staticmethod
    def _ensure_reason_code(payload: dict[str, Any]) -> None:
        if payload.get("reason_code"):
            return
        action = str(payload.get("action") or "").strip()
        inferred = {
            FinalReviewAction.approve.value: FinalReviewReasonCode.grounded_complete.value,
            FinalReviewAction.revise_answer.value: FinalReviewReasonCode.missing_caveat_or_framing.value,
            FinalReviewAction.replan.value: FinalReviewReasonCode.insufficient_evidence_or_workflow.value,
            FinalReviewAction.ask_clarification.value: FinalReviewReasonCode.ambiguous_question.value,
            FinalReviewAction.abort.value: FinalReviewReasonCode.unsafe_to_finalize.value,
        }.get(action)
        if inferred:
            payload["reason_code"] = inferred

    @classmethod
    def _review_answer_contract(cls, answer_package: dict[str, Any]) -> dict[str, Any]:
        answer_markdown = str(answer_package.get("answer_markdown") or "").strip()
        artifacts = cls._normalize_artifacts(answer_package.get("artifacts"))
        referenced_ids = cls._artifact_ids_from_markdown(answer_markdown)
        issues: list[str] = []

        if not answer_markdown and not artifacts:
            return {
                "checked": False,
                "artifact_placeholders": [],
                "available_artifact_ids": [],
                "issues": [],
            }

        malformed_refs = cls._MALFORMED_ARTIFACT_REF_RE.findall(answer_markdown)
        if malformed_refs:
            issues.append(
                "Answer markdown contains malformed artifact placeholders; use {{artifact:artifact_id}} syntax."
            )

        for artifact_id in referenced_ids:
            if artifact_id not in artifacts:
                issues.append(f"Answer markdown references unavailable artifact '{artifact_id}'.")

        for artifact_id, artifact in artifacts.items():
            if artifact_id in referenced_ids or not referenced_ids:
                issues.extend(cls._artifact_payload_issues(artifact_id=artifact_id, artifact=artifact))

        primary_ids = [
            artifact_id
            for artifact_id, artifact in artifacts.items()
            if str(artifact.get("role") or "").strip() == "primary_result"
        ]
        for artifact_id in primary_ids:
            if answer_markdown and artifact_id not in referenced_ids:
                issues.append(f"Primary result artifact '{artifact_id}' is not positioned in answer_markdown.")

        return {
            "checked": True,
            "artifact_placeholders": referenced_ids,
            "available_artifact_ids": list(artifacts),
            "issues": issues,
        }

    @classmethod
    def _artifact_ids_from_markdown(cls, answer_markdown: str) -> list[str]:
        ids: list[str] = []
        for match in cls._ARTIFACT_REF_RE.finditer(answer_markdown):
            artifact_id = match.group(1).strip()
            if artifact_id and artifact_id not in ids:
                ids.append(artifact_id)
        return ids

    @staticmethod
    def _normalize_artifacts(raw_artifacts: Any) -> dict[str, dict[str, Any]]:
        artifacts: dict[str, dict[str, Any]] = {}
        if isinstance(raw_artifacts, dict):
            if raw_artifacts.get("id") or raw_artifacts.get("artifact_id"):
                iterable = (raw_artifacts,)
            else:
                iterable = (
                    {"id": artifact_id, **artifact}
                    for artifact_id, artifact in raw_artifacts.items()
                    if isinstance(artifact, dict)
                )
        elif isinstance(raw_artifacts, list):
            iterable = (artifact for artifact in raw_artifacts if isinstance(artifact, dict))
        else:
            iterable = ()
        for artifact in iterable:
            artifact_id = str(
                artifact.get("id")
                or artifact.get("artifact_id")
                or artifact.get("artifactId")
                or artifact.get("key")
                or ""
            ).strip()
            if artifact_id:
                artifacts[artifact_id] = dict(artifact, id=artifact_id)
        return artifacts

    @classmethod
    def _artifact_payload_issues(cls, *, artifact_id: str, artifact: dict[str, Any]) -> list[str]:
        artifact_type = str(artifact.get("type") or artifact.get("kind") or "").strip().lower()
        payload = artifact.get("payload") if isinstance(artifact.get("payload"), dict) else artifact
        if artifact_type == "table":
            if not isinstance(payload.get("columns"), list) or not isinstance(payload.get("rows"), list):
                return [f"Table artifact '{artifact_id}' is missing columns or rows payload."]
        elif artifact_type == "chart":
            if not any(payload.get(key) for key in ("chart_type", "chartType", "type")):
                return [f"Chart artifact '{artifact_id}' is missing chart type payload."]
        elif artifact_type == "sql":
            if not any(payload.get(key) for key in ("sql_executable", "sql_canonical", "sql", "query")):
                return [f"SQL artifact '{artifact_id}' is missing SQL payload."]
        elif artifact_type == "diagnostics":
            if not artifact.get("payload"):
                return [f"Diagnostics artifact '{artifact_id}' is missing diagnostics payload."]
        return []

    @staticmethod
    def _extract_mapping(task: AgentTask, key: str) -> dict[str, Any] | None:
        for candidate in (task.context.get(key), task.input.get(key)):
            if isinstance(candidate, dict):
                return candidate
        return None

    @staticmethod
    def _extract_sequence(task: AgentTask, key: str) -> list[dict[str, Any]] | None:
        for candidate in (task.context.get(key), task.input.get(key)):
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]
        return None

    @staticmethod
    def _parse_json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("Final review LLM response did not contain a JSON object.")
        parsed = json.loads(text[start : end + 1])
        if not isinstance(parsed, dict):
            raise ValueError("Final review LLM response JSON must be an object.")
        return parsed


__all__ = ["FinalReviewAction", "FinalReviewAgent", "FinalReviewDecision", "FinalReviewReasonCode"]
