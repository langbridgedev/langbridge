"""Presentation response assembly for the markdown-first contract."""

from __future__ import annotations

import json
from typing import Any

from langbridge.ai.agents.presentation.artifacts import (
    PRIMARY_RESULT_ARTIFACT_ID,
    PRIMARY_VISUALIZATION_ARTIFACT_ID,
    resolve_referenced_artifacts,
    sanitize_artifact_placeholders,
)
from langbridge.ai.agents.presentation.contracts import (
    MARKDOWN_ARTIFACT_RESPONSE_VERSION,
    PresentationLLMOutput,
    PresentationResponseContract,
)


class PresentationResponseAssembler:
    """Builds the public assistant response from verified backend artifacts."""

    def assemble(
        self,
        *,
        question: str,
        mode: str,
        context: dict[str, Any],
        parsed: dict[str, Any],
        available_artifacts: list[dict[str, Any]],
        analysis_payload: dict[str, Any] | None,
        research_payload: dict[str, Any] | None,
        answer_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        llm_output = PresentationLLMOutput.model_validate(parsed or {})
        answer_markdown = self._resolve_answer_markdown(
            mode=mode,
            context=context,
            llm_output=llm_output,
        )
        answer_markdown = self._ensure_requested_visualization_placeholder(
            answer_markdown=answer_markdown,
            available_artifacts=available_artifacts,
            question=question,
            context=context,
        )
        answer_markdown = self._ensure_primary_result_placeholder(
            answer_markdown=answer_markdown,
            available_artifacts=available_artifacts,
            mode=mode,
        )
        answer_markdown = sanitize_artifact_placeholders(
            answer_markdown=answer_markdown,
            available_artifacts=available_artifacts,
        )
        diagnostics = self._diagnostics(
            mode=mode,
            context=context,
            llm_output=llm_output,
            answer_markdown=answer_markdown,
        )
        metadata = self._metadata(mode=mode, llm_output=llm_output)
        contract = PresentationResponseContract(
            answer_markdown=answer_markdown,
            artifacts=resolve_referenced_artifacts(
                parsed=llm_output.model_dump(mode="json", exclude_none=True),
                answer_markdown=answer_markdown,
                available_artifacts=available_artifacts,
            ),
            diagnostics=diagnostics,
            metadata=metadata,
        )
        return contract.model_dump(mode="json", exclude_none=True)

    def _resolve_answer_markdown(
        self,
        *,
        mode: str,
        context: dict[str, Any],
        llm_output: PresentationLLMOutput,
    ) -> str:
        candidates: list[Any] = []
        if mode == "clarification":
            candidates.extend(
                [
                    llm_output.answer_markdown,
                    context.get("clarification_question"),
                ]
            )
        elif mode == "failure":
            candidates.extend(
                [
                    llm_output.answer_markdown,
                    context.get("error"),
                ]
            )
        else:
            candidates.append(llm_output.answer_markdown)

        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        raise ValueError("Presentation LLM response missing answer_markdown.")

    def _ensure_requested_visualization_placeholder(
        self,
        *,
        answer_markdown: str,
        available_artifacts: list[dict[str, Any]],
        question: str,
        context: dict[str, Any],
    ) -> str:
        placeholder = f"{{{{artifact:{PRIMARY_VISUALIZATION_ARTIFACT_ID}}}}}"
        if placeholder in answer_markdown:
            return answer_markdown
        if not self._has_artifact(available_artifacts, PRIMARY_VISUALIZATION_ARTIFACT_ID):
            return answer_markdown
        if not self._question_or_context_requests_chart(question=question, context=context):
            return answer_markdown
        return f"{answer_markdown.rstrip()}\n\n{placeholder}".strip()

    def _ensure_primary_result_placeholder(
        self,
        *,
        answer_markdown: str,
        available_artifacts: list[dict[str, Any]],
        mode: str,
    ) -> str:
        if mode != "final":
            return answer_markdown
        placeholder = f"{{{{artifact:{PRIMARY_RESULT_ARTIFACT_ID}}}}}"
        if placeholder in answer_markdown:
            return answer_markdown
        primary_result = self._artifact(available_artifacts, PRIMARY_RESULT_ARTIFACT_ID)
        if primary_result is None:
            return answer_markdown
        if str(primary_result.get("role") or "").strip() != "primary_result":
            return answer_markdown
        return f"{answer_markdown.rstrip()}\n\n{placeholder}".strip()

    @staticmethod
    def _has_artifact(available_artifacts: list[dict[str, Any]], artifact_id: str) -> bool:
        return any(
            str(artifact.get("id") or "").strip() == artifact_id
            for artifact in available_artifacts
        )

    @staticmethod
    def _artifact(available_artifacts: list[dict[str, Any]], artifact_id: str) -> dict[str, Any] | None:
        return next(
            (
                artifact
                for artifact in available_artifacts
                if str(artifact.get("id") or "").strip() == artifact_id
            ),
            None,
        )

    @staticmethod
    def _question_or_context_requests_chart(*, question: str, context: dict[str, Any]) -> bool:
        text_parts = [
            question,
            str(context.get("chart_request") or ""),
            json.dumps(context.get("presentation_revision_request") or {}, default=str),
            json.dumps(context.get("visualization_recommendation") or {}, default=str),
            json.dumps(context.get("recommended_visualization") or {}, default=str),
        ]
        text = " ".join(text_parts).casefold()
        return any(token in text for token in ("chart", "graph", "plot", "visual", "pie", "bar", "line", "scatter"))

    @staticmethod
    def _diagnostics(
        *,
        mode: str,
        context: dict[str, Any],
        llm_output: PresentationLLMOutput,
        answer_markdown: str,
    ) -> dict[str, Any]:
        diagnostics = dict(llm_output.diagnostics or {})
        diagnostics.setdefault("mode", mode)
        if mode == "clarification":
            clarification = (
                str(context.get("clarification_question") or "").strip()
                or answer_markdown.strip()
            )
            if clarification:
                diagnostics["clarifying_question"] = clarification
        return diagnostics

    @staticmethod
    def _metadata(*, mode: str, llm_output: PresentationLLMOutput) -> dict[str, Any]:
        metadata = dict(llm_output.metadata or {})
        return {
            "contract_version": MARKDOWN_ARTIFACT_RESPONSE_VERSION,
            "mode": mode,
            **metadata,
        }


__all__ = ["PresentationResponseAssembler"]
