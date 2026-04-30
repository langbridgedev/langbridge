"""Final response presentation agent for Langbridge AI."""
import json
from typing import Any

from langbridge.ai.base import (
    AgentIOContract,
    AgentResult,
    AgentResultStatus,
    AgentRoutingSpec,
    AgentSpecification,
    AgentTask,
    AgentTaskKind,
    AgentToolSpecification,
    BaseAgent,
)
from langbridge.ai.events import AIEventEmitter, AIEventSource
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.agents.presentation.artifacts import (
    build_available_artifacts,
    resolve_referenced_artifacts,
    sanitize_artifact_placeholders,
)
from langbridge.ai.agents.presentation.prompts import build_presentation_prompt
from langbridge.ai.tools.charting import ChartSpec, ChartingTool


class PresentationAgent(AIEventSource, BaseAgent):
    """Composes final user-facing responses from verified outputs."""

    def __init__(
        self,
        *,
        llm_provider: LLMProvider,
        charting_tool: ChartingTool | None = None,
        event_emitter: AIEventEmitter | None = None,
    ) -> None:
        super().__init__(event_emitter=event_emitter)
        self._llm = llm_provider
        self._charting_tool = charting_tool

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="presentation",
            description="Composes final user-facing responses from verified agent outputs.",
            task_kinds=[AgentTaskKind.presentation],
            capabilities=["compose final response", "summarize tables", "render research", "request charts"],
            constraints=["Does not perform source execution directly."],
            routing=AgentRoutingSpec(keywords=["present", "response", "chart"], direct_threshold=99),
            can_execute_direct=False,
            output_contract=AgentIOContract(required_keys=["response"]),
            tools=[
                AgentToolSpecification(
                    name="charting",
                    description="Builds chart specifications from tabular result data when configured.",
                    output_contract=AgentIOContract(required_keys=["chart_type"]),
                )
            ],
        )

    async def execute(self, task: AgentTask) -> AgentResult:
        response = await self.compose(
            question=task.question,
            context=task.context,
            mode=str(task.input.get("mode") or "final"),
        )
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"response": response},
        )

    async def compose(self, *, question: str, context: dict[str, Any], mode: str = "final") -> dict[str, Any]:
        await self._emit_ai_event(
            event_type="PresentationCompositionStarted",
            message="Composing final answer.",
            source="presentation",
            details={"mode": mode},
        )
        step_results = [item for item in context.get("step_results", []) if isinstance(item, dict)]
        data_payload = self._find_data_payload(step_results)
        analysis_payload = self._find_key_payload(step_results, "analysis")
        research_payload = self._find_key_payload(step_results, "synthesis")
        answer_payload = self._find_key_payload(step_results, "answer")
        presentation_guidance = (
            context.get("presentation_guidance")
            if isinstance(context.get("presentation_guidance"), dict)
            else None
        )
        visualization_recommendation = self._find_visualization_recommendation(context, analysis_payload, research_payload, answer_payload)
        visualization = await self._maybe_chart(
            question=question,
            data_payload=data_payload,
            context=context,
            visualization_recommendation=visualization_recommendation,
        )
        available_artifacts = build_available_artifacts(
            data_payload=data_payload,
            visualization=visualization,
            step_results=step_results,
            presentation_guidance=presentation_guidance,
        )
        prompt = build_presentation_prompt(
            question=question,
            mode=mode,
            context=context,
            data_payload=data_payload,
            analysis_payload=analysis_payload,
            research_payload=research_payload,
            answer_payload=answer_payload,
            visualization_recommendation=visualization_recommendation,
            visualization=visualization,
            available_artifacts=available_artifacts,
            presentation_guidance=presentation_guidance,
        )
        parsed = self._parse_json_object(
            await self._llm.acomplete(
                prompt,
                temperature=0.0,
                max_tokens=2400,
            )
        )
        parsed_result = parsed.get("result")
        parsed_research = parsed.get("research")
        result_payload = parsed_result if isinstance(parsed_result, dict) and parsed_result else data_payload
        if isinstance(result_payload, dict):
            result_payload = self._with_artifact_formatting(
                result_payload=result_payload,
                available_artifacts=available_artifacts,
            )
        answer = self._resolve_answer(
            parsed=parsed,
            mode=mode,
            context=context,
            summary=str(parsed.get("summary") or ""),
            analysis_payload=analysis_payload,
            research_payload=research_payload,
            answer_payload=answer_payload,
        )
        answer_markdown = self._resolve_answer_markdown(
            parsed=parsed,
            answer=answer,
            summary=str(parsed.get("summary") or ""),
        )
        answer_markdown = sanitize_artifact_placeholders(
            answer_markdown=answer_markdown,
            available_artifacts=available_artifacts,
        )
        answer_markdown = self._ensure_primary_visualization_placeholder(
            answer_markdown=answer_markdown,
            available_artifacts=available_artifacts,
            question=question,
            context=context,
        )
        answer = self._resolve_legacy_answer(
            mode=mode,
            answer=answer,
            answer_markdown=answer_markdown,
        )
        response = {
            "summary": str(parsed.get("summary") or ""),
            "result": result_payload if isinstance(result_payload, dict) and result_payload else None,
            "visualization": parsed.get("visualization")
            if isinstance(parsed.get("visualization"), dict)
            else (visualization.model_dump(mode="json") if visualization else None),
            "research": (
                parsed_research
                if isinstance(parsed_research, dict) and parsed_research
                else research_payload or {}
            ),
            "answer": answer,
            "answer_markdown": answer_markdown,
            "artifacts": resolve_referenced_artifacts(
                parsed=parsed,
                answer_markdown=answer_markdown,
                available_artifacts=available_artifacts,
            ),
            "diagnostics": parsed.get("diagnostics") if isinstance(parsed.get("diagnostics"), dict) else {"mode": mode},
        }
        if not response["summary"]:
            raise ValueError("Presentation LLM response missing summary.")
        await self._emit_ai_event(
            event_type="PresentationCompositionCompleted",
            message="Answer composed.",
            source="presentation",
            details={"has_visualization": isinstance(response.get("visualization"), dict)},
        )
        return response

    @staticmethod
    def _with_artifact_formatting(
        *,
        result_payload: dict[str, Any],
        available_artifacts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if isinstance(result_payload.get("formatting"), dict):
            return result_payload
        if not {"columns", "rows"}.issubset(result_payload):
            return result_payload
        for artifact in available_artifacts:
            payload = artifact.get("payload") if isinstance(artifact.get("payload"), dict) else {}
            formatting = payload.get("formatting") if isinstance(payload.get("formatting"), dict) else artifact.get("formatting")
            if isinstance(formatting, dict) and artifact.get("type") == "table":
                return {**result_payload, "formatting": formatting}
        return result_payload

    async def _maybe_chart(
        self,
        *,
        question: str,
        data_payload: dict[str, Any] | None,
        context: dict[str, Any],
        visualization_recommendation: dict[str, Any] | None,
    ) -> ChartSpec | None:
        if self._charting_tool is None or not data_payload:
            return None
        if visualization_recommendation is not None:
            if self._recommendation_requests_no_visual(visualization_recommendation):
                return None
            recommended_chart = self._chart_from_recommendation(
                question=question,
                recommendation=visualization_recommendation,
                data_payload=data_payload,
            )
            if recommended_chart is not None:
                return recommended_chart
        return await self._charting_tool.build_chart(
            data_payload,
            question=question,
            title=(
                str(visualization_recommendation.get("title") or visualization_recommendation.get("chart_title")).strip()
                if visualization_recommendation is not None and (
                    visualization_recommendation.get("title") or visualization_recommendation.get("chart_title")
                )
                else None
            ),
            user_intent=self._visualization_user_intent(
                recommendation=visualization_recommendation,
                context=context,
            ),
        )

    @staticmethod
    def _find_data_payload(step_results: list[dict[str, Any]]) -> dict[str, Any] | None:
        for item in reversed(step_results):
            output = item.get("output")
            if not isinstance(output, dict):
                continue
            result = output.get("result")
            if isinstance(result, dict) and {"columns", "rows"}.issubset(result):
                return result
            artifact_payload = PresentationAgent._find_artifact_table_payload(
                item.get("artifacts"),
                output.get("artifacts"),
            )
            if artifact_payload is not None:
                return artifact_payload
        return None

    @staticmethod
    def _find_artifact_table_payload(*artifact_sources: Any) -> dict[str, Any] | None:
        for artifact_source in artifact_sources:
            if not artifact_source:
                continue
            if isinstance(artifact_source, dict):
                legacy_tabular = artifact_source.get("tabular")
                if isinstance(legacy_tabular, dict) and {"columns", "rows"}.issubset(legacy_tabular):
                    return legacy_tabular
                artifacts = [
                    {"id": artifact_id, **artifact}
                    for artifact_id, artifact in artifact_source.items()
                    if isinstance(artifact, dict)
                ]
            elif isinstance(artifact_source, list):
                artifacts = [artifact for artifact in artifact_source if isinstance(artifact, dict)]
            else:
                continue

            for artifact in artifacts:
                payload = artifact.get("payload") if isinstance(artifact.get("payload"), dict) else artifact
                artifact_type = str(artifact.get("type") or "").strip().lower()
                artifact_id = str(artifact.get("id") or "").strip().lower()
                if (
                    {"columns", "rows"}.issubset(payload)
                    and (artifact_type == "table" or artifact_id in {"primary_result", "result_table"})
                ):
                    return payload
        return None

    @staticmethod
    def _find_key_payload(step_results: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
        for item in reversed(step_results):
            output = item.get("output")
            if isinstance(output, dict) and key in output:
                return output
        return None

    @staticmethod
    def _find_visualization_recommendation(
        context: dict[str, Any],
        *payloads: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        candidates = [context.get("visualization_recommendation"), context.get("recommended_visualization")]
        for payload in payloads:
            if isinstance(payload, dict):
                candidates.extend(
                    payload.get(key)
                    for key in ("visualization_recommendation", "recommended_visualization")
                )
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate:
                return candidate
        return None

    @staticmethod
    def _resolve_answer(
        *,
        parsed: dict[str, Any],
        mode: str,
        context: dict[str, Any],
        summary: str,
        analysis_payload: dict[str, Any] | None,
        research_payload: dict[str, Any] | None,
        answer_payload: dict[str, Any] | None,
    ) -> Any:
        if mode == "clarification":
            return context.get("clarification_question") or parsed.get("answer") or summary or None
        if parsed.get("answer") is not None:
            return parsed.get("answer")
        if parsed.get("answer_markdown") is not None:
            return parsed.get("answer_markdown")
        if mode == "failure":
            return context.get("error") or summary or None
        for payload, key in (
            (answer_payload, "answer"),
            (analysis_payload, "analysis"),
            (research_payload, "synthesis"),
        ):
            if not isinstance(payload, dict):
                continue
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return summary or None

    @staticmethod
    def _recommendation_requests_no_visual(recommendation: dict[str, Any]) -> bool:
        should_visualize = recommendation.get("should_visualize")
        if should_visualize is False:
            return True
        if str(recommendation.get("recommendation") or "").strip().lower() == "none":
            return True
        if isinstance(recommendation.get("chart_type"), str) and recommendation.get("chart_type") == "none":
            return True
        return False

    @staticmethod
    def _chart_from_recommendation(
        *,
        question: str,
        recommendation: dict[str, Any],
        data_payload: dict[str, Any] | None,
    ) -> ChartSpec | None:
        chart_type = recommendation.get("chart_type")
        title = recommendation.get("title") or recommendation.get("chart_title") or question
        if not isinstance(chart_type, str) or not chart_type.strip():
            return None
        chart_payload = {
            "chart_type": chart_type,
            "title": title,
            "x": recommendation.get("x"),
            "y": recommendation.get("y"),
            "series": recommendation.get("series"),
            "encoding": recommendation.get("encoding") if isinstance(recommendation.get("encoding"), dict) else {},
            "rationale": recommendation.get("rationale") or recommendation.get("reason"),
        }
        try:
            chart = ChartSpec.model_validate(chart_payload)
            columns = data_payload.get("columns") if isinstance(data_payload, dict) else None
            rows = data_payload.get("rows") if isinstance(data_payload, dict) else None
            if isinstance(columns, list) and isinstance(rows, list):
                return ChartingTool._normalize_chart_spec(
                    chart.model_dump(mode="json"),
                    columns=[str(column) for column in columns],
                    rows=rows[:20],
                    question=question,
                    title=str(title),
                    user_intent=str(recommendation.get("rationale") or recommendation.get("reason") or ""),
                )
            return chart
        except Exception:
            return None

    @staticmethod
    def _visualization_user_intent(
        *,
        recommendation: dict[str, Any] | None,
        context: dict[str, Any],
    ) -> str | None:
        if recommendation is not None:
            rationale = recommendation.get("rationale") or recommendation.get("reason")
            if isinstance(rationale, str) and rationale.strip():
                return rationale.strip()
        revision = context.get("presentation_revision_request")
        if isinstance(revision, dict):
            rationale = revision.get("rationale") or revision.get("reason") or revision.get("follow_up_hint")
            if isinstance(rationale, str) and rationale.strip():
                return rationale.strip()
        return None

    @staticmethod
    def _resolve_answer_markdown(
        *,
        parsed: dict[str, Any],
        answer: Any,
        summary: str,
    ) -> str:
        for key in ("answer_markdown", "answer"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        if isinstance(answer, str) and answer.strip():
            return answer.strip()
        return summary.strip()

    @staticmethod
    def _resolve_legacy_answer(
        *,
        mode: str,
        answer: Any,
        answer_markdown: str,
    ) -> Any:
        if mode in {"clarification", "failure"}:
            return answer
        return answer_markdown or answer

    @staticmethod
    def _ensure_primary_visualization_placeholder(
        *,
        answer_markdown: str,
        available_artifacts: list[dict[str, Any]],
        question: str,
        context: dict[str, Any],
    ) -> str:
        if "{{artifact:primary_visualization}}" in answer_markdown:
            return answer_markdown
        has_visualization = any(
            str(artifact.get("id") or "").strip() == "primary_visualization"
            for artifact in available_artifacts
        )
        if not has_visualization:
            return answer_markdown
        if not PresentationAgent._question_or_context_requests_chart(question=question, context=context):
            return answer_markdown

        insertion = "Here is the requested visualization:\n\n{{artifact:primary_visualization}}"
        if "{{artifact:result_table}}" in answer_markdown:
            return answer_markdown.replace(
                "{{artifact:result_table}}",
                insertion + "\n\n{{artifact:result_table}}",
                1,
            )
        if "{{artifact:primary_result}}" in answer_markdown:
            return answer_markdown.replace(
                "{{artifact:primary_result}}",
                insertion + "\n\n{{artifact:primary_result}}",
                1,
            )
        return f"{answer_markdown.rstrip()}\n\n{insertion}".strip()

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
    def _parse_json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("Presentation LLM response did not contain a JSON object.")
        parsed = json.loads(text[start : end + 1])
        if not isinstance(parsed, dict):
            raise ValueError("Presentation LLM response JSON must be an object.")
        return parsed


__all__ = ["PresentationAgent"]
