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
)
from langbridge.ai.agents.presentation.prompts import build_presentation_prompt
from langbridge.ai.agents.presentation.response import PresentationResponseAssembler
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
        self._response_assembler = PresentationResponseAssembler()

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
        response = self._response_assembler.assemble(
            question=question,
            mode=mode,
            context=context,
            parsed=parsed,
            available_artifacts=available_artifacts,
            analysis_payload=analysis_payload,
            research_payload=research_payload,
            answer_payload=answer_payload,
        )
        await self._emit_ai_event(
            event_type="PresentationCompositionCompleted",
            message="Answer composed.",
            source="presentation",
            details={
                "artifact_count": len(response.get("artifacts", [])),
                "has_visualization": any(
                    str(artifact.get("id") or "") == "primary_visualization"
                    for artifact in response.get("artifacts", [])
                    if isinstance(artifact, dict)
                ),
            },
        )
        return response

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
