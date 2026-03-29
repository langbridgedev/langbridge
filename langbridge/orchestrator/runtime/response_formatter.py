
import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage

from langbridge.orchestrator.definitions import (
    GuardrailConfig,
    OutputFormat,
    OutputSchema,
    PromptContract,
    ResponseMode,
)
from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.runtime.analysis_grounding import (
    build_analyst_grounding,
    render_analyst_grounding_for_prompt,
)


@dataclass(slots=True, frozen=True)
class ResponsePresentation:
    prompt_contract: PromptContract | None = None
    output_schema: OutputSchema | None = None
    guardrails: GuardrailConfig | None = None
    response_mode: ResponseMode = ResponseMode.analyst

    @property
    def render_structured_artifacts(self) -> bool:
        return self.response_mode == ResponseMode.analyst


class ResponseFormatter:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    async def generate_chat_response(
        self,
        llm_provider: LLMProvider,
        question: str,
        *,
        conversation_context: str | None,
        presentation: ResponsePresentation,
        request_id: str | None = None,
    ) -> str:
        prompt_sections: list[str] = []
        if conversation_context:
            prompt_sections.append(f"Conversation so far:\n{conversation_context}")
        prompt_sections.append(f"User: {question.strip()}")
        self._append_output_requirements(prompt_sections, presentation.output_schema)
        prompt_sections.append("Assistant:")

        messages: list[BaseMessage] = self._build_messages(
            human_prompt="\n\n".join(prompt_sections),
            presentation=presentation,
            mode_prompt=self._chat_mode_prompt(presentation.response_mode),
        )

        try:
            llm_response = await llm_provider.ainvoke(messages, temperature=0.4, max_tokens=900)
        except Exception as exc:  # pragma: no cover - defensive guard against transient LLM failures
            suffix = f" request_id={request_id}" if request_id else ""
            self._logger.warning("Failed to generate chat response%s: %s", suffix, exc, exc_info=True)
            return "Response unavailable due to temporary AI service issues."

        response_text = self._coerce_message_text(llm_response)
        if not response_text:
            return "No response produced."
        return self._enforce_guardrails(response_text, presentation.guardrails)

    async def summarize_response(
        self,
        llm_provider: LLMProvider,
        question: str,
        response_payload: dict[str, Any],
        *,
        presentation: ResponsePresentation,
        request_id: str | None = None,
    ) -> str:
        summary_intro, summary_tail = self._summary_prompt_parts(presentation.response_mode)
        preview = self._render_tabular_preview(response_payload.get("result"))
        viz_summary = self._summarise_visualization(response_payload.get("visualization"))
        grounding = build_analyst_grounding(question, response_payload.get("result"))
        grounding_text = render_analyst_grounding_for_prompt(grounding)

        prompt_sections = [
            summary_intro,
            f"Original question:\n{question.strip()}",
            f"Tabular result preview:\n{preview}",
        ]
        if grounding_text:
            prompt_sections.append(f"Key analytical facts:\n{grounding_text}")
        if viz_summary:
            prompt_sections.append(f"Visualization guidance:\n{viz_summary}")
        self._append_output_requirements(prompt_sections, presentation.output_schema)
        if summary_tail:
            prompt_sections.append(summary_tail)

        messages = self._build_messages(
            human_prompt="\n\n".join(prompt_sections),
            presentation=presentation,
        )

        try:
            llm_response = await llm_provider.ainvoke(messages)
        except Exception as exc:  # pragma: no cover - defensive guard against transient LLM failures
            suffix = f" request_id={request_id}" if request_id else ""
            self._logger.warning("Failed to generate summary%s: %s", suffix, exc, exc_info=True)
            return "Summary unavailable due to temporary AI service issues."

        summary_text = self._coerce_message_text(llm_response)
        if not summary_text:
            return "No summary produced."
        return self._enforce_guardrails(summary_text, presentation.guardrails)

    def _build_messages(
        self,
        *,
        human_prompt: str,
        presentation: ResponsePresentation,
        mode_prompt: str | None = None,
    ) -> list[BaseMessage]:
        messages: list[BaseMessage] = []
        system_sections: list[str] = []
        if mode_prompt:
            system_sections.append(mode_prompt)
        if presentation.prompt_contract:
            for section in (
                presentation.prompt_contract.system_prompt,
                presentation.prompt_contract.user_instructions,
                presentation.prompt_contract.style_guidance,
            ):
                if section:
                    system_sections.append(section.strip())
        if system_sections:
            messages.append(SystemMessage(content="\n\n".join(system_sections)))
        messages.append(HumanMessage(content=human_prompt))
        return messages

    @staticmethod
    def _append_output_requirements(
        prompt_sections: list[str],
        output_schema: OutputSchema | None,
    ) -> None:
        if not output_schema:
            return
        prompt_sections.append(f"Output format: {output_schema.format.value}.")
        if output_schema.format == OutputFormat.json and output_schema.json_schema:
            schema_text = json.dumps(output_schema.json_schema, indent=2, sort_keys=True)
            prompt_sections.append(f"JSON schema:\n{schema_text}")
        if output_schema.format == OutputFormat.markdown and output_schema.markdown_template:
            prompt_sections.append(f"Markdown template:\n{output_schema.markdown_template}")

    @staticmethod
    def _coerce_message_text(value: Any) -> str:
        if isinstance(value, BaseMessage):
            return str(value.content).strip()
        return str(value).strip()

    @staticmethod
    def _chat_mode_prompt(response_mode: ResponseMode | None) -> str | None:
        if response_mode == ResponseMode.chat:
            return (
                "You are a helpful conversational assistant. Answer directly, keep a friendly tone, "
                "and ask a concise clarifying question when needed."
            )
        if response_mode == ResponseMode.executive:
            return "You are an executive briefing assistant. Keep responses concise and decision-focused."
        if response_mode == ResponseMode.explainer:
            return "You are a plain-language explainer. Use simple terms and avoid jargon."
        return None

    @staticmethod
    def _summary_prompt_parts(response_mode: ResponseMode | None) -> tuple[str, str]:
        mode = response_mode or ResponseMode.analyst
        if mode == ResponseMode.chat:
            mode = ResponseMode.analyst
        if mode == ResponseMode.executive:
            return (
                "You are an executive briefing assistant. Summarize the findings for a leadership audience.",
                "Return 3 bullet points and 1 recommended action. Mention if the dataset is empty.",
            )
        if mode == ResponseMode.explainer:
            return (
                "You are a data explainer. Summarize for a non-technical audience in 3-5 sentences.",
                "Avoid jargon, define any terms, and mention if the dataset is empty.",
            )
        return (
            "You are a senior analytics assistant. Answer the user's question directly using only the provided analytical context.",
            (
                "Interpret the result instead of restating the table. Call out leaders, laggards, comparisons, "
                "rank order, drivers, or trends only when the returned rows support them. Distinguish observed facts "
                "from reasonable interpretation, avoid filler, and mention uncertainty or limits when the result is sparse."
            ),
        )

    @staticmethod
    def _summarise_visualization(visualization: Any) -> str:
        if not isinstance(visualization, dict) or not visualization:
            return ""

        parts: list[str] = []
        chart_type = visualization.get("chart_type")
        if chart_type:
            parts.append(f"type={chart_type}")
        x_axis = visualization.get("x")
        if x_axis:
            parts.append(f"x={x_axis}")
        y_axis = visualization.get("y")
        if isinstance(y_axis, (list, tuple)):
            if y_axis:
                parts.append(f"y={', '.join(map(str, y_axis))}")
        elif y_axis:
            parts.append(f"y={y_axis}")
        group_by = visualization.get("group_by")
        if group_by:
            parts.append(f"group_by={group_by}")
        return ", ".join(parts)

    @staticmethod
    def _render_tabular_preview(result: Any, *, max_rows: int = 8) -> str:
        if not isinstance(result, dict) or not result:
            return "No tabular result was returned."

        columns = result.get("columns") or []
        rows = result.get("rows") or []
        if not columns:
            return "Result did not include column metadata."
        if not rows:
            return "No rows matched the query."

        header = " | ".join(str(column) for column in columns)
        separator = "-+-".join("-" * max(len(str(column)), 3) for column in columns)

        preview_lines: list[str] = []
        for raw_row in rows[:max_rows]:
            row_values = ResponseFormatter._coerce_row_values(columns, raw_row)
            formatted = " | ".join(ResponseFormatter._format_cell(value) for value in row_values)
            preview_lines.append(formatted)

        if len(rows) > max_rows:
            preview_lines.append(f"... ({len(rows) - max_rows} additional rows truncated)")

        return "\n".join([header, separator, *preview_lines])

    @staticmethod
    def _coerce_row_values(columns: list[str], row: Any) -> list[Any]:
        if isinstance(row, dict):
            return [row.get(column) for column in columns]
        if isinstance(row, (list, tuple)):
            values = list(row)
            if len(values) >= len(columns):
                return values[: len(columns)]
            values.extend([None] * (len(columns) - len(values)))
            return values
        return [row] + [None] * (len(columns) - 1)

    @staticmethod
    def _format_cell(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, float):
            formatted = f"{value:.4f}".rstrip("0").rstrip(".")
            return formatted or "0"
        return str(value)

    @staticmethod
    def _enforce_guardrails(
        summary: str,
        guardrails: GuardrailConfig | None,
    ) -> str:
        if not guardrails or not guardrails.moderation_enabled:
            return summary
        if not guardrails.regex_denylist:
            return summary

        for pattern in guardrails.regex_denylist:
            try:
                if re.search(pattern, summary):
                    return guardrails.escalation_message or "Response blocked by content guardrails."
            except re.error:
                continue
        return summary


__all__ = ["ResponseFormatter", "ResponsePresentation"]
