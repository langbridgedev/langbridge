import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage

from langbridge.orchestrator.definitions import (
    AgentDefinitionModel,
    GuardrailConfig,
    OutputFormat,
    OutputSchema,
    PromptContract,
    ResponseMode,
)
from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.runtime.analysis_grounding import (
    build_analyst_grounding,
    compose_analyst_summary,
    render_analyst_grounding_for_prompt,
)
from langbridge.orchestrator.tools.sql_analyst.interfaces import AnalystOutcomeStatus, AnalystQueryResponse


@dataclass(slots=True, frozen=True)
class ResponsePresentation:
    prompt_contract: PromptContract | None = None
    output_schema: OutputSchema | None = None
    guardrails: GuardrailConfig | None = None
    response_mode: ResponseMode = ResponseMode.analyst

    @classmethod
    def from_definition(cls, definition: AgentDefinitionModel) -> "ResponsePresentation":
        return cls(
            prompt_contract=definition.prompt,
            output_schema=definition.output,
            guardrails=definition.guardrails,
            response_mode=definition.execution.response_mode,
        )

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
        messages = self._build_messages(
            human_prompt="\n\n".join(prompt_sections),
            presentation=presentation,
            mode_prompt=self._chat_mode_prompt(presentation.response_mode),
        )
        try:
            llm_response = await llm_provider.ainvoke(messages, temperature=0.4, max_tokens=900)
        except Exception as exc:  # pragma: no cover
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
        clarifying_question = self._clean_text(response_payload.get("clarifying_question"))
        if clarifying_question:
            return self._enforce_guardrails(
                f"I need one clarification before continuing: {clarifying_question}",
                presentation.guardrails,
            )

        prompt_sections = self._build_summary_prompt(question, response_payload, presentation)
        messages = self._build_messages(
            human_prompt="\n\n".join(prompt_sections),
            presentation=presentation,
        )
        try:
            llm_response = await llm_provider.ainvoke(messages)
        except Exception as exc:  # pragma: no cover
            suffix = f" request_id={request_id}" if request_id else ""
            self._logger.warning("Failed to generate summary%s: %s", suffix, exc, exc_info=True)
            return self._fallback_summary(question, response_payload, presentation)
        summary_text = self._coerce_message_text(llm_response)
        if not summary_text:
            return self._fallback_summary(question, response_payload, presentation)
        return self._enforce_guardrails(summary_text, presentation.guardrails)

    def _build_summary_prompt(
        self,
        question: str,
        response_payload: dict[str, Any],
        presentation: ResponsePresentation,
    ) -> list[str]:
        analyst_result = response_payload.get("analyst_result")
        if not isinstance(analyst_result, AnalystQueryResponse):
            analyst_result = None
        result_payload = response_payload.get("result") if isinstance(response_payload.get("result"), dict) else {}
        visualization = response_payload.get("visualization")
        summary_intro, summary_tail = self._summary_prompt_parts(presentation.response_mode)
        prompt_sections = [summary_intro, f"Original question:\n{question.strip()}"]
        prompt_sections.append(f"Tabular result preview:\n{self._render_tabular_preview(result_payload)}")

        grounding = build_analyst_grounding(question, result_payload)
        grounding_text = render_analyst_grounding_for_prompt(grounding)
        if grounding_text:
            prompt_sections.append(f"Key analytical facts:\n{grounding_text}")

        outcome_text = self._render_analyst_outcome(analyst_result)
        if outcome_text:
            prompt_sections.append(f"Analyst outcome:\n{outcome_text}")

        viz_summary = self._summarise_visualization(visualization)
        if viz_summary:
            prompt_sections.append(f"Visualization guidance:\n{viz_summary}")

        research_text = self._render_research_context(response_payload.get("research_result"))
        if research_text:
            prompt_sections.append(f"Research context:\n{research_text}")

        web_text = self._render_web_context(response_payload.get("web_search_result"))
        if web_text:
            prompt_sections.append(f"Web context:\n{web_text}")

        assumptions = self._coerce_assumptions(response_payload.get("assumptions"))
        if assumptions:
            prompt_sections.append("Assumptions applied:\n" + "\n".join(f"- {item}" for item in assumptions))

        if analyst_result and analyst_result.outcome:
            if analyst_result.outcome.status == AnalystOutcomeStatus.access_denied:
                prompt_sections.append(
                    "Explain clearly that analytical access was blocked by policy and do not imply that any data was returned."
                )
            elif analyst_result.outcome.status == AnalystOutcomeStatus.empty_result:
                prompt_sections.append("State clearly that no rows matched and do not invent findings.")
            elif analyst_result.outcome.is_error:
                prompt_sections.append("Explain the analytical failure succinctly and do not claim results were returned.")

        self._append_output_requirements(prompt_sections, presentation.output_schema)
        if summary_tail:
            prompt_sections.append(summary_tail)
        return prompt_sections

    def _fallback_summary(
        self,
        question: str,
        response_payload: dict[str, Any],
        presentation: ResponsePresentation,
    ) -> str:
        analyst_result = response_payload.get("analyst_result")
        analyst_result = analyst_result if isinstance(analyst_result, AnalystQueryResponse) else None
        result_payload = response_payload.get("result") if isinstance(response_payload.get("result"), dict) else {}
        visualization = response_payload.get("visualization")
        assumptions = self._coerce_assumptions(response_payload.get("assumptions"))

        research_result = response_payload.get("research_result")
        if self._is_deep_research_result(research_result):
            return self._enforce_guardrails(
                self._append_assumptions(self._format_research_summary(research_result, user_query=question), assumptions),
                presentation.guardrails,
            )

        web_search_result = response_payload.get("web_search_result")
        if self._is_web_search_result(web_search_result) and not result_payload:
            if web_search_result.answer:
                summary = web_search_result.answer
            elif web_search_result.results:
                summary = f"Found {len(web_search_result.results)} web sources for '{question}'."
            else:
                follow_up = web_search_result.follow_up_question or "Could you narrow the topic or provide a target source?"
                summary = f"I could not find strong enough external sources to answer confidently. {follow_up}"
            return self._enforce_guardrails(self._append_assumptions(summary, assumptions), presentation.guardrails)

        if analyst_result and analyst_result.outcome and analyst_result.outcome.is_error:
            return self._enforce_guardrails(
                self._append_assumptions(self._format_analyst_failure(analyst_result), assumptions),
                presentation.guardrails,
            )

        if presentation.response_mode == ResponseMode.analyst:
            grounding = build_analyst_grounding(question, result_payload)
            summary = compose_analyst_summary(
                grounding,
                assumptions=assumptions,
                extra_note=self._build_visualization_note(user_query=question, visualization=visualization),
            )
            return self._enforce_guardrails(summary, presentation.guardrails)

        rows = result_payload.get("rows") if isinstance(result_payload, dict) else None
        columns = result_payload.get("columns") if isinstance(result_payload, dict) else None
        row_count = len(rows) if isinstance(rows, list) else 0
        col_count = len(columns) if isinstance(columns, list) else 0
        if row_count == 0:
            return self._enforce_guardrails(
                self._append_assumptions("Completed, but no tabular rows were returned.", assumptions),
                presentation.guardrails,
            )
        return self._enforce_guardrails(
            self._append_assumptions(f"Found {row_count} rows across {col_count} columns for '{question}'.", assumptions),
            presentation.guardrails,
        )

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
    def _append_output_requirements(prompt_sections: list[str], output_schema: OutputSchema | None) -> None:
        if not output_schema:
            return
        prompt_sections.append(f"Output format: {output_schema.format.value}.")
        if output_schema.format == OutputFormat.json and output_schema.json_schema:
            prompt_sections.append(f"JSON schema:\n{json.dumps(output_schema.json_schema, indent=2, sort_keys=True)}")
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
        if mode == ResponseMode.executive:
            return (
                "You are an executive briefing assistant. Summarize the findings for a leadership audience.",
                "Return 3 bullet points and 1 recommended action. Mention clearly when the result is empty or the analyst failed.",
            )
        if mode == ResponseMode.explainer:
            return (
                "You are a data explainer. Summarize for a non-technical audience in 3-5 sentences.",
                "Avoid jargon, define any terms, and explain clearly when the result is empty or the analyst failed.",
            )
        if mode == ResponseMode.chat:
            return (
                "You are a grounded assistant. Answer conversationally using only the provided execution context.",
                "If the result is empty or failed, say so plainly and suggest the most relevant next detail to refine.",
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
    def _render_analyst_outcome(analyst_result: AnalystQueryResponse | None) -> str:
        if not analyst_result or not analyst_result.outcome:
            return ""
        outcome = analyst_result.outcome
        lines = [
            f"status={outcome.status.value}",
            f"terminal={str(outcome.terminal).lower()}",
            f"recoverable={str(outcome.recoverable).lower()}",
            f"retry_count={outcome.retry_count}",
        ]
        if outcome.stage:
            lines.append(f"stage={outcome.stage.value}")
        if outcome.selected_tool_name:
            lines.append(f"selected_tool={outcome.selected_tool_name}")
        if outcome.selected_asset_name:
            lines.append(f"selected_asset={outcome.selected_asset_name}")
        if outcome.message:
            lines.append(f"message={outcome.message}")
        if outcome.retry_rationale:
            lines.append(f"retry_rationale={outcome.retry_rationale}")
        if outcome.recovery_actions:
            lines.append("recovery_actions=" + ", ".join(action.action for action in outcome.recovery_actions))
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        policy_rationale = metadata.get("policy_rationale")
        if isinstance(policy_rationale, str) and policy_rationale.strip():
            lines.append(f"policy_rationale={policy_rationale.strip()}")
        recovery_hint = metadata.get("recovery_hint")
        if isinstance(recovery_hint, str) and recovery_hint.strip():
            lines.append(f"recovery_hint={recovery_hint.strip()}")
        requested_asset = metadata.get("requested_asset_name")
        if isinstance(requested_asset, str) and requested_asset.strip():
            lines.append(f"requested_asset={requested_asset.strip()}")
        return "\n".join(lines)

    @staticmethod
    def _render_research_context(value: Any) -> str:
        if not ResponseFormatter._is_deep_research_result(value):
            return ""
        if value.report:
            lines = [f"Executive summary: {value.report.executive_summary.strip()}"]
            for finding in value.report.key_findings[:4]:
                lines.append(f"- {finding.claim} [{finding.confidence}]")
            if value.report.follow_up_question:
                lines.append(f"Follow-up question: {value.report.follow_up_question}")
            return "\n".join(lines)
        return value.synthesis or ""

    @staticmethod
    def _render_web_context(value: Any) -> str:
        if not ResponseFormatter._is_web_search_result(value):
            return ""
        lines = [f"query={value.query}", f"weak_results={str(value.weak_results).lower()}"]
        if value.answer:
            lines.append(f"answer={value.answer}")
        if value.results:
            for item in value.results[:3]:
                lines.append(f"- {item.title} ({item.url})")
        if value.follow_up_question:
            lines.append(f"follow_up_question={value.follow_up_question}")
        return "\n".join(lines)

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
        if isinstance(y_axis, (list, tuple)) and y_axis:
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
            preview_lines.append(" | ".join(ResponseFormatter._format_cell(value) for value in row_values))
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
    def _format_analyst_failure(analyst_result: AnalystQueryResponse) -> str:
        outcome = analyst_result.outcome
        if not outcome:
            return "I could not complete that analytical request."
        asset_name = outcome.selected_asset_name or analyst_result.asset_name or "the selected asset"
        retry_note = ""
        if outcome.retry_attempted and outcome.retry_count:
            retry_note = f" I retried {outcome.retry_count} time"
            if outcome.retry_count != 1:
                retry_note += "s"
            retry_note += "."
        if outcome.status == AnalystOutcomeStatus.invalid_request:
            return f"I could not run that analysis because the request was not specific enough. {outcome.message or ''}".strip()
        if outcome.status == AnalystOutcomeStatus.access_denied:
            recovery_hint = ""
            if isinstance(outcome.metadata, dict):
                raw_recovery_hint = outcome.metadata.get("recovery_hint")
                if isinstance(raw_recovery_hint, str) and raw_recovery_hint.strip():
                    recovery_hint = f" {raw_recovery_hint.strip()}"
            if asset_name == "the selected asset":
                return (
                    "I could not access the requested analytical data because it is outside this agent's "
                    f"connector access policy. {outcome.message or ''}{recovery_hint}"
                ).strip()
            return (
                f"I could not access the requested analytical data for {asset_name} because it is outside "
                f"this agent's connector access policy. {outcome.message or ''}{recovery_hint}"
            ).strip()
        if outcome.status == AnalystOutcomeStatus.selection_error:
            return f"I could not map that request to an analytical context. {outcome.message or ''}".strip()
        if outcome.status == AnalystOutcomeStatus.query_error:
            return f"I could not translate that request into a valid query for {asset_name}. {outcome.message or ''}{retry_note}".strip()
        if outcome.status == AnalystOutcomeStatus.execution_error:
            return f"I could not execute the generated query for {asset_name}. {outcome.message or ''}{retry_note}".strip()
        return f"I could not complete that analytical request: {outcome.message or 'unknown failure'}"

    @staticmethod
    def _enforce_guardrails(summary: str, guardrails: GuardrailConfig | None) -> str:
        if not guardrails or not guardrails.moderation_enabled or not guardrails.regex_denylist:
            return summary
        for pattern in guardrails.regex_denylist:
            try:
                if re.search(pattern, summary):
                    return guardrails.escalation_message or "Response blocked by content guardrails."
            except re.error:
                continue
        return summary

    @staticmethod
    def _coerce_assumptions(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if isinstance(item, str) and item.strip()]

    @staticmethod
    def _append_assumptions(summary: str, assumptions: list[str]) -> str:
        if not assumptions:
            return summary
        return summary + " Assumptions: " + "; ".join(assumptions)

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        return cleaned or None

    @staticmethod
    def _build_visualization_note(*, user_query: str, visualization: dict[str, Any] | None) -> str | None:
        requested_chart = ResponseFormatter._detect_requested_chart_type(user_query)
        if not isinstance(visualization, dict) or not visualization:
            return f"I could not prepare the requested {requested_chart} chart from this dataset." if requested_chart else None
        chart_type_raw = visualization.get("chart_type") or visualization.get("chartType")
        chart_type = chart_type_raw.lower() if isinstance(chart_type_raw, str) else None
        options = visualization.get("options") if isinstance(visualization.get("options"), dict) else {}
        warning = options.get("visualization_warning") if isinstance(options, dict) else None
        if isinstance(warning, str) and warning.strip():
            return warning.strip()
        if requested_chart and chart_type and chart_type != requested_chart:
            return f"I could not prepare the requested {requested_chart} chart from this dataset."
        if chart_type and chart_type != "table":
            return f"I also prepared a {chart_type} visualization."
        return None

    @staticmethod
    def _detect_requested_chart_type(question: str) -> str | None:
        text = str(question or "").lower()
        if not text:
            return None
        if "pie chart" in text or "pie " in text or "donut" in text or "doughnut" in text:
            return "pie"
        if "bar chart" in text or "bar graph" in text:
            return "bar"
        if "line chart" in text or "line graph" in text:
            return "line"
        if "scatter plot" in text or "scatter chart" in text:
            return "scatter"
        return None

    @staticmethod
    def _is_deep_research_result(value: Any) -> bool:
        return hasattr(value, "synthesis") and (
            hasattr(value, "findings") or hasattr(value, "report")
        )

    @staticmethod
    def _is_web_search_result(value: Any) -> bool:
        return hasattr(value, "query") and hasattr(value, "results")

    @staticmethod
    def _format_research_summary(result: Any, *, user_query: str) -> str:
        if not result.report:
            return result.synthesis or f"Completed deep research for '{user_query}'."
        report = result.report
        lines = ["Executive summary:", report.executive_summary.strip() or f"Completed deep research for '{user_query}'."]
        lines.extend(["", "Key findings:"])
        if report.key_findings:
            for finding in report.key_findings[:5]:
                citations = ", ".join(finding.citations) if finding.citations else "no citation"
                lines.append(f"- {finding.claim} [{finding.confidence}] ({citations})")
        else:
            lines.append("- No high-confidence findings could be supported by current evidence.")
        return "\n".join(lines).strip()


__all__ = ["ResponseFormatter", "ResponsePresentation"]
