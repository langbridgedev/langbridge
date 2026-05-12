"""Prompt builders for the Langbridge presentation agent."""
import json
from typing import Any

from langbridge.ai.tools.charting import ChartSpec


def _json(value: Any) -> str:
    return json.dumps(value, default=str, ensure_ascii=False)


def build_presentation_prompt(
    *,
    question: str,
    mode: str,
    context: dict[str, Any],
    data_payload: dict[str, Any] | None,
    analysis_payload: dict[str, Any] | None,
    research_payload: dict[str, Any] | None,
    answer_payload: dict[str, Any] | None,
    visualization_recommendation: dict[str, Any] | None,
    visualization: ChartSpec | None,
    available_artifacts: list[dict[str, Any]] | None = None,
    presentation_guidance: dict[str, Any] | None = None,
) -> str:
    return (
        "Compose the final Langbridge response.\n"
        "Return STRICT JSON only with keys: answer_markdown, artifact_ids, diagnostics, metadata.\n\n"
        "General rules:\n"
        "- Do not invent facts beyond provided step outputs and context.\n"
        "- Put the primary user-facing result in answer_markdown as GitHub-flavored Markdown.\n"
        "- Prefer the verified analytical payload as the answer backbone when it is available.\n"
        "- Preserve material metrics, findings, caveats, and evidence from Analysis and Research instead of flattening them.\n"
        "- Decide the answer depth from the question and evidence — concise for simple lookups, detailed for comparisons, diagnostics, and multi-source reasoning.\n"
        "- For comparative, ranking, trend, or relationship questions, lead with the verdict and then include the key values or comparisons that support it.\n"
        "- Explain errors and blockers clearly without inventing recovery success.\n"
        "- Follow Profile presentation guidance for wording, currency, rounding, and numeric display unless it conflicts with verified evidence.\n"
        "- Keep diagnostics as a compact object. Put response-level metadata in metadata only when it helps the client.\n\n"
        "Artifact rules:\n"
        "- Artifact placeholder syntax is exactly {{artifact:artifact_id}} on its own paragraph line — for example: {{artifact:primary_result}}\n"
        "- Only reference artifact IDs listed in Available artifacts; never invent artifact IDs.\n"
        "- Treat primary_result artifacts as the authoritative result; use supporting_result artifacts for evidence, SQL, or backing tables.\n"
        "- Introduce each artifact in prose before its placeholder and explain what it shows after when useful.\n"
        "- If Available artifacts does not include primary_visualization, do not reference a visualization artifact.\n"
        "- If a structured visualization recommendation is provided, honor it when it is compatible with the verified data.\n"
        "- Return artifact_ids as the subset of Available artifact IDs referenced in answer_markdown or needed for supporting context.\n"
        "- If no artifact is useful or available, omit artifact placeholders entirely.\n"
        "- Do not copy result tables, visualization specs, or research objects into top-level JSON; the runtime owns those as artifacts.\n\n"
        "Mode-specific rules:\n"
        "- clarification: put only the exact clarification question in answer_markdown. No preamble or explanatory text.\n"
        "- failure: explain the concrete blocker or error in answer_markdown without inventing recovery success.\n"
        "- final: if a presentation_revision_request is provided, satisfy that request directly using the verified data and visualization context.\n\n"
        f"Mode: {mode}\n"
        f"Question: {question}\n"
        + (f"Context error: {context['error']}\n" if context.get("error") else "")
        + (f"Clarification: {context['clarification_question']}\n" if context.get("clarification_question") else "")
        + f"Presentation revision request: {_json(context.get('presentation_revision_request') or {})}\n"
        f"Conversation memory: {context.get('memory_context') or ''}\n"
        f"Data: {_json(data_payload or {})}\n"
        f"Analysis: {_json(analysis_payload or {})}\n"
        f"Research: {_json(research_payload or {})}\n"
        f"Answer: {_json(answer_payload or {})}\n"
        f"Visualization recommendation: {_json(visualization_recommendation or {})}\n"
        f"Visualization: {_json(visualization.model_dump(mode='json') if visualization else None)}\n"
        f"Profile presentation guidance: {_json(presentation_guidance or {})}\n"
        f"Available artifacts: {_json(available_artifacts or [])}\n"
    )


__all__ = ["build_presentation_prompt"]
