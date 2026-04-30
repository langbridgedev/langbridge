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
        "Return STRICT JSON only with keys: summary, result, visualization, research, answer, answer_markdown, artifacts, diagnostics.\n"
        "Rules:\n"
        "- Do not invent facts beyond provided step outputs and context.\n"
        "- Keep summary concise and user-facing.\n"
        "- Put the primary user-facing result in answer_markdown as GitHub-flavored Markdown.\n"
        "- Keep answer backward-compatible by mirroring answer_markdown unless clarification or failure mode requires a plain direct answer.\n"
        "- Prefer the verified analytical payload as the answer backbone when it is available.\n"
        "- Preserve material metrics, findings, caveats, and evidence from Analysis and Research instead of flattening them.\n"
        "- Decide the answer depth from the question and evidence, not from a fixed template.\n"
        "- Explain errors and blockers clearly without inventing recovery success.\n"
        "- Use a detailed answer when the user asks for explanation, evidence, comparisons, drivers, caveats, or source-backed reasoning.\n"
        "- Use a concise answer only when the question is straightforward and the evidence is simple.\n"
        "- When detailed explanation is needed, include the concrete values, findings, caveats, and evidence-backed reasoning needed to fully answer the question.\n"
        "- For comparative, ranking, trend, or relationship questions, lead with the verdict and then include the key values or comparisons that support it.\n"
        "- If a structured visualization recommendation is provided, honor it when it is compatible with the verified data and avoid replacing it with a weaker guess.\n"
        "- For clarification mode, put the exact clarification question in answer.\n"
        "- For failure mode, explain the concrete blocker or error without inventing recovery success.\n"
        "- If a presentation_revision_request is provided, satisfy that request directly using the verified data and visualization context instead of merely describing what could be done.\n"
        "- When verified tabular data supports a useful visual, prefer a concrete chart-ready answer over a text-only summary.\n"
        "- Use artifact placeholders inside answer_markdown to position verified artifacts inline.\n"
        "- Artifact placeholder syntax is exactly {{artifact:artifact_id}} on its own paragraph.\n"
        "- Only reference artifact IDs listed in Available artifacts; never invent artifact IDs.\n"
        "- Available artifacts are typed render contracts with id, type, role, title, payload, provenance, and data_ref when present.\n"
        "- Artifact types may include chart, table, sql, and diagnostics; roles may be primary_result, supporting_result, or diagnostic.\n"
        "- Treat analyst-owned primary_result artifacts as the authoritative result artifact when present.\n"
        "- Prefer primary_result artifacts for the main answer and supporting_result artifacts for evidence, SQL, or backing tables.\n"
        "- Introduce each artifact in prose before the placeholder and explain what the artifact shows after it when useful.\n"
        "- If no artifact is useful or available, do not include artifact placeholders.\n"
        "- Return artifacts as the subset of Available artifacts referenced in answer_markdown or explicitly selected in artifacts.\n"
        "- Preserve provided result data when present.\n"
        "- Include visualization only when supported by provided visualization context.\n"
        "- Keep diagnostics as a compact object.\n"
        "- Follow Profile presentation guidance for wording, currency, rounding, and numeric display unless it conflicts with verified evidence.\n"
        "- If Profile presentation guidance specifies currency or number formatting, apply it consistently in answer_markdown.\n\n"
        f"Mode: {mode}\n"
        f"Question: {question}\n"
        f"Context error: {context.get('error') or ''}\n"
        f"Clarification: {context.get('clarification_question') or ''}\n"
        f"Presentation revision request: {_json(context.get('presentation_revision_request') or {})}\n"
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
