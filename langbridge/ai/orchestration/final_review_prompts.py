"""Prompt builders for the Langbridge AI semantic final reviewer."""
import json
from typing import Any


def _summarise_step_results(step_results: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not step_results:
        return []
    summary = []
    for item in step_results:
        if not isinstance(item, dict):
            continue
        summary.append({
            "agent_name": item.get("agent_name"),
            "status": item.get("status"),
            "output_keys": sorted(item["output"].keys()) if isinstance(item.get("output"), dict) else [],
            "error": item.get("error"),
        })
    return summary


def build_final_review_prompt(
    *,
    question: str,
    answer_package: dict[str, Any],
    evidence: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    research: dict[str, Any] | None = None,
    step_results: list[dict[str, Any]] | None = None,
    answer_contract: dict[str, Any] | None = None,
    reason_codes: list[str] | None = None,
) -> str:
    allowed_reason_codes = reason_codes or [
        "grounded_complete",
        "missing_caveat_or_framing",
        "insufficient_evidence_or_workflow",
        "ambiguous_question",
        "unsafe_to_finalize",
        "review_error",
    ]
    return (
        "Review the final Langbridge answer package.\n"
        "You are the semantic final reviewer. Evaluate whether the answer is grounded in the available evidence, "
        "answers the user question directly, includes necessary caveats, avoids unsupported claims, and uses final "
        "markdown/artifact references correctly when present.\n"
        "Return STRICT JSON only:\n"
        "{\n"
        '  "action": "approve|revise_answer|replan|ask_clarification|abort",\n'
        f'  "reason_code": "{"| ".join(allowed_reason_codes)}",\n'
        '  "rationale": "short concrete reason",\n'
        '  "issues": ["issue"],\n'
        '  "updated_context": {},\n'
        '  "clarification_question": "question for user or null"\n'
        "}\n"
        "Decision rules:\n"
        "- Choose approve when the answer directly addresses the question, is supported by the supplied evidence, and has no material issues. If no evidence is present but the answer makes specific claims, choose revise_answer instead.\n"
        "- Choose revise_answer when the evidence is probably sufficient but the answer misses caveats, framing, or synthesis quality — the same agent can improve the answer without a new data retrieval.\n"
        "- Choose replan when the available evidence is insufficient or points to the wrong workflow — a fresh analytical pass is needed.\n"
        "- Choose ask_clarification when the question is materially ambiguous and cannot be resolved from the current context.\n"
        "- Choose abort only when safe finalization is not possible.\n"
        f"- reason_code must be one of: {', '.join(allowed_reason_codes)}.\n"
        "- When answer_markdown is present, verify that it positions the material artifacts needed to support the answer.\n"
        "- When artifacts are present, verify that referenced tables, charts, SQL, and diagnostics are compatible with the answer claims.\n"
        "- Treat answer_contract issues as blocking presentation-quality issues unless the answer no longer depends on the affected artifact.\n"
        "- Keep rationale short and concrete.\n"
        "- Put reviewer observations into issues.\n"
        "- Use updated_context for structured corrections the next agent or planner should act on — for example: revised metric name, clarified timeframe, corrected scope, or missing dimension. Do not use it for free-text notes.\n\n"
        f"Question:\n{question}\n\n"
        f"Current answer package:\n{json.dumps(answer_package, default=str, indent=2)}\n\n"
        f"Evidence package:\n{json.dumps(evidence or {}, default=str, indent=2)}\n\n"
        f"Structured result:\n{json.dumps(result or {}, default=str, indent=2)}\n\n"
        f"Research package:\n{json.dumps(research or {}, default=str, indent=2)}\n\n"
        f"Answer contract review:\n{json.dumps(answer_contract or {}, default=str, indent=2)}\n\n"
        f"Prior step summary:\n{json.dumps(_summarise_step_results(step_results), default=str, indent=2)}\n"
    )


__all__ = ["build_final_review_prompt"]
