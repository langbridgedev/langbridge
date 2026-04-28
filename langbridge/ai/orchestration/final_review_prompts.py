"""Prompt builders for the Langbridge AI semantic final reviewer."""
import json
from typing import Any


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
        "{"
        "\"action\":\"approve|revise_answer|replan|ask_clarification|abort\","
        f"\"reason_code\":\"{'|'.join(allowed_reason_codes)}\","
        "\"rationale\":\"short concrete reason\","
        "\"issues\":[\"issue\"],"
        "\"updated_context\":{},"
        "\"clarification_question\":\"question for user or null\""
        "}\n"
        "Decision rules:\n"
        "- Choose approve only when the answer directly addresses the question and is supported by the supplied evidence.\n"
        "- Choose revise_answer when the evidence is probably sufficient but the answer misses caveats, framing, or synthesis quality.\n"
        "- Choose replan when the available evidence is insufficient or points to the wrong workflow.\n"
        "- Choose ask_clarification when the question is materially ambiguous and cannot be resolved from the current context.\n"
        "- Choose abort only when safe finalization is not possible.\n"
        f"- reason_code must be one of: {', '.join(allowed_reason_codes)}.\n"
        "- When answer_markdown is present, verify that it positions the material artifacts needed to support the answer.\n"
        "- When artifacts are present, verify that referenced tables, charts, SQL, and diagnostics are compatible with the answer claims.\n"
        "- Treat answer_contract issues as blocking presentation-quality issues unless the answer no longer depends on the affected artifact.\n"
        "- Keep rationale short and concrete.\n"
        "- Put reviewer observations into issues.\n"
        "- Use updated_context only for structured follow-up hints.\n\n"
        f"Question:\n{question}\n\n"
        f"Current answer package:\n{json.dumps(answer_package, default=str, indent=2)}\n\n"
        f"Evidence package:\n{json.dumps(evidence or {}, default=str, indent=2)}\n\n"
        f"Structured result:\n{json.dumps(result or {}, default=str, indent=2)}\n\n"
        f"Research package:\n{json.dumps(research or {}, default=str, indent=2)}\n\n"
        f"Answer contract review:\n{json.dumps(answer_contract or {}, default=str, indent=2)}\n\n"
        f"Prior step results:\n{json.dumps(step_results or [], default=str, indent=2)}\n"
    )


__all__ = ["build_final_review_prompt"]
