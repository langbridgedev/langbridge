"""Prompt builders for the Langbridge AI meta-controller."""
import json


def build_meta_controller_route_prompt(
    *,
    question: str,
    context: dict[str, object],
    force_plan: bool,
    requested_agent_mode: str | None,
    specification_payloads: list[dict[str, object]],
) -> str:
    return (
        "Decide Langbridge agent route.\n"
        "You are the runtime gateway. Route from agent specifications, not keyword scores.\n"
        "Return STRICT JSON only:\n"
        "{"
        "\"action\":\"direct|plan|clarify|abort\","
        "\"rationale\":\"short reason\","
        "\"agent_name\":\"exact agent name or null\","
        "\"task_kind\":\"supported task kind or null\","
        "\"input\":{},"
        "\"clarification_question\":\"question or null\","
        "\"plan_guidance\":\"guidance for planner or null\""
        "}\n"
        "Decision rules:\n"
        "- direct: choose when one available agent can answer safely within its scope, tools, output contract, and bounded internal workflow. Use this when one specialist can complete the request end-to-end, including governed analysis and any allowed synthesis.\n"
        "- plan: choose only when the request needs staged execution across multiple substantive steps, replanning, or more than one specialist.\n"
        "- clarify: choose when a truly blocking detail is missing and no available analyst can inspect governed data to choose a defensible default or proxy.\n"
        "- abort: choose only when the request is unsupported, unsafe, or impossible with available agents.\n"
        "- Do not choose clarify for exploratory or diagnostic analyst questions solely because the exact metric, KPI definition, or timeframe is omitted.\n"
        "- For broad analytical terms such as marketing efficiency, support load, underperformance, growth, slowdown, performance, or root cause, prefer direct analyst routing with input.agent_mode research when research is available. The analyst should inspect governed data first, choose/derive the best available proxy, and state assumptions.\n"
        "- Prefer clarify only when no governed evidence path or defensible proxy can be attempted, or the user explicitly requires a formal exact KPI definition.\n"
        "- Prefer direct over plan when one specialist can complete the request cleanly.\n"
        "- Requests for source-backed, current, or web-augmented synthesis can still be direct when one analyst can do that work inside research mode.\n"
        "- Presentation formatting, chart rendering, or optional source augmentation inside one analyst do not require plan by themselves.\n"
        "- If a governed analytical question was just clarified in a short follow-up, prefer input.agent_mode auto or sql over research unless external/source-backed synthesis is actually required.\n"
        "- agent_name must exactly match one available agent when action is direct.\n"
        "- task_kind must be one of the selected agent's supported task kinds when action is direct.\n"
        "- If you choose a direct analyst route for a fresh non-diagnostic question, default input.agent_mode to auto unless the user explicitly requested sql or research.\n"
        "- Use context_analysis only for verified-result follow-ups; do not pick it as the public mode for a fresh question.\n"
        "- Never emit answer, analysis, deep_research, mode, or any other alias. Use input.agent_mode only.\n"
        "- If requested_agent_mode is set, preserve it for direct analyst routes unless clarification is required.\n"
        "- If force_plan is true, do not choose direct.\n"
        "- Keep rationale short and concrete.\n\n"
        f"Force plan: {force_plan}\n"
        f"Requested agent mode: {requested_agent_mode or ''}\n"
        f"Question: {question}\n"
        f"Follow-up resolution: {json.dumps(context.get('follow_up_resolution') or {}, default=str)}\n"
        f"Conversation context:\n{context.get('conversation_context') or ''}\n"
        f"Memory context:\n{context.get('memory_context') or ''}\n"
        f"Runtime context keys: {json.dumps(sorted(context.keys()))}\n"
        f"Available agent specifications:\n{json.dumps(specification_payloads, default=str, indent=2)}\n"
    )


__all__ = ["build_meta_controller_route_prompt"]
