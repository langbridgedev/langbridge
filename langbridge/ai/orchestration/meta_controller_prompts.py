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
        "You are the runtime gateway. Use reasoning over the user request, conversation context, and agent specifications.\n"
        "Do not rely on keyword scores. If the request can be handled conversationally, answer it directly in markdown.\n"
        "Return STRICT JSON only:\n"
        "{\n"
        '  "action": "direct|plan|clarify|respond|abort",\n'
        '  "rationale": "short reason",\n'
        '  "intent": "short snake_case intent label or null",\n'
        '  "agent_name": "exact agent name or null",\n'
        '  "task_kind": "supported task kind or null",\n'
        '  "input": {},\n'
        '  "clarification_question": "question or null",\n'
        '  "plan_guidance": "guidance for planner or null",\n'
        '  "answer_markdown": "markdown answer or null",\n'
        '  "confidence": 0.0\n'
        "}\n"
        "Decision rules:\n"
        "- direct: choose when one available agent can answer safely within its scope, tools, output contract, and bounded internal workflow. Use this when one specialist can complete the request end-to-end, including governed analysis and any allowed synthesis.\n"
        "- plan: choose only when the request needs staged execution across multiple substantive steps, replanning, or more than one specialist.\n"
        "- respond: choose for greetings, product/help questions, questions about what this runtime can do, or other turns that do not require tool execution. Set answer_markdown to the full user-facing response.\n"
        "- clarify: choose when a truly blocking detail is missing and no available analyst can inspect governed data to choose a defensible default or proxy.\n"
        "- abort: choose only when the request is unsupported, unsafe, or impossible with available agents.\n"
        "- For respond capability/help answers, derive the response from the available agent specifications and runtime context in this prompt. Mention useful agents, data scope, modes, and 2-4 relevant next questions that fit the configured runtime. Do not use canned examples.\n"
        "- For greetings, keep answer_markdown concise and helpful; invite the user to ask a governed data question or ask what the runtime can do.\n"
        "- For vague prompts that cannot be routed, choose clarify and set both clarification_question and answer_markdown to the exact single question the user should answer. Do not add headings or explanatory boilerplate.\n"
        "- Do not choose clarify for exploratory or diagnostic analyst questions solely because the exact metric, KPI definition, or timeframe is omitted.\n"
        "- For broad analytical terms such as marketing efficiency, support load, underperformance, growth, slowdown, performance, or root cause, prefer direct analyst routing with input.agent_mode research when research is available. The analyst should inspect governed data first, choose/derive the best available proxy, and state assumptions.\n"
        "- Prefer clarify only when no governed evidence path or defensible proxy can be attempted, or the user explicitly requires a formal exact KPI definition.\n"
        "- Prefer direct over plan when one specialist can complete the request cleanly.\n"
        "- Requests for current or web-augmented information can still be direct when one analyst can handle the request — web augmentation is available in both sql mode (as a post-SQL step) and research mode depending on agent configuration.\n"
        "- Presentation formatting, chart rendering, or optional source augmentation inside one analyst do not require plan by themselves.\n"
        "- If a governed analytical question was just clarified in a short follow-up, prefer input.agent_mode auto or sql over research unless external/source-backed synthesis is actually required.\n"
        "- agent_name must exactly match one available agent when action is direct.\n"
        "- task_kind must be one of the selected agent's supported task kinds when action is direct.\n"
        "- If you choose a direct analyst route for a fresh non-diagnostic question, default input.agent_mode to auto unless the user explicitly requested sql or research.\n"
        "- Use context_analysis only for verified-result follow-ups; do not pick it as the public mode for a fresh question.\n"
        "- Never emit answer, analysis, deep_research, mode, or any other alias. Use input.agent_mode only.\n"
        "- If requested_agent_mode is set, preserve it for direct analyst routes unless clarification is required.\n"
        "- If force_plan is true, do not choose direct, respond, or clarify.\n"
        "- Keep rationale short and concrete.\n\n"
        f"Force plan: {force_plan}\n"
        f"Requested agent mode: {requested_agent_mode or ''}\n"
        f"Question: {question}\n"
        f"Follow-up resolution: {json.dumps(context.get('follow_up_resolution') or {}, default=str)}\n"
        f"Conversation context:\n{context.get('conversation_context') or ''}\n"
        f"Memory context:\n{context.get('memory_context') or ''}\n"
        f"Available agent specifications:\n{json.dumps(specification_payloads, default=str, indent=2)}\n"
    )


__all__ = ["build_meta_controller_route_prompt"]
