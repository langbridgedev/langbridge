"""Prompt builders for the Langbridge AI planner."""
import json


def build_execution_plan_prompt(
    *,
    question: str,
    context: dict[str, object],
    requested_agent_mode: str | None,
    specification_payloads: list[dict[str, object]],
    revision_count: int,
) -> str:
    return (
        "Build Langbridge execution plan.\n"
        "You are the runtime planner. Plan from agent specifications and observed context.\n"
        "Return STRICT JSON only:\n"
        "{"
        "\"route\":\"planned\","
        "\"rationale\":\"short reason\","
        "\"clarification_question\":\"question for user or null\","
        "\"steps\":["
        "{"
        "\"agent_name\":\"exact agent name\","
        "\"task_kind\":\"one supported task kind\","
        "\"question\":\"step question\","
        "\"input\":{},"
        "\"depends_on\":[]"
        "}"
        "]"
        "}\n"
        "Planning rules:\n"
        "- Use only available agent names and supported task kinds.\n"
        "- Keep the plan minimal. Use one step when one step is enough.\n"
        "- Do not split a single analyst's internal evidence-building, optional augmentation, and synthesis into separate plan steps.\n"
        "- Do not build a one-step plan solely because a single analyst research step may use web search or source-backed synthesis.\n"
        "- Prefer plans that preserve verifiable intermediate outputs.\n"
        "- Use analyst input.agent_mode 'research' only when source-backed or external/current synthesis is truly required.\n"
        "- For analyst steps, input.agent_mode may only be auto, sql, context_analysis, or research.\n"
        "- Never use input.mode, answer, analysis, or deep_research as analyst mode values.\n"
        "- If requested_agent_mode is set, preserve it for analyst steps unless clarification is required.\n"
        "- Use prior execution failures, weak evidence, and avoid_agents when replanning.\n"
        "- Do not include presentation or orchestration agents in steps.\n"
        "- If the user needs clarification before safe execution, return an empty steps list and set clarification_question.\n"
        "- Keep rationale short and concrete.\n\n"
        f"Revision count: {revision_count}\n"
        f"Requested agent mode: {requested_agent_mode or ''}\n"
        f"Question: {question}\n"
        f"Conversation context:\n{context.get('conversation_context') or ''}\n"
        f"Memory context:\n{context.get('memory_context') or ''}\n"
        f"Planner guidance: {context.get('plan_guidance') or ''}\n"
        f"Prior observations: {json.dumps(context.get('verification_failure') or context.get('last_error') or {}, default=str)}\n"
        f"Avoid agents: {json.dumps(context.get('avoid_agents') or [])}\n"
        f"Available agent specifications:\n{json.dumps(specification_payloads, default=str, indent=2)}\n"
    )


__all__ = ["build_execution_plan_prompt"]
