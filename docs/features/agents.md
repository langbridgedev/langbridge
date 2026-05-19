# Agent Features

Langbridge includes runtime primitives for agent-style analytical execution.

## Agent Stack

- planner-style routing for workload selection
- supervisor and orchestration flow for multi-step execution
- tools for semantic, SQL, retrieval, and analytical operations

## Relationship To Data Execution

- agents use the same runtime guardrails as direct workloads
- semantic and structured queries still go through the runtime execution path
- federation, limits, and result policies apply consistently to agent-initiated work

## Core Value

Agents are consumers of the same runtime primitives as the rest of the system:

- semantic layer
- SQL execution
- federated execution engine
- runtime-managed datasets

## Local Runtime Agent Config

For local/runtime YAML, the canonical agent authoring path is `ai.profiles`.
Profiles are markdown-first analyst contracts over configured runtime resources.
Connector access is derived from the selected semantic models and datasets; users
do not author connector allow or deny lists in the agent contract.

## Runtime Agent Selection

Chat and ask APIs support two selection modes:

- `agent_selection: auto` lets the runtime route across all runtime-available agent profiles. The router can select a profile, answer simple runtime prompts directly, or ask one blocking clarification question.
- `agent_selection: pinned` runs the requested agent profile. This remains the behavior when `agent_id` or `agent_name` is supplied without an explicit selection mode.

Clients should omit `agent_name` when using auto-selection. The run diagnostics include
`diagnostics.agent_selection` with the selected action, candidate count, confidence,
and selected agent name when applicable.

```yaml
ai:
  profiles:
  - name: commerce_analyst
    description: Commerce analyst for governed order and revenue analysis.
    default: true
    availability:
      runtime: true
      mcp: false
    llm:
      llm_connection: local_openai
    data_scope:
      semantic_models: [commerce_performance]
      datasets: [shopify_orders]
      query_policy: semantic_preferred
    capabilities:
      source_sql: false
      research:
        enabled: true
        extended_thinking: false
      web_search:
        enabled: false
    instructions:
      system: You are a Langbridge analytics agent.
      user: Answer from governed runtime data first.
      presentation: Keep answers concise and clearly grounded in query results.
    orchestration:
      policy: balanced_governed
```

For local YAML authoring, `data_scope.semantic_models` and `data_scope.datasets`
can reference configured names. The runtime normalizes those names into canonical
runtime IDs and stores derived `effective_access.connectors` for inspection only.
Use `orchestration.policy` for high-level behavior instead of exposing low-level
execution tuning knobs.
