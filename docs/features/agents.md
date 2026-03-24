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

For local/runtime YAML, the canonical agent authoring path is `agents[].definition`.
Use `definition.tools` to declare agent tool bindings instead of the old top-level
single `semantic_model` or `dataset` shortcut.

```yaml
agents:
  - name: commerce_analyst
    llm_connection: local_openai
    default: true
    definition:
      prompt:
        system_prompt: You are a Langbridge analytics agent.
      memory:
        strategy: database
      features:
        bi_copilot_enabled: false
        deep_research_enabled: false
        visualization_enabled: true
        mcp_enabled: false
      tools:
        - name: governed_sql
          tool_type: sql
          config:
            semantic_model_ids: [commerce_performance, support_performance]
        - name: dataset_sql
          tool_type: sql
          config:
            dataset_ids: [shopify_orders, support_tickets]
      access_policy:
        allowed_connectors: [commerce_demo]
        denied_connectors: []
      execution:
        mode: iterative
        response_mode: analyst
        max_iterations: 3
        max_steps_per_iteration: 5
        allow_parallel_tools: false
      output:
        format: markdown
```

For local YAML authoring, `semantic_model_ids`, `dataset_ids`, and connector lists
can reference configured names. The runtime normalizes those names into the
canonical runtime IDs before storing the agent definition. SQL tools must still
follow the existing rule: define either `dataset_ids` or `semantic_model_ids`.
