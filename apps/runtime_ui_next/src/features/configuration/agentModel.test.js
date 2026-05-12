import test from "node:test";
import assert from "node:assert/strict";

import {
  agentWorkspaceStats,
  buildAgentTestPayload,
  normalizeAgentWorkspace,
} from "./agentModel.js";

test("normalizeAgentWorkspace reads detailed analyst setup", () => {
  const agent = normalizeAgentWorkspace({
    id: "agent-1",
    name: "commerce_analyst",
    description: "Commerce analytics",
    default: true,
    llm_connection: "local_openai",
    semantic_models: ["commerce_performance"],
    datasets: ["sales_orders"],
    instructions: "Prefer semantic models first.",
    tools: [{ name: "commerce_sql", description: "Run governed SQL" }],
    definition: {
      llm: {
        model: "gpt-5.4",
        reasoning_effort: "medium",
      },
      availability: {
        runtime: true,
        mcp: true,
      },
      data_scope: {
        query_policy: "semantic_only",
      },
      capabilities: {
        source_sql: true,
        research: {
          enabled: false,
        },
        web_search: {
          enabled: true,
          allowed_domains: ["example.com"],
        },
      },
      instructions: {
        system: "You are an analyst.",
        presentation: "Keep answers concise.",
      },
      orchestration: {
        policy: "strict_governed",
      },
      effective_access: {
        connectors: ["warehouse"],
      },
    },
  });

  assert.equal(agent.name, "commerce_analyst");
  assert.equal(agent.default, true);
  assert.equal(agent.llm.connection, "local_openai");
  assert.equal(agent.llm.model, "gpt-5.4");
  assert.equal(agent.availability.mcp, true);
  assert.equal(agent.dataScope.queryPolicy, "semantic_only");
  assert.deepEqual(agent.dataScope.semanticModels, ["commerce_performance"]);
  assert.deepEqual(agent.dataScope.datasets, ["sales_orders"]);
  assert.equal(agent.capabilities.sourceSql, true);
  assert.equal(agent.instructions.user, "Prefer semantic models first.");
  assert.equal(agent.instructions.system, "You are an analyst.");
  assert.equal(agent.orchestration.policy, "strict_governed");
  assert.deepEqual(agent.capabilities.webSearch.allowedDomains, ["example.com"]);
  assert.deepEqual(agent.effectiveAccess.connectors, ["warehouse"]);
  assert.equal(agent.tools[0].name, "commerce_sql");
});

test("normalizeAgentWorkspace tolerates summary-only payloads", () => {
  const agent = normalizeAgentWorkspace({
    name: "summary_agent",
    tool_count: 0,
  });

  assert.equal(agent.name, "summary_agent");
  assert.equal(agent.dataScope.queryPolicy, "semantic_preferred");
  assert.deepEqual(agent.dataScope.semanticModels, []);
  assert.equal(agent.instructions.user, "");
});

test("buildAgentTestPayload creates ask API payloads", () => {
  assert.deepEqual(
    buildAgentTestPayload({
      agent: { name: "commerce_analyst" },
      message: "  Which channel won Q3? ",
      agentMode: "sql",
    }),
    {
      agent_name: "commerce_analyst",
      message: "Which channel won Q3?",
      agent_mode: "sql",
      title: "Configuration test: commerce_analyst",
      metadata_json: {
        source: "runtime_ui_next.configuration.agent_test",
      },
    },
  );
});

test("buildAgentTestPayload validates prompt and normalizes mode", () => {
  assert.throws(
    () => buildAgentTestPayload({ agent: { name: "commerce_analyst" }, message: "" }),
    /Enter a test prompt/,
  );

  const payload = buildAgentTestPayload({
    agent: { name: "commerce_analyst" },
    message: "Hello",
    agentMode: "unknown",
  });
  assert.equal(payload.agent_mode, "auto");
});

test("agentWorkspaceStats counts visible setup sections", () => {
  assert.deepEqual(
    agentWorkspaceStats({
      name: "commerce_analyst",
      semantic_models: ["commerce"],
      datasets: ["orders", "customers"],
      instructions: "Use governed data.",
      tools: ["commerce_sql"],
    }),
    {
      semanticModels: 1,
      datasets: 2,
      capabilities: 0,
      instructions: 1,
    },
  );
});
