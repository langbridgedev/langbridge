import assert from "node:assert/strict";
import test from "node:test";

import { buildRunInspectorModel } from "./runInspectorModel.js";

test("buildRunInspectorModel renders investigation trace as primary flow", () => {
  const model = buildRunInspectorModel({
    execution: {
      status: "completed",
      route: "direct:analyst.product_performance",
      execution_mode: "direct",
      selected_agent: "analyst.product_performance",
      step_results: [
        {
          task_id: "step-1",
          agent_name: "analyst.product_performance",
          diagnostics: {
            investigation_trace: [
              {
                id: "entity-resolution",
                type: "entity_resolution",
                title: "Entity resolved",
                status: "resolved",
                summary: "Resolved 140 Summer Partners (F1008740) before metric analysis.",
              },
              {
                id: "benchmark",
                type: "governed_query",
                title: "Benchmark compared",
                status: "success",
                summary: "Compared 2022 performance against the primary benchmark.",
                query_scope: "semantic",
                rowcount: 2,
              },
            ],
          },
        },
      ],
      sql: [],
    },
  });

  assert.equal(model.flowItems[0].title, "Routed to analyst.product_performance");
  assert.equal(model.flowItems[1].title, "Entity resolved");
  assert.equal(model.flowItems[2].title, "Benchmark compared");
  assert.equal(model.flowItems[2].meta, "Governed Query | Success | semantic | 2 rows");
});

test("buildRunInspectorModel renders intent-only runs", () => {
  const model = buildRunInspectorModel({
    intent: {
      intent: "greeting",
      action: "respond",
      rationale: "The prompt is a greeting.",
      confidence: 0.96,
    },
    execution: {
      status: "completed",
      route: "intent:greeting",
      execution_mode: null,
      sql: [],
    },
    route_decision: {
      action: "respond",
      intent: "greeting",
      rationale: "The user is greeting the runtime.",
    },
    ai_run: {
      status: "completed",
      route: "intent:greeting",
      diagnostics: {
        intent: {
          intent: "greeting",
          action: "respond",
          rationale: "The prompt is a greeting.",
          confidence: 0.96,
        },
      },
    },
  });

  assert.equal(model.summary.route, "intent:greeting");
  assert.equal(model.flowItems.length, 1);
  assert.equal(model.flowItems[0].type, "intent");
  assert.equal(model.flowItems[0].title, "Intent: Greeting");
  assert.equal(model.flowItems[0].description, "The prompt is a greeting.");
  assert.equal(model.flowItems[0].meta, "Respond | 96% confidence");
});

test("buildRunInspectorModel renders auto agent selection before execution", () => {
  const model = buildRunInspectorModel({
    agent_selection: {
      action: "select",
      agent_name: "support_analyst",
      rationale: "The request is about support tickets.",
      confidence: 0.91,
      candidate_count: 2,
    },
    execution: {
      status: "completed",
      route: "direct:analyst.support_analyst",
      execution_mode: "direct",
      selected_agent: "analyst.support_analyst",
      step_results: [],
      sql: [],
    },
  });

  assert.equal(model.summary.selectedAgent, "analyst.support_analyst");
  assert.equal(model.flowItems[0].type, "agent-selection");
  assert.equal(model.flowItems[0].title, "Auto selected support_analyst");
  assert.equal(model.flowItems[0].meta, "2 candidate(s) | 91% confidence");
});
