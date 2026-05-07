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
