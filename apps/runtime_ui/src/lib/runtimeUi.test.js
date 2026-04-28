import test from "node:test";
import assert from "node:assert/strict";

import {
  buildConversationTurns,
  normalizeRuntimeArtifactType,
} from "./runtimeUi.js";

test("primary_result artifact IDs normalize to table artifacts", () => {
  assert.equal(normalizeRuntimeArtifactType("", "primary_result"), "table");
});

test("buildConversationTurns preserves persisted markdown artifacts", () => {
  const turns = buildConversationTurns(
    [
      {
        id: "user-1",
        role: "user",
        content: { text: "Which order channels drove Q3 revenue?" },
        created_at: "2026-04-27T10:00:00Z",
      },
      {
        id: "assistant-1",
        role: "assistant",
        parent_message_id: "user-1",
        content: {
          summary: "Paid Social led Q3 channel performance.",
          answer_markdown: "Paid Social led.\n\n{{artifact:primary_result}}",
          result: {
            columns: ["order_channel", "net_revenue"],
            rows: [["Paid Social", 9139.54]],
          },
          artifacts: [
            {
              id: "primary_result",
              type: "table",
              role: "primary_result",
              payload: {
                columns: ["order_channel", "net_revenue"],
                rows: [["Paid Social", 9139.54]],
              },
            },
          ],
        },
        model_snapshot: { agent_id: "growth" },
      },
    ],
    [{ id: "growth", name: "growth_analyst" }],
  );

  assert.equal(turns.length, 1);
  assert.equal(turns[0].assistantAnswerMarkdown, "Paid Social led.\n\n{{artifact:primary_result}}");
  assert.equal(turns[0].assistantArtifacts[0].id, "primary_result");
  assert.equal(turns[0].assistantArtifacts[0].type, "table");
  assert.deepEqual(turns[0].assistantArtifacts[0].payload.rows, [["Paid Social", 9139.54]]);
});

test("buildConversationTurns infers primary_result from result when older messages lack artifacts", () => {
  const turns = buildConversationTurns(
    [
      {
        id: "user-1",
        role: "user",
        content: { text: "Which order channels drove Q3 revenue?" },
      },
      {
        id: "assistant-1",
        role: "assistant",
        parent_message_id: "user-1",
        content: {
          answer: "Paid Social led.\n\n{{artifact:primary_result}}",
          result: {
            columns: ["order_channel", "net_revenue"],
            rows: [["Paid Social", 9139.54]],
          },
        },
      },
    ],
    [],
  );

  assert.equal(turns[0].assistantArtifacts[0].id, "primary_result");
  assert.equal(turns[0].assistantArtifacts[0].type, "table");
});
