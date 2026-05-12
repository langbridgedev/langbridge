import assert from "node:assert/strict";
import test from "node:test";

import {
  AUTO_AGENT_SELECTION_VALUE,
  buildRuntimeAgentRunPayload,
  deriveRuntimeResultState,
} from "./runtimeUi.js";

test("deriveRuntimeResultState treats markdown-only responses as answers", () => {
  const state = deriveRuntimeResultState({
    status: "completed",
    result: null,
    visualization: null,
    diagnostics: {},
    answerMarkdown: "Hi. Ask me a governed data question.",
  });

  assert.equal(state.kind, "success_answer");
  assert.equal(state.description, "");
  assert.equal(state.showChart, false);
  assert.equal(state.showTable, false);
});

test("deriveRuntimeResultState still surfaces explicit empty tabular results", () => {
  const state = deriveRuntimeResultState({
    status: "completed",
    result: { columns: ["region"], rows: [] },
    visualization: null,
    diagnostics: {},
    answerMarkdown: "No rows matched the current filters.",
  });

  assert.equal(state.kind, "empty_result");
  assert.equal(state.showTable, true);
});

test("buildRuntimeAgentRunPayload omits agent_name for auto selection", () => {
  const payload = buildRuntimeAgentRunPayload({
    message: "Hello",
    selectedAgentName: AUTO_AGENT_SELECTION_VALUE,
    threadId: "thread-1",
    agentMode: "auto",
  });

  assert.equal(payload.agent_selection, "auto");
  assert.equal(payload.agent_name, undefined);
  assert.equal(payload.thread_id, "thread-1");
});

test("buildRuntimeAgentRunPayload pins explicit agent selection", () => {
  const payload = buildRuntimeAgentRunPayload({
    message: "Revenue by channel",
    selectedAgentName: "commerce_analyst",
    threadId: "thread-1",
    agentMode: "research",
  });

  assert.equal(payload.agent_selection, "pinned");
  assert.equal(payload.agent_name, "commerce_analyst");
  assert.equal(payload.agent_mode, "research");
});
