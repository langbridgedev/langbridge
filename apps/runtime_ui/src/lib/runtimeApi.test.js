import test from "node:test";
import assert from "node:assert/strict";

import { parseSseEventBlock, streamRuntimeRun } from "./runtimeApi.js";

test("parseSseEventBlock parses structured SSE payloads", () => {
  const event = parseSseEventBlock(`event: run.progress
data: {"sequence":2,"stage":"running_query","status":"in_progress","message":"Running query"}

`);

  assert.deepEqual(event, {
    event: "run.progress",
    sequence: 2,
    stage: "running_query",
    status: "in_progress",
    message: "Running query",
  });
});

test("parseSseEventBlock joins multiline data payloads", () => {
  const event = parseSseEventBlock(`event: run.completed
data: {"message":"done",
data: "terminal":true}

`);

  assert.equal(event.event, "run.completed");
  assert.equal(event.message, "done");
  assert.equal(event.terminal, true);
});

test("parseSseEventBlock ignores comment-only blocks", () => {
  const event = parseSseEventBlock(`: stream-open
: keep-alive

`);

  assert.equal(event, null);
});

test("parseSseEventBlock captures SSE ids", () => {
  const event = parseSseEventBlock(`id: 3f0d:4
event: run.progress
data: {"sequence":4,"run_id":"3f0d","message":"still running"}

`);

  assert.equal(event.id, "3f0d:4");
  assert.equal(event.run_id, "3f0d");
});

test("streamRuntimeRun reads a run-scoped SSE stream", async () => {
  const originalFetch = global.fetch;
  const seen = [];
  try {
    global.fetch = async (url) => {
      assert.equal(url, "/api/runtime/v1/runs/run-123/stream?after_sequence=2");
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(`id: run-123:3
event: run.progress
data: {"sequence":3,"run_id":"run-123","stage":"running_query","status":"in_progress","message":"Running query"}

id: run-123:4
event: run.completed
data: {"sequence":4,"run_id":"run-123","stage":"completed","status":"completed","terminal":true,"message":"Done"}

`),
            );
            controller.close();
          },
        }),
        {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        },
      );
    };

    const events = await streamRuntimeRun("run-123", {
      afterSequence: 2,
      onEvent: (event) => seen.push(event.event),
    });

    assert.equal(events.length, 2);
    assert.deepEqual(seen, ["run.progress", "run.completed"]);
    assert.equal(events[0].id, "run-123:3");
    assert.equal(events[1].terminal, true);
  } finally {
    global.fetch = originalFetch;
  }
});
