import test from "node:test";
import assert from "node:assert/strict";

import {
  createAgentRun,
  parseSseEventBlock,
  runDatasetSync,
  runSqlQuery,
  streamRuntimeJob,
} from "./runtimeApi.js";

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
data: {"sequence":4,"job_id":"3f0d","message":"still running"}

`);

  assert.equal(event.id, "3f0d:4");
  assert.equal(event.job_id, "3f0d");
});

test("streamRuntimeJob reads a job-scoped SSE stream", async () => {
  const originalFetch = global.fetch;
  const seen = [];
  try {
    global.fetch = async (url) => {
      assert.equal(url, "/api/runtime/v1/jobs/job-123/stream?after_sequence=2");
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(`id: job-123:3
event: run.progress
data: {"sequence":3,"job_id":"job-123","stage":"running_query","status":"in_progress","message":"Running query"}

id: job-123:4
event: run.completed
data: {"sequence":4,"job_id":"job-123","stage":"completed","status":"completed","terminal":true,"message":"Done"}

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

    const events = await streamRuntimeJob("job-123", {
      afterSequence: 2,
      onEvent: (event) => seen.push(event.event),
    });

    assert.equal(events.length, 2);
    assert.deepEqual(seen, ["run.progress", "run.completed"]);
    assert.equal(events[0].id, "job-123:3");
    assert.equal(events[1].terminal, true);
  } finally {
    global.fetch = originalFetch;
  }
});

test("runDatasetSync waits for a queued dataset sync job result", async () => {
  const originalFetch = global.fetch;
  const seen = [];
  try {
    global.fetch = async (url, options = {}) => {
      if (url === "/api/runtime/v1/datasets/billing_customers/sync") {
        assert.equal(options.method, "POST");
        return Response.json(
          {
            status: "queued",
            job_id: "job-sync-1",
            job_type: "dataset.sync",
            dataset_name: "billing_customers",
            resources: [],
            summary: "Dataset sync queued.",
          },
          { status: 202 },
        );
      }
      if (url === "/api/runtime/v1/jobs/job-sync-1/stream") {
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode(`event: dataset.sync.started
data: {"sequence":2,"job_id":"job-sync-1","message":"Dataset sync started."}

event: dataset.sync.succeeded
data: {"sequence":3,"job_id":"job-sync-1","terminal":true,"message":"Dataset sync completed."}

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
      }
      if (url === "/api/runtime/v1/jobs/job-sync-1") {
        return Response.json({
          id: "job-sync-1",
          job_type: "dataset.sync",
          status: "succeeded",
          result: {
            status: "succeeded",
            dataset_name: "billing_customers",
            resources: [
              {
                resource_name: "customers",
                records_synced: 2,
              },
            ],
            summary: "Dataset sync completed.",
          },
        });
      }
      throw new Error(`Unexpected request ${options.method || "GET"} ${url}`);
    };

    const result = await runDatasetSync(
      "billing_customers",
      { sync_mode: "INCREMENTAL" },
      { onEvent: (event) => seen.push(event.event) },
    );

    assert.equal(result.status, "succeeded");
    assert.equal(result.job_id, "job-sync-1");
    assert.equal(result.resources[0].records_synced, 2);
    assert.deepEqual(seen, ["dataset.sync.started", "dataset.sync.succeeded"]);
  } finally {
    global.fetch = originalFetch;
  }
});

test("runSqlQuery waits for a queued SQL query job result", async () => {
  const originalFetch = global.fetch;
  const seen = [];
  try {
    global.fetch = async (url, options = {}) => {
      if (url === "/api/runtime/v1/sql/query/jobs") {
        assert.equal(options.method, "POST");
        assert.deepEqual(JSON.parse(options.body), {
          query_scope: "source",
          query: "SELECT 42 AS answer",
          connection_name: "demo",
        });
        return Response.json(
          {
            status: "queued",
            job_id: "job-sql-1",
            job_type: "sql.query",
            query_scope: "source",
            stream_path: "/api/runtime/v1/jobs/job-sql-1/stream",
          },
          { status: 202 },
        );
      }
      if (url === "/api/runtime/v1/jobs/job-sql-1/stream") {
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode(`event: sql.query.started
data: {"sequence":2,"job_id":"job-sql-1","message":"SQL query started."}

event: sql.query.succeeded
data: {"sequence":3,"job_id":"job-sql-1","terminal":true,"message":"SQL query completed."}

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
      }
      if (url === "/api/runtime/v1/jobs/job-sql-1") {
        return Response.json({
          id: "job-sql-1",
          job_type: "sql.query",
          status: "succeeded",
          result: {
            status: "succeeded",
            query_scope: "source",
            query: "SELECT 42 AS answer",
            generated_sql: "SELECT 42 AS answer",
            columns: [{ name: "answer", type: null }],
            rows: [{ answer: 42 }],
            row_count_preview: 1,
            duration_ms: 5,
          },
          artifacts: [
            {
              artifact_key: "result_table",
              data: {
                columns: [{ name: "answer", type: null }],
                rows: [{ answer: 42 }],
                row_count_preview: 1,
              },
            },
            {
              artifact_key: "sql_diagnostics",
              data: { generated_sql: "SELECT 42 AS answer" },
            },
          ],
        });
      }
      throw new Error(`Unexpected request ${options.method || "GET"} ${url}`);
    };

    const result = await runSqlQuery(
      {
        query_scope: "source",
        query: "SELECT 42 AS answer",
        connection_name: "demo",
      },
      { onEvent: (event) => seen.push(event.event) },
    );

    assert.equal(result.status, "succeeded");
    assert.equal(result.job_id, "job-sql-1");
    assert.equal(result.query_scope, "source");
    assert.deepEqual(result.rows, [{ answer: 42 }]);
    assert.equal(result.generated_sql, "SELECT 42 AS answer");
    assert.deepEqual(seen, ["sql.query.started", "sql.query.succeeded"]);
  } finally {
    global.fetch = originalFetch;
  }
});

test("createAgentRun queues a durable agent run job", async () => {
  const originalFetch = global.fetch;
  try {
    global.fetch = async (url, options = {}) => {
      assert.equal(url, "/api/runtime/v1/agents/run");
      assert.equal(options.method, "POST");
      assert.deepEqual(JSON.parse(options.body), {
        message: "Show Q3 revenue",
        agent_name: "growth_analyst",
        thread_id: "thread-1",
        agent_mode: "auto",
      });
      return Response.json(
        {
          status: "queued",
          job_id: "job-agent-1",
          job_type: "agent.run",
          thread_id: "thread-1",
          message_id: "message-1",
          agent_name: "growth_analyst",
          stream_path: "/api/runtime/v1/jobs/job-agent-1/stream",
        },
        { status: 202 },
      );
    };

    const queued = await createAgentRun({
      message: "Show Q3 revenue",
      agent_name: "growth_analyst",
      thread_id: "thread-1",
      agent_mode: "auto",
    });

    assert.equal(queued.status, "queued");
    assert.equal(queued.job_id, "job-agent-1");
    assert.equal(queued.job_type, "agent.run");
    assert.equal(queued.stream_path, "/api/runtime/v1/jobs/job-agent-1/stream");
  } finally {
    global.fetch = originalFetch;
  }
});
