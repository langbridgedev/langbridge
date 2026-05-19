import test from "node:test";
import assert from "node:assert/strict";

import {
  getCreateTemplate,
  getConfigurationResource,
  getResourceActions,
  getSectionCapabilities,
  getUpdateTemplate,
  listConfigurationResources,
  runAgentConfigurationTest,
  runConfigurationResourceAction,
} from "./configurationService.js";

test("listConfigurationResources normalizes live connector payloads", async () => {
  await withMockFetch(async ({ calls }) => {
    const resources = await listConfigurationResources("connectors");

    assert.equal(calls[0].path, "/api/runtime/v1/connectors");
    assert.equal(resources.length, 1);
    assert.equal(resources[0].name, "warehouse");
    assert.equal(resources[0].status, "Sync capable");
    assert.equal(resources[0].management, "runtime_managed");
    assert.deepEqual(resources[0].relationships, ["orders"]);
  }, [
    {
      items: [
        {
          id: "connector-1",
          name: "warehouse",
          description: "Warehouse connector",
          connector_family: "warehouse",
          connector_type: "POSTGRES",
          supports_sync: true,
          managed: true,
          supported_resources: ["orders"],
        },
      ],
    },
  ]);
});

test("listConfigurationResources throws instead of falling back to static data", async () => {
  await withMockFetch(async () => {
    await assert.rejects(
      () => listConfigurationResources("datasets"),
      /Runtime unavailable/,
    );
  }, [{ detail: "Runtime unavailable" }], { status: 503 });
});

test("getConfigurationResource fetches live resource detail by reference", async () => {
  await withMockFetch(async ({ calls }) => {
    const resource = await getConfigurationResource("datasets", { ref: "sales_orders" });

    assert.equal(calls[0].path, "/api/runtime/v1/datasets/sales_orders");
    assert.equal(resource.name, "sales_orders");
    assert.equal(resource.section, "datasets");
    assert.equal(resource.details.Source.table, "public.sales_orders");
  }, {
    id: "dataset-1",
    name: "sales_orders",
    source: { table: "public.sales_orders" },
    materialization: { mode: "live" },
  });
});

test("listConfigurationResources normalizes analyst agent setup", async () => {
  await withMockFetch(async ({ calls }) => {
    const resources = await listConfigurationResources("agents");

    assert.equal(calls[0].path, "/api/runtime/v1/agents");
    assert.equal(resources.length, 1);
    assert.equal(resources[0].name, "commerce_analyst");
    assert.equal(resources[0].management, "config_managed");
    assert.deepEqual(resources[0].runtimeState, [
      ["LLM connection", "local_openai"],
      ["Query policy", "semantic_only"],
      ["Semantic models", "1"],
      ["Datasets", "1"],
      ["Orchestration", "balanced_governed"],
      ["Tools", "1"],
      ["Default", "Yes"],
    ]);
    assert.deepEqual(resources[0].relationships, ["commerce_performance", "sales_orders", "commerce_sql"]);
  }, {
    items: [
      {
        id: "agent-1",
        name: "commerce_analyst",
        default: true,
        llm_connection: "local_openai",
        semantic_models: ["commerce_performance"],
        datasets: ["sales_orders"],
        tools: [{ name: "commerce_sql" }],
        definition: {
          data_scope: {
            query_policy: "semantic_only",
          },
        },
      },
    ],
  });
});

test("listConfigurationResources normalizes LLM connections", async () => {
  await withMockFetch(async ({ calls }) => {
    const resources = await listConfigurationResources("llm-connections");

    assert.equal(calls[0].path, "/api/runtime/v1/llm-connections");
    assert.equal(resources.length, 1);
    assert.equal(resources[0].name, "local_openai");
    assert.equal(resources[0].status, "Default");
    assert.equal(resources[0].management, "runtime_managed");
    assert.deepEqual(resources[0].relationships, ["commerce_analyst"]);
    assert.deepEqual(resources[0].runtimeState.slice(0, 4), [
      ["Provider", "openai"],
      ["Model", "gpt-4.1-mini"],
      ["Structured outputs", "native"],
      ["Credentials", "configured"],
    ]);
  }, {
    items: [
      {
        id: "llm-1",
        name: "local_openai",
        provider: "openai",
        model: "gpt-4.1-mini",
        credential_state: "configured",
        structured_outputs: "native",
        management_mode: "runtime_managed",
        default: true,
        agents: [{ name: "commerce_analyst" }],
      },
    ],
  });
});

test("getSectionCapabilities keeps agents read-only", () => {
  assert.equal(getSectionCapabilities("connectors").canCreate, true);
  assert.equal(getSectionCapabilities("datasets").canUpdate, true);
  assert.equal(getSectionCapabilities("semantic-models").canDelete, true);
  assert.equal(getSectionCapabilities("llm-connections").canCreate, true);
  assert.equal(getSectionCapabilities("llm-connections").canUpdate, true);
  assert.equal(getSectionCapabilities("llm-connections").canDelete, true);
  assert.equal(getSectionCapabilities("agents").canCreate, false);
  assert.equal(getSectionCapabilities("agents").canUpdate, false);
  assert.equal(getSectionCapabilities("agents").canDelete, false);
});

test("semantic models do not expose generic JSON templates", () => {
  assert.deepEqual(getCreateTemplate("semantic-models"), {});
  assert.deepEqual(getUpdateTemplate("semantic-models", { rawPayload: { description: "Existing" } }), {});
});

test("getResourceActions enables synced dataset actions only when applicable", () => {
  const liveDatasetActions = getResourceActions("datasets", {
    rawPayload: { materialization: { mode: "live" } },
  });
  const syncedDatasetActions = getResourceActions("datasets", {
    rawPayload: { materialization: { mode: "synced" } },
  });

  assert.equal(liveDatasetActions.find((action) => action.id === "run_sync").disabled, true);
  assert.equal(liveDatasetActions.find((action) => action.id === "full_refresh").disabled, true);
  assert.equal(syncedDatasetActions.find((action) => action.id === "run_sync").disabled, false);
  assert.equal(syncedDatasetActions.find((action) => action.id === "full_refresh").disabled, false);
});

test("runConfigurationResourceAction uses the expected runtime endpoints", async () => {
  await withMockFetch(async ({ calls }) => {
    await runConfigurationResourceAction("connectors", { ref: "warehouse" }, "discover_resources");
    await runConfigurationResourceAction("connectors", { ref: "warehouse" }, "sync_states");
    await runConfigurationResourceAction("datasets", { ref: "sales_orders" }, "preview");
    await runConfigurationResourceAction("datasets", { ref: "sales_orders" }, "sync_status");
    await runConfigurationResourceAction("datasets", { ref: "sales_orders" }, "run_sync");
    await runConfigurationResourceAction("datasets", { ref: "sales_orders" }, "full_refresh");
    await runConfigurationResourceAction("llm-connections", { ref: "local_openai" }, "test_connection");

    assert.deepEqual(calls.map((call) => call.path), [
      "/api/runtime/v1/connectors/warehouse/sync/resources",
      "/api/runtime/v1/connectors/warehouse/sync/states",
      "/api/runtime/v1/datasets/sales_orders/preview",
      "/api/runtime/v1/datasets/sales_orders/sync",
      "/api/runtime/v1/datasets/sales_orders/sync",
      "/api/runtime/v1/datasets/sales_orders/sync",
      "/api/runtime/v1/llm-connections/local_openai/test",
    ]);
    assert.equal(calls[2].options.method, "POST");
    assert.equal(calls[4].options.method, "POST");
    assert.equal(calls[6].options.method, "POST");
    assert.deepEqual(JSON.parse(calls[4].options.body), {
      sync_mode: "INCREMENTAL",
      force_full_refresh: false,
    });
    assert.deepEqual(JSON.parse(calls[5].options.body), {
      sync_mode: "FULL_REFRESH",
      force_full_refresh: true,
    });
  }, [{ ok: true }, { ok: true }, { rows: [] }, { status: "idle" }, { job_id: "job-1" }, { job_id: "job-2" }, { status: "success" }]);
});

test("runAgentConfigurationTest calls the runtime ask endpoint", async () => {
  await withMockFetch(async ({ calls }) => {
    const result = await runAgentConfigurationTest(
      { name: "commerce_analyst" },
      {
        message: "Test question",
        agent_mode: "auto",
      },
    );

    assert.equal(result.thread_id, "thread-1");
    assert.equal(calls[0].path, "/api/runtime/v1/agents/ask");
    assert.equal(calls[0].options.method, "POST");
    assert.deepEqual(JSON.parse(calls[0].options.body), {
      agent_name: "commerce_analyst",
      message: "Test question",
      agent_mode: "auto",
    });
  }, {
    thread_id: "thread-1",
    answer_markdown: "Ready",
  });
});

async function withMockFetch(assertions, responses, options = {}) {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const responseQueue = Array.isArray(responses) ? [...responses] : [responses];
  globalThis.fetch = async (path, requestOptions = {}) => {
    calls.push({ path, options: requestOptions });
    const payload = responseQueue.length > 0 ? responseQueue.shift() : {};
    return jsonResponse(payload, options.status || 200);
  };

  try {
    await assertions({ calls });
  } finally {
    globalThis.fetch = originalFetch;
  }
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}
