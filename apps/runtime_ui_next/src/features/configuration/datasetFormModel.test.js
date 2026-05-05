import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDatasetCreateFormState,
  buildDatasetEditFormState,
  buildDatasetSubmitPayload,
  datasetConnectorFamilyOptions,
  detectDatasetSourceMode,
  normalizeDatasetConnectors,
  splitDatasetTags,
} from "./datasetFormModel.js";

const postgresConnector = {
  name: "warehouse",
  connector_family: "DATABASE",
  capabilities: {
    supports_live_datasets: true,
    supports_synced_datasets: false,
    supports_query_pushdown: true,
  },
};

const syncConnector = {
  name: "shopify",
  connector_family: "API",
  capabilities: {
    supports_live_datasets: true,
    supports_synced_datasets: true,
    supports_query_pushdown: false,
  },
};

test("normalizeDatasetConnectors and family options prepare connector selectors", () => {
  const connectors = normalizeDatasetConnectors([postgresConnector, syncConnector]);

  assert.deepEqual(connectors.map((item) => item.value), ["shopify", "warehouse"]);
  assert.deepEqual(datasetConnectorFamilyOptions(connectors), [
    { value: "api", label: "Api" },
    { value: "database", label: "Database" },
  ]);
});

test("buildDatasetSubmitPayload creates table dataset payloads", () => {
  const form = {
    ...buildDatasetCreateFormState(),
    name: "sales_orders",
    label: "Sales orders",
    description: "Orders table",
    connector: "warehouse",
    table: "public.sales_orders",
    tags: "sales, governed",
  };

  assert.deepEqual(buildDatasetSubmitPayload({ mode: "create", form, connector: postgresConnector }), {
    name: "sales_orders",
    connector: "warehouse",
    source: {
      kind: "table",
      table: "public.sales_orders",
    },
    materialization: {
      mode: "live",
    },
    label: "Sales orders",
    description: "Orders table",
    tags: ["sales", "governed"],
  });
});

test("buildDatasetSubmitPayload creates sql dataset payloads", () => {
  const form = {
    ...buildDatasetCreateFormState(),
    name: "sales_rollup",
    connector: "warehouse",
    sourceMode: "sql",
    sql: "SELECT * FROM sales_orders",
  };

  assert.deepEqual(buildDatasetSubmitPayload({ mode: "create", form, connector: postgresConnector }), {
    name: "sales_rollup",
    connector: "warehouse",
    source: {
      kind: "sql",
      sql: "SELECT * FROM sales_orders",
    },
    materialization: {
      mode: "live",
    },
  });
});

test("buildDatasetSubmitPayload supports connectorless file datasets", () => {
  const form = {
    ...buildDatasetCreateFormState(),
    name: "uploaded_orders",
    connector: "",
    sourceMode: "file",
    path: "/tmp/orders.csv",
    format: "csv",
    header: true,
    delimiter: ",",
    quote: "\"",
  };

  assert.deepEqual(buildDatasetSubmitPayload({ mode: "create", form }), {
    name: "uploaded_orders",
    connector: null,
    source: {
      kind: "file",
      path: "/tmp/orders.csv",
      format: "csv",
      header: true,
      delimiter: ",",
      quote: "\"",
    },
    materialization: {
      mode: "live",
    },
  });
});

test("buildDatasetSubmitPayload creates synced resource payloads", () => {
  const form = {
    ...buildDatasetCreateFormState(),
    name: "shopify_orders",
    connector: "shopify",
    materializationMode: "synced",
    sourceMode: "table",
    resource: "orders",
  };

  assert.deepEqual(buildDatasetSubmitPayload({ mode: "create", form, connector: syncConnector }), {
    name: "shopify_orders",
    connector: "shopify",
    source: {
      kind: "resource",
      resource: "orders",
    },
    materialization: {
      mode: "synced",
      sync: {},
    },
  });
});

test("buildDatasetSubmitPayload validates connector requirements and capabilities", () => {
  assert.throws(
    () =>
      buildDatasetSubmitPayload({
        mode: "create",
        form: { ...buildDatasetCreateFormState(), name: "orders", table: "orders" },
      }),
    /Select a connector/,
  );
  assert.throws(
    () =>
      buildDatasetSubmitPayload({
        mode: "create",
        form: {
          ...buildDatasetCreateFormState(),
          name: "orders",
          connector: "warehouse",
          materializationMode: "synced",
          resource: "orders",
        },
        connector: postgresConnector,
      }),
    /synced dataset support/,
  );
});

test("buildDatasetEditFormState reads existing runtime dataset detail", () => {
  const form = buildDatasetEditFormState({
    rawPayload: {
      name: "sales_orders",
      label: "Sales orders",
      description: "Runtime dataset",
      connector: "warehouse",
      materialization: { mode: "live" },
      source: { kind: "file", storage_uri: "s3://bucket/orders.csv", format: "csv", header: true },
      tags: ["sales", "runtime"],
    },
  });

  assert.equal(form.name, "sales_orders");
  assert.equal(form.sourceMode, "file");
  assert.equal(form.fileLocationField, "storage_uri");
  assert.equal(form.path, "s3://bucket/orders.csv");
  assert.equal(form.tags, "sales, runtime");
});

test("buildDatasetSubmitPayload omits unchanged source and materialization on update", () => {
  const resource = {
    rawPayload: {
      name: "sales_orders",
      label: "Sales orders",
      description: "Old",
      connector: "warehouse",
      materialization: { mode: "live" },
      source: { kind: "table", table: "public.sales_orders" },
      tags: ["sales"],
    },
  };
  const form = {
    ...buildDatasetEditFormState(resource),
    description: "Updated",
  };

  assert.deepEqual(buildDatasetSubmitPayload({ mode: "edit", form, originalResource: resource }), {
    label: "Sales orders",
    description: "Updated",
    tags: ["sales"],
  });
});

test("buildDatasetSubmitPayload updates supported simple source changes", () => {
  const resource = {
    rawPayload: {
      name: "sales_orders",
      connector: "warehouse",
      materialization: { mode: "live" },
      source: { kind: "table", table: "public.sales_orders" },
    },
  };
  const form = {
    ...buildDatasetEditFormState(resource),
    table: "analytics.orders",
  };

  assert.deepEqual(buildDatasetSubmitPayload({ mode: "edit", form, originalResource: resource }).source, {
    kind: "table",
    table: "analytics.orders",
  });
});

test("buildDatasetSubmitPayload preserves existing sync policy when synced source changes", () => {
  const resource = {
    rawPayload: {
      name: "shopify_orders",
      connector: "shopify",
      materialization: { mode: "synced" },
      sync: { strategy: "INCREMENTAL", cursor_field: "updated_at" },
      source: { kind: "resource", resource: "orders" },
    },
  };
  const form = {
    ...buildDatasetEditFormState(resource),
    resource: "customers",
  };

  assert.deepEqual(buildDatasetSubmitPayload({ mode: "edit", form, originalResource: resource }), {
    label: "shopify_orders",
    description: null,
    source: {
      kind: "resource",
      resource: "customers",
    },
    tags: [],
  });
});

test("unsupported request sources are inspect-only and preserve source by omission", () => {
  const resource = {
    rawPayload: {
      name: "api_dataset",
      connector: "api",
      materialization: { mode: "live" },
      source: { kind: "request", request: { path: "/orders" } },
      tags: [],
    },
  };
  const form = {
    ...buildDatasetEditFormState(resource),
    description: "Metadata only",
  };

  assert.equal(detectDatasetSourceMode(resource.rawPayload.source), "unsupported");
  assert.deepEqual(buildDatasetSubmitPayload({ mode: "edit", form, originalResource: resource }), {
    label: "api_dataset",
    description: "Metadata only",
    tags: [],
  });
});

test("splitDatasetTags trims empty values", () => {
  assert.deepEqual(splitDatasetTags("sales, , finance "), ["sales", "finance"]);
});
