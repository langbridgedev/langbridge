import test from "node:test";
import assert from "node:assert/strict";

import {
  buildSqlQueryPayload,
  buildSqlCompletionItems,
  buildScopeResourceHints,
  buildScopeResourceTree,
  createQueryArtifactBundle,
  filterSqlConnectorOptions,
  normalizeQueryRunResult,
} from "./queryWorkspaceModel.js";

test("buildSqlQueryPayload creates semantic SQL requests without resource selectors", () => {
  const payload = buildSqlQueryPayload({
    queryScope: "semantic",
    query: " SELECT region FROM commerce_performance ",
    connector: { value: "postgres" },
    selectedDatasets: [{ value: "dataset-id" }],
    requestedLimit: "25",
    requestedTimeoutSeconds: "10",
    explain: true,
  });

  assert.deepEqual(payload, {
    query_scope: "semantic",
    query: "SELECT region FROM commerce_performance",
    explain: true,
    requested_limit: 25,
    requested_timeout_seconds: 10,
  });
});

test("buildSqlQueryPayload creates dataset SQL requests with selected datasets", () => {
  const payload = buildSqlQueryPayload({
    queryScope: "dataset",
    query: "SELECT * FROM sales_orders",
    selectedDatasets: [{ value: "213cd01d-7d6d-5e02-8a9d-6a340754e1ea" }],
  });

  assert.equal(payload.query_scope, "dataset");
  assert.deepEqual(payload.selected_datasets, ["213cd01d-7d6d-5e02-8a9d-6a340754e1ea"]);
});

test("buildSqlQueryPayload creates source SQL requests with connector names and dialect", () => {
  const payload = buildSqlQueryPayload({
    queryScope: "source",
    query: "SELECT 1",
    connector: {
      value: "commerce_warehouse",
      raw: { connector_type: "POSTGRES" },
    },
  });

  assert.equal(payload.query_scope, "source");
  assert.equal(payload.connection_name, "commerce_warehouse");
  assert.equal(payload.query_dialect, "postgres");
});

test("filterSqlConnectorOptions keeps SQL-capable connectors when metadata is available", () => {
  const connectors = filterSqlConnectorOptions([
    { name: "warehouse", connector_type: "POSTGRES" },
    { name: "object_store", connector_type: "S3" },
  ]);

  assert.deepEqual(connectors.map((item) => item.value), ["warehouse"]);
});

test("normalizeQueryRunResult extracts rows, columns, generated SQL, and diagnostics", () => {
  const run = normalizeQueryRunResult(
    {
      status: "succeeded",
      job_id: "job-1",
      query_scope: "dataset",
      query: "SELECT region FROM sales_orders",
      columns: [{ name: "region" }, { name: "net_sales" }],
      rows: [{ region: "Europe", net_sales: 440 }],
      row_count_preview: 1,
      duration_ms: 42,
      job: {
        tasks: [
          {
            diagnostics: {
              generated_sql: "SELECT region FROM sales_orders",
            },
          },
        ],
      },
    },
    { queryScope: "dataset" },
  );

  assert.equal(run.id, "job-1");
  assert.deepEqual(run.columns, ["region", "net_sales"]);
  assert.equal(run.rowCount, 1);
  assert.equal(run.generatedSql, "SELECT region FROM sales_orders");
  assert.equal(run.diagnostics.query_scope, "dataset");
});

test("createQueryArtifactBundle creates markdown-first artifacts for query results", () => {
  const bundle = createQueryArtifactBundle({
    title: "Query result",
    queryScope: "semantic",
    query: "SELECT region FROM commerce_performance",
    generatedSql: "SELECT region FROM sales_orders",
    columns: ["region"],
    rows: [{ region: "Europe" }],
    rowCount: 1,
  });

  assert.match(bundle.answer_markdown, /{{artifact:query_result}}/);
  assert.match(bundle.answer_markdown, /{{artifact:query_sql}}/);
  assert.equal(bundle.artifacts[0].type, "table");
  assert.equal(bundle.artifacts[1].type, "sql");
});

test("buildSqlCompletionItems includes dataset resources and SQL keywords", () => {
  const completions = buildSqlCompletionItems({
    queryScope: "dataset",
    resources: {
      datasets: [
        {
          label: "Sales Orders",
          name: "sales_orders",
          value: "sales_orders",
          raw: {
            columns: [{ name: "order_channel" }],
          },
        },
      ],
    },
  });

  assert.ok(completions.some((item) => item.insertText === "sales_orders"));
  assert.ok(completions.some((item) => item.insertText === "order_channel"));
  assert.ok(completions.some((item) => item.insertText === "SELECT"));
});

test("buildScopeResourceHints describes dataset scope without requiring a selected dataset", () => {
  const hints = buildScopeResourceHints({
    queryScope: "dataset",
    resources: {
      datasets: [{ label: "Sales Orders", value: "sales_orders", raw: { columns: ["order_id"] } }],
    },
  });

  assert.equal(hints.title, "Dataset context");
  assert.match(hints.description, /No dataset selector is required/);
  assert.equal(hints.items[0].insertText, "sales_orders");
  assert.equal(hints.secondaryItems[0].label, "order_id");
});

test("buildScopeResourceTree groups datasets under table nodes with columns", () => {
  const tree = buildScopeResourceTree({
    queryScope: "dataset",
    resources: {
      datasets: [
        {
          label: "Sales Orders",
          name: "sales_orders",
          value: "sales_orders",
          raw: { columns: [{ name: "order_id" }, { name: "net_revenue" }] },
        },
      ],
    },
  });

  assert.equal(tree.title, "Datasets");
  assert.equal(tree.groups[0].kind, "table");
  assert.equal(tree.groups[0].insertText, "sales_orders");
  assert.deepEqual(tree.groups[0].children.map((item) => item.insertText), ["order_id", "net_revenue"]);
});

test("buildScopeResourceTree groups semantic models into datasets, fields, and metrics", () => {
  const tree = buildScopeResourceTree({
    queryScope: "semantic",
    resources: {
      semanticModels: [
        {
          label: "Commerce Performance",
          name: "commerce_performance",
          raw: {
            content_json: {
              datasets: {
                sales_orders: {
                  dimensions: [{ name: "order_channel" }],
                  measures: [{ name: "net_revenue" }],
                  metrics: [{ name: "average_order_value" }],
                },
              },
              metrics: {
                gross_margin_rate: {
                  expression: "SUM(gross_margin) / NULLIF(SUM(net_revenue), 0)",
                },
              },
            },
          },
        },
      ],
    },
  });

  assert.equal(tree.title, "Semantic");
  const model = tree.groups[0];
  const dataset = model.children.find((item) => item.kind === "semantic dataset");
  const dimensions = dataset.children.find((item) => item.label === "Dimensions");
  const measures = dataset.children.find((item) => item.label === "Measures");
  const datasetMetrics = dataset.children.find((item) => item.label === "Metrics");
  const modelMetrics = model.children.find((item) => item.kind === "metric group");

  assert.equal(model.kind, "model");
  assert.equal(dataset.label, "sales_orders");
  assert.equal(dimensions.children[0].insertText, "order_channel");
  assert.equal(measures.children[0].insertText, "net_revenue");
  assert.equal(datasetMetrics.children[0].insertText, "average_order_value");
  assert.equal(modelMetrics.children[0].insertText, "gross_margin_rate");
});

test("buildScopeResourceTree reads semantic detail when content_json is serialized", () => {
  const tree = buildScopeResourceTree({
    queryScope: "semantic",
    resources: {
      semanticModels: [
        {
          label: "Commerce Performance",
          name: "commerce_performance",
          raw: {
            content_json: JSON.stringify({
              datasets: [
                {
                  name: "sales_orders",
                  measures: [{ name: "net_revenue" }],
                },
              ],
            }),
          },
        },
      ],
    },
  });

  const dataset = tree.groups[0].children[0];
  const measures = dataset.children.find((item) => item.label === "Measures");

  assert.equal(dataset.label, "sales_orders");
  assert.equal(measures.children[0].insertText, "net_revenue");
});
