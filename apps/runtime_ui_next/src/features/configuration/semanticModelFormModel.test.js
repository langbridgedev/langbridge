import test from "node:test";
import assert from "node:assert/strict";

import {
  buildSemanticDatasetFromRuntimeDataset,
  buildSemanticModelCreateFormState,
  buildSemanticModelEditFormState,
  buildSemanticModelSubmitPayload,
  createEmptySemanticRelationship,
  normalizeSemanticDatasetOptions,
  sanitizeSemanticKey,
} from "./semanticModelFormModel.js";

test("buildSemanticModelCreateFormState creates an empty guided model", () => {
  assert.deepEqual(buildSemanticModelCreateFormState(), {
    name: "",
    description: "",
    sqlInstructions: "",
    semanticDatasets: [],
    metrics: [],
    relationships: [],
    unsupported: false,
  });
});

test("normalizeSemanticDatasetOptions prepares runtime dataset selectors", () => {
  const options = normalizeSemanticDatasetOptions([
    {
      id: "dataset-1",
      name: "orders",
      label: "Orders",
      columns: ["order_id", { name: "net_revenue", data_type: "decimal" }],
    },
    { name: "" },
  ]);

  assert.equal(options.length, 1);
  assert.equal(options[0].value, "orders");
  assert.deepEqual(options[0].columns.map((column) => column.name), ["order_id", "net_revenue"]);
});

test("buildSemanticDatasetFromRuntimeDataset infers dimensions and measures", () => {
  const semanticDataset = buildSemanticDatasetFromRuntimeDataset({
    name: "sales_orders",
    label: "Sales orders",
    table_name: "sales_orders",
    columns: [
      { name: "order_id", data_type: "varchar" },
      { name: "order_date", data_type: "date" },
      { name: "net_revenue", data_type: "decimal" },
      { name: "quantity", data_type: "integer" },
    ],
  });

  assert.equal(semanticDataset.semanticKey, "sales_orders");
  assert.deepEqual(semanticDataset.dimensions.map((field) => field.name), ["order_id", "order_date"]);
  assert.deepEqual(semanticDataset.measures.map((field) => field.name), ["net_revenue", "quantity"]);
  assert.equal(semanticDataset.measures[0].aggregation, "sum");
});

test("buildSemanticModelSubmitPayload creates runtime semantic model payloads", () => {
  const form = {
    name: "commerce_performance",
    description: "Commerce model",
    sqlInstructions: "Use net_revenue for revenue questions.",
    semanticDatasets: [
      {
        sourceDatasetName: "sales_orders",
        semanticKey: "orders",
        relationName: "sales_orders",
        description: "Orders",
        dimensions: [
          { name: "order_id", expression: "order_id", type: "string", primaryKey: true },
        ],
        measures: [
          { name: "net_revenue", expression: "net_revenue", type: "number", aggregation: "sum" },
        ],
      },
    ],
    metrics: [
      {
        name: "average_order_value",
        expression: "SUM(orders.net_revenue) / NULLIF(COUNT(orders.order_id), 0)",
        description: "Average value per order",
      },
    ],
    relationships: [],
  };

  assert.deepEqual(buildSemanticModelSubmitPayload({ mode: "create", form }), {
    name: "commerce_performance",
    description: "Commerce model",
    datasets: ["sales_orders"],
    model: {
      version: "1",
      name: "commerce_performance",
      description: "Commerce model",
      sql_instructions: "Use net_revenue for revenue questions.",
      datasets: {
        orders: {
          relation_name: "sales_orders",
          description: "Orders",
          dimensions: [
            {
              name: "order_id",
              expression: "order_id",
              type: "string",
              primary_key: true,
            },
          ],
          measures: [
            {
              name: "net_revenue",
              expression: "net_revenue",
              type: "number",
              aggregation: "sum",
            },
          ],
        },
      },
      metrics: {
        average_order_value: {
          expression: "SUM(orders.net_revenue) / NULLIF(COUNT(orders.order_id), 0)",
          description: "Average value per order",
        },
      },
      relationships: [],
    },
  });
});

test("buildSemanticModelSubmitPayload omits stable name on update", () => {
  const form = {
    name: "commerce_performance",
    description: "Updated",
    metrics: [],
    semanticDatasets: [
      {
        sourceDatasetName: "sales_orders",
        semanticKey: "orders",
        relationName: "sales_orders",
        dimensions: [],
        measures: [{ name: "net_revenue", expression: "", type: "number", aggregation: "sum" }],
      },
    ],
    relationships: [],
  };

  const payload = buildSemanticModelSubmitPayload({ mode: "edit", form });

  assert.equal("name" in payload, false);
  assert.equal(payload.description, "Updated");
  assert.deepEqual(payload.datasets, ["sales_orders"]);
  assert.equal(payload.model.name, "commerce_performance");
});

test("buildSemanticModelSubmitPayload validates duplicate keys and incomplete relationships", () => {
  const baseForm = {
    name: "commerce",
    description: "",
    metrics: [],
    semanticDatasets: [
      { sourceDatasetName: "orders", semanticKey: "orders", relationName: "orders", dimensions: [], measures: [] },
      { sourceDatasetName: "customers", semanticKey: "orders", relationName: "customers", dimensions: [], measures: [] },
    ],
    relationships: [],
  };

  assert.throws(
    () => buildSemanticModelSubmitPayload({ mode: "create", form: baseForm }),
    /Semantic dataset keys must be unique/,
  );

  assert.throws(
    () =>
      buildSemanticModelSubmitPayload({
        mode: "create",
        form: {
          ...baseForm,
          semanticDatasets: [
            { sourceDatasetName: "orders", semanticKey: "orders", relationName: "orders", dimensions: [], measures: [] },
          ],
          metrics: [
            { name: "revenue", expression: "SUM(orders.net_revenue)" },
            { name: "revenue", expression: "SUM(orders.gross_revenue)" },
          ],
        },
      }),
    /Metric names must be unique/,
  );

  const relationship = createEmptySemanticRelationship(["orders", "customers"]);
  assert.throws(
    () =>
      buildSemanticModelSubmitPayload({
        mode: "create",
        form: {
          ...baseForm,
          semanticDatasets: [
            { sourceDatasetName: "orders", semanticKey: "orders", relationName: "orders", dimensions: [], measures: [] },
            { sourceDatasetName: "customers", semanticKey: "customers", relationName: "customers", dimensions: [], measures: [] },
          ],
          relationships: [{ ...relationship, sourceField: "customer_id" }],
        },
      }),
    /Complete or remove unfinished relationships/,
  );
});

test("buildSemanticModelEditFormState reads existing detail and flags graph models as inspect-only", () => {
  const datasets = normalizeSemanticDatasetOptions([{ name: "orders", label: "Orders" }]);
  const state = buildSemanticModelEditFormState({
    rawPayload: {
      name: "commerce",
      description: "Commerce",
      dataset_names: ["orders"],
      content_json: {
        version: "1",
        sql_instructions: "Prefer order_date for time analysis.",
        datasets: {
          orders: {
            relation_name: "orders",
            dimensions: [{ name: "order_id", expression: "order_id", primary_key: true }],
            measures: [{ name: "net_revenue", expression: "net_revenue", aggregation: "sum" }],
          },
        },
        metrics: {
          average_order_value: {
            expression: "SUM(orders.net_revenue) / NULLIF(COUNT(orders.order_id), 0)",
            description: "Average value per order",
          },
        },
      },
    },
  }, datasets);

  assert.equal(state.name, "commerce");
  assert.equal(state.unsupported, false);
  assert.equal(state.sqlInstructions, "Prefer order_date for time analysis.");
  assert.equal(state.semanticDatasets[0].sourceDatasetName, "orders");
  assert.equal(state.semanticDatasets[0].dimensions[0].primaryKey, true);
  assert.equal(state.metrics[0].name, "average_order_value");

  const orchestrationState = buildSemanticModelEditFormState({
    rawPayload: {
      name: "performance",
      content_json: {
        version: "1",
        orchestration: {
          orchestration: "sql_generation",
          steps: [
            { name: "sql_generation", instructions: "Use ILIKE for product lookups." },
            { name: "relative_metrics", instructions: "Use aligned_returns for alpha and beta." },
          ],
        },
        datasets: {
          aligned_returns: {
            relation_name: "aligned_returns",
            dimensions: [{ name: "product_id", expression: "product_id" }],
          },
        },
      },
    },
  });
  assert.equal(
    orchestrationState.sqlInstructions,
    "Use ILIKE for product lookups.\n\nUse aligned_returns for alpha and beta.",
  );

  const graphState = buildSemanticModelEditFormState({
    rawPayload: {
      name: "commerce_graph",
      content_json: { source_models: [{ id: "model-a" }] },
    },
  });
  assert.equal(graphState.unsupported, true);
  assert.throws(
    () => buildSemanticModelSubmitPayload({ mode: "edit", form: graphState }),
    /inspect-only/,
  );
});

test("sanitizeSemanticKey normalizes user-entered semantic keys", () => {
  assert.equal(sanitizeSemanticKey("Sales Orders!"), "sales_orders");
});
