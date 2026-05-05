import test from "node:test";
import assert from "node:assert/strict";

import {
  DASHBOARD_SCHEMA_VERSION,
  buildDashboardWidgetQueryPayload,
  canRunDashboardWidget,
  createDashboardBoard,
  createDashboardWidget,
  duplicateDashboardBoard,
  extractDashboardSemanticResources,
  normalizeDashboardImportPayload,
  normalizeDashboardQueryResult,
  normalizeDashboardState,
  removeDashboardBoard,
  reorderDashboardWidgets,
  serializeDashboardExport,
  serializeDashboardState,
} from "./dashboardModel.js";

test("extractDashboardSemanticResources exposes datasets, measures, and model metrics", () => {
  const resources = extractDashboardSemanticResources({
    content_json: {
      datasets: {
        sales_orders: {
          dimensions: [{ name: "order_channel", type: "string" }],
          measures: [{ name: "net_revenue", aggregation: "sum" }],
        },
      },
      metrics: {
        gross_margin_rate: { expression: "SUM(gross_margin) / SUM(net_revenue)" },
      },
    },
  });

  assert.equal(resources.datasets[0].name, "sales_orders");
  assert.equal(resources.dimensions[0].value, "sales_orders.order_channel");
  assert.equal(resources.measures[0].value, "sales_orders.net_revenue");
  assert.equal(resources.metrics[0].value, "gross_margin_rate");
});

test("extractDashboardSemanticResources handles serialized content_json", () => {
  const resources = extractDashboardSemanticResources({
    content_json: JSON.stringify({
      datasets: [{ name: "orders", measures: [{ name: "order_count" }] }],
    }),
  });

  assert.equal(resources.datasets[0].name, "orders");
  assert.equal(resources.measures[0].value, "orders.order_count");
});

test("buildDashboardWidgetQueryPayload creates semantic query payloads", () => {
  const board = createDashboardBoard({
    selectedModel: "commerce_performance",
    globalFilters: [{ member: "sales_orders.country", operator: "equals", values: "UK" }],
  });
  const widget = createDashboardWidget({
    dimensions: ["sales_orders.order_channel"],
    measures: ["sales_orders.net_revenue", "gross_margin_rate"],
    filters: [{ member: "sales_orders.status", operator: "in", values: "paid,shipped" }],
    orderBys: [{ member: "sales_orders.net_revenue", direction: "desc" }],
    timeDimension: "sales_orders.order_date",
    timeGrain: "month",
    timeRangePreset: "last_30_days",
    limit: "25",
  });

  const payload = buildDashboardWidgetQueryPayload(board, widget);

  assert.deepEqual(payload.semantic_models, ["commerce_performance"]);
  assert.deepEqual(payload.measures, ["sales_orders.net_revenue", "gross_margin_rate"]);
  assert.deepEqual(payload.dimensions, ["sales_orders.order_channel"]);
  assert.deepEqual(payload.filters[0], {
    member: "sales_orders.country",
    operator: "equals",
    values: ["UK"],
  });
  assert.deepEqual(payload.time_dimensions[0], {
    dimension: "sales_orders.order_date",
    granularity: "month",
    dateRange: "last_30_days",
  });
  assert.equal(payload.limit, 25);
});

test("createDashboardWidget keeps chart bindings aligned with selected fields", () => {
  const widget = createDashboardWidget({
    dimensions: ["orders.channel"],
    measures: ["orders.revenue"],
    chartX: "orders.removed_dimension",
    chartY: "orders.removed_measure",
  });

  assert.equal(widget.chartX, "orders.channel");
  assert.equal(widget.chartY, "orders.revenue");
});

test("createDashboardWidget allows time dimension as chart x binding", () => {
  const widget = createDashboardWidget({
    measures: ["orders.revenue"],
    timeDimension: "orders.created_at",
    chartX: "orders.created_at",
  });

  assert.equal(widget.chartX, "orders.created_at");
});

test("buildDashboardWidgetQueryPayload groups widget filters with OR logic", () => {
  const board = createDashboardBoard({
    selectedModel: "commerce_performance",
    globalFilters: [{ member: "sales_orders.country", operator: "equals", values: "UK" }],
  });
  const widget = createDashboardWidget({
    measures: ["sales_orders.net_revenue"],
    filterLogic: "or",
    filters: [
      { member: "sales_orders.status", operator: "equals", values: "paid" },
      { member: "sales_orders.status", operator: "equals", values: "shipped" },
    ],
  });

  const payload = buildDashboardWidgetQueryPayload(board, widget);

  assert.deepEqual(payload.filters, [
    { member: "sales_orders.country", operator: "equals", values: ["UK"] },
    {
      or: [
        { member: "sales_orders.status", operator: "equals", values: ["paid"] },
        { member: "sales_orders.status", operator: "equals", values: ["shipped"] },
      ],
    },
  ]);
});

test("buildDashboardWidgetQueryPayload resolves custom time ranges", () => {
  const board = createDashboardBoard({ selectedModel: "commerce_performance" });
  const widget = createDashboardWidget({
    measures: ["sales_orders.net_revenue"],
    timeDimension: "sales_orders.order_date",
    timeRangePreset: "custom_between",
    timeRangeFrom: "2025-07-01",
    timeRangeTo: "2025-09-30",
  });

  const payload = buildDashboardWidgetQueryPayload(board, widget);

  assert.deepEqual(payload.time_dimensions[0].dateRange, ["2025-07-01", "2025-09-30"]);
});

test("serializeDashboardState strips transient widget execution state", () => {
  const state = normalizeDashboardState({
    boards: [
      {
        id: "board-1",
        widgets: [
          {
            id: "widget-1",
            title: "Revenue",
            running: true,
            error: "failed",
            result: { rows: [{ value: 1 }] },
          },
        ],
      },
    ],
  });

  const serialized = serializeDashboardState(state);

  assert.equal(serialized.boards[0].widgets[0].running, undefined);
  assert.equal(serialized.boards[0].widgets[0].error, undefined);
  assert.equal(serialized.boards[0].widgets[0].result, undefined);
  assert.equal(serialized.schemaVersion, DASHBOARD_SCHEMA_VERSION);
});

test("normalizeDashboardState accepts exported dashboard payloads", () => {
  const state = normalizeDashboardState({
    dashboard: {
      id: "board-export",
      name: "Imported board",
      widgets: [{ id: "widget-export", measures: ["orders.revenue"] }],
    },
  });

  assert.equal(state.schemaVersion, DASHBOARD_SCHEMA_VERSION);
  assert.equal(state.activeBoardId, "board-export");
  assert.equal(state.boards[0].name, "Imported board");
  assert.equal(state.boards[0].widgets[0].measures[0], "orders.revenue");
});

test("normalizeDashboardImportPayload returns importable boards", () => {
  const imported = normalizeDashboardImportPayload({
    schemaVersion: DASHBOARD_SCHEMA_VERSION,
    dashboard: {
      id: "board-1",
      name: "Portable board",
      widgets: [{ id: "widget-1", title: "Revenue" }],
    },
  });

  assert.equal(imported.activeBoardId, "board-1");
  assert.equal(imported.boards.length, 1);
  assert.equal(imported.boards[0].name, "Portable board");
});

test("normalizeDashboardImportPayload rejects non-dashboard JSON", () => {
  assert.throws(
    () => normalizeDashboardImportPayload({ hello: "world" }),
    /dashboard definition/,
  );
});

test("serializeDashboardExport removes execution payloads from portable JSON", () => {
  const board = createDashboardBoard({
    id: "board-1",
    lastRefreshedAt: "2026-01-01T00:00:00Z",
    widgets: [
      {
        id: "widget-1",
        lastRunAt: "2026-01-01T00:00:00Z",
        result: { rows: [{ value: 1 }] },
        running: true,
        error: "failed",
      },
    ],
  });

  const exported = serializeDashboardExport(board);

  assert.equal(exported.schemaVersion, DASHBOARD_SCHEMA_VERSION);
  assert.equal(exported.dashboard.lastRefreshedAt, undefined);
  assert.equal(exported.dashboard.widgets[0].lastRunAt, undefined);
  assert.equal(exported.dashboard.widgets[0].result, undefined);
  assert.equal(exported.dashboard.widgets[0].running, undefined);
  assert.equal(exported.dashboard.widgets[0].error, undefined);
});

test("duplicateDashboardBoard rekeys board and widgets", () => {
  const board = createDashboardBoard({
    id: "board-1",
    name: "Trading",
    widgets: [{ id: "widget-1", title: "Revenue", result: { rows: [] }, lastRunAt: "2026-01-01T00:00:00Z" }],
  });

  const duplicate = duplicateDashboardBoard(board);

  assert.equal(duplicate.name, "Trading copy");
  assert.notEqual(duplicate.id, board.id);
  assert.notEqual(duplicate.widgets[0].id, board.widgets[0].id);
  assert.equal(duplicate.widgets[0].result, null);
  assert.equal(duplicate.widgets[0].lastRunAt, null);
});

test("removeDashboardBoard keeps one dashboard available", () => {
  const state = normalizeDashboardState({
    activeBoardId: "board-1",
    boards: [{ id: "board-1", widgets: [{ id: "widget-1" }] }],
  });

  const next = removeDashboardBoard(state, "board-1");

  assert.equal(next.boards.length, 1);
  assert.notEqual(next.boards[0].id, "board-1");
});

test("reorderDashboardWidgets moves widgets by id", () => {
  const widgets = [
    createDashboardWidget({ id: "a" }),
    createDashboardWidget({ id: "b" }),
    createDashboardWidget({ id: "c" }),
  ];

  assert.deepEqual(reorderDashboardWidgets(widgets, "c", "a").map((item) => item.id), ["c", "a", "b"]);
});

test("normalizeDashboardQueryResult converts semantic response data to table result", () => {
  const result = normalizeDashboardQueryResult({
    data: [{ channel: "Paid Social", revenue: 120 }],
    generated_sql: "SELECT 1",
  });

  assert.deepEqual(result.columns, ["channel", "revenue"]);
  assert.equal(result.rowCount, 1);
  assert.equal(result.generated_sql, "SELECT 1");
});

test("normalizeDashboardQueryResult preserves nested result formatting", () => {
  const result = normalizeDashboardQueryResult({
    result: {
      columns: ["channel", "revenue"],
      rows: [["Paid Social", 120]],
      rowcount: 10,
      source_sql: "SELECT channel, revenue FROM result",
      formatting: {
        columns: {
          revenue: { kind: "currency", symbol: "$" },
        },
      },
    },
  });

  assert.equal(result.rowCount, 10);
  assert.equal(result.generated_sql, "SELECT channel, revenue FROM result");
  assert.deepEqual(result.formatting.columns.revenue, { kind: "currency", symbol: "$" });
});

test("canRunDashboardWidget requires a selected model and measures", () => {
  assert.equal(
    canRunDashboardWidget(createDashboardWidget({ measures: ["orders.revenue"] }), { selectedModel: "commerce" }),
    true,
  );
  assert.equal(
    canRunDashboardWidget(createDashboardWidget({ type: "note", measures: ["orders.revenue"] }), { selectedModel: "commerce" }),
    false,
  );
});
