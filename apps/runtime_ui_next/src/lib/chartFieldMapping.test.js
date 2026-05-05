import test from "node:test";
import assert from "node:assert/strict";

import { resolveChartDataKey } from "./chartFieldMapping.js";

test("resolveChartDataKey maps grain-aware semantic time keys to result columns", () => {
  const key = resolveChartDataKey({
    selectedKey: "orders.created_at_month",
    rowKeys: ["orders.created_at_month", "orders.revenue"],
    metadata: [{ column: "orders__created_at_month", source: "orders.created_at", name: "created_at (month)" }],
    fallbackKey: "",
  });

  assert.equal(key, "orders.created_at_month");
});

test("resolveChartDataKey maps grain-aware semantic time keys to raw aliases", () => {
  const key = resolveChartDataKey({
    selectedKey: "orders.created_at_year",
    rowKeys: ["orders__created_at_year", "orders__revenue"],
    metadata: [{ column: "orders__created_at_year", source: "orders.created_at", name: "created_at (year)" }],
    fallbackKey: "",
  });

  assert.equal(key, "orders__created_at_year");
});
