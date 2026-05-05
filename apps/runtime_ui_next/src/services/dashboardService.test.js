import test from "node:test";
import assert from "node:assert/strict";

import {
  DASHBOARD_BUILDER_STORAGE_KEY,
  LEGACY_DASHBOARD_BUILDER_STORAGE_KEY,
} from "../features/dashboards/dashboardModel.js";
import {
  readDashboardState,
  writeDashboardState,
} from "./dashboardService.js";

function createStorage(seed = {}) {
  const values = new Map(Object.entries(seed));
  return {
    getItem(key) {
      return values.has(key) ? values.get(key) : null;
    },
    setItem(key, value) {
      values.set(key, String(value));
    },
    removeItem(key) {
      values.delete(key);
    },
  };
}

test("writeDashboardState persists normalized dashboard state", () => {
  const storage = createStorage();

  const normalized = writeDashboardState({
    boards: [{ id: "board-1", name: "Trading", widgets: [{ id: "widget-1" }] }],
    activeBoardId: "board-1",
  }, storage);

  const stored = JSON.parse(storage.getItem(DASHBOARD_BUILDER_STORAGE_KEY));
  assert.equal(normalized.activeBoardId, "board-1");
  assert.equal(stored.boards[0].name, "Trading");
  assert.equal(stored.boards[0].widgets[0].running, undefined);
});

test("readDashboardState migrates legacy dashboard storage", () => {
  const storage = createStorage({
    [LEGACY_DASHBOARD_BUILDER_STORAGE_KEY]: JSON.stringify({
      activeBoardId: "legacy-board",
      boards: [{ id: "legacy-board", name: "Legacy", widgets: [{ id: "widget-1" }] }],
    }),
  });

  const state = readDashboardState(storage);

  assert.equal(state.activeBoardId, "legacy-board");
  assert.equal(state.boards[0].name, "Legacy");
  assert.equal(storage.getItem(LEGACY_DASHBOARD_BUILDER_STORAGE_KEY), null);
  assert.ok(storage.getItem(DASHBOARD_BUILDER_STORAGE_KEY));
});
