import {
  fetchSemanticModel,
  fetchSemanticModels,
  querySemantic,
} from "../lib/runtimeApi.js";
import { getItems } from "./langbridgeApiClient.js";
import {
  DASHBOARD_BUILDER_STORAGE_KEY,
  LEGACY_DASHBOARD_BUILDER_STORAGE_KEY,
  buildDashboardWidgetQueryPayload,
  dashboardProjectsFromState,
  dashboardRecentsFromState,
  normalizeDashboardQueryResult,
  normalizeDashboardState,
  serializeDashboardState,
} from "../features/dashboards/dashboardModel.js";

export async function listDashboardSemanticModels() {
  return getItems(await fetchSemanticModels());
}

export function getDashboardSemanticModel(modelRef) {
  return fetchSemanticModel(modelRef);
}

export async function runDashboardWidget(board, widget) {
  const payload = buildDashboardWidgetQueryPayload(board, widget);
  const response = await querySemantic(payload);
  return normalizeDashboardQueryResult(response);
}

export function listDashboardRecents() {
  return Promise.resolve(dashboardRecentsFromState(readDashboardState()));
}

export function listDashboardProjects() {
  return Promise.resolve(dashboardProjectsFromState(readDashboardState()));
}

function browserStorage() {
  return typeof window !== "undefined" && window.localStorage ? window.localStorage : null;
}

function readStorageJson(storage, key) {
  if (!storage) {
    return null;
  }
  const raw = storage.getItem(key);
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function readDashboardState(storage = browserStorage()) {
  if (!storage) {
    return normalizeDashboardState(null);
  }
  const stored = readStorageJson(storage, DASHBOARD_BUILDER_STORAGE_KEY);
  if (stored) {
    return normalizeDashboardState(stored);
  }
  const legacy = readStorageJson(storage, LEGACY_DASHBOARD_BUILDER_STORAGE_KEY);
  if (legacy) {
    const migrated = normalizeDashboardState(legacy);
    writeDashboardState(migrated, storage);
    storage.removeItem(LEGACY_DASHBOARD_BUILDER_STORAGE_KEY);
    return migrated;
  }
  return normalizeDashboardState(null);
}

export function writeDashboardState(state, storage = browserStorage()) {
  const normalized = serializeDashboardState(state);
  if (storage) {
    storage.setItem(DASHBOARD_BUILDER_STORAGE_KEY, JSON.stringify(normalized));
  }
  return normalized;
}
