import {
  cancelRuntimeJob,
  fetchConnectors,
  fetchDatasets,
  fetchSemanticModel,
  fetchSemanticModels,
  runSqlQuery,
} from "../lib/runtimeApi.js";
import { getItems } from "./langbridgeApiClient.js";
import {
  QUERY_SCOPE_OPTIONS,
  QUERY_WORKSPACE_STORAGE_KEYS,
  buildSqlQueryPayload,
  createQueryArtifactBundle,
  createQueryRecent,
  filterSqlConnectorOptions,
  normalizeQueryRunResult,
  normalizeResourceOptions,
} from "../features/query-workspace/queryWorkspaceModel.js";

const MAX_LOCAL_RECENTS = 12;
const MAX_LOCAL_SAVED = 24;

export function getQueryScopes() {
  return Promise.resolve(QUERY_SCOPE_OPTIONS);
}

export async function listQueryResources() {
  const [connectorsPayload, datasetsPayload, semanticModelsPayload] = await Promise.all([
    fetchConnectors(),
    fetchDatasets(),
    fetchSemanticModels(),
  ]);
  const semanticModelSummaries = getItems(semanticModelsPayload);
  const semanticModelDetails = await hydrateSemanticModelDetails(semanticModelSummaries);

  return {
    connectors: filterSqlConnectorOptions(getItems(connectorsPayload)),
    datasets: normalizeResourceOptions(getItems(datasetsPayload), "Dataset"),
    semanticModels: normalizeResourceOptions(semanticModelDetails, "Semantic model"),
  };
}

export async function getSourceConnectors() {
  return filterSqlConnectorOptions(getItems(await fetchConnectors()));
}

export async function executeQueryRun(request, options = {}) {
  const payload = buildSqlQueryPayload(request);
  const response = await runSqlQuery(payload, {
    waitForCompletion: true,
    ...options,
  });
  const run = normalizeQueryRunResult(response, request);
  recordQueryRecent(run);
  return {
    ...run,
    artifactBundle: createQueryArtifactBundle(run),
  };
}

export function cancelQueryRun(jobId, reason = "Cancelled from Query Workspace.") {
  return cancelRuntimeJob(jobId, { reason });
}

async function hydrateSemanticModelDetails(models) {
  return Promise.all(
    (Array.isArray(models) ? models : []).map(async (model) => {
      const modelRef = String(model?.id || model?.name || "").trim();
      if (!modelRef) {
        return model;
      }
      try {
        const detail = await fetchSemanticModel(modelRef);
        return {
          ...model,
          ...detail,
        };
      } catch {
        return model;
      }
    }),
  );
}

export function listQueryRecents() {
  return Promise.resolve(readStoredList(QUERY_WORKSPACE_STORAGE_KEYS.recents));
}

export function listQueryProjects() {
  return Promise.resolve([
    {
      id: "local-saved-queries",
      name: "Saved queries",
      meta: `${readStoredList(QUERY_WORKSPACE_STORAGE_KEYS.saved).length} saved`,
      path: "/query-workspace",
    },
  ]);
}

export function listSavedQueries() {
  return Promise.resolve(readStoredList(QUERY_WORKSPACE_STORAGE_KEYS.saved));
}

export function saveQueryDraft({ title, query, queryScope, connector }) {
  const normalizedQuery = String(query || "").trim();
  if (!normalizedQuery) {
    throw new Error("Enter a SQL query before saving.");
  }
  const saved = readStoredList(QUERY_WORKSPACE_STORAGE_KEYS.saved);
  const now = new Date().toISOString();
  const record = {
    id: `saved-query-${now}`,
    title: String(title || firstQueryLine(normalizedQuery) || "Saved query").slice(0, 90),
    query: normalizedQuery,
    queryScope,
    connector,
    createdAt: now,
    path: "/query-workspace",
  };
  writeStoredList(QUERY_WORKSPACE_STORAGE_KEYS.saved, [record, ...saved].slice(0, MAX_LOCAL_SAVED));
  return record;
}

export function recordQueryRecent(run) {
  const recent = createQueryRecent(run);
  const recents = readStoredList(QUERY_WORKSPACE_STORAGE_KEYS.recents)
    .filter((item) => String(item.id || "") !== recent.id && String(item.query || "") !== recent.query);
  writeStoredList(QUERY_WORKSPACE_STORAGE_KEYS.recents, [recent, ...recents].slice(0, MAX_LOCAL_RECENTS));
  return recent;
}

function firstQueryLine(query) {
  return String(query || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find(Boolean);
}

function readStoredList(key) {
  if (typeof window === "undefined" || !window.localStorage) {
    return [];
  }
  try {
    const payload = JSON.parse(window.localStorage.getItem(key) || "[]");
    return Array.isArray(payload) ? payload : [];
  } catch {
    return [];
  }
}

function writeStoredList(key, items) {
  if (typeof window === "undefined" || !window.localStorage) {
    return;
  }
  window.localStorage.setItem(key, JSON.stringify(Array.isArray(items) ? items : []));
}
