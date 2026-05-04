import { resolveAsync } from "./runtimeService.js";
import { langbridgeList, langbridgeRequest } from "./langbridgeApiClient.js";
import { configurationCopy, configurationResources, configurationSections } from "../mocks/configuration.mock.js";

const resourceEndpoints = {
  connectors: "/api/runtime/v1/connectors",
  datasets: "/api/runtime/v1/datasets",
  "semantic-models": "/api/runtime/v1/semantic-models",
  agents: "/api/runtime/v1/agents",
};

const mutableSections = new Set(["connectors", "datasets", "semantic-models"]);

export function listConfigurationSections() {
  return resolveAsync(configurationSections);
}

export function getConfigurationCopy(section) {
  return resolveAsync(configurationCopy[section] || configurationCopy.connectors);
}

export async function listConfigurationResources(section) {
  const fallback = configurationResources[section] || [];
  const endpoint = resourceEndpoints[section];
  if (!endpoint) {
    return resolveAsync(fallback);
  }

  try {
    return (await langbridgeList(endpoint)).map((item) => normalizeResource(section, item));
  } catch {
    return resolveAsync(fallback);
  }
}

export async function getConfigurationResource(section, resource) {
  const endpoint = getResourceEndpoint(section, resource);
  if (!endpoint) {
    return resolveAsync(resource);
  }

  try {
    return normalizeResource(section, await langbridgeRequest(endpoint));
  } catch {
    return resolveAsync(resource);
  }
}

export async function createConfigurationResource(section, payload) {
  const endpoint = resourceEndpoints[section];
  if (!endpoint || !mutableSections.has(section)) {
    throw new Error(`${formatSectionLabel(section)} resources cannot be created from this workspace.`);
  }
  return normalizeResource(
    section,
    await langbridgeRequest(endpoint, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  );
}

export async function updateConfigurationResource(section, resource, payload) {
  const endpoint = getResourceEndpoint(section, resource);
  if (!endpoint || !mutableSections.has(section)) {
    throw new Error(`${formatSectionLabel(section)} resources cannot be updated from this workspace.`);
  }
  return normalizeResource(
    section,
    await langbridgeRequest(endpoint, {
      method: "PATCH",
      body: JSON.stringify(payload),
    }),
  );
}

export async function deleteConfigurationResource(section, resource) {
  const endpoint = getResourceEndpoint(section, resource);
  if (!endpoint || !mutableSections.has(section)) {
    throw new Error(`${formatSectionLabel(section)} resources cannot be deleted from this workspace.`);
  }
  return langbridgeRequest(endpoint, { method: "DELETE" });
}

export async function runConfigurationResourceAction(section, resource, actionId) {
  const ref = getResourceRef(resource);
  if (!ref) {
    throw new Error("Resource reference is missing.");
  }

  const encodedRef = encodeURIComponent(ref);
  if (section === "connectors" && actionId === "discover_resources") {
    return langbridgeRequest(`/api/runtime/v1/connectors/${encodedRef}/sync/resources`);
  }
  if (section === "connectors" && actionId === "sync_states") {
    return langbridgeRequest(`/api/runtime/v1/connectors/${encodedRef}/sync/states`);
  }
  if (section === "datasets" && actionId === "preview") {
    return langbridgeRequest(`/api/runtime/v1/datasets/${encodedRef}/preview`, {
      method: "POST",
      body: JSON.stringify({ limit: 25 }),
    });
  }
  if (section === "datasets" && actionId === "sync_status") {
    return langbridgeRequest(`/api/runtime/v1/datasets/${encodedRef}/sync`);
  }
  if (section === "datasets" && actionId === "run_sync") {
    return langbridgeRequest(`/api/runtime/v1/datasets/${encodedRef}/sync`, {
      method: "POST",
      body: JSON.stringify({ sync_mode: "INCREMENTAL", force_full_refresh: false }),
    });
  }
  if (actionId === "refresh_detail") {
    return getConfigurationResource(section, resource);
  }

  throw new Error(`Action '${actionId}' is not supported for ${formatSectionLabel(section)}.`);
}

export function getCreateTemplate(section) {
  if (section === "connectors") {
    return {
      name: "new_connector",
      type: "POSTGRES",
      description: "Runtime-managed connector",
      connection: {},
      secrets: {},
      metadata: {},
    };
  }
  if (section === "datasets") {
    return {
      name: "new_dataset",
      label: "New dataset",
      description: "Runtime-managed dataset",
      connector: "",
      source: {
        kind: "table",
        table: "schema.table_name",
      },
      materialization: {
        mode: "live",
      },
    };
  }
  if (section === "semantic-models") {
    return {
      name: "new_semantic_model",
      description: "Runtime-managed semantic model",
      datasets: ["existing_dataset_name"],
      model: {
        datasets: [],
      },
    };
  }
  return {};
}

export function getUpdateTemplate(section, resource) {
  if (section === "connectors") {
    return {
      description: resource.description || resource.rawPayload?.description || "",
      metadata: resource.rawPayload?.metadata || {},
    };
  }
  if (section === "datasets") {
    return {
      label: resource.rawPayload?.label || resource.name || "",
      description: resource.rawPayload?.description || "",
    };
  }
  if (section === "semantic-models") {
    return {
      description: resource.rawPayload?.description || "",
    };
  }
  return {};
}

export function getResourceActions(section, resource) {
  const common = [
    {
      id: "refresh_detail",
      label: "Refresh detail",
      description: "Reload the latest runtime detail for this resource.",
    },
  ];

  if (section === "connectors") {
    return [
      {
        id: "discover_resources",
        label: "Discover resources",
        description: "List resources this connector can sync or expose.",
      },
      {
        id: "sync_states",
        label: "View sync states",
        description: "Inspect runtime sync state tracked for this connector.",
      },
      ...common,
    ];
  }

  if (section === "datasets") {
    const isSynced = String(resource?.rawPayload?.materialization_mode || resource?.rawPayload?.materialization?.mode || "").toLowerCase() === "synced";
    return [
      {
        id: "preview",
        label: "Preview rows",
        description: "Fetch a small governed preview from the dataset.",
      },
      {
        id: "sync_status",
        label: "Sync status",
        description: "Read dataset sync status when sync is configured.",
      },
      {
        id: "run_sync",
        label: "Run sync",
        description: "Start an incremental sync for synced datasets.",
        disabled: !isSynced,
      },
      ...common,
    ];
  }

  return common;
}

export function getSectionCapabilities(section) {
  return {
    canCreate: mutableSections.has(section),
    canUpdate: mutableSections.has(section),
    canDelete: mutableSections.has(section),
    createLabel: section === "semantic-models" ? "Add semantic model" : `Add ${formatSectionLabel(section, { singular: true })}`,
  };
}

function normalizeResource(section, item) {
  if (section === "connectors") {
    return normalizeConnector(item);
  }
  if (section === "datasets") {
    return normalizeDataset(item);
  }
  if (section === "semantic-models") {
    return normalizeSemanticModel(item);
  }
  if (section === "agents") {
    return normalizeAgent(item);
  }
  return normalizeGenericResource(item);
}

function normalizeConnector(item) {
  const name = item.name || item.id || "connector";
  const supportedResources = toArray(item.supported_resources);
  return {
    id: stableId(item, name),
    ref: name,
    section: "connectors",
    rawPayload: item,
    name,
    description: item.description || "",
    subtitle: item.description || labelFromParts(item.connector_family, item.connector_type, "Connector"),
    status: item.supports_sync ? "Sync capable" : "Available",
    management: normalizeManagementMode(item),
    owner: "Runtime API",
    lastUpdated: "Live",
    runtimeState: compactRows([
      ["Family", item.connector_family],
      ["Type", item.connector_type],
      ["Supports sync", yesNo(item.supports_sync)],
      ["Default sync", item.default_sync_strategy],
      ["Resources", formatCount(supportedResources.length, "resource")],
    ]),
    configDefinition: compactRows([
      ["Name", name],
      ["Description", item.description],
      ["Managed", yesNo(item.managed)],
      ["Management mode", item.management_mode],
    ]),
    relationships: supportedResources.length > 0 ? supportedResources : ["No resources advertised"],
    details: {
      Capabilities: item.capabilities || {},
      Connection: item.connection || {},
      Metadata: item.metadata || {},
      "Supported resources": supportedResources,
      "Connector id": item.id || "n/a",
    },
  };
}

function normalizeDataset(item) {
  const name = item.name || item.label || item.id || "dataset";
  const semanticModels = toArray(item.semantic_models || item.semantic_model);
  return {
    id: stableId(item, name),
    ref: item.id || name,
    section: "datasets",
    rawPayload: item,
    name,
    description: item.description || "",
    subtitle: item.description || item.label || "Governed runtime dataset",
    status: item.sync_status || item.status || "Ready",
    management: normalizeManagementMode(item),
    owner: "Runtime API",
    lastUpdated: formatDate(item.last_sync_at) || "Live",
    runtimeState: compactRows([
      ["Status", item.status],
      ["Sync status", item.sync_status],
      ["Materialization", item.materialization_mode || item.materialization?.mode],
      ["Last sync", formatDate(item.last_sync_at)],
    ]),
    configDefinition: compactRows([
      ["Connector", item.connector],
      ["Semantic model", item.semantic_model],
      ["Semantic models", semanticModels.join(", ")],
      ["Source", describeSource(item.source)],
      ["Schema hint", formatObject(item.schema_hint)],
    ]),
    relationships: semanticModels.length > 0 ? semanticModels : compactList([item.connector, item.semantic_model]),
    details: {
      Materialization: item.materialization || {},
      Source: item.source || {},
      "Schema hint": item.schema_hint || {},
      Policy: item.policy || {},
      Columns: item.columns || [],
      "Dataset id": item.id || "n/a",
    },
  };
}

function normalizeSemanticModel(item) {
  const name = item.name || item.id || "semantic_model";
  const datasetNames = toArray(item.dataset_names);
  return {
    id: stableId(item, name),
    ref: item.id || name,
    section: "semantic-models",
    rawPayload: item,
    name,
    description: item.description || "",
    subtitle: item.description || "Business layer for governed analysis",
    status: item.default ? "Default" : "Ready",
    management: normalizeManagementMode(item),
    owner: "Runtime API",
    lastUpdated: "Live",
    runtimeState: compactRows([
      ["Datasets", item.dataset_count],
      ["Measures", item.measure_count],
      ["Dimensions", item.dimension_count],
      ["Default", yesNo(item.default)],
    ]),
    configDefinition: compactRows([
      ["Dataset names", datasetNames.join(", ")],
      ["Management mode", item.management_mode],
      ["Managed", yesNo(item.managed)],
    ]),
    relationships: datasetNames.length > 0 ? datasetNames : ["No datasets listed"],
    details: {
      "Semantic model id": item.id || "n/a",
      Description: item.description || "n/a",
      "Content JSON": item.content_json || item.model || {},
      "Content YAML": item.content_yaml || "",
      Datasets: item.datasets || item.dataset_names || [],
    },
  };
}

function normalizeAgent(item) {
  const name = item.name || item.id || "agent";
  const tools = toArray(item.tools);
  const toolLabels = tools.map(formatRelationshipValue);
  return {
    id: stableId(item, name),
    ref: item.id || name,
    section: "agents",
    rawPayload: item,
    name,
    description: item.description || "",
    subtitle: item.description || "Runtime analyst agent",
    status: item.default ? "Default" : "Ready",
    management: "config_managed",
    owner: "Runtime API",
    lastUpdated: "Live",
    runtimeState: compactRows([
      ["LLM connection", item.llm_connection],
      ["Tools", item.tool_count ?? tools.length],
      ["Default", yesNo(item.default)],
    ]),
    configDefinition: compactRows([
      ["Name", name],
      ["Description", item.description],
      ["Tools", toolLabels.join(", ")],
    ]),
    relationships: toolLabels.length > 0 ? toolLabels : ["No tools listed"],
    details: {
      "Agent id": item.id || "n/a",
      Tools: item.tools || tools,
      Definition: item.definition || {},
      "Semantic models": item.semantic_models || [],
      Datasets: item.datasets || [],
      Instructions: item.instructions || "n/a",
    },
  };
}

function normalizeGenericResource(item) {
  const name = item.name || item.label || item.id || "resource";
  return {
    id: stableId(item, name),
    ref: getResourceRef(item) || name,
    rawPayload: item,
    name,
    description: item.description || "",
    subtitle: item.description || "Runtime resource",
    status: item.status || "Available",
    management: normalizeManagementMode(item),
    owner: "Runtime API",
    lastUpdated: "Live",
    runtimeState: [["Payload", "Available"]],
    configDefinition: [["Name", name]],
    relationships: [],
    details: { Payload: formatObject(item) },
  };
}

function stableId(item, fallback) {
  return String(item.id || item.name || item.key || fallback).replace(/\s+/g, "-").toLowerCase();
}

function getResourceEndpoint(section, resource) {
  const base = resourceEndpoints[section];
  const ref = getResourceRef(resource);
  return base && ref ? `${base}/${encodeURIComponent(ref)}` : "";
}

function getResourceRef(resource) {
  return String(resource?.ref || resource?.id || resource?.name || resource?.rawPayload?.id || resource?.rawPayload?.name || "").trim();
}

function formatSectionLabel(section, options = {}) {
  const labels = {
    connectors: options.singular ? "connector" : "connectors",
    datasets: options.singular ? "dataset" : "datasets",
    "semantic-models": options.singular ? "semantic model" : "semantic models",
    agents: options.singular ? "agent" : "agents",
  };
  return labels[section] || section || "resource";
}

function normalizeManagementMode(item) {
  return String(item.management_mode || (item.managed ? "runtime_managed" : "config_managed"));
}

function labelFromParts(...parts) {
  const value = parts.filter(Boolean).join(" ");
  return value || "Runtime resource";
}

function compactRows(rows) {
  return rows
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .map(([label, value]) => [label, formatValue(value)]);
}

function compactList(items) {
  return items.filter((item) => item !== undefined && item !== null && item !== "").map(String);
}

function toArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).map(formatValue);
  }
  if (value === undefined || value === null || value === "") {
    return [];
  }
  return [formatValue(value)];
}

function formatCount(count, singular) {
  const numeric = Number(count || 0);
  return `${numeric.toLocaleString()} ${numeric === 1 ? singular : `${singular}s`}`;
}

function formatDate(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString();
}

function formatObject(value) {
  if (value === undefined || value === null || value === "") {
    return "n/a";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function formatValue(value) {
  if (typeof value === "boolean") {
    return yesNo(value);
  }
  if (typeof value === "number") {
    return value.toLocaleString();
  }
  return value;
}

function describeSource(source) {
  if (!source || typeof source !== "object") {
    return "";
  }
  return source.table || source.path || source.sql || source.resource || formatObject(source);
}

function yesNo(value) {
  if (value === undefined || value === null || value === "") {
    return "";
  }
  return value ? "Yes" : "No";
}

function formatRelationshipValue(value) {
  if (value === undefined || value === null || value === "") {
    return "n/a";
  }
  if (typeof value === "object") {
    return String(value.name || value.label || value.id || value.tool_name || `${Object.keys(value).length} fields`);
  }
  return String(value);
}
