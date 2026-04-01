const MANAGEMENT_MODE_LABELS = {
  config_managed: "config_managed",
  runtime_managed: "runtime_managed",
};

export const CONNECTOR_TYPE_OPTIONS = [
  "POSTGRES",
  "MYSQL",
  "MARIADB",
  "MONGODB",
  "SNOWFLAKE",
  "REDSHIFT",
  "BIGQUERY",
  "SQLSERVER",
  "ORACLE",
  "SQLITE",
  "FAISS",
  "QDRANT",
  "SHOPIFY",
  "STRIPE",
  "HUBSPOT",
  "GITHUB",
  "JIRA",
  "ASANA",
  "GOOGLE_ANALYTICS",
  "SALESFORCE",
  "FILE",
];

export function normalizeConnectorFamily(value) {
  return String(value || "")
    .trim()
    .replaceAll("-", "_")
    .replaceAll(" ", "_")
    .toLowerCase();
}

export function formatConnectorFamilyLabel(value) {
  const normalized = normalizeConnectorFamily(value);
  if (!normalized) {
    return "Unclassified";
  }
  return normalized
    .split("_")
    .filter(Boolean)
    .map((segment) => segment.toUpperCase())
    .join(" ");
}

export function normalizeManagementMode(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "runtime_managed") {
    return "runtime_managed";
  }
  return "config_managed";
}

export function formatManagementModeLabel(value) {
  const normalized = normalizeManagementMode(value);
  return MANAGEMENT_MODE_LABELS[normalized];
}

export function describeManagementMode(value) {
  const normalized = normalizeManagementMode(value);
  if (normalized === "runtime_managed") {
    return "Created from this runtime UI or the runtime API.";
  }
  return "Loaded from runtime config and intentionally read-only in this UI.";
}

export function isRuntimeManagedResource(value) {
  return normalizeManagementMode(value?.management_mode || value) === "runtime_managed";
}

export function isConfigManagedResource(value) {
  return normalizeManagementMode(value?.management_mode || value) === "config_managed";
}

export function stringifyJsonInput(value, fallback = {}) {
  return JSON.stringify(value ?? fallback, null, 2);
}

export function parseJsonInput(value, label, fallback = {}) {
  const text = String(value || "").trim();
  if (!text) {
    return fallback;
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`${label} must be valid JSON.`);
  }
}

export function parseJsonObjectInput(value, label, fallback = {}) {
  const parsed = parseJsonInput(value, label, fallback);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object.`);
  }
  return parsed;
}

export function buildConnectorConnectionTemplate(connectorType) {
  const type = String(connectorType || "").trim().toUpperCase();
  switch (type) {
    case "POSTGRES":
      return {
        host: "localhost",
        port: 5432,
        database: "analytics",
        user: "postgres",
        password: "",
      };
    case "MYSQL":
    case "MARIADB":
      return {
        host: "localhost",
        port: type === "MYSQL" ? 3306 : 3306,
        database: "analytics",
        user: "root",
        password: "",
      };
    case "SQLSERVER":
      return {
        host: "localhost",
        port: 1433,
        database: "analytics",
        user: "sa",
        password: "",
      };
    case "ORACLE":
      return {
        host: "localhost",
        port: 1521,
        service_name: "xe",
        user: "system",
        password: "",
      };
    case "SNOWFLAKE":
      return {
        account: "",
        user: "",
        password: "",
        warehouse: "",
        database: "",
        schema: "PUBLIC",
      };
    case "BIGQUERY":
      return {
        project: "",
        dataset: "",
      };
    case "REDSHIFT":
      return {
        host: "",
        port: 5439,
        database: "dev",
        user: "",
        password: "",
      };
    case "MONGODB":
      return {
        connection_uri: "mongodb://localhost:27017",
        database: "analytics",
      };
    case "QDRANT":
      return {
        host: "localhost",
        port: 6333,
      };
    case "FAISS":
      return {
        index_path: "/var/lib/langbridge/faiss.index",
      };
    case "FILE":
      return {
        path: "/var/lib/langbridge/data.csv",
        format: "csv",
      };
    default:
      return {};
  }
}

export function buildSemanticModelDraft({ name, description, datasets }) {
  const draft = {
    version: "1",
    name: String(name || "").trim() || "runtime_semantic_model",
    datasets: Object.fromEntries(
      (Array.isArray(datasets) ? datasets : [])
        .filter(Boolean)
        .map((datasetName) => [
          datasetName,
          {
            dimensions: [],
            measures: [],
          },
        ]),
    ),
  };
  const normalizedDescription = String(description || "").trim();
  if (normalizedDescription) {
    draft.description = normalizedDescription;
  }
  return stringifyJsonInput(draft);
}
