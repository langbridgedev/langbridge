import {
  formatConnectorFamilyLabel,
  normalizeConnectorFamily,
} from "./connectorFormModel.js";

export const DATASET_SOURCE_MODES = [
  { value: "table", label: "Table" },
  { value: "sql", label: "SQL" },
  { value: "file", label: "File" },
  { value: "resource", label: "Synced resource" },
];

const SUPPORTED_SOURCE_MODES = new Set(DATASET_SOURCE_MODES.map((item) => item.value));

export function normalizeDatasetConnectors(items) {
  return (Array.isArray(items) ? items : [])
    .map((item) => {
      const name = String(item?.name || item?.id || "").trim();
      if (!name) {
        return null;
      }
      return {
        ...item,
        value: name,
        label: item?.label || name,
        family: normalizeConnectorFamily(item?.connector_family || item?.family),
        capabilities: normalizeConnectorCapabilities(item?.capabilities || item?.capabilities_schema),
      };
    })
    .filter(Boolean)
    .sort((left, right) => `${left.family}-${left.label}`.localeCompare(`${right.family}-${right.label}`));
}

export function datasetConnectorFamilyOptions(connectors) {
  const seen = new Set();
  return normalizeDatasetConnectors(connectors)
    .map((item) => item.family)
    .filter((family) => {
      if (!family || seen.has(family)) {
        return false;
      }
      seen.add(family);
      return true;
    })
    .map((family) => ({ value: family, label: formatConnectorFamilyLabel(family) }));
}

export function buildDatasetCreateFormState() {
  return {
    name: "",
    label: "",
    description: "",
    connectorFamily: "",
    connector: "",
    materializationMode: "live",
    sourceMode: "table",
    table: "",
    sql: "",
    path: "",
    fileLocationField: "path",
    format: "csv",
    header: true,
    delimiter: ",",
    quote: "\"",
    resource: "",
    tags: "",
  };
}

export function buildDatasetEditFormState(resource) {
  const raw = resource?.rawPayload || resource || {};
  const source = objectValue(raw.source);
  const materializationMode = normalizeMaterializationMode(raw.materialization?.mode || raw.materialization_mode);
  const sourceMode = detectDatasetSourceMode(source, raw);
  const isFileStorageUri = Boolean(source.storage_uri || (!source.path && raw.storage_uri));

  return {
    name: String(raw.name || resource?.name || ""),
    label: String(raw.label || raw.name || resource?.name || ""),
    description: String(raw.description || ""),
    connectorFamily: normalizeConnectorFamily(raw.connector_family),
    connector: String(raw.connector || ""),
    materializationMode,
    sourceMode: materializationMode === "synced" ? "resource" : sourceMode,
    table: source.table || raw.table_name || "",
    sql: source.sql || raw.sql_text || "",
    path: source.path || source.storage_uri || raw.storage_uri || "",
    fileLocationField: isFileStorageUri ? "storage_uri" : "path",
    format: source.format || source.file_format || raw.file_config?.format || raw.storage_kind || "csv",
    header: Boolean(source.header ?? raw.file_config?.header ?? true),
    delimiter: source.delimiter || raw.file_config?.delimiter || ",",
    quote: source.quote || raw.file_config?.quote || "\"",
    resource: source.resource || source.request?.path || "",
    tags: Array.isArray(raw.tags) ? raw.tags.join(", ") : "",
  };
}

export function buildDatasetSubmitPayload({ mode, form, originalResource = null, connector = null }) {
  const isCreate = mode === "create";
  const payload = {};
  const effectiveSourceMode = resolveDatasetSourceMode(form);
  const supportedSourceMode = SUPPORTED_SOURCE_MODES.has(effectiveSourceMode);

  if (isCreate) {
    payload.name = requiredText(form?.name, "Dataset name is required.");
    payload.connector = normalizedConnector(form?.connector, effectiveSourceMode, form?.materializationMode);
    payload.source = buildDatasetSourcePayload(form, effectiveSourceMode);
    payload.materialization = buildDatasetMaterializationPayload(form, originalResource);
  } else {
    if (!supportedSourceMode) {
      payload.label = nullableText(form?.label);
      payload.description = nullableText(form?.description);
      payload.tags = splitDatasetTags(form?.tags);
      return compactPayload(payload);
    }

    const nextSource = buildDatasetSourcePayload(form, effectiveSourceMode);
    const nextMaterialization = buildDatasetMaterializationPayload(form, originalResource);
    const initialSource = buildComparableOriginalSource(originalResource);
    const initialMaterialization = buildComparableOriginalMaterialization(originalResource);

    if (!samePayload(nextSource, initialSource)) {
      payload.source = nextSource;
    }
    if (!samePayload(nextMaterialization, initialMaterialization)) {
      payload.materialization = nextMaterialization;
    }
  }

  const label = nullableText(form?.label);
  const description = nullableText(form?.description);
  if (isCreate) {
    if (label) {
      payload.label = label;
    }
    if (description) {
      payload.description = description;
    }
  } else {
    payload.label = label;
    payload.description = description;
  }

  const tags = splitDatasetTags(form?.tags);
  if (isCreate) {
    if (tags.length > 0) {
      payload.tags = tags;
    }
  } else {
    payload.tags = tags;
  }

  validateDatasetConnectorCompatibility({
    connector,
    connectorName: payload.connector ?? form?.connector,
    materializationMode: normalizeMaterializationMode(form?.materializationMode),
    sourceMode: effectiveSourceMode,
  });
  return compactPayload(payload);
}

export function resolveDatasetSourceMode(form) {
  const materializationMode = normalizeMaterializationMode(form?.materializationMode);
  if (materializationMode === "synced") {
    return "resource";
  }
  return String(form?.sourceMode || "table").trim().toLowerCase();
}

export function detectDatasetSourceMode(source, raw = {}) {
  const normalizedSource = objectValue(source);
  const kind = String(normalizedSource.kind || "").trim().toLowerCase();
  if (SUPPORTED_SOURCE_MODES.has(kind)) {
    return kind;
  }
  if (kind) {
    return "unsupported";
  }
  if (normalizedSource.request) {
    return "unsupported";
  }
  if (normalizedSource.resource) {
    return "resource";
  }
  if (normalizedSource.sql || raw.sql_text) {
    return "sql";
  }
  if (normalizedSource.path || normalizedSource.storage_uri || raw.storage_uri) {
    return "file";
  }
  return "table";
}

export function splitDatasetTags(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function describeDatasetSource(raw) {
  const source = objectValue(raw?.source);
  if (source.table) {
    return `Table ${source.table}`;
  }
  if (source.sql || raw?.sql_text) {
    return "SQL projection";
  }
  if (source.resource) {
    return `Resource ${source.resource}`;
  }
  if (source.request?.path) {
    return `Request ${source.request.path}`;
  }
  if (source.path || source.storage_uri || raw?.storage_uri) {
    return `File ${source.path || source.storage_uri || raw.storage_uri}`;
  }
  return "No source configured";
}

export function isSyncedDataset(raw) {
  return normalizeMaterializationMode(raw?.materialization?.mode || raw?.materialization_mode) === "synced";
}

function buildDatasetSourcePayload(form, sourceMode) {
  if (sourceMode === "table") {
    return {
      kind: "table",
      table: requiredText(form?.table, "Dataset source table is required."),
    };
  }
  if (sourceMode === "sql") {
    return {
      kind: "sql",
      sql: requiredText(form?.sql, "Dataset source SQL is required."),
    };
  }
  if (sourceMode === "file") {
    const path = requiredText(form?.path, "Dataset file path or storage URI is required.");
    const payload = {
      kind: "file",
      format: String(form?.format || "csv").trim() || "csv",
      header: Boolean(form?.header),
    };
    payload[form?.fileLocationField === "storage_uri" ? "storage_uri" : "path"] = path;
    if (String(form?.delimiter || "").trim()) {
      payload.delimiter = form.delimiter;
    }
    if (String(form?.quote || "").trim()) {
      payload.quote = form.quote;
    }
    return payload;
  }
  if (sourceMode === "resource") {
    return {
      kind: "resource",
      resource: requiredText(form?.resource, "Dataset connector resource is required."),
    };
  }
  throw new Error("This dataset source type is inspect-only in the guided editor.");
}

function buildDatasetMaterializationPayload(form, originalResource = null) {
  const materializationMode = normalizeMaterializationMode(form?.materializationMode);
  if (materializationMode === "synced") {
    const raw = originalResource?.rawPayload || originalResource || {};
    return {
      mode: "synced",
      sync: objectValue(raw.materialization?.sync || raw.sync),
    };
  }
  return { mode: "live" };
}

function buildComparableOriginalSource(resource) {
  const raw = resource?.rawPayload || resource || {};
  const sourceMode = detectDatasetSourceMode(raw.source, raw);
  if (!SUPPORTED_SOURCE_MODES.has(sourceMode)) {
    return null;
  }
  return buildDatasetSourcePayload(buildDatasetEditFormState(raw), sourceMode);
}

function buildComparableOriginalMaterialization(resource) {
  const raw = resource?.rawPayload || resource || {};
  return buildDatasetMaterializationPayload(
    buildDatasetEditFormState(raw),
    raw,
  );
}

function validateDatasetConnectorCompatibility({ connector, connectorName, materializationMode, sourceMode }) {
  const normalizedConnector = String(connectorName || "").trim();
  if (materializationMode === "synced" && !normalizedConnector) {
    throw new Error("Select a connector for synced datasets.");
  }
  if (["table", "sql", "resource"].includes(sourceMode) && !normalizedConnector) {
    throw new Error("Select a connector for this dataset source.");
  }
  if (!connector) {
    return;
  }

  const capabilities = normalizeConnectorCapabilities(connector.capabilities || connector.capabilities_schema);
  if (materializationMode === "synced" && capabilities.supports_synced_datasets === false) {
    throw new Error(`Connector '${normalizedConnector}' does not advertise synced dataset support.`);
  }
  if (materializationMode === "live" && ["table", "sql"].includes(sourceMode)) {
    if (capabilities.supports_live_datasets === false) {
      throw new Error(`Connector '${normalizedConnector}' does not advertise live dataset support.`);
    }
    if (capabilities.supports_query_pushdown === false) {
      throw new Error(`Connector '${normalizedConnector}' does not advertise query pushdown for table/sql datasets.`);
    }
  }
}

function normalizeConnectorCapabilities(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeMaterializationMode(value) {
  const normalized = String(value || "live").trim().toLowerCase();
  return normalized === "synced" ? "synced" : "live";
}

function normalizedConnector(value, sourceMode, materializationMode) {
  const connector = String(value || "").trim();
  if (!connector && (materializationMode === "synced" || ["table", "sql", "resource"].includes(sourceMode))) {
    throw new Error("Select a connector for this dataset source.");
  }
  return connector || null;
}

function requiredText(value, message) {
  const text = String(value || "").trim();
  if (!text) {
    throw new Error(message);
  }
  return text;
}

function nullableText(value) {
  return String(value || "").trim() || null;
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function compactPayload(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entryValue]) => entryValue !== undefined),
  );
}

function samePayload(left, right) {
  return JSON.stringify(left || null) === JSON.stringify(right || null);
}
