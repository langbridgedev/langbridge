export const SECRET_PROVIDER_OPTIONS = [
  { value: "env", label: "Environment variable" },
  { value: "kubernetes", label: "Kubernetes secret" },
  { value: "vault", label: "Vault" },
  { value: "azure_key_vault", label: "Azure Key Vault" },
  { value: "aws_secrets_manager", label: "AWS Secrets Manager" },
];

const VALUE_TYPES = new Set(["string", "number", "boolean"]);
const SECRET_PROVIDER_VALUES = new Set(SECRET_PROVIDER_OPTIONS.map((option) => option.value));

export function createLocalFormId(prefix = "row") {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

export function normalizeConnectorTypeName(value) {
  return String(value || "").trim().toUpperCase();
}

export function normalizeConnectorFamily(value) {
  return String(value || "").trim().toLowerCase();
}

export function formatConnectorFamilyLabel(value) {
  const normalized = normalizeConnectorFamily(value);
  if (!normalized) {
    return "Other";
  }
  return normalized
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

export function normalizeConnectorTypes(items) {
  return (Array.isArray(items) ? items : [])
    .map((item) => {
      const value = normalizeConnectorTypeName(item?.name || item?.connector_type || item?.value);
      if (!value) {
        return null;
      }
      return {
        ...item,
        value,
        label: item?.label || value,
        family: normalizeConnectorFamily(item?.family || item?.connector_family),
      };
    })
    .filter(Boolean)
    .sort((left, right) => `${left.family}-${left.label}`.localeCompare(`${right.family}-${right.label}`));
}

export function connectorFamilyOptions(connectorTypes) {
  const seen = new Set();
  return normalizeConnectorTypes(connectorTypes)
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

export function schemaEntries(schema) {
  return Array.isArray(schema?.config) ? schema.config : [];
}

export function buildConnectorConfigValues(schema, connection = {}) {
  const normalizedConnection = objectValue(connection);
  const entries = schemaEntries(schema);
  if (entries.length === 0) {
    return Object.fromEntries(
      Object.entries(normalizedConnection)
        .filter(([, value]) => value !== undefined && value !== null)
        .map(([key, value]) => [key, stringifyFieldValue(value)]),
    );
  }

  const values = Object.fromEntries(
    entries.map((entry) => [
      entry.field,
      stringifyFieldValue(
        normalizedConnection[entry.field] ?? entry.default ?? entry.value ?? "",
      ),
    ]),
  );
  for (const [key, value] of Object.entries(normalizedConnection)) {
    if (!Object.prototype.hasOwnProperty.call(values, key)) {
      values[key] = value;
    }
  }
  return values;
}

export function buildMetadataRows(metadata = {}) {
  return Object.entries(objectValue(metadata)).map(([key, value]) => ({
    id: createLocalFormId("metadata"),
    key,
    value: stringifyFieldValue(value),
    valueType: inferMetadataValueType(value),
  }));
}

export function buildSecretRows(secrets = {}) {
  return Object.entries(objectValue(secrets)).map(([field, reference]) => {
    const payload = objectValue(reference);
    return {
      id: createLocalFormId("secret"),
      field,
      provider_type: SECRET_PROVIDER_VALUES.has(payload.provider_type) ? payload.provider_type : "env",
      identifier: stringifyFieldValue(payload.identifier),
      key: stringifyFieldValue(payload.key),
      version: stringifyFieldValue(payload.version),
    };
  });
}

export function createBlankMetadataRow() {
  return {
    id: createLocalFormId("metadata"),
    key: "",
    value: "",
    valueType: "string",
  };
}

export function createBlankSecretRow(field = "") {
  return {
    id: createLocalFormId("secret"),
    field,
    provider_type: "env",
    identifier: "",
    key: "",
    version: "",
  };
}

export function buildConnectorCreateFormState({ connectorTypes = [] } = {}) {
  const types = normalizeConnectorTypes(connectorTypes);
  const selectedType = types[0] || null;
  return {
    name: "",
    family: selectedType?.family || "",
    type: selectedType?.value || "",
    description: "",
    configValues: {},
    metadataRows: [],
    secretRows: [],
  };
}

export function buildConnectorEditFormState(resource, schema) {
  const raw = resource?.rawPayload || resource || {};
  const type = normalizeConnectorTypeName(raw.connector_type || raw.type);
  return {
    name: String(raw.name || resource?.name || ""),
    family: normalizeConnectorFamily(raw.connector_family || raw.family),
    type,
    description: String(raw.description || ""),
    configValues: buildConnectorConfigValues(schema, raw.connection || {}),
    metadataRows: buildMetadataRows(raw.metadata || {}),
    secretRows: buildSecretRows(raw.secrets || {}),
  };
}

export function buildConnectorSubmitPayload({ mode, form, schema }) {
  const isCreate = mode === "create";
  const normalizedName = String(form?.name || "").trim();
  const normalizedType = normalizeConnectorTypeName(form?.type);

  if (isCreate && !normalizedName) {
    throw new Error("Connector name is required.");
  }
  if (isCreate && !normalizedType) {
    throw new Error("Connector type is required.");
  }
  if (!schema) {
    throw new Error("Connector schema is unavailable for the selected connector type.");
  }

  const payload = {};
  if (isCreate) {
    payload.name = normalizedName;
    payload.type = normalizedType;
  }

  const description = String(form?.description || "").trim();
  payload.description = description || null;
  payload.connection = buildConnectorConnectionPayload(schema, form?.configValues || {}, form?.secretRows || []);
  payload.metadata = buildMetadataPayload(form?.metadataRows || []);
  payload.secrets = buildSecretReferencesPayload(form?.secretRows || []);
  return payload;
}

export function buildConnectorConnectionPayload(schema, configValues, secretRows = []) {
  const payload = {};
  const missingFields = [];
  const knownFields = new Set(schemaEntries(schema).map((entry) => entry.field));
  const secretFields = new Set(
    (Array.isArray(secretRows) ? secretRows : [])
      .map((row) => String(row?.field || "").trim())
      .filter(Boolean),
  );

  for (const entry of schemaEntries(schema)) {
    const rawValue = configValues?.[entry.field] ?? "";
    const trimmedValue = String(rawValue ?? "").trim();
    if (!trimmedValue) {
      if (entry.required && !secretFields.has(entry.field)) {
        missingFields.push(entry.label || entry.field);
      }
      continue;
    }
    payload[entry.field] = coerceConfigValue(entry, rawValue);
  }

  for (const [field, value] of Object.entries(configValues || {})) {
    if (knownFields.has(field) || String(value ?? "").trim() === "") {
      continue;
    }
    payload[field] = value;
  }

  if (missingFields.length > 0) {
    throw new Error(
      `Complete the required field${missingFields.length === 1 ? "" : "s"}: ${missingFields.join(", ")}.`,
    );
  }

  return payload;
}

export function buildMetadataPayload(rows) {
  const payload = {};
  for (const row of Array.isArray(rows) ? rows : []) {
    const key = String(row?.key || "").trim();
    const rawValue = row?.value ?? "";
    const hasValue = String(rawValue).trim() !== "";
    if (!key && !hasValue) {
      continue;
    }
    if (!key) {
      throw new Error("Metadata key is required when a metadata value is provided.");
    }
    payload[key] = coerceMetadataValue(rawValue, row?.valueType);
  }
  return payload;
}

export function buildSecretReferencesPayload(rows) {
  const payload = {};
  const seen = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    const field = String(row?.field || "").trim();
    const identifier = String(row?.identifier || "").trim();
    const providerType = SECRET_PROVIDER_VALUES.has(row?.provider_type) ? row.provider_type : "env";
    const hasAnyValue = [field, identifier, row?.key, row?.version].some((value) => String(value || "").trim());
    if (!hasAnyValue) {
      continue;
    }
    if (!field) {
      throw new Error("Secret target field is required.");
    }
    if (!identifier) {
      throw new Error(`Secret identifier is required for ${field}.`);
    }
    if (seen.has(field)) {
      throw new Error(`Secret target field '${field}' is duplicated.`);
    }
    seen.add(field);
    payload[field] = compactObject({
      provider_type: providerType,
      identifier,
      key: String(row?.key || "").trim(),
      version: String(row?.version || "").trim(),
    });
  }
  return payload;
}

function coerceConfigValue(entry, value) {
  const type = String(entry?.type || "string").trim().toLowerCase();
  if (type === "number") {
    const parsed = Number(value);
    if (Number.isNaN(parsed)) {
      throw new Error(`${entry.label || entry.field} must be a number.`);
    }
    return parsed;
  }
  if (type === "boolean") {
    const normalized = String(value).trim().toLowerCase();
    if (!["true", "false"].includes(normalized)) {
      throw new Error(`${entry.label || entry.field} must be true or false.`);
    }
    return normalized === "true";
  }
  return value;
}

function coerceMetadataValue(value, valueType = "string") {
  const type = VALUE_TYPES.has(valueType) ? valueType : "string";
  if (type === "number") {
    const parsed = Number(value);
    if (Number.isNaN(parsed)) {
      throw new Error("Metadata value must be a number.");
    }
    return parsed;
  }
  if (type === "boolean") {
    const normalized = String(value).trim().toLowerCase();
    if (!["true", "false"].includes(normalized)) {
      throw new Error("Metadata value must be true or false.");
    }
    return normalized === "true";
  }
  return String(value);
}

function inferMetadataValueType(value) {
  if (typeof value === "number") {
    return "number";
  }
  if (typeof value === "boolean") {
    return "boolean";
  }
  return "string";
}

function stringifyFieldValue(value) {
  if (value === undefined || value === null) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  return String(value);
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entryValue]) => entryValue !== undefined && entryValue !== null && entryValue !== ""),
  );
}
