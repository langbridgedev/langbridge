function normalizeRuntimeDateString(value) {
  const text = String(value || "").trim();
  if (!text) {
    return text;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return `${text}T00:00:00Z`;
  }
  if (
    /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(text)
  ) {
    return `${text.replace(" ", "T")}Z`;
  }
  return text;
}

export function parseRuntimeDate(value) {
  if (value instanceof Date) {
    return new Date(value.getTime());
  }
  if (typeof value === "number") {
    return new Date(value);
  }
  const normalized = normalizeRuntimeDateString(value);
  return new Date(normalized);
}

export function getRuntimeTimestamp(value) {
  const date = parseRuntimeDate(value);
  return Number.isNaN(date.getTime()) ? 0 : date.getTime();
}

export function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return value.toLocaleString();
    }
    return value.toLocaleString(undefined, {
      maximumFractionDigits: 3,
    });
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (value instanceof Date) {
    return value.toLocaleString();
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

export function formatDateTime(value) {
  if (!value) {
    return "n/a";
  }
  const date = parseRuntimeDate(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString();
}

export function formatList(values) {
  const items = Array.isArray(values) ? values.filter(Boolean) : [];
  return items.length > 0 ? items.join(", ") : "n/a";
}

export function toSqlAlias(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || "dataset";
}

export function splitCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function getErrorMessage(error) {
  if (error && typeof error === "object" && typeof error.message === "string") {
    return error.message;
  }
  return "Unexpected runtime error.";
}
