import {
  normalizeRuntimeArtifactType,
  normalizeTabularResult,
  normalizeVisualizationSpec,
  toCsvText,
} from "../../lib/runtimeUi.js";

export const ARTIFACT_PLACEHOLDER_PATTERN = /{{\s*artifact:([A-Za-z0-9_.:-]+)\s*}}/g;

export function splitMarkdownArtifacts(markdown) {
  const text = String(markdown || "");
  if (!text) {
    return [];
  }

  ARTIFACT_PLACEHOLDER_PATTERN.lastIndex = 0;
  const parts = [];
  let lastIndex = 0;
  let match = ARTIFACT_PLACEHOLDER_PATTERN.exec(text);

  while (match) {
    if (match.index > lastIndex) {
      parts.push({
        type: "markdown",
        value: text.slice(lastIndex, match.index),
      });
    }

    parts.push({
      type: "artifact",
      id: String(match[1] || "").trim(),
      value: match[0],
    });

    lastIndex = match.index + match[0].length;
    match = ARTIFACT_PLACEHOLDER_PATTERN.exec(text);
  }

  if (lastIndex < text.length) {
    parts.push({
      type: "markdown",
      value: text.slice(lastIndex),
    });
  }

  ARTIFACT_PLACEHOLDER_PATTERN.lastIndex = 0;
  return parts;
}

export function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

export function hasTabularPayload(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      (Array.isArray(value.rows) || Array.isArray(value.data) || Array.isArray(value.columns)),
  );
}

export function findArtifact(artifacts, id) {
  const normalizedId = String(id || "").trim();
  if (!normalizedId) {
    return null;
  }

  return (
    (Array.isArray(artifacts) ? artifacts : []).find(
      (artifact) => String(artifact?.id || "").trim() === normalizedId,
    ) || null
  );
}

export function artifactId(artifact) {
  return String(artifact?.id || artifact?.artifact_id || artifact?.key || "").trim();
}

export function artifactKind(artifact, fallbackId = "") {
  return normalizeRuntimeArtifactType(
    artifact?.type || artifact?.kind || artifact?.source,
    fallbackId || artifactId(artifact),
  );
}

export function artifactFormatting(artifact) {
  const payload = objectValue(artifact?.payload);
  return objectValue(artifact?.formatting) || objectValue(payload?.formatting);
}

export function normalizeArtifactView(artifact, fallbackId = "") {
  if (!artifact || typeof artifact !== "object") {
    return null;
  }

  const id = artifactId(artifact) || String(fallbackId || "").trim();
  if (!id) {
    return null;
  }
  const rawDataRef = artifact.data_ref ?? artifact.dataRef ?? null;

  return {
    raw: artifact,
    id,
    type: artifactKind(artifact, id),
    title: String(artifact.title || artifact.label || id.replaceAll("_", " ")).trim(),
    role: String(artifact.role || "").trim(),
    payload: objectValue(artifact.payload) || {},
    dataRef: typeof rawDataRef === "string" ? rawDataRef : objectValue(rawDataRef),
    provenance: objectValue(artifact.provenance) || {},
    formatting: artifactFormatting(artifact),
  };
}

export function readArtifactReferenceId(artifact) {
  const view = normalizeArtifactView(artifact);
  const dataRef = view?.dataRef;
  if (!dataRef) {
    return "";
  }
  if (typeof dataRef === "string") {
    return dataRef.trim();
  }
  return String(dataRef.artifact_id || dataRef.artifactId || dataRef.id || "").trim();
}

export function findTableArtifact(artifacts, preferredReference = "") {
  const list = Array.isArray(artifacts) ? artifacts : [];
  const normalizedReference = String(preferredReference || "").trim();

  if (normalizedReference) {
    const referenced = findArtifact(list, normalizedReference);
    if (referenced && artifactKind(referenced) === "table") {
      return referenced;
    }
  }

  return (
    list.find((artifact) => artifactKind(artifact) === "table" && artifactId(artifact) === "primary_result") ||
    list.find((artifact) => artifactKind(artifact) === "table" && artifact?.role === "primary_result") ||
    list.find((artifact) => artifactKind(artifact) === "table") ||
    null
  );
}

export function resolveArtifactTableResult(artifact, fallbackResult) {
  const view = normalizeArtifactView(artifact);
  const payload = view?.payload || {};
  const raw = view?.raw || {};
  const candidates = [
    objectValue(raw.result),
    objectValue(raw.table),
    objectValue(raw.data),
    objectValue(payload.result),
    objectValue(payload.table),
    objectValue(payload.data),
    payload,
    objectValue(fallbackResult),
  ];
  const candidate = candidates.find(hasTabularPayload);
  const formatting = view?.formatting;
  const normalized = candidate ? normalizeTabularResult(candidate) : null;

  return normalized && formatting && !normalized.formatting
    ? { ...normalized, formatting }
    : normalized;
}

export function looksLikeVisualizationPayload(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      (value.chartType ||
        value.chart_type ||
        value.x ||
        value.x_axis ||
        value.y ||
        value.y_axis ||
        value.measure ||
        value.measures ||
        value.type),
  );
}

export function resolveArtifactVisualization(artifact, fallbackVisualization) {
  const view = normalizeArtifactView(artifact);
  const payload = view?.payload || {};
  const raw = view?.raw || {};
  const candidates = [
    objectValue(raw.visualization),
    objectValue(raw.spec),
    objectValue(raw.chart),
    objectValue(payload.visualization),
    objectValue(payload.spec),
    objectValue(payload.chart),
    looksLikeVisualizationPayload(payload) ? payload : null,
    objectValue(fallbackVisualization),
  ].filter(Boolean);
  const candidate = candidates.find((item) => looksLikeVisualizationPayload(item));

  return candidate ? normalizeVisualizationSpec(candidate) : null;
}

export function resolveArtifactSqlPair(artifact) {
  const view = normalizeArtifactView(artifact);
  const payload = view?.payload || {};
  const raw = view?.raw || {};
  const executable = String(
    raw.sql_executable ||
      raw.executable_sql ||
      payload.sql_executable ||
      payload.executable_sql ||
      raw.sql ||
      raw.query ||
      payload.sql ||
      payload.query ||
      "",
  ).trim();
  const canonical = String(
    raw.sql_canonical ||
      raw.generated_sql ||
      payload.sql_canonical ||
      payload.generated_sql ||
      "",
  ).trim();

  return { executable, canonical };
}

export function resolveArtifactSql(artifact) {
  const { executable, canonical } = resolveArtifactSqlPair(artifact);
  return executable || canonical;
}

export function resolveArtifactDiagnostics(artifact, fallbackDiagnostics) {
  const view = normalizeArtifactView(artifact);
  const payload = view?.payload || {};
  const raw = view?.raw || {};

  return (
    objectValue(raw.diagnostics) ||
    objectValue(payload.diagnostics) ||
    objectValue(payload) ||
    objectValue(fallbackDiagnostics)
  );
}

export function buildArtifactRenderPlan(parts, artifacts, fallbackVisualization) {
  const aliasById = new Map();
  const skipIds = new Set();
  const artifactParts = parts.filter((part) => part.type === "artifact");

  artifactParts.forEach((part, index) => {
    const artifact = findArtifact(artifacts, part.id);
    if (!artifact || artifactKind(artifact, part.id) !== "chart") {
      return;
    }

    const visualization = resolveArtifactVisualization(artifact, fallbackVisualization);
    if (visualization?.chartType !== "table") {
      return;
    }

    const referencedTable = findTableArtifact(artifacts, readArtifactReferenceId(artifact));
    if (!referencedTable) {
      return;
    }

    const referencedId = artifactId(referencedTable);
    if (!referencedId || referencedId === part.id) {
      return;
    }

    aliasById.set(part.id, referencedId);

    const laterDuplicate = artifactParts
      .slice(index + 1)
      .some((candidate) => candidate.id === referencedId);
    if (laterDuplicate) {
      skipIds.add(referencedId);
    }
  });

  return { aliasById, skipIds };
}

export function buildUnreferencedPrimaryArtifactIds(parts, artifacts) {
  const referencedIds = new Set(
    (Array.isArray(parts) ? parts : [])
      .filter((part) => part?.type === "artifact")
      .map((part) => String(part.id || "").trim())
      .filter(Boolean),
  );
  const hasReferencedPrimaryDataArtifact = (Array.isArray(artifacts) ? artifacts : []).some((artifact) => {
    const id = artifactId(artifact);
    const kind = artifactKind(artifact, id);
    return referencedIds.has(id) && (kind === "chart" || kind === "table");
  });
  if (hasReferencedPrimaryDataArtifact) {
    return [];
  }
  const candidates = (Array.isArray(artifacts) ? artifacts : []).filter((artifact) => {
    const id = artifactId(artifact);
    if (!id || referencedIds.has(id)) {
      return false;
    }
    const kind = artifactKind(artifact, id);
    if (kind !== "chart" && kind !== "table") {
      return false;
    }
    const role = String(artifact?.role || "").trim();
    return role === "primary_result" || id === "primary_visualization" || id === "primary_result";
  });
  const charts = candidates.filter((artifact) => artifactKind(artifact) === "chart");
  const selected = charts.length > 0 ? charts : candidates.filter((artifact) => artifactKind(artifact) === "table");
  return selected.map((artifact) => artifactId(artifact)).filter(Boolean);
}

export async function copyText(value) {
  const text = String(value || "");
  if (
    !text ||
    typeof navigator === "undefined" ||
    !navigator.clipboard ||
    typeof navigator.clipboard.writeText !== "function"
  ) {
    return false;
  }

  await navigator.clipboard.writeText(text);
  return true;
}

export function safeFileName(value, extension) {
  const base = String(value || "artifact")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return `${base || "artifact"}.${extension}`;
}

export function downloadText({ fileName, mimeType, text }) {
  if (typeof document === "undefined") {
    return;
  }

  const blob = new Blob([String(text || "")], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

export function csvForResult(result) {
  if (!result) {
    return "";
  }
  return toCsvText(result);
}
