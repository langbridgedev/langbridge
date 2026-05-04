function readString(value) {
  return typeof value === "string" ? value.trim() : "";
}

function unique(values) {
  const seen = new Set();
  const output = [];
  values.forEach((value) => {
    if (!value || seen.has(value)) {
      return;
    }
    seen.add(value);
    output.push(value);
  });
  return output;
}

function buildKeyCandidates(value) {
  const key = String(value || "").trim();
  if (!key) {
    return [];
  }

  const candidates = [key, key.replace(/\./g, "__")];
  const parts = key.split(".").filter((part) => part.length > 0);
  if (parts.length >= 2) {
    const tableAndColumn = `${parts[parts.length - 2]}.${parts[parts.length - 1]}`;
    candidates.push(tableAndColumn, tableAndColumn.replace(/\./g, "__"));
  }
  if (parts.length >= 1) {
    candidates.push(parts[parts.length - 1]);
  }
  return unique(candidates);
}

export function resolveChartDataKey({
  selectedKey,
  rowKeys,
  metadata,
  fallbackKey,
  excludeKey,
}) {
  if (!Array.isArray(rowKeys) || rowKeys.length === 0) {
    return "";
  }

  const availableKeys = new Set(rowKeys);
  const sourceToColumn = new Map();
  const nameToColumn = new Map();

  (Array.isArray(metadata) ? metadata : []).forEach((entry) => {
    const column = readString(entry?.column);
    if (!column) {
      return;
    }
    const source = readString(entry?.source);
    if (source) {
      sourceToColumn.set(source, column);
    }
    const name = readString(entry?.name);
    if (name) {
      nameToColumn.set(name, column);
    }
  });

  const resolveCandidate = (candidate) => {
    if (availableKeys.has(candidate) && candidate !== excludeKey) {
      return candidate;
    }
    const sourceMapped = sourceToColumn.get(candidate);
    if (sourceMapped && availableKeys.has(sourceMapped) && sourceMapped !== excludeKey) {
      return sourceMapped;
    }
    const nameMapped = nameToColumn.get(candidate);
    if (nameMapped && availableKeys.has(nameMapped) && nameMapped !== excludeKey) {
      return nameMapped;
    }
    return null;
  };

  const selectedCandidates = buildKeyCandidates(selectedKey);
  for (const candidate of selectedCandidates) {
    const resolved = resolveCandidate(candidate);
    if (resolved) {
      return resolved;
    }
  }

  const fallbackCandidates = buildKeyCandidates(fallbackKey || "");
  for (const candidate of fallbackCandidates) {
    const resolved = resolveCandidate(candidate);
    if (resolved) {
      return resolved;
    }
  }

  return rowKeys.find((key) => key !== excludeKey) || rowKeys[0];
}
