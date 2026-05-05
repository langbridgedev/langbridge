export const QUERY_SCOPE_OPTIONS = [
  { value: "semantic", label: "Semantic" },
  { value: "dataset", label: "Dataset" },
  { value: "source", label: "Source" },
];

export const SQL_COMPLETION_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "GROUP BY",
  "ORDER BY",
  "HAVING",
  "LIMIT",
  "JOIN",
  "LEFT JOIN",
  "INNER JOIN",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "DATE_TRUNC",
  "CAST",
  "COALESCE",
];

export const DEFAULT_QUERY_BY_SCOPE = {
  semantic: ``,
  dataset: ``,
  source: ``,
};

export const QUERY_WORKSPACE_STORAGE_KEYS = {
  draft: "langbridge.runtime_ui_next.query_workspace.draft",
  scope: "langbridge.runtime_ui_next.query_workspace.scope",
  connector: "langbridge.runtime_ui_next.query_workspace.connector",
  dataset: "langbridge.runtime_ui_next.query_workspace.dataset",
  semanticModel: "langbridge.runtime_ui_next.query_workspace.semantic_model",
  contextOpen: "langbridge.runtime_ui_next.query_workspace.context_open",
  limit: "langbridge.runtime_ui_next.query_workspace.limit",
  timeout: "langbridge.runtime_ui_next.query_workspace.timeout",
  explain: "langbridge.runtime_ui_next.query_workspace.explain",
  recents: "langbridge.runtime_ui_next.query_workspace.recents",
  saved: "langbridge.runtime_ui_next.query_workspace.saved",
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function normalizeQueryScope(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return QUERY_SCOPE_OPTIONS.some((scope) => scope.value === normalized)
    ? normalized
    : "semantic";
}

export function defaultQueryForScope(scope) {
  return DEFAULT_QUERY_BY_SCOPE[normalizeQueryScope(scope)] || DEFAULT_QUERY_BY_SCOPE.semantic;
}

export function isDefaultQuery(value) {
  const query = normalizeSqlText(value);
  return Object.values(DEFAULT_QUERY_BY_SCOPE).some((candidate) => normalizeSqlText(candidate) === query);
}

export function normalizeSqlText(value) {
  return String(value || "").trim();
}

export function normalizePositiveInteger(value, fallback = null) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return fallback;
  }
  return Math.floor(numeric);
}

export function getResourceRef(resource) {
  return String(
    resource?.ref ||
      resource?.value ||
      resource?.name ||
      resource?.id ||
      resource?.raw?.name ||
      resource?.raw?.id ||
      "",
  ).trim();
}

export function getResourceLabel(resource, fallback = "Resource") {
  return String(
    resource?.label ||
      resource?.name ||
      resource?.title ||
      resource?.raw?.label ||
      resource?.raw?.name ||
      resource?.raw?.id ||
      fallback,
  ).trim();
}

export function inferConnectorDialect(connector) {
  const raw = connector?.raw || connector || {};
  const value = [
    connector?.queryDialect,
    raw.query_dialect,
    raw.sql_dialect,
    raw.connector_type,
    raw.type,
    raw.name,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (value.includes("postgres")) {
    return "postgres";
  }
  if (value.includes("mysql")) {
    return "mysql";
  }
  if (value.includes("sqlite")) {
    return "sqlite";
  }
  if (value.includes("snowflake")) {
    return "snowflake";
  }
  if (value.includes("bigquery")) {
    return "bigquery";
  }
  if (value.includes("sqlserver") || value.includes("sql server") || value.includes("mssql")) {
    return "tsql";
  }
  return "tsql";
}

export function isSqlConnector(connector) {
  const raw = connector?.raw || connector || {};
  const searchable = [
    raw.connector_family,
    raw.connector_type,
    raw.type,
    raw.runtime_type,
    raw.name,
    raw.description,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  return /\bsql\b|postgres|mysql|sqlite|snowflake|bigquery|sqlserver|sql server|mssql|duckdb/.test(searchable);
}

export function normalizeResourceOption(item, fallbackLabel = "Resource") {
  const raw = item?.raw || item || {};
  const ref = getResourceRef(item) || getResourceRef(raw);
  const label = getResourceLabel(item, fallbackLabel);
  return {
    id: String(raw.id || item?.id || ref || label).trim(),
    ref,
    value: ref,
    name: String(raw.name || item?.name || label).trim(),
    label,
    description: String(raw.description || item?.description || item?.subtitle || "").trim(),
    queryDialect: inferConnectorDialect({ ...item, raw }),
    raw,
  };
}

export function normalizeResourceOptions(items, fallbackLabel = "Resource") {
  const seen = new Set();
  return (Array.isArray(items) ? items : [])
    .map((item) => normalizeResourceOption(item, fallbackLabel))
    .filter((item) => {
      const key = item.value || item.id || item.label;
      if (!key || seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
}

export function filterSqlConnectorOptions(items) {
  const normalized = normalizeResourceOptions(items, "Connector");
  const sqlConnectors = normalized.filter((item) => isSqlConnector(item));
  return sqlConnectors.length > 0 ? sqlConnectors : normalized;
}

export function buildSqlCompletionItems({ queryScope, resources }) {
  const scope = normalizeQueryScope(queryScope);
  const baseItems = SQL_COMPLETION_KEYWORDS.map((keyword) => ({
    label: keyword,
    insertText: keyword,
    kind: keyword.includes("(") ? "function" : "keyword",
    detail: "SQL",
  }));
  const resourceItems = resourceCompletionItems(scope, resources || {});
  return uniqueCompletionItems([...resourceItems, ...baseItems]);
}

export function buildScopeResourceHints({ queryScope, resources }) {
  const scope = normalizeQueryScope(queryScope);
  const datasets = Array.isArray(resources?.datasets) ? resources.datasets : [];
  const semanticModels = Array.isArray(resources?.semanticModels) ? resources.semanticModels : [];
  const connectors = Array.isArray(resources?.connectors) ? resources.connectors : [];

  if (scope === "semantic") {
    return {
      title: "Semantic context",
      description: "Semantic SQL can use the runtime semantic layer. These models and fields are available as reference.",
      primaryLabel: "Models",
      items: semanticModels.map((model) => ({
        id: model.id || model.value || model.label,
        label: model.label,
        insertText: sqlIdentifier(model.name || model.value || model.label),
        meta: semanticModelSummary(model),
        detail: model.description,
      })),
      secondaryLabel: "Fields",
      secondaryItems: semanticFieldItems(semanticModels).slice(0, 18),
    };
  }

  if (scope === "dataset") {
    return {
      title: "Dataset context",
      description: "Dataset SQL runs over the runtime dataset layer. No dataset selector is required.",
      primaryLabel: "Datasets",
      items: datasets.map((dataset) => ({
        id: dataset.id || dataset.value || dataset.label,
        label: dataset.label,
        insertText: sqlIdentifier(dataset.name || dataset.value || dataset.label),
        meta: datasetMeta(dataset),
        detail: dataset.description,
      })),
      secondaryLabel: "Columns",
      secondaryItems: datasetColumnItems(datasets).slice(0, 18),
    };
  }

  return {
    title: "Source context",
    description: "Source SQL runs against the selected connector. Use connector-native table names and dialect.",
    primaryLabel: "Connectors",
    items: connectors.map((connector) => ({
      id: connector.id || connector.value || connector.label,
      label: connector.label,
      insertText: sqlIdentifier(connector.value || connector.name || connector.label),
      meta: connector.queryDialect,
      detail: connector.description,
    })),
    secondaryLabel: "Hints",
    secondaryItems: connectors.slice(0, 8).map((connector) => ({
      label: connector.value || connector.label,
      kind: "connector",
      detail: connector.queryDialect,
    })),
  };
}

export function buildScopeResourceTree({ queryScope, resources }) {
  const scope = normalizeQueryScope(queryScope);
  const datasets = Array.isArray(resources?.datasets) ? resources.datasets : [];
  const semanticModels = Array.isArray(resources?.semanticModels) ? resources.semanticModels : [];
  const connectors = Array.isArray(resources?.connectors) ? resources.connectors : [];

  if (scope === "semantic") {
    return {
      title: "Semantic",
      description: "Models and governed fields available to semantic SQL.",
      emptyLabel: "No semantic models returned.",
      groups: semanticModels.map((model) => {
        const raw = model?.raw || {};
        const children = [
          ...semanticDatasetNodes(model),
          ...semanticMetricGroupNodes(raw, model),
        ];
        return {
          id: model.id || model.value || model.label,
          label: model.label,
          insertText: sqlIdentifier(model.name || model.value || model.label),
          kind: "model",
          meta: semanticModelSummary(model),
          children,
        };
      }),
    };
  }

  if (scope === "dataset") {
    return {
      title: "Datasets",
      description: "Tables and columns available to dataset SQL.",
      emptyLabel: "No datasets returned.",
      groups: datasets.map((dataset) => {
        const columns = datasetColumnItems([dataset]).map((item) => ({
          id: `${dataset.id || dataset.value || dataset.label}-${item.label}`,
          label: item.label,
          insertText: sqlIdentifier(item.insertText || item.label),
          kind: "column",
          meta: item.detail,
        }));
        return {
          id: dataset.id || dataset.value || dataset.label,
          label: dataset.label,
          insertText: sqlIdentifier(dataset.name || dataset.value || dataset.label),
          kind: "table",
          meta: datasetMeta(dataset),
          children: columns,
        };
      }),
    };
  }

  return {
    title: "Sources",
    description: "Connectors available for source SQL.",
    emptyLabel: "No SQL-capable connectors returned.",
    groups: connectors.map((connector) => ({
      id: connector.id || connector.value || connector.label,
      label: connector.label,
      insertText: sqlIdentifier(connector.value || connector.name || connector.label),
      kind: "connector",
      meta: connector.queryDialect,
      children: [
        connector.value
          ? {
              id: `${connector.id || connector.value}-name`,
              label: connector.value,
              insertText: sqlIdentifier(connector.value),
              kind: "name",
              meta: "connection",
            }
          : null,
        connector.queryDialect
          ? {
              id: `${connector.id || connector.value}-dialect`,
              label: connector.queryDialect,
              insertText: "",
              kind: "dialect",
              meta: "dialect",
            }
          : null,
      ].filter(Boolean),
    })),
  };
}

export function findOptionByValue(options, value) {
  const normalizedValue = String(value || "").trim();
  if (!Array.isArray(options) || options.length === 0) {
    return null;
  }
  if (!normalizedValue) {
    return options[0];
  }
  return (
    options.find(
      (item) =>
        String(item.value || "").trim() === normalizedValue ||
        String(item.id || "").trim() === normalizedValue ||
        String(item.name || "").trim() === normalizedValue,
    ) || options[0]
  );
}

function resourceCompletionItems(scope, resources) {
  if (scope === "semantic") {
    const semanticModels = Array.isArray(resources.semanticModels) ? resources.semanticModels : [];
    return [
      ...semanticModels.map((model) => ({
        label: model.label,
        insertText: sqlIdentifier(model.name || model.value || model.label),
        kind: "semantic model",
        detail: "Semantic model",
      })),
      ...semanticFieldItems(semanticModels).map((item) => ({
        ...item,
        insertText: sqlIdentifier(item.insertText || item.label),
      })),
    ];
  }

  if (scope === "dataset") {
    const datasets = Array.isArray(resources.datasets) ? resources.datasets : [];
    return [
      ...datasets.map((dataset) => ({
        label: dataset.label,
        insertText: sqlIdentifier(dataset.name || dataset.value || dataset.label),
        kind: "dataset",
        detail: datasetMeta(dataset),
      })),
      ...datasetColumnItems(datasets).map((item) => ({
        ...item,
        insertText: sqlIdentifier(item.insertText || item.label),
      })),
    ];
  }

  const connectors = Array.isArray(resources.connectors) ? resources.connectors : [];
  return connectors.map((connector) => ({
    label: connector.label,
    insertText: sqlIdentifier(connector.value || connector.name || connector.label),
    kind: "connector",
    detail: connector.queryDialect,
  }));
}

function semanticModelSummary(model) {
  const raw = model?.raw || {};
  const datasetCount =
    raw.dataset_count ??
    Object.keys(extractSemanticDatasets(raw)).length ??
    normalizeList(raw.dataset_names).length;
  const measureCount =
    raw.measure_count ??
    semanticFieldItems([model]).filter((item) => item.kind === "measure" || item.kind === "metric").length;
  const dimensionCount =
    raw.dimension_count ?? semanticFieldItems([model]).filter((item) => item.kind === "dimension").length;
  return [
    datasetCount ? `${datasetCount} datasets` : "",
    measureCount ? `${measureCount} measures` : "",
    dimensionCount ? `${dimensionCount} dimensions` : "",
  ].filter(Boolean).join(" | ");
}

function datasetMeta(dataset) {
  const raw = dataset?.raw || {};
  return [
    raw.connector || raw.connection || "",
    raw.materialization_mode || raw.materialization?.mode || "",
    normalizeList(raw.columns).length ? `${normalizeList(raw.columns).length} columns` : "",
  ].filter(Boolean).join(" | ");
}

function datasetColumnItems(datasets) {
  return (Array.isArray(datasets) ? datasets : []).flatMap((dataset) => {
    const raw = dataset?.raw || {};
    return normalizeList(raw.columns || raw.schema_hint?.columns || raw.fields).map((column) => {
      const name = columnName(column);
      return {
        label: name,
        insertText: name,
        kind: "column",
        detail: dataset.label,
      };
    });
  }).filter((item) => item.label);
}

function semanticFieldItems(models) {
  return (Array.isArray(models) ? models : []).flatMap((model) => {
    const raw = model?.raw || {};
    return [
      ...semanticDatasetNodes(model).flatMap(flattenTreeNode),
      ...semanticMetricGroupNodes(raw, model).flatMap(flattenTreeNode),
    ];
  }).filter((item) => item.label);
}

function semanticDatasetNodes(model) {
  const raw = model?.raw || {};
  const datasets = extractSemanticDatasets(raw);
  return Object.entries(datasets).map(([datasetName, dataset]) => {
    const children = [
      ...semanticFieldGroupNode(datasetName, "Dimensions", "dimension", dataset?.dimensions),
      ...semanticFieldGroupNode(datasetName, "Measures", "measure", dataset?.measures),
      ...semanticFieldGroupNode(datasetName, "Metrics", "metric", dataset?.metrics),
    ];
    return {
      id: `${model.id || model.value || model.label}-${datasetName}`,
      label: datasetName,
      insertText: sqlIdentifier(datasetName),
      kind: "semantic dataset",
      meta: model.label,
      children,
    };
  });
}

function semanticMetricGroupNodes(raw, model) {
  const metrics = rootSemanticMetrics(raw);
  if (metrics.length === 0) {
    return [];
  }
  return [
    {
      id: `${model?.id || model?.value || model?.label || "semantic"}-metrics`,
      label: "Metrics",
      insertText: "",
      kind: "metric group",
      meta: `${metrics.length} metrics`,
      children: metrics.map((metric) => ({
        id: `${model?.id || model?.value || model?.label || "semantic"}-metric-${metric.label}`,
        label: metric.label,
        insertText: sqlIdentifier(metric.insertText || metric.label),
        kind: "metric",
        meta: metric.detail || "metric",
      })),
    },
  ];
}

function semanticFieldGroupNode(datasetName, label, kind, value) {
  const fields = namedFieldItems(value, datasetName, kind);
  if (fields.length === 0) {
    return [];
  }
  return [
    {
      id: `${datasetName}-${kind}s`,
      label,
      insertText: "",
      kind: `${kind} group`,
      meta: `${fields.length} ${label.toLowerCase()}`,
      children: fields.map((field) => ({
        id: `${datasetName}-${kind}-${field.label}`,
        label: field.label,
        insertText: sqlIdentifier(field.insertText || field.label),
        kind,
        meta: field.detail,
      })),
    },
  ];
}

function rootSemanticMetrics(raw) {
  const candidates = semanticModelPayloadCandidates(raw);
  return candidates.flatMap((candidate) => namedFieldItems(candidate.metrics, "model", "metric"));
}

function namedFieldItems(value, detail, kind) {
  return normalizeNamedEntries(value).map(({ key, value: field }) => {
    const name = columnName(field) || key;
    return {
      label: name,
      insertText: name,
      kind,
      detail,
    };
  }).filter((item) => item.label);
}

function flattenTreeNode(node) {
  const current = node?.insertText
    ? [{
        label: node.label,
        insertText: node.insertText,
        kind: node.kind,
        detail: node.meta,
      }]
    : [];
  const children = Array.isArray(node?.children) ? node.children.flatMap(flattenTreeNode) : [];
  return [...current, ...children];
}

function extractSemanticDatasets(raw) {
  const candidates = semanticModelPayloadCandidates(raw);
  for (const candidate of candidates) {
    const datasets = normalizeSemanticDatasetMap(candidate.datasets || candidate.tables);
    if (datasets) {
      return datasets;
    }
  }
  const datasetNames = normalizeList(raw?.dataset_names || raw?.datasets);
  return datasetNames.reduce((accumulator, name) => {
    const label = typeof name === "string" ? name : name?.name || name?.label || "";
    if (label) {
      accumulator[label] = {};
    }
    return accumulator;
  }, {});
}

function semanticModelPayloadCandidates(raw) {
  return [
    parseObjectLike(raw?.content_json),
    parseObjectLike(raw?.model),
    parseObjectLike(raw?.definition),
    parseObjectLike(raw),
  ].filter((item) => item && typeof item === "object");
}

function normalizeSemanticDatasetMap(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value;
  }
  if (!Array.isArray(value)) {
    return null;
  }
  return value.reduce((accumulator, item) => {
    const dataset = item && typeof item === "object" ? item : { name: item };
    const name = columnName(dataset);
    if (name) {
      accumulator[name] = dataset;
    }
    return accumulator;
  }, {});
}

function parseObjectLike(value) {
  if (!value) {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  if (typeof value !== "string") {
    return null;
  }
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function normalizeNamedEntries(value) {
  if (Array.isArray(value)) {
    return value
      .filter(Boolean)
      .map((item) => ({
        key: columnName(item),
        value: item,
      }));
  }
  if (value && typeof value === "object") {
    return Object.entries(value).map(([key, item]) => ({
      key,
      value: item && typeof item === "object" ? { name: key, ...item } : { name: key },
    }));
  }
  return [];
}

function columnName(column) {
  if (typeof column === "string") {
    return column.trim();
  }
  if (!column || typeof column !== "object") {
    return "";
  }
  return String(column.name || column.label || column.key || column.field || "").trim();
}

function normalizeList(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function sqlIdentifier(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  return /^[A-Za-z_][A-Za-z0-9_$.]*$/.test(text) ? text : `"${text.replaceAll('"', '""')}"`;
}

function uniqueCompletionItems(items) {
  const seen = new Set();
  return (Array.isArray(items) ? items : [])
    .filter((item) => item?.label && item?.insertText)
    .filter((item) => {
      const key = `${String(item.insertText).toLowerCase()}-${String(item.kind || "").toLowerCase()}`;
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
}

export function buildSqlQueryPayload({
  queryScope,
  query,
  connector,
  selectedDatasets = [],
  requestedLimit,
  requestedTimeoutSeconds,
  explain = false,
}) {
  const scope = normalizeQueryScope(queryScope);
  const normalizedQuery = normalizeSqlText(query);
  if (!normalizedQuery) {
    throw new Error("Enter a SQL query before running.");
  }

  const payload = {
    query_scope: scope,
    query: normalizedQuery,
    explain: Boolean(explain),
  };

  const limit = normalizePositiveInteger(requestedLimit);
  if (limit !== null) {
    payload.requested_limit = limit;
  }

  const timeout = normalizePositiveInteger(requestedTimeoutSeconds);
  if (timeout !== null) {
    payload.requested_timeout_seconds = timeout;
  }

  if (scope === "dataset") {
    const datasetIds = (Array.isArray(selectedDatasets) ? selectedDatasets : [selectedDatasets])
      .map((item) => getResourceRef(item))
      .filter(Boolean);
    if (datasetIds.length > 0) {
      payload.selected_datasets = datasetIds;
    }
    return payload;
  }

  if (scope === "source") {
    const connectorRef = getResourceRef(connector);
    if (!connectorRef) {
      throw new Error("Choose a source connector before running source SQL.");
    }
    if (UUID_PATTERN.test(connectorRef)) {
      payload.connection_id = connectorRef;
    } else {
      payload.connection_name = connectorRef;
    }
    payload.query_dialect = inferConnectorDialect(connector);
    return payload;
  }

  return payload;
}

export function normalizeColumnName(column, index = 0) {
  if (typeof column === "string" && column.trim()) {
    return column.trim();
  }
  if (column && typeof column === "object") {
    const candidate = column.name || column.key || column.label || column.field;
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }
  return `Column ${index + 1}`;
}

export function buildColumnsFromRows(rows) {
  const firstRow = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
  if (firstRow && typeof firstRow === "object" && !Array.isArray(firstRow)) {
    return Object.keys(firstRow);
  }
  if (Array.isArray(firstRow)) {
    return firstRow.map((_, index) => `Column ${index + 1}`);
  }
  return [];
}

export function normalizeRuntimeRows(rows) {
  return Array.isArray(rows) ? rows : [];
}

export function extractRuntimeDiagnostics(response) {
  const job = response?.job && typeof response.job === "object" ? response.job : null;
  const taskDiagnostics = Array.isArray(job?.tasks)
    ? job.tasks.map((task) => task?.diagnostics).find((item) => item && typeof item === "object")
    : null;
  const diagnosticsArtifact = Array.isArray(response?.artifacts)
    ? response.artifacts.find((artifact) => artifact?.artifact_key === "sql_diagnostics")
    : null;
  const artifactDiagnostics =
    diagnosticsArtifact?.data && typeof diagnosticsArtifact.data === "object"
      ? diagnosticsArtifact.data
      : null;

  return {
    ...(artifactDiagnostics || {}),
    ...(taskDiagnostics || {}),
    ...(response?.federation_diagnostics ? { federation_diagnostics: response.federation_diagnostics } : {}),
  };
}

export function extractGeneratedSql(response) {
  const diagnostics = extractRuntimeDiagnostics(response);
  return String(
    response?.generated_sql ||
      diagnostics.generated_sql ||
      diagnostics.query_sql ||
      diagnostics.sql ||
      "",
  ).trim();
}

export function normalizeQueryRunResult(response, request = {}) {
  const rows = normalizeRuntimeRows(response?.rows);
  const columns = Array.isArray(response?.columns) && response.columns.length > 0
    ? response.columns.map(normalizeColumnName)
    : buildColumnsFromRows(rows);
  const rowCount =
    normalizePositiveInteger(response?.total_rows_estimate) ??
    normalizePositiveInteger(response?.row_count_preview) ??
    rows.length;
  const generatedSql = extractGeneratedSql(response);
  const diagnostics = extractRuntimeDiagnostics(response);
  const queryScope = normalizeQueryScope(response?.query_scope || request.queryScope);

  return {
    id: String(response?.job_id || response?.sql_job_id || cryptoSafeId("query-run")).trim(),
    status: String(response?.status || response?.job?.status || "succeeded").trim(),
    title: request.title || "Query result",
    queryScope,
    query: String(response?.query || request.query || "").trim(),
    generatedSql,
    columns,
    rows,
    rowCount,
    rowCountPreview: normalizePositiveInteger(response?.row_count_preview, rows.length),
    totalRowsEstimate: normalizePositiveInteger(response?.total_rows_estimate),
    durationMs: normalizePositiveInteger(response?.duration_ms),
    bytesScanned: normalizePositiveInteger(response?.bytes_scanned),
    redactionApplied: Boolean(response?.redaction_applied),
    selectedDatasets: Array.isArray(request.selectedDatasets) ? request.selectedDatasets : [],
    connector: request.connector || null,
    semanticModel: request.semanticModel || null,
    artifacts: Array.isArray(response?.artifacts) ? response.artifacts : [],
    diagnostics: {
      ...diagnostics,
      job_id: response?.job_id || response?.sql_job_id || response?.job?.id || null,
      query_scope: queryScope,
      status: response?.status || response?.job?.status || "succeeded",
      duration_ms: response?.duration_ms ?? diagnostics.duration_ms ?? null,
      bytes_scanned: response?.bytes_scanned ?? diagnostics.bytes_scanned ?? null,
      generated_sql: generatedSql || null,
      error: response?.error || response?.job?.error || null,
      events: Array.isArray(response?.job?.events) ? response.job.events : [],
      tasks: Array.isArray(response?.job?.tasks) ? response.job.tasks : [],
    },
    raw: response,
  };
}

export function createQueryArtifactBundle(run) {
  const tableArtifact = {
    id: "query_result",
    type: "table",
    role: "primary_result",
    title: run?.title || "Query result",
    payload: {
      columns: Array.isArray(run?.columns) ? run.columns : [],
      rows: Array.isArray(run?.rows) ? run.rows : [],
      rowcount: run?.rowCount ?? run?.rows?.length ?? 0,
      source_sql: run?.query || "",
      generated_sql: run?.generatedSql || "",
    },
    provenance: {
      source: "query_workspace",
      query_scope: run?.queryScope || "",
      selected_datasets: (run?.selectedDatasets || []).map((item) => getResourceRef(item)).filter(Boolean),
      connector: getResourceRef(run?.connector) || null,
      row_count: run?.rowCount ?? null,
    },
    placeholder: "{{artifact:query_result}}",
  };

  const sqlArtifact = {
    id: "query_sql",
    type: "sql",
    role: "supporting",
    title: run?.generatedSql ? "Executed SQL" : "Query SQL",
    payload: {
      sql_executable: run?.generatedSql || run?.query || "",
      sql_canonical: run?.query || "",
      query_scope: run?.queryScope || "",
    },
    provenance: {
      source: "query_workspace",
      query_scope: run?.queryScope || "",
    },
    placeholder: "{{artifact:query_sql}}",
  };

  const artifacts = [tableArtifact, sqlArtifact].filter((artifact) =>
    artifact.type === "table" || artifact.payload.sql_executable,
  );
  const answerMarkdown = [
    "Query result:",
    "",
    "{{artifact:query_result}}",
    "",
    "SQL:",
    "",
    "{{artifact:query_sql}}",
  ].join("\n");

  return {
    answer_markdown: answerMarkdown,
    artifacts,
  };
}

export function createQueryRecent(run) {
  const query = String(run?.query || "").trim();
  const title =
    query
      .split(/\r?\n/)
      .map((line) => line.trim())
      .find(Boolean)
      ?.slice(0, 80) || "Query run";

  return {
    id: String(run?.id || cryptoSafeId("query")).trim(),
    title,
    query,
    queryScope: run?.queryScope || "semantic",
    rowCount: run?.rowCount ?? null,
    durationMs: run?.durationMs ?? null,
    createdAt: new Date().toISOString(),
    path: "/query-workspace",
  };
}

function cryptoSafeId(prefix) {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
