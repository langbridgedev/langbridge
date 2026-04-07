export const SQL_HISTORY_STORAGE_KEY = "langbridge.runtime_ui.sql_history";
export const SQL_SAVED_STORAGE_KEY = "langbridge.runtime_ui.sql_saved";
export const DASHBOARD_BUILDER_STORAGE_KEY = "langbridge.runtime_ui.dashboard_builder";

export const DEFAULT_SQL_QUERY = `SELECT country, SUM(net_revenue) AS net_sales
FROM shopify_orders
GROUP BY country
ORDER BY net_sales DESC`;

export const DEFAULT_CHAT_MESSAGE =
  "Summarize the current runtime state and call out any operational issues.";

export const SQL_TEMPLATES = [
  {
    label: "Revenue by country",
    description: "Federated runtime query against the default orders dataset.",
    query: `SELECT country, SUM(net_revenue) AS net_sales
FROM shopify_orders
GROUP BY country
ORDER BY net_sales DESC`,
  },
  {
    label: "Latest orders",
    description: "Quick operational spot-check for recent records.",
    query: `SELECT order_id, order_date, country, net_revenue
FROM shopify_orders
ORDER BY order_date DESC
LIMIT 25`,
  },
  {
    label: "Connector direct SQL",
    description: "Starter pattern for direct connector workbench queries.",
    query: `SELECT country, SUM(net_revenue) AS net_sales
FROM orders_enriched
GROUP BY country
ORDER BY net_sales DESC`,
  },
];

export const CHAT_STARTERS = [
  "Summarize runtime health and the most important operational signals.",
  "What datasets and semantic models are currently available in this runtime?",
  "Recommend the next connector or sync action worth checking.",
];

export function createLocalId(prefix = "item") {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

export async function copyTextToClipboard(value) {
  if (
    typeof navigator === "undefined" ||
    !navigator.clipboard ||
    typeof navigator.clipboard.writeText !== "function"
  ) {
    throw new Error("Clipboard access is not available in this browser.");
  }
  await navigator.clipboard.writeText(String(value || ""));
}

export function downloadTextFile(
  filename,
  content,
  contentType = "text/plain;charset=utf-8",
) {
  if (typeof document === "undefined") {
    return;
  }
  const blob = new Blob([String(content || "")], { type: contentType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export function formatRelativeTime(value) {
  if (!value) {
    return "just now";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  const diffMs = date.getTime() - Date.now();
  const minute = 60 * 1000;
  const hour = 60 * minute;
  const day = 24 * hour;
  const week = 7 * day;
  const month = 30 * day;
  const year = 365 * day;
  const formatter =
    typeof Intl !== "undefined" && Intl.RelativeTimeFormat
      ? new Intl.RelativeTimeFormat(undefined, { numeric: "auto" })
      : null;

  const format = (amount, unit) =>
    formatter ? formatter.format(amount, unit) : date.toLocaleString();

  if (Math.abs(diffMs) < hour) {
    return format(Math.round(diffMs / minute), "minute");
  }
  if (Math.abs(diffMs) < day) {
    return format(Math.round(diffMs / hour), "hour");
  }
  if (Math.abs(diffMs) < week) {
    return format(Math.round(diffMs / day), "day");
  }
  if (Math.abs(diffMs) < month) {
    return format(Math.round(diffMs / week), "week");
  }
  if (Math.abs(diffMs) < year) {
    return format(Math.round(diffMs / month), "month");
  }
  return format(Math.round(diffMs / year), "year");
}

export function buildItemRef(item) {
  return encodeURIComponent(String(item?.id || item?.name || ""));
}

export function resolveItemByRef(items, ref) {
  if (!Array.isArray(items) || items.length === 0) {
    return null;
  }
  const normalized = String(ref || "").trim();
  if (!normalized) {
    return items[0];
  }
  return (
    items.find(
      (item) =>
        String(item?.id || "").trim() === normalized ||
        String(item?.name || "").trim() === normalized,
    ) || items[0]
  );
}

export function countUniqueValues(items, getValue) {
  const values = new Set();
  (Array.isArray(items) ? items : []).forEach((item) => {
    const value = getValue(item);
    if (value !== null && value !== undefined && String(value).trim()) {
      values.add(String(value).trim());
    }
  });
  return values.size;
}

export function buildColumnsFromRows(rows) {
  const sample = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
  if (!sample || typeof sample !== "object" || Array.isArray(sample)) {
    return [];
  }
  return Object.keys(sample);
}

export function normalizeResultRows(result) {
  if (Array.isArray(result?.rows)) {
    return result.rows;
  }
  if (Array.isArray(result?.data)) {
    return result.data;
  }
  return [];
}

export function normalizeTabularResult(result) {
  const rows = normalizeResultRows(result);
  const columns =
    Array.isArray(result?.columns) && result.columns.length > 0
      ? result.columns
      : buildColumnsFromRows(rows);
  return {
    ...result,
    columns,
    rows,
    rowCount:
      result?.rowCount ?? result?.row_count ?? result?.row_count_preview ?? rows.length,
  };
}

export function toCsvText(result) {
  const normalized = normalizeTabularResult(result);
  const columns = Array.isArray(normalized.columns) ? normalized.columns : [];
  const rows = Array.isArray(normalized.rows) ? normalized.rows : [];
  const header = columns.join(",");
  const lines = rows.map((row) => {
    const record =
      row && typeof row === "object" && !Array.isArray(row)
        ? row
        : Array.isArray(row)
          ? columns.reduce((accumulator, column, index) => {
              accumulator[column] = row[index];
              return accumulator;
            }, {})
          : { [columns[0] || "value"]: row };
    return columns
      .map((column) => {
        const raw = record?.[column];
        const text =
          raw === null || raw === undefined
            ? ""
            : String(raw).replaceAll('"', '""');
        return `"${text}"`;
      })
      .join(",");
  });
  return [header, ...lines].join("\n");
}

export function detectSqlWarnings(query) {
  const sql = String(query || "").trim();
  if (!sql) {
    return [];
  }
  const lowered = sql.toLowerCase();
  const warnings = [];
  if (/select\s+\*/i.test(sql)) {
    warnings.push("`SELECT *` can inflate payload size and make result review noisy.");
  }
  if (!/\bwhere\b/.test(lowered)) {
    warnings.push("No `WHERE` clause detected.");
  }
  if (!/\blimit\b/.test(lowered) && !/\btop\b/.test(lowered)) {
    warnings.push("No explicit row cap detected.");
  }
  if ((lowered.match(/\bjoin\b/g) || []).length >= 3) {
    warnings.push("Query joins three or more relations.");
  }
  if (/\b(drop|truncate|delete|update|insert|alter|merge)\b/.test(lowered)) {
    warnings.push("Mutation keywords detected. The runtime SQL surface is safest for read workflows.");
  }
  return warnings;
}

export function extractSemanticFields(detail) {
  const datasets = detail?.content_json?.datasets;
  const dimensions = [];
  const measures = [];

  if (!datasets || typeof datasets !== "object") {
    return { dimensions, measures };
  }

  Object.entries(datasets).forEach(([datasetName, dataset]) => {
    const datasetValue = dataset && typeof dataset === "object" ? dataset : {};
    const datasetDimensions = Array.isArray(datasetValue.dimensions)
      ? datasetValue.dimensions
      : [];
    const datasetMeasures = Array.isArray(datasetValue.measures)
      ? datasetValue.measures
      : [];

    datasetDimensions.forEach((item) => {
      if (!item?.name) {
        return;
      }
      dimensions.push({
        value: `${datasetName}.${item.name}`,
        label: `${datasetName}.${item.name}`,
        type: item.type || "dimension",
      });
    });

    datasetMeasures.forEach((item) => {
      if (!item?.name) {
        return;
      }
      measures.push({
        value: `${datasetName}.${item.name}`,
        label: `${datasetName}.${item.name}`,
        type: item.type || "measure",
        aggregation: item.aggregation || null,
      });
    });
  });

  return { dimensions, measures };
}

export function extractSemanticDatasets(detail) {
  const datasets = detail?.content_json?.datasets;
  if (!datasets || typeof datasets !== "object") {
    return [];
  }

  return Object.entries(datasets).map(([datasetName, dataset]) => {
    const datasetValue = dataset && typeof dataset === "object" ? dataset : {};
    const dimensions = Array.isArray(datasetValue.dimensions)
      ? datasetValue.dimensions
      : [];
    const measures = Array.isArray(datasetValue.measures)
      ? datasetValue.measures
      : [];
    return {
      name: datasetName,
      relationName:
        datasetValue.relation_name || datasetValue.relationName || null,
      dimensions,
      measures,
    };
  });
}

export function renderJson(value) {
  return JSON.stringify(value, null, 2);
}

export function normalizeChartType(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "bar";
  }
  if (normalized.includes("stack") && normalized.includes("bar")) {
    return "stacked-bar";
  }
  if (normalized.includes("area")) {
    return "area";
  }
  if (normalized.includes("scatter")) {
    return "scatter";
  }
  if (normalized.includes("donut") || normalized.includes("doughnut")) {
    return "donut";
  }
  if (normalized.includes("pie")) {
    return "pie";
  }
  if (normalized.includes("line")) {
    return "line";
  }
  if (normalized.includes("kpi") || normalized.includes("stat") || normalized.includes("metric")) {
    return "stat";
  }
  if (normalized.includes("table")) {
    return "table";
  }
  if (normalized.includes("bar")) {
    return "bar";
  }
  return "bar";
}

export function normalizeVisualizationSpec(visualization) {
  if (!visualization || typeof visualization !== "object") {
    return null;
  }
  const raw = visualization;
  const options = raw.options && typeof raw.options === "object" ? raw.options : {};
  const yValue = raw.y ?? raw.y_axis ?? raw.measure ?? raw.measures ?? null;
  const rawChartType =
    raw.chartType ||
    raw.chart_type ||
    raw.type ||
    options.chart_type ||
    options.type ||
    "bar";
  const innerRadiusRaw =
    options.inner_radius ?? options.innerRadius ?? options.pieInnerRadius ?? null;
  const innerRadius =
    typeof innerRadiusRaw === "number" && Number.isFinite(innerRadiusRaw) ? innerRadiusRaw : null;
  return {
    title: raw.title || raw.chart_title || "Runtime chart",
    subtitle: raw.subtitle || raw.chart_subtitle || raw.description || options.subtitle || "",
    chartType: normalizeChartType(rawChartType),
    x: raw.x || raw.x_axis || "",
    y: Array.isArray(yValue) ? yValue.filter(Boolean) : [yValue].filter(Boolean),
    groupBy: raw.groupBy || raw.group_by || raw.group || raw.series || "",
    options,
    warning:
      typeof options.visualization_warning === "string"
        ? options.visualization_warning
        : typeof raw.visualization_warning === "string"
          ? raw.visualization_warning
          : "",
    stacked:
      normalizeChartType(rawChartType) === "stacked-bar" ||
      options.stacked === true ||
      options.stack === true,
    donut:
      normalizeChartType(rawChartType) === "donut" ||
      innerRadius !== null ||
      options.variant === "donut",
    innerRadius,
  };
}

export function hasRenderableVisualization(visualization) {
  const normalized = normalizeVisualizationSpec(visualization);
  return Boolean(normalized?.chartType && normalized.chartType !== "table");
}

function normalizeErrorStatus(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function normalizeAnalystOutcome(diagnostics) {
  if (diagnostics?.analyst_outcome && typeof diagnostics.analyst_outcome === "object") {
    const raw = diagnostics.analyst_outcome;
    return {
      status: String(raw.status || "").trim().toLowerCase() || null,
      stage: String(raw.stage || "").trim().toLowerCase() || null,
      message: typeof raw.message === "string" ? raw.message : "",
      recoverable: Boolean(raw.recoverable),
      terminal: Boolean(raw.terminal),
      retryAttempted: Boolean(raw.retry_attempted),
      retryCount: Number(raw.retry_count || 0),
      retryRationale: typeof raw.retry_rationale === "string" ? raw.retry_rationale : "",
      selectedToolName:
        typeof raw.selected_tool_name === "string" ? raw.selected_tool_name : "",
      selectedAssetName:
        typeof raw.selected_asset_name === "string" ? raw.selected_asset_name : "",
      selectedAssetType:
        typeof raw.selected_asset_type === "string" ? raw.selected_asset_type : "",
      recoveryActions: Array.isArray(raw.recovery_actions) ? raw.recovery_actions : [],
      metadata: raw.metadata && typeof raw.metadata === "object" ? raw.metadata : {},
    };
  }

  if (typeof diagnostics?.clarifying_question === "string" && diagnostics.clarifying_question.trim()) {
    return {
      status: "needs_clarification",
      stage: "clarification",
      message: diagnostics.clarifying_question.trim(),
      recoverable: true,
      terminal: false,
      retryAttempted: false,
      retryCount: 0,
      retryRationale: "",
      selectedToolName: "",
      selectedAssetName: "",
      selectedAssetType: "",
      recoveryActions: [],
      metadata: {},
    };
  }

  return null;
}

export function deriveRuntimeResultState({
  status,
  result,
  visualization,
  diagnostics,
  errorMessage,
  errorStatus,
}) {
  const normalizedResult = result ? normalizeTabularResult(result) : null;
  const normalizedVisualization = normalizeVisualizationSpec(visualization);
  const outcome = normalizeAnalystOutcome(diagnostics);
  const rowCount = Number(normalizedResult?.rowCount ?? normalizedResult?.rows?.length ?? 0);
  const hasRows = rowCount > 0;
  const hasChart = hasRenderableVisualization(normalizedVisualization);
  const message =
    String(
      errorMessage ||
        outcome?.message ||
        diagnostics?.clarifying_question ||
        diagnostics?.error ||
        "",
    ).trim() || "";
  const deniedPattern = /\b(access denied|denied|forbidden|unauthori[sz]ed|blocked)\b/i;
  const requestStatus = String(status || "").trim().toLowerCase();
  const requestErrorStatus = normalizeErrorStatus(errorStatus);
  const isAccessDenied =
    requestErrorStatus === 401 ||
    requestErrorStatus === 403 ||
    deniedPattern.test(message);

  if (requestStatus === "pending") {
    return {
      kind: "pending",
      tone: "warning",
      label: "Running",
      title: "Waiting for the runtime to finish this turn",
      description: "The request is still executing.",
      showChart: false,
      showTable: false,
    };
  }

  if (isAccessDenied) {
    return {
      kind: "access_denied",
      tone: "danger",
      label: "Access denied",
      title: "The runtime blocked this request",
      description: message || "The current actor or agent could not access the requested data.",
      showChart: false,
      showTable: false,
    };
  }

  switch (outcome?.status) {
    case "access_denied":
      return {
        kind: "access_denied",
        tone: "danger",
        label: "Access denied",
        title: "The runtime blocked this request",
        description:
          message || "The current actor or agent could not access the requested analytical data.",
        showChart: false,
        showTable: false,
      };
    case "success":
      return {
        kind: hasChart ? "success_chart" : "success_rows",
        tone: "success",
        label: hasChart ? "Chart ready" : "Rows returned",
        title: hasChart ? "Structured result with visualization" : "Structured result returned",
        description: hasChart
          ? "The runtime returned both a visualization and underlying rows."
          : "The runtime returned tabular rows for this request.",
        showChart: hasChart,
        showTable: Boolean(normalizedResult),
      };
    case "empty_result":
      return {
        kind: "empty_result",
        tone: "warning",
        label: "No rows matched",
        title: "The request completed without matching rows",
        description: message || "Try widening the filters or adjusting the question.",
        showChart: false,
        showTable: Boolean(normalizedResult),
      };
    case "invalid_request":
      return {
        kind: "invalid_request",
        tone: "warning",
        label: "Invalid request",
        title: "The runtime could not act on this request",
        description: message || "Refine the prompt so the analysis target is more specific.",
        showChart: false,
        showTable: false,
      };
    case "needs_clarification":
      return {
        kind: "needs_clarification",
        tone: "warning",
        label: "Clarification needed",
        title: "The runtime needs one more detail before continuing",
        description: message || "Provide the missing context and retry.",
        showChart: false,
        showTable: false,
      };
    case "query_error":
      return {
        kind: "query_error",
        tone: "danger",
        label: "Query error",
        title: "The runtime could not translate the request into a valid query",
        description: message || "Adjust the question or inspect the execution notes.",
        showChart: false,
        showTable: false,
      };
    case "selection_error":
      return {
        kind: "invalid_request",
        tone: "warning",
        label: "No analytical context",
        title: "The runtime could not map the request to a dataset or semantic model",
        description: message || "Try naming the asset, metric, or subject more explicitly.",
        showChart: false,
        showTable: false,
      };
    case "execution_error":
      return {
        kind: "execution_failure",
        tone: "danger",
        label: "Execution failed",
        title: "The runtime could not complete execution",
        description: message || "Inspect the diagnostics for more detail.",
        showChart: false,
        showTable: false,
      };
    default:
      break;
  }

  if (requestStatus === "error") {
    return {
      kind: "execution_failure",
      tone: "danger",
      label: "Execution failed",
      title: "The runtime failed to complete this request",
      description: message || "No structured result was returned.",
      showChart: false,
      showTable: false,
    };
  }

  if (message && !outcome && !normalizedResult) {
    return {
      kind: "execution_failure",
      tone: "danger",
      label: "Execution failed",
      title: "The runtime completed with an error",
      description: message,
      showChart: false,
      showTable: false,
    };
  }

  if (normalizedResult && rowCount === 0) {
    return {
      kind: "empty_result",
      tone: "warning",
      label: "No rows matched",
      title: "The request completed without matching rows",
      description: "Try widening the filters or adjusting the question.",
      showChart: false,
      showTable: true,
    };
  }

  if (hasChart || hasRows) {
    return {
      kind: hasChart ? "success_chart" : "success_rows",
      tone: "success",
      label: hasChart ? "Chart ready" : "Rows returned",
      title: hasChart ? "Structured result with visualization" : "Structured result returned",
      description: hasChart
        ? "The runtime returned both a visualization and underlying rows."
        : "The runtime returned tabular rows for this request.",
      showChart: hasChart,
      showTable: Boolean(normalizedResult),
    };
  }

  return {
    kind: "success_summary",
    tone: "info",
    label: "Completed",
    title: "The runtime completed this request",
    description: "No structured rows or visualization were returned.",
    showChart: false,
    showTable: false,
  };
}

export function buildDiagnosticsHighlights(diagnostics) {
  if (!diagnostics || typeof diagnostics !== "object") {
    return [];
  }
  const outcome = normalizeAnalystOutcome(diagnostics);
  const highlights = [
    outcome?.status
      ? { label: "Outcome", value: outcome.status.replaceAll("_", " ") }
      : null,
    outcome?.stage ? { label: "Stage", value: outcome.stage.replaceAll("_", " ") } : null,
    diagnostics?.asset_name ? { label: "Asset", value: diagnostics.asset_name } : null,
    outcome?.selectedToolName ? { label: "Tool", value: outcome.selectedToolName } : null,
    outcome?.retryCount > 0
      ? { label: "Retries", value: String(outcome.retryCount) }
      : null,
    diagnostics?.response_mode ? { label: "Mode", value: diagnostics.response_mode } : null,
    diagnostics?.reasoning?.iterations
      ? { label: "Iterations", value: String(diagnostics.reasoning.iterations) }
      : null,
    diagnostics?.execution_mode ? { label: "Execution", value: diagnostics.execution_mode } : null,
    diagnostics?.analysis_path ? { label: "Path", value: diagnostics.analysis_path } : null,
  ];

  return highlights.filter(Boolean);
}

export function buildDiagnosticsNotes(diagnostics, visualization) {
  if (!diagnostics || typeof diagnostics !== "object") {
    return [];
  }
  const outcome = normalizeAnalystOutcome(diagnostics);
  const normalizedVisualization = normalizeVisualizationSpec(visualization);
  const notes = [];

  if (outcome?.message) {
    notes.push(outcome.message);
  }
  if (outcome?.retryRationale) {
    notes.push(outcome.retryRationale);
  }
  if (typeof outcome?.metadata?.recovery_hint === "string" && outcome.metadata.recovery_hint.trim()) {
    notes.push(outcome.metadata.recovery_hint.trim());
  }
  if (Array.isArray(outcome?.recoveryActions) && outcome.recoveryActions.length > 0) {
    notes.push(
      `Recovery actions: ${outcome.recoveryActions
        .map((action) => action?.action)
        .filter(Boolean)
        .join(", ")}`,
    );
  }
  if (normalizedVisualization?.warning) {
    notes.push(normalizedVisualization.warning);
  }
  if (Array.isArray(diagnostics?.assumptions_applied) && diagnostics.assumptions_applied.length > 0) {
    notes.push(`Assumptions: ${diagnostics.assumptions_applied.join("; ")}`);
  }
  if (typeof diagnostics?.reasoning?.final_rationale === "string" && diagnostics.reasoning.final_rationale.trim()) {
    notes.push(diagnostics.reasoning.final_rationale.trim());
  }
  if (typeof diagnostics?.clarifying_question === "string" && diagnostics.clarifying_question.trim()) {
    notes.push(diagnostics.clarifying_question.trim());
  }

  return [...new Set(notes.filter(Boolean))];
}

function readAssistantText(message) {
  const content =
    message?.content && typeof message.content === "object" ? message.content : {};
  return (
    content.summary ||
    content.text ||
    (typeof content.result?.text === "string" ? content.result.text : "") ||
    ""
  );
}

function normalizeAssistantTable(message) {
  const content =
    message?.content && typeof message.content === "object" ? message.content : {};
  const result = content.result;
  if (!result || typeof result !== "object") {
    return null;
  }

  if (Array.isArray(result.rows) || Array.isArray(result.data)) {
    return normalizeTabularResult(result);
  }

  return null;
}

export function buildConversationTurns(messages, agents) {
  const assistantByParent = new Map();
  const agentLabelById = new Map(
    (Array.isArray(agents) ? agents : []).map((agent) => [
      String(agent.id || ""),
      agent.name,
    ]),
  );

  (Array.isArray(messages) ? messages : [])
    .filter((message) => message.role === "assistant" && message.parent_message_id)
    .forEach((message) => {
      assistantByParent.set(String(message.parent_message_id), message);
    });

  return (Array.isArray(messages) ? messages : [])
    .filter((message) => message.role === "user")
    .map((message) => {
      const assistant = assistantByParent.get(String(message.id));
      const assistantContent =
        assistant?.content && typeof assistant.content === "object"
          ? assistant.content
          : {};
      const assistantTable = normalizeAssistantTable(assistant);
      const diagnostics =
        assistantContent.diagnostics &&
        typeof assistantContent.diagnostics === "object"
          ? assistantContent.diagnostics
          : null;
      const assistantError =
        assistant?.error && typeof assistant.error === "object" ? assistant.error : null;
      const agentId = String(assistant?.model_snapshot?.agent_id || "");
      return {
        id: String(message.id || createLocalId("turn")),
        prompt: message?.content?.text || "",
        createdAt: message.created_at,
        assistantSummary: assistant ? readAssistantText(assistant) : "",
        assistantTable,
        assistantVisualization: assistantContent.visualization || null,
        diagnostics,
        errorMessage:
          (typeof assistantError?.message === "string" ? assistantError.message : "") ||
          (typeof assistant?.error === "string" ? assistant.error : ""),
        errorStatus:
          normalizeErrorStatus(assistantError?.status) ||
          normalizeErrorStatus(assistantError?.status_code) ||
          null,
        agentId,
        agentLabel: agentLabelById.get(agentId) || null,
        status: assistant?.error ? "error" : assistant ? "ready" : "pending",
      };
    });
}

export function readAgentSystemPrompt(detail) {
  return (
    detail?.definition?.prompt?.system_prompt ||
    detail?.definition?.prompt?.systemPrompt ||
    detail?.definition?.system_prompt ||
    detail?.system_prompt ||
    ""
  );
}

export function readAgentFeatureFlags(detail) {
  const features = detail?.definition?.features;
  if (!features || typeof features !== "object") {
    return [];
  }
  return Object.entries(features)
    .filter(([, enabled]) => Boolean(enabled))
    .map(([name]) => name.replaceAll("_", " "));
}

export function readAgentAllowedConnectors(detail) {
  const connectors = detail?.definition?.access_policy?.allowed_connectors;
  return Array.isArray(connectors) ? connectors : [];
}

export function buildActivityFeed(payload) {
  const items = [];

  (payload?.threads || []).forEach((thread) => {
    items.push({
      id: `thread-${thread.id}`,
      href: `/chat/${buildItemRef(thread)}`,
      title: thread.title || `Thread ${String(thread.id).slice(0, 8)}`,
      kind: "Thread",
      description: "Resume a runtime investigation thread.",
      timestamp: thread.updated_at || thread.created_at,
    });
  });

  (payload?.datasets || []).forEach((dataset) => {
    items.push({
      id: `dataset-${dataset.id || dataset.name}`,
      href: `/datasets/${buildItemRef(dataset)}`,
      title: dataset.label || dataset.name,
      kind: "Dataset",
      description:
        dataset.description ||
        `${dataset.connector || "Runtime"} dataset ready for SQL, Dashboard Builder, and agent use.`,
      timestamp: dataset.updated_at || dataset.created_at,
    });
  });

  (payload?.models || []).forEach((model) => {
    items.push({
      id: `model-${model.id || model.name}`,
      href: `/semantic-models/${buildItemRef(model)}`,
      title: model.name,
      kind: "Semantic model",
      description:
        model.description || "Semantic model available for runtime query and dashboard-builder flows.",
      timestamp: model.updated_at || model.updatedAt || model.created_at,
    });
  });

  (payload?.agents || []).forEach((agent) => {
    items.push({
      id: `agent-${agent.id || agent.name}`,
      href: `/agents/${buildItemRef(agent)}`,
      title: agent.name,
      kind: "Agent",
      description:
        agent.description || "Runtime agent definition ready for threads and quick runs.",
      timestamp: agent.updated_at || agent.updatedAt || agent.created_at,
    });
  });

  (payload?.connectors || []).forEach((connector) => {
    items.push({
      id: `connector-${connector.id || connector.name}`,
      href: `/connectors/${buildItemRef(connector)}`,
      title: connector.name,
      kind: "Connector",
      description:
        connector.description ||
        `${connector.connector_type || "Runtime connector"} exposed to the local runtime.`,
      timestamp: connector.updated_at || connector.updatedAt || connector.created_at,
    });
  });

  return items
    .sort((left, right) => {
      const leftTime = new Date(left.timestamp || 0).getTime();
      const rightTime = new Date(right.timestamp || 0).getTime();
      return rightTime - leftTime;
    })
    .slice(0, 8);
}

export function readAgentJson(text) {
  const candidate = String(text || "").trim();
  if (!candidate) {
    return null;
  }

  const direct = tryParseJson(candidate);
  if (direct) {
    return direct;
  }

  const codeBlockMatch = candidate.match(/```json\s*([\s\S]+?)```/i);
  if (codeBlockMatch?.[1]) {
    return tryParseJson(codeBlockMatch[1]);
  }

  const bracketMatch = candidate.match(/\{[\s\S]+\}/);
  if (bracketMatch?.[0]) {
    return tryParseJson(bracketMatch[0]);
  }

  return null;
}

function tryParseJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}
