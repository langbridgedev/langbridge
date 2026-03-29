export const SQL_HISTORY_STORAGE_KEY = "langbridge.runtime_ui.sql_history";
export const SQL_SAVED_STORAGE_KEY = "langbridge.runtime_ui.sql_saved";
export const BI_STUDIO_STORAGE_KEY = "langbridge.runtime_ui.bi_studio";

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

export function normalizeVisualizationSpec(visualization) {
  if (!visualization || typeof visualization !== "object") {
    return null;
  }
  const raw = visualization;
  const yValue = raw.y ?? raw.y_axis ?? null;
  return {
    title: raw.title || raw.chart_title || "Runtime chart",
    chartType: raw.chartType || raw.chart_type || "bar",
    x: raw.x || raw.x_axis || raw.groupBy || raw.group_by || "",
    y: Array.isArray(yValue) ? yValue.filter(Boolean) : [yValue].filter(Boolean),
  };
}

export function hasRenderableVisualization(visualization) {
  const normalized = normalizeVisualizationSpec(visualization);
  return Boolean(normalized?.chartType && normalized.chartType !== "table");
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
      const agentId = String(assistant?.model_snapshot?.agent_id || "");
      return {
        id: String(message.id || createLocalId("turn")),
        prompt: message?.content?.text || "",
        createdAt: message.created_at,
        assistantSummary: assistant ? readAssistantText(assistant) : "",
        assistantTable,
        assistantVisualization: assistantContent.visualization || null,
        diagnostics,
        errorMessage: assistant?.error?.message || "",
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
        `${dataset.connector || "Runtime"} dataset ready for SQL, BI, and agent use.`,
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
        model.description || "Semantic model available for runtime query and BI flows.",
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
