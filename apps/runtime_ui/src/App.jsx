import { startTransition, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { Link, Navigate, Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  Activity,
  ArrowRight,
  Bot,
  BrainCircuit,
  Cable,
  Copy,
  Database,
  Download,
  Edit3,
  History,
  LayoutGrid,
  Layers3,
  MessageSquareText,
  Plus,
  RefreshCw,
  Save,
  SearchCheck,
  ShieldCheck,
  Sparkles,
  Table2,
  Trash2,
  Workflow,
} from "lucide-react";

import { AppShell } from "./components/AppShell";
import {
  BootstrapScreen,
  ErrorScreen,
  LoadingScreen,
  LoginScreen,
  UnsupportedAuthScreen,
} from "./components/AuthScreens";
import { ChartPreview } from "./components/ChartPreview";
import { ResultTable } from "./components/ResultTable";
import { useAsyncData } from "./hooks/useAsyncData";
import {
  askAgent,
  bootstrapAdmin,
  createThread,
  deleteThread,
  fetchAgent,
  fetchAgents,
  fetchAuthBootstrapStatus,
  fetchAuthMe,
  fetchConnectorResources,
  fetchConnectors,
  fetchConnectorStates,
  fetchDataset,
  fetchDatasets,
  fetchRuntimeInfo,
  fetchRuntimeSummary,
  fetchSemanticModel,
  fetchSemanticModels,
  fetchThread,
  fetchThreadMessages,
  fetchThreads,
  login,
  logout,
  previewDataset,
  querySemantic,
  querySql,
  runConnectorSync,
  updateThread,
} from "./lib/runtimeApi";
import {
  formatDateTime,
  formatList,
  formatValue,
  getErrorMessage,
  splitCsv,
  toSqlAlias,
} from "./lib/format";

const DEFAULT_SQL_QUERY = `SELECT country, SUM(net_revenue) AS net_sales
FROM shopify_orders
GROUP BY country
ORDER BY net_sales DESC`;

const DEFAULT_CHAT_MESSAGE =
  "Summarize the current runtime state and call out any operational issues.";

const SQL_TEMPLATES = [
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

const CHAT_STARTERS = [
  "Summarize runtime health and the most important operational signals.",
  "What datasets and semantic models are currently available in this runtime?",
  "Recommend the next connector or sync action worth checking.",
];

const SQL_HISTORY_STORAGE_KEY = "langbridge.runtime_ui.sql_history";
const SQL_SAVED_STORAGE_KEY = "langbridge.runtime_ui.sql_saved";
const BI_STUDIO_STORAGE_KEY = "langbridge.runtime_ui.bi_studio";

function createLocalId(prefix = "item") {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

function readStoredJson(key, fallback) {
  if (typeof window === "undefined") {
    return fallback;
  }
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) {
      return fallback;
    }
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function usePersistentState(key, initialValue) {
  const [value, setValue] = useState(() => readStoredJson(key, initialValue));

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(key, JSON.stringify(value));
  }, [key, value]);

  return [value, setValue];
}

async function copyTextToClipboard(value) {
  if (typeof navigator === "undefined" || !navigator.clipboard || typeof navigator.clipboard.writeText !== "function") {
    throw new Error("Clipboard access is not available in this browser.");
  }
  await navigator.clipboard.writeText(String(value || ""));
}

function downloadTextFile(filename, content, contentType = "text/plain;charset=utf-8") {
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

function formatRelativeTime(value) {
  if (!value) {
    return "just now";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return formatDateTime(value);
  }
  const diffMs = date.getTime() - Date.now();
  const minute = 60 * 1000;
  const hour = 60 * minute;
  const day = 24 * hour;
  const week = 7 * day;
  const month = 30 * day;
  const year = 365 * day;
  const formatter = typeof Intl !== "undefined" && Intl.RelativeTimeFormat
    ? new Intl.RelativeTimeFormat(undefined, { numeric: "auto" })
    : null;

  const format = (amount, unit) => {
    if (formatter) {
      return formatter.format(amount, unit);
    }
    return formatDateTime(value);
  };

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

function toCsvText(result) {
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
        const text = raw === null || raw === undefined ? "" : String(raw).replaceAll('"', '""');
        return `"${text}"`;
      })
      .join(",");
  });
  return [header, ...lines].join("\n");
}

function detectSqlWarnings(query) {
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

function createSqlSavedQuery() {
  return {
    id: createLocalId("sql"),
    name: "",
    tags: [],
    query: DEFAULT_SQL_QUERY,
    connectionName: "",
    requestedLimit: "200",
    updatedAt: new Date().toISOString(),
  };
}

function createBiWidget(seed = {}) {
  return {
    id: seed.id || createLocalId("widget"),
    title: seed.title || "Untitled widget",
    description: seed.description || "",
    dimension: seed.dimension || "",
    measure: seed.measure || "",
    chartType: seed.chartType || "bar",
    limit: seed.limit || "12",
    result: seed.result || null,
    running: Boolean(seed.running),
    error: seed.error || "",
    lastRunAt: seed.lastRunAt || null,
  };
}

function createBiBoard(seed = {}) {
  const widgets =
    Array.isArray(seed.widgets) && seed.widgets.length > 0
      ? seed.widgets.map((item) => createBiWidget(item))
      : [createBiWidget()];
  return {
    id: seed.id || createLocalId("board"),
    name: seed.name || "Runtime dashboard",
    description:
      seed.description ||
      "Local dashboard state derived from runtime semantic models without control-plane storage.",
    selectedModel: seed.selectedModel || "",
    lastRefreshedAt: seed.lastRefreshedAt || null,
    widgets,
  };
}

function loadBiStudioState() {
  const stored = readStoredJson(BI_STUDIO_STORAGE_KEY, null);
  const boards = Array.isArray(stored?.boards) ? stored.boards.map((item) => createBiBoard(item)) : [];
  const normalizedBoards = boards.length > 0 ? boards : [createBiBoard()];
  const preferredActiveBoardId = String(stored?.activeBoardId || "");
  return {
    boards: normalizedBoards,
    activeBoardId: normalizedBoards.some((board) => board.id === preferredActiveBoardId)
      ? preferredActiveBoardId
      : normalizedBoards[0].id,
  };
}

function buildItemRef(item) {
  return encodeURIComponent(String(item?.id || item?.name || ""));
}

function resolveItemByRef(items, ref) {
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
        String(item?.id || "").trim() === normalized || String(item?.name || "").trim() === normalized,
    ) || items[0]
  );
}

function buildColumnsFromRows(rows) {
  const sample = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
  if (!sample || typeof sample !== "object" || Array.isArray(sample)) {
    return [];
  }
  return Object.keys(sample);
}

function normalizeResultRows(result) {
  if (Array.isArray(result?.rows)) {
    return result.rows;
  }
  if (Array.isArray(result?.data)) {
    return result.data;
  }
  return [];
}

function normalizeTabularResult(result) {
  const rows = normalizeResultRows(result);
  const columns =
    Array.isArray(result?.columns) && result.columns.length > 0 ? result.columns : buildColumnsFromRows(rows);
  return {
    ...result,
    columns,
    rows,
    rowCount: result?.rowCount ?? result?.row_count ?? result?.row_count_preview ?? rows.length,
  };
}

function extractSemanticFields(detail) {
  const datasets = detail?.content_json?.datasets;
  const dimensions = [];
  const measures = [];

  if (!datasets || typeof datasets !== "object") {
    return { dimensions, measures };
  }

  Object.entries(datasets).forEach(([datasetName, dataset]) => {
    const datasetValue = dataset && typeof dataset === "object" ? dataset : {};
    const datasetDimensions = Array.isArray(datasetValue.dimensions) ? datasetValue.dimensions : [];
    const datasetMeasures = Array.isArray(datasetValue.measures) ? datasetValue.measures : [];

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

function extractSemanticDatasets(detail) {
  const datasets = detail?.content_json?.datasets;
  if (!datasets || typeof datasets !== "object") {
    return [];
  }

  return Object.entries(datasets).map(([datasetName, dataset]) => {
    const datasetValue = dataset && typeof dataset === "object" ? dataset : {};
    const dimensions = Array.isArray(datasetValue.dimensions) ? datasetValue.dimensions : [];
    const measures = Array.isArray(datasetValue.measures) ? datasetValue.measures : [];
    return {
      name: datasetName,
      relationName: datasetValue.relation_name || datasetValue.relationName || null,
      dimensions,
      measures,
    };
  });
}

function renderJson(value) {
  return JSON.stringify(value, null, 2);
}

function readAssistantText(message) {
  const content = message?.content && typeof message.content === "object" ? message.content : {};
  return (
    content.summary ||
    content.text ||
    (typeof content.result?.text === "string" ? content.result.text : "") ||
    ""
  );
}

function normalizeAssistantTable(message) {
  const content = message?.content && typeof message.content === "object" ? message.content : {};
  const result = content.result;
  if (!result || typeof result !== "object") {
    return null;
  }

  if (Array.isArray(result.rows)) {
    return normalizeTabularResult(result);
  }

  if (Array.isArray(result.data)) {
    return normalizeTabularResult(result);
  }

  return null;
}

function normalizeVisualizationSpec(visualization) {
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

function hasRenderableVisualization(visualization) {
  const normalized = normalizeVisualizationSpec(visualization);
  return Boolean(normalized?.chartType && normalized.chartType !== "table");
}

function buildConversationTurns(messages, agents) {
  const assistantByParent = new Map();
  const agentLabelById = new Map(
    (Array.isArray(agents) ? agents : []).map((agent) => [String(agent.id || ""), agent.name]),
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
      const assistantContent = assistant?.content && typeof assistant.content === "object" ? assistant.content : {};
      const assistantTable = normalizeAssistantTable(assistant);
      const diagnostics =
        assistantContent.diagnostics && typeof assistantContent.diagnostics === "object"
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

function PageEmpty({ title, message, action }) {
  return (
    <div className="empty-box page-empty">
      <strong>{title}</strong>
      <span>{message}</span>
      {action}
    </div>
  );
}

function Panel({ title, eyebrow, actions, children, className = "" }) {
  return (
    <section className={`panel ${className}`.trim()}>
      {title || eyebrow || actions ? (
        <header className="panel-header">
          <div>
            {eyebrow ? <p className="eyebrow">{eyebrow}</p> : null}
            {title ? <h2>{title}</h2> : null}
          </div>
          {actions ? <div className="panel-actions">{actions}</div> : null}
        </header>
      ) : null}
      {children}
    </section>
  );
}

function DetailList({ items }) {
  return (
    <dl className="detail-list">
      {items.map((item) => (
        <div key={item.label}>
          <dt>{item.label}</dt>
          <dd>{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

function SectionTabs({ tabs, value, onChange }) {
  return (
    <div className="section-tabs" role="tablist" aria-label="Section tabs">
      {tabs.map((tab) => (
        <button
          key={tab.value}
          className={`section-tab ${value === tab.value ? "active" : ""}`}
          type="button"
          role="tab"
          aria-selected={value === tab.value}
          onClick={() => onChange(tab.value)}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}

function MetricCard({ icon: Icon, label, value, detail }) {
  return (
    <article className="metric-card">
      <div className="metric-card-top">
        <span className="metric-card-icon">
          <Icon className="metric-card-icon-svg" aria-hidden="true" />
        </span>
        <p>{label}</p>
      </div>
      <strong>{value}</strong>
      {detail ? <span>{detail}</span> : null}
    </article>
  );
}

function FeatureCard({ to, icon: Icon, metric, title, description, cta }) {
  return (
    <Link className="feature-card" to={to}>
      <div className="feature-card-top">
        <span className="feature-card-icon">
          <Icon className="feature-card-icon-svg" aria-hidden="true" />
        </span>
        <p>{metric}</p>
      </div>
      <div>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
      <span className="feature-card-cta">
        {cta}
        <ArrowRight className="feature-card-arrow" aria-hidden="true" />
      </span>
    </Link>
  );
}

function countUniqueValues(items, getValue) {
  const values = new Set();
  (Array.isArray(items) ? items : []).forEach((item) => {
    const value = getValue(item);
    if (value !== null && value !== undefined && String(value).trim()) {
      values.add(String(value).trim());
    }
  });
  return values.size;
}

function readAgentSystemPrompt(detail) {
  return (
    detail?.definition?.prompt?.system_prompt ||
    detail?.definition?.prompt?.systemPrompt ||
    detail?.definition?.system_prompt ||
    detail?.system_prompt ||
    ""
  );
}

function readAgentFeatureFlags(detail) {
  const features = detail?.definition?.features;
  if (!features || typeof features !== "object") {
    return [];
  }
  return Object.entries(features)
    .filter(([, enabled]) => Boolean(enabled))
    .map(([name]) => name.replaceAll("_", " "));
}

function readAgentAllowedConnectors(detail) {
  const connectors = detail?.definition?.access_policy?.allowed_connectors;
  return Array.isArray(connectors) ? connectors : [];
}

function OverviewPage() {
  const { data, loading, error, reload } = useAsyncData(fetchRuntimeSummary);
  const summary = data || {};
  const counts = summary.counts || {};
  const auth = summary.auth || {};
  const runtime = summary.runtime || {};
  const features = Array.isArray(summary.features) ? summary.features : [];
  const semanticModels = Array.isArray(summary.semantic_models) ? summary.semantic_models : [];
  const agents = Array.isArray(summary.agents) ? summary.agents : [];

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Runtime-first foundation</p>
          <h2>Single-workspace runtime shell with a stronger operational surface</h2>
          <p className="hero-copy">
            The runtime UI keeps the cloud app&apos;s stronger hierarchy, card language, and feature
            density where they improve operator workflows, while keeping runtime ownership, local
            posture, and single-workspace routing intact.
          </p>
        </div>
        <div className="hero-actions">
          <button className="primary-button" type="button" onClick={reload} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh overview"}
          </button>
          <div className="hint-list">
            <span>Health: {formatValue(summary.health?.status || (error ? "error" : "loading"))}</span>
            <span>Auth: {auth.auth_enabled ? auth.auth_mode : "disabled"}</span>
            <span>Features: {features.length > 0 ? features.join(", ") : "none"}</span>
          </div>
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="metric-grid">
        <MetricCard
          icon={Cable}
          label="Connectors"
          value={formatValue(counts.connectors || 0)}
          detail="Runtime ingress and direct SQL targets."
        />
        <MetricCard
          icon={Database}
          label="Datasets"
          value={formatValue(counts.datasets || 0)}
          detail="Datasets exposed without org or project scoping."
        />
        <MetricCard
          icon={Layers3}
          label="Semantic Models"
          value={formatValue(counts.semantic_models || 0)}
          detail="Model layer driving BI, chat, and semantic query."
        />
        <MetricCard
          icon={Bot}
          label="Agents"
          value={formatValue(counts.agents || 0)}
          detail="Local agent definitions and attached tools."
        />
        <MetricCard
          icon={MessageSquareText}
          label="Threads"
          value={formatValue(counts.threads || 0)}
          detail="Persisted runtime conversations and working context."
        />
      </section>

      <section className="feature-grid">
        <FeatureCard
          to="/connectors"
          icon={Cable}
          metric={`${counts.connectors || 0} registered`}
          title="Connector management"
          description="Inspect connector posture, sync resources, and runtime sync state from one place."
          cta="Inspect connectors"
        />
        <FeatureCard
          to="/datasets"
          icon={Database}
          metric={`${counts.datasets || 0} datasets`}
          title="Dataset management"
          description="Review dataset bindings, schema metadata, and row previews without cloud scope layers."
          cta="Open datasets"
        />
        <FeatureCard
          to="/semantic-models"
          icon={Layers3}
          metric={`${counts.semantic_models || 0} semantic models`}
          title="Semantic model management"
          description="Explore semantic datasets, measures, and YAML definitions in the runtime product model."
          cta="Browse models"
        />
        <FeatureCard
          to="/sql"
          icon={Table2}
          metric="Workbench"
          title="SQL workspace"
          description="Run federated or direct connector SQL with starter templates and runtime references."
          cta="Launch SQL"
        />
        <FeatureCard
          to="/chat"
          icon={MessageSquareText}
          metric={`${counts.threads || 0} threads`}
          title="Threaded chat"
          description="Use runtime agents with persisted threads, starter prompts, and tabular result playback."
          cta="Open chat"
        />
        <FeatureCard
          to="/bi"
          icon={Sparkles}
          metric="Semantic BI"
          title="Lightweight BI"
          description="Stay light, but keep a serious semantic explorer, chart preview, and runtime query feedback."
          cta="Open BI"
        />
      </section>

      <section className="panel-grid panel-grid--triple">
        <Panel
          title="Runtime Identity"
          eyebrow="Workspace"
          actions={<Link className="ghost-link" to="/settings">Open settings</Link>}
        >
          <DetailList
            items={[
              { label: "Mode", value: formatValue(runtime.mode) },
              { label: "Workspace ID", value: formatValue(runtime.workspace_id) },
              { label: "Actor ID", value: formatValue(runtime.actor_id) },
              { label: "Default semantic model", value: formatValue(runtime.default_semantic_model) },
              { label: "Default agent", value: formatValue(runtime.default_agent) },
            ]}
          />
        </Panel>

        <Panel title="Session Posture" eyebrow="Auth">
          <DetailList
            items={[
              { label: "Auth enabled", value: formatValue(auth.auth_enabled) },
              { label: "Auth mode", value: formatValue(auth.auth_mode) },
              { label: "Bootstrap required", value: formatValue(auth.bootstrap_required) },
              { label: "Admin exists", value: formatValue(auth.has_admin) },
              { label: "Browser login", value: formatValue(auth.login_allowed) },
            ]}
          />
        </Panel>

        <Panel title="Runtime Capabilities" eyebrow="Product Surface">
          {features.length > 0 ? (
            <>
              <div className="tag-list">
                {features.map((item) => (
                  <span key={item} className="tag">
                    {item}
                  </span>
                ))}
              </div>
              <div className="callout">
                <strong>Boundary preserved</strong>
                <span>
                  Cloud tenancy, signup, workspace selection, and control-plane admin surfaces remain
                  intentionally outside this runtime app.
                </span>
              </div>
            </>
          ) : (
            <PageEmpty title="No capabilities" message="This runtime did not advertise UI capabilities." />
          )}
        </Panel>
      </section>

      <section className="panel-grid panel-grid--triple">
        <Panel title="Recent Datasets" eyebrow="Data">
          {Array.isArray(summary.datasets) && summary.datasets.length > 0 ? (
            <div className="stack-list">
              {summary.datasets.map((item) => (
                <Link key={item.id || item.name} className="list-card" to={`/datasets/${buildItemRef(item)}`}>
                  <strong>{item.name}</strong>
                  <span>{[item.connector, item.semantic_model].filter(Boolean).join(" | ") || "No bindings yet"}</span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No datasets" message="This runtime does not have datasets configured yet." />
          )}
        </Panel>

        <Panel title="Recent Connectors" eyebrow="Ingress">
          {Array.isArray(summary.connectors) && summary.connectors.length > 0 ? (
            <div className="stack-list">
              {summary.connectors.map((item) => (
                <Link key={item.id || item.name} className="list-card" to={`/connectors/${buildItemRef(item)}`}>
                  <strong>{item.name}</strong>
                  <span>{[item.connector_type, item.supports_sync ? "sync enabled" : "query only"].filter(Boolean).join(" | ")}</span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No connectors" message="This runtime does not have connectors configured yet." />
          )}
        </Panel>

        <Panel title="Semantic Models" eyebrow="Model Layer">
          {semanticModels.length > 0 ? (
            <div className="stack-list">
              {semanticModels.map((item) => (
                <Link key={item.id || item.name} className="list-card" to={`/semantic-models/${buildItemRef(item)}`}>
                  <strong>{item.name}</strong>
                  <span>
                    {[`${item.dataset_count || 0} datasets`, `${item.measure_count || 0} measures`, item.default ? "default" : null]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No semantic models" message="Define semantic models to drive BI and agent analysis." />
          )}
        </Panel>
      </section>

      <section className="summary-grid">
        <Panel title="Agents" eyebrow="Automation">
          {agents.length > 0 ? (
            <div className="stack-list">
              {agents.map((item) => (
                <Link key={item.id || item.name} className="list-card" to={`/agents/${buildItemRef(item)}`}>
                  <strong>{item.name}</strong>
                  <span>
                    {[item.llm_connection, `${item.tool_count || 0} tools`, item.default ? "default" : null]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No agents" message="Add runtime agents to power chat and BI workflows." />
          )}
        </Panel>

        <Panel title="Recent Threads" eyebrow="Conversation">
          {Array.isArray(summary.threads) && summary.threads.length > 0 ? (
            <div className="stack-list">
              {summary.threads.map((item) => (
                <Link key={item.id} className="list-card" to={`/chat/${item.id}`}>
                  <strong>{item.title || "Untitled thread"}</strong>
                  <span>
                    {[formatValue(item.state), formatDateTime(item.updated_at)].filter(Boolean).join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No threads" message="Start a runtime chat to seed operational threads." />
          )}
        </Panel>
      </section>
    </div>
  );
}

function ConnectorsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload } = useAsyncData(fetchConnectors);
  const connectors = Array.isArray(data?.items) ? data.items : [];
  const selected = resolveItemByRef(connectors, params.id);
  const filteredConnectors = connectors.filter((item) => {
    const haystack = [
      item.name,
      item.description,
      item.connector_type,
      ...(Array.isArray(item.supported_resources) ? item.supported_resources : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [resources, setResources] = useState({ items: [], loading: false, error: "" });
  const [states, setStates] = useState({ items: [], loading: false, error: "" });
  const [selectedResources, setSelectedResources] = useState([]);
  const [syncMode, setSyncMode] = useState("INCREMENTAL");
  const [forceFullRefresh, setForceFullRefresh] = useState(false);
  const [syncResult, setSyncResult] = useState(null);
  const [syncing, setSyncing] = useState(false);
  const [syncError, setSyncError] = useState("");
  const syncEnabledCount = connectors.filter((item) => item.supports_sync).length;
  const selectedDatasetCount = Array.from(
    new Set(
      resources.items.flatMap((item) => (Array.isArray(item?.dataset_names) ? item.dataset_names : [])),
    ),
  ).length;
  const stateByResource = useMemo(
    () =>
      Object.fromEntries(
        (Array.isArray(states.items) ? states.items : []).map((item) => [String(item.resource_name), item]),
      ),
    [states.items],
  );
  const selectedResourceItems = Array.isArray(resources.items)
    ? resources.items.filter((item) => selectedResources.includes(item.name))
    : [];

  useEffect(() => {
    let cancelled = false;

    async function loadSyncDetails() {
      if (!selected?.supports_sync) {
        setResources({ items: [], loading: false, error: "" });
        setStates({ items: [], loading: false, error: "" });
        setSelectedResources([]);
        return;
      }

      setResources({ items: [], loading: true, error: "" });
      setStates({ items: [], loading: true, error: "" });

      try {
        const [resourcePayload, statePayload] = await Promise.all([
          fetchConnectorResources(selected.name),
          fetchConnectorStates(selected.name),
        ]);
        if (cancelled) {
          return;
        }
        const resourceItems = Array.isArray(resourcePayload?.items) ? resourcePayload.items : [];
        const stateItems = Array.isArray(statePayload?.items) ? statePayload.items : [];
        setResources({ items: resourceItems, loading: false, error: "" });
        setStates({ items: stateItems, loading: false, error: "" });
        setSelectedResources((current) => {
          const available = new Set(resourceItems.map((item) => item.name));
          const retained = current.filter((value) => available.has(value));
          return retained.length > 0 ? retained : resourceItems.slice(0, 3).map((item) => item.name);
        });
      } catch (caughtError) {
        if (cancelled) {
          return;
        }
        const message = getErrorMessage(caughtError);
        setResources({ items: [], loading: false, error: message });
        setStates({ items: [], loading: false, error: message });
      }
    }

    void loadSyncDetails();

    return () => {
      cancelled = true;
    };
  }, [selected?.name, selected?.supports_sync]);

  async function handleSync(event) {
    event.preventDefault();
    if (!selected || selectedResources.length === 0) {
      return;
    }
    setSyncing(true);
    setSyncError("");
    setSyncResult(null);
    try {
      const payload = await runConnectorSync(selected.name, {
        resource_names: selectedResources,
        sync_mode: syncMode,
        force_full_refresh: forceFullRefresh,
      });
      setSyncResult(payload);
      const [resourcePayload, statePayload] = await Promise.all([
        fetchConnectorResources(selected.name),
        fetchConnectorStates(selected.name),
      ]);
      setResources({
        items: Array.isArray(resourcePayload?.items) ? resourcePayload.items : [],
        loading: false,
        error: "",
      });
      setStates({
        items: Array.isArray(statePayload?.items) ? statePayload.items : [],
        loading: false,
        error: "",
      });
      void reload();
    } catch (caughtError) {
      setSyncError(getErrorMessage(caughtError));
    } finally {
      setSyncing(false);
    }
  }

  function handleToggleResource(resourceName, checked) {
    setSelectedResources((current) => {
      if (checked) {
        return current.includes(resourceName) ? current : [...current, resourceName];
      }
      return current.filter((value) => value !== resourceName);
    });
  }

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Connector management</p>
          <h2>Operational ingress with runtime sync posture and direct SQL context</h2>
          <p className="hero-copy">
            Restore the richer connector surface from the cloud UI patterns, but keep it focused on a
            single runtime: inventory, sync coverage, resource catalog, and execution state.
          </p>
        </div>
        <div className="hint-list">
          <span>Connectors: {connectors.length}</span>
          <span>Sync-enabled: {syncEnabledCount}</span>
          <span>Selected: {selected?.name || "none"}</span>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={Cable}
          label="Registered"
          value={formatValue(connectors.length)}
          detail="Connector definitions available to this runtime."
        />
        <MetricCard
          icon={Workflow}
          label="Sync-enabled"
          value={formatValue(syncEnabledCount)}
          detail="Connectors exposing resource sync operations."
        />
        <MetricCard
          icon={Database}
          label="Selected resources"
          value={formatValue(resources.items.length)}
          detail="Resource catalog loaded for the active connector."
        />
        <MetricCard
          icon={Activity}
          label="Tracked state"
          value={formatValue(states.items.length)}
          detail="Resource sync state records returned by the runtime."
        />
      </section>

      <section className="toolbar">
        <input
          className="text-input search-input"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Filter connectors by name, type, or resource"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh connectors"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Connector Inventory" eyebrow="Runtime" className="list-panel">
          {filteredConnectors.length > 0 ? (
            <div className="stack-list">
              {filteredConnectors.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/connectors/${buildItemRef(item)}`}
                >
                  <strong>{item.name}</strong>
                  <span>{[item.connector_type, item.supports_sync ? "sync enabled" : "query only"].filter(Boolean).join(" | ")}</span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No connectors found" message="Adjust the filter or add connectors to the runtime config." />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel
                title={selected.name}
                eyebrow="Connector Detail"
                actions={
                  <div className="panel-actions-inline">
                    <span className="chip">{selected.connector_type || "runtime connector"}</span>
                    <button className="ghost-button" type="button" onClick={() => void reload()} disabled={loading}>
                      {loading ? "Refreshing..." : "Refresh"}
                    </button>
                  </div>
                }
              >
                <div className="inline-notes">
                  <span>{selected.supports_sync ? "Sync workflow available" : "Query-only connector"}</span>
                  <span>{selected.sync_strategy || "No sync strategy advertised"}</span>
                  <span>{selected.managed ? "Managed by config" : "User-defined runtime entry"}</span>
                </div>
                <DetailList
                  items={[
                    { label: "Description", value: formatValue(selected.description) },
                    { label: "Supports sync", value: formatValue(selected.supports_sync) },
                    { label: "Sync strategy", value: formatValue(selected.sync_strategy) },
                    { label: "Managed", value: formatValue(selected.managed) },
                    { label: "Supported resources", value: formatList(selected.supported_resources) },
                  ]}
                  />
              </Panel>

              <section className="summary-grid">
                <Panel title="Operational Posture" eyebrow="Coverage">
                  <DetailList
                    items={[
                      { label: "Resource definitions", value: formatValue(resources.items.length) },
                      { label: "Sync state rows", value: formatValue(states.items.length) },
                      { label: "Dataset bindings", value: formatValue(selectedDatasetCount) },
                      { label: "Primary route", value: selected.supports_sync ? "Sync + query" : "Query only" },
                    ]}
                  />
                </Panel>

                <Panel title="Supported Resources" eyebrow="Catalog">
                  {Array.isArray(selected.supported_resources) && selected.supported_resources.length > 0 ? (
                    <div className="tag-list">
                      {selected.supported_resources.map((item) => (
                        <span key={item} className="tag">
                          {item}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty title="No resource types" message="This connector did not expose resource types." />
                  )}
                </Panel>
              </section>

              {selected.supports_sync ? (
                <>
                  <Panel
                    title="Resource Catalog"
                    eyebrow="Select Sync Scope"
                    actions={
                      <div className="panel-actions-inline">
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => setSelectedResources(resources.items.map((item) => item.name))}
                          disabled={resources.loading || resources.items.length === 0}
                        >
                          Select all
                        </button>
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => setSelectedResources([])}
                          disabled={selectedResources.length === 0}
                        >
                          Clear
                        </button>
                      </div>
                    }
                  >
                    {resources.error ? <div className="error-banner">{resources.error}</div> : null}
                    {resources.loading ? (
                      <div className="empty-box">Loading connector resources...</div>
                    ) : resources.items.length > 0 ? (
                      <div className="resource-grid">
                        {resources.items.map((item) => {
                          const state = stateByResource[item.name];
                          const datasetPairs = Array.isArray(item.dataset_names)
                            ? item.dataset_names.map((name, index) => ({
                                name,
                                id: Array.isArray(item.dataset_ids) ? item.dataset_ids[index] : null,
                              }))
                            : [];
                          const selectedResource = selectedResources.includes(item.name);
                          return (
                            <label
                              key={item.name}
                              className={`resource-card ${selectedResource ? "active" : ""}`}
                            >
                              <div className="resource-card-top">
                                <div className="resource-card-heading">
                                  <input
                                    type="checkbox"
                                    checked={selectedResource}
                                    onChange={(event) => handleToggleResource(item.name, event.target.checked)}
                                  />
                                  <div>
                                    <strong>{item.label || item.name}</strong>
                                    <span>{item.name}</span>
                                  </div>
                                </div>
                                <span className="tag">
                                  {(state?.status || item.status || "never_synced").replaceAll("_", " ")}
                                </span>
                              </div>
                              <div className="resource-card-meta">
                                <span>{item.default_sync_mode || "FULL_REFRESH"}</span>
                                <span>{item.supports_incremental ? "incremental" : "full refresh"}</span>
                                <span>{item.primary_key || "no primary key"}</span>
                              </div>
                              <p className="resource-card-copy">
                                Last sync: {formatDateTime(state?.last_sync_at || item.last_sync_at)} | Records synced:{" "}
                                {formatValue(state?.records_synced ?? item.records_synced ?? 0)}
                              </p>
                              {datasetPairs.length > 0 ? (
                                <div className="tag-list">
                                  {datasetPairs.map((dataset) =>
                                    dataset.id ? (
                                      <button
                                        key={`${item.name}-${dataset.id}`}
                                        className="tag-action"
                                        type="button"
                                        onClick={() => navigate(`/datasets/${encodeURIComponent(String(dataset.id))}`)}
                                      >
                                        {dataset.name}
                                      </button>
                                    ) : (
                                      <span key={`${item.name}-${dataset.name}`} className="tag">
                                        {dataset.name}
                                      </span>
                                    ),
                                  )}
                                </div>
                              ) : (
                                <span className="tag muted">No datasets materialized yet</span>
                              )}
                            </label>
                          );
                        })}
                      </div>
                    ) : (
                      <PageEmpty title="No resources" message="This connector did not expose sync resources." />
                    )}
                  </Panel>

                  <section className="summary-grid">
                    <Panel title="Sync Control" eyebrow="Action">
                      <form className="form-grid compact" onSubmit={handleSync}>
                        <label className="field">
                          <span>Sync mode</span>
                          <select
                            className="select-input"
                            value={syncMode}
                            onChange={(event) => setSyncMode(event.target.value)}
                            disabled={syncing}
                          >
                            <option value="INCREMENTAL">INCREMENTAL</option>
                            <option value="FULL_REFRESH">FULL_REFRESH</option>
                          </select>
                        </label>
                        <label className="checkbox-field">
                          <input
                            type="checkbox"
                            checked={forceFullRefresh}
                            onChange={(event) => setForceFullRefresh(event.target.checked)}
                            disabled={syncing}
                          />
                          <span>Force full refresh</span>
                        </label>
                        <div className="field field-full">
                          <div className="callout">
                            <strong>Selected resources: {selectedResources.length}</strong>
                            <span>
                              Runtime sync stays local and single-workspace. Dataset materialization is still runtime-owned,
                              not a cloud orchestration flow.
                            </span>
                          </div>
                        </div>
                        {syncError ? <div className="error-banner field-full">{syncError}</div> : null}
                        <div className="page-actions field-full">
                          <button
                            className="primary-button"
                            type="submit"
                            disabled={syncing || selectedResources.length === 0}
                          >
                            {syncing ? "Running sync..." : "Run connector sync"}
                          </button>
                        </div>
                      </form>
                      {syncResult ? (
                        <div className="callout success">
                          <strong>{syncResult.summary || "Sync completed"}</strong>
                          <span>
                            {Array.isArray(syncResult.resources)
                              ? syncResult.resources
                                  .map((item) => `${item.resource_name}: ${item.records_synced || 0} records`)
                                  .join(" | ")
                              : "The connector reported a completed sync."}
                          </span>
                        </div>
                      ) : null}
                    </Panel>

                    <Panel title="Selected Output" eyebrow="Scope">
                      {selectedResourceItems.length > 0 ? (
                        <div className="stack-list">
                          {selectedResourceItems.map((item) => (
                            <div key={`selected-${item.name}`} className="list-card static">
                              <strong>{item.label || item.name}</strong>
                              <span>
                                {[item.default_sync_mode, item.supports_incremental ? "incremental" : "full refresh"]
                                  .filter(Boolean)
                                  .join(" | ")}
                              </span>
                              <small>
                                {Array.isArray(item.dataset_names) && item.dataset_names.length > 0
                                  ? item.dataset_names.join(", ")
                                  : "No datasets materialized yet"}
                              </small>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <PageEmpty title="No scope selected" message="Pick one or more resources to define the next sync." />
                      )}
                    </Panel>
                  </section>

                  <Panel title="Runtime Sync State" eyebrow="History">
                    {states.error ? <div className="error-banner">{states.error}</div> : null}
                    {states.loading ? (
                      <div className="empty-box">Loading sync state...</div>
                    ) : states.items.length > 0 ? (
                      <div className="stack-list">
                        {states.items.map((item) => (
                          <div key={item.id || item.resource_name} className="list-card static">
                            <strong>{item.resource_name}</strong>
                            <span>
                              {[item.status, item.sync_mode, `${item.records_synced || 0} records`]
                                .filter(Boolean)
                                .join(" | ")}
                            </span>
                            <small>
                              {[`Datasets: ${formatList(item.dataset_names)}`, `Last sync: ${formatDateTime(item.last_sync_at)}`]
                                .filter(Boolean)
                                .join(" | ")}
                            </small>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <PageEmpty title="No sync history" message="Run a sync to create runtime sync state." />
                    )}
                  </Panel>
                </>
              ) : (
                <Panel title="Sync" eyebrow="Operational">
                  <PageEmpty
                    title="Query-only connector"
                    message="This connector does not expose runtime sync resources, so the first migration slice keeps it read-only."
                  />
                </Panel>
              )}
            </>
          ) : (
            <Panel title="Connector Detail" eyebrow="Runtime">
              <PageEmpty title="No connector selected" message="Pick a connector to inspect its runtime capabilities." />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}

function DatasetsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload } = useAsyncData(fetchDatasets);
  const datasets = Array.isArray(data?.items) ? data.items : [];
  const selected = resolveItemByRef(datasets, params.id);
  const filteredDatasets = datasets.filter((item) => {
    const haystack = [
      item.name,
      item.label,
      item.description,
      item.connector,
      item.semantic_model,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [preview, setPreview] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const [previewLimit, setPreviewLimit] = useState("25");
  const boundConnectorCount = countUniqueValues(datasets, (item) => item.connector);
  const boundSemanticModelCount = countUniqueValues(datasets, (item) => item.semantic_model);
  const schemaColumns = Array.isArray(detail?.columns) ? detail.columns : [];
  const nullableColumns = schemaColumns.filter((column) => column.nullable).length;
  const computedColumns = schemaColumns.filter((column) => column.is_computed).length;
  const policy = detail?.policy && typeof detail.policy === "object" ? detail.policy : null;
  const previewResult = preview ? normalizeTabularResult(preview) : null;

  async function loadDatasetDetail(target = selected) {
    if (!target) {
      setDetail(null);
      setPreview(null);
      return;
    }
    setDetailLoading(true);
    setDetailError("");
    setPreviewLoading(true);
    setPreviewError("");
    try {
      const [detailPayload, previewPayload] = await Promise.all([
        fetchDataset(String(target.id || target.name)),
        previewDataset(String(target.id || target.name), {
          limit: Number(previewLimit) > 0 ? Number(previewLimit) : 25,
        }),
      ]);
      setDetail(detailPayload);
      setPreview(previewPayload);
    } catch (caughtError) {
      const message = getErrorMessage(caughtError);
      setDetail(null);
      setDetailError(message);
      setPreview(null);
      setPreviewError(message);
    } finally {
      setDetailLoading(false);
      setPreviewLoading(false);
    }
  }

  useEffect(() => {
    void loadDatasetDetail(selected);
  }, [selected?.id, selected?.name]);

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Dataset management</p>
          <h2>Runtime datasets with binding, schema, and execution detail restored</h2>
          <p className="hero-copy">
            Dataset surfaces stay runtime-first, but no longer collapse to a thin metadata list. The
            active dataset keeps connector and semantic bindings, schema inspection, and query preview in
            one operational view.
          </p>
        </div>
        <div className="hint-list">
          <span>Datasets: {datasets.length}</span>
          <span>Bound connectors: {boundConnectorCount}</span>
          <span>Semantic coverage: {boundSemanticModelCount}</span>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={Database}
          label="Datasets"
          value={formatValue(datasets.length)}
          detail="Datasets available to runtime SQL, BI, and agents."
        />
        <MetricCard
          icon={Cable}
          label="Connectors in use"
          value={formatValue(boundConnectorCount)}
          detail="Distinct connector bindings across runtime datasets."
        />
        <MetricCard
          icon={Layers3}
          label="Semantic links"
          value={formatValue(boundSemanticModelCount)}
          detail="Datasets already attached to semantic models."
        />
        <MetricCard
          icon={SearchCheck}
          label="Selected columns"
          value={formatValue(schemaColumns.length)}
          detail="Column metadata exposed for the active dataset."
        />
      </section>

      <section className="toolbar">
        <input
          className="text-input search-input"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Filter datasets by name, connector, or semantic model"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh datasets"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Dataset Inventory" eyebrow="Runtime" className="list-panel">
          {filteredDatasets.length > 0 ? (
            <div className="stack-list">
              {filteredDatasets.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/datasets/${buildItemRef(item)}`}
                >
                  <strong>{item.label || item.name}</strong>
                  <span>{[item.connector, item.semantic_model].filter(Boolean).join(" | ") || "No bindings"}</span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No datasets found" message="Adjust the filter or define datasets in the runtime config." />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel
                title={detail?.label || selected.label || selected.name}
                eyebrow="Dataset Detail"
                actions={
                  <div className="panel-actions-inline">
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => navigate("/sql")}
                    >
                      Open SQL
                    </button>
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => void loadDatasetDetail()}
                      disabled={detailLoading || previewLoading}
                    >
                      {detailLoading || previewLoading ? "Refreshing..." : "Refresh detail"}
                    </button>
                  </div>
                }
              >
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading dataset detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.connector || "No connector binding"}</span>
                      <span>{detail.semantic_model || "No semantic model binding"}</span>
                      <span>{detail.dataset_type || "runtime dataset"}</span>
                    </div>
                    {Array.isArray(detail.tags) && detail.tags.length > 0 ? (
                      <div className="tag-list">
                        {detail.tags.map((tag) => (
                          <span key={tag} className="tag">
                            #{tag}
                          </span>
                        ))}
                      </div>
                    ) : null}
                    <DetailList
                      items={[
                        { label: "Name", value: formatValue(detail.name) },
                        { label: "SQL alias", value: formatValue(detail.sql_alias) },
                        { label: "Connector", value: formatValue(detail.connector) },
                        { label: "Semantic model", value: formatValue(detail.semantic_model) },
                        { label: "Type", value: formatValue(detail.dataset_type) },
                        { label: "Managed", value: formatValue(detail.managed) },
                        { label: "Tags", value: formatList(detail.tags) },
                      ]}
                    />
                  </>
                ) : (
                  <PageEmpty title="No detail" message="The runtime did not return dataset detail for this item." />
                )}
              </Panel>

              <section className="summary-grid">
                <Panel title="Bindings and Execution" eyebrow="Operational">
                  {detail ? (
                    <DetailList
                      items={[
                        { label: "Source kind", value: formatValue(detail.source_kind) },
                        { label: "Storage kind", value: formatValue(detail.storage_kind) },
                        { label: "Storage URI", value: formatValue(detail.storage_uri) },
                        { label: "Table name", value: formatValue(detail.table_name) },
                        { label: "Dialect", value: formatValue(detail.dialect) },
                        { label: "Preview row count", value: formatValue(preview?.rowCount || preview?.row_count_preview) },
                      ]}
                    />
                  ) : (
                    <PageEmpty title="No runtime binding" message="Select a dataset to inspect execution metadata." />
                  )}
                </Panel>

                <Panel title="Schema Signals" eyebrow="Columns">
                  {detail ? (
                    <DetailList
                      items={[
                        { label: "Columns", value: formatValue(schemaColumns.length) },
                        { label: "Nullable columns", value: formatValue(nullableColumns) },
                        { label: "Computed columns", value: formatValue(computedColumns) },
                        { label: "Preview limit", value: formatValue(previewLimit) },
                      ]}
                    />
                  ) : (
                    <PageEmpty title="No schema signals" message="Select a dataset to inspect schema detail." />
                  )}
                </Panel>
              </section>

              <Panel title="Dataset Workspace" eyebrow="Inspect">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "schema", label: "Schema" },
                    { value: "preview", label: "Preview" },
                    { value: "runtime", label: "Runtime Meta" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="page-stack">
                    <div className="detail-card-grid">
                      <article className="detail-card">
                        <strong>Connector binding</strong>
                        <span>{detail?.connector || "None"}</span>
                        {detail?.connector_id ? (
                          <button
                            className="ghost-button"
                            type="button"
                            onClick={() => navigate(`/connectors/${encodeURIComponent(String(detail.connector_id))}`)}
                          >
                            Open connector
                          </button>
                        ) : null}
                      </article>
                      <article className="detail-card">
                        <strong>Semantic binding</strong>
                        <span>{detail?.semantic_model || "Not attached"}</span>
                        <button className="ghost-button" type="button" onClick={() => navigate("/semantic-models")}>
                          Open semantic models
                        </button>
                      </article>
                      <article className="detail-card">
                        <strong>SQL alias</strong>
                        <span>{detail?.sql_alias || toSqlAlias(detail?.name || selected.name)}</span>
                        <small>Use this alias from the runtime SQL workspace.</small>
                      </article>
                      <article className="detail-card">
                        <strong>Policy posture</strong>
                        <span>{policy ? `${policy.max_rows_preview || "n/a"} preview rows` : "No policy metadata"}</span>
                        <small>Runtime UI intentionally excludes cloud revisioning and governance workflows.</small>
                      </article>
                    </div>
                  </div>
                ) : null}

                {activeTab === "schema" ? (
                  schemaColumns.length > 0 ? (
                    <div className="table-wrap">
                      <table className="result-table">
                        <thead>
                          <tr>
                            <th>Name</th>
                            <th>Type</th>
                            <th>Nullable</th>
                            <th>Computed</th>
                            <th>Description</th>
                          </tr>
                        </thead>
                        <tbody>
                          {schemaColumns.map((column) => (
                            <tr key={column.id || column.name}>
                              <td>{column.name}</td>
                              <td>{formatValue(column.data_type)}</td>
                              <td>{formatValue(column.nullable)}</td>
                              <td>{formatValue(column.is_computed)}</td>
                              <td>{formatValue(column.description)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <PageEmpty title="No column metadata" message="This dataset did not expose column metadata." />
                  )
                ) : null}

                {activeTab === "preview" ? (
                  <>
                    <div className="panel-actions-inline">
                      <input
                        className="text-input narrow-input"
                        type="number"
                        min="1"
                        value={previewLimit}
                        onChange={(event) => setPreviewLimit(event.target.value)}
                      />
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => void loadDatasetDetail()}
                        disabled={previewLoading}
                      >
                        {previewLoading ? "Loading..." : "Run preview"}
                      </button>
                      {previewResult ? (
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() =>
                            downloadTextFile(
                              `${toSqlAlias(detail?.name || selected.name)}-preview.csv`,
                              toCsvText(previewResult),
                              "text/csv;charset=utf-8",
                            )
                          }
                        >
                          Download CSV
                        </button>
                      ) : null}
                    </div>
                    {previewError ? <div className="error-banner">{previewError}</div> : null}
                    {previewLoading ? (
                      <div className="empty-box">Running dataset preview...</div>
                    ) : previewResult ? (
                      <>
                        <div className="inline-notes">
                          <span>Rows: {formatValue(preview.rowCount || preview.row_count_preview)}</span>
                          <span>Limit: {formatValue(preview.effective_limit || previewLimit)}</span>
                          <span>Redaction: {formatValue(preview.redaction_applied)}</span>
                        </div>
                        <ResultTable result={previewResult} maxPreviewRows={12} />
                        {preview.generated_sql ? <pre className="code-block">{preview.generated_sql}</pre> : null}
                      </>
                    ) : (
                      <PageEmpty title="No preview" message="Run a preview to inspect dataset rows from the runtime." />
                    )}
                  </>
                ) : null}

                {activeTab === "runtime" ? (
                  <div className="summary-grid">
                    <Panel title="Policy" eyebrow="Runtime Guardrails" className="panel--flat">
                      {policy ? (
                        <DetailList
                          items={[
                            { label: "Max preview rows", value: formatValue(policy.max_rows_preview) },
                            { label: "Max export rows", value: formatValue(policy.max_export_rows) },
                            { label: "Allow DML", value: formatValue(policy.allow_dml) },
                            { label: "Redaction rules", value: formatValue(Object.keys(policy.redaction_rules || {}).length) },
                            { label: "Row filters", value: formatValue((policy.row_filters || []).length) },
                          ]}
                        />
                      ) : (
                        <PageEmpty title="No policy metadata" message="This dataset did not expose runtime policy data." />
                      )}
                    </Panel>
                    <Panel title="Execution" eyebrow="Runtime Contracts" className="panel--flat">
                      {detail ? (
                        <>
                          <div className="detail-card">
                            <strong>Relation identity</strong>
                            <pre className="code-block compact">{renderJson(detail.relation_identity || {})}</pre>
                          </div>
                          <div className="detail-card">
                            <strong>Execution capabilities</strong>
                            <pre className="code-block compact">{renderJson(detail.execution_capabilities || {})}</pre>
                          </div>
                        </>
                      ) : (
                        <PageEmpty title="No runtime metadata" message="Select a dataset to inspect runtime execution metadata." />
                      )}
                    </Panel>
                  </div>
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Dataset Detail" eyebrow="Runtime">
              <PageEmpty title="No dataset selected" message="Pick a dataset to inspect its metadata and preview rows." />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}

function SemanticModelsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [fieldSearch, setFieldSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload } = useAsyncData(fetchSemanticModels);
  const models = Array.isArray(data?.items) ? data.items : [];
  const selected = resolveItemByRef(models, params.id);
  const filteredModels = models.filter((item) => {
    const haystack = [
      item.name,
      item.description,
      ...(Array.isArray(item.dataset_names) ? item.dataset_names : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const semanticDatasets = extractSemanticDatasets(detail);
  const semanticFields = extractSemanticFields(detail);
  const deferredFieldSearch = useDeferredValue(fieldSearch);
  const filteredSemanticDatasets = semanticDatasets
    .map((dataset) => {
      const searchTerm = String(deferredFieldSearch || "").trim().toLowerCase();
      if (!searchTerm) {
        return dataset;
      }
      return {
        ...dataset,
        dimensions: dataset.dimensions.filter((item) =>
          String(item?.name || "").toLowerCase().includes(searchTerm),
        ),
        measures: dataset.measures.filter((item) =>
          String(item?.name || "").toLowerCase().includes(searchTerm),
        ),
      };
    })
    .filter(
      (dataset) =>
        !deferredFieldSearch ||
        String(dataset.name).toLowerCase().includes(String(deferredFieldSearch).toLowerCase()) ||
        dataset.dimensions.length > 0 ||
        dataset.measures.length > 0,
    );

  useEffect(() => {
    let cancelled = false;

    async function loadDetail() {
      if (!selected) {
        setDetail(null);
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchSemanticModel(String(selected.id || selected.name));
        if (!cancelled) {
          setDetail(payload);
        }
      } catch (caughtError) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadDetail();

    return () => {
      cancelled = true;
    };
  }, [selected?.id, selected?.name]);

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Semantic models</p>
          <h2>Field-rich model exploration instead of a YAML-only migration stub</h2>
          <p className="hero-copy">
            The runtime semantic surface now restores dataset grouping, measure and dimension inventory,
            and model-level context alongside the YAML definition that still anchors the runtime contract.
          </p>
        </div>
        <div className="hint-list">
          <span>Models: {models.length}</span>
          <span>Active: {selected?.name || "none"}</span>
          <span>Datasets in active model: {semanticDatasets.length}</span>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={Layers3}
          label="Models"
          value={formatValue(models.length)}
          detail="Semantic models exposed by the runtime."
        />
        <MetricCard
          icon={Database}
          label="Datasets"
          value={formatValue(detail?.dataset_count || semanticDatasets.length)}
          detail="Semantic dataset groups in the active model."
        />
        <MetricCard
          icon={SearchCheck}
          label="Dimensions"
          value={formatValue(detail?.dimension_count || semanticFields.dimensions.length)}
          detail="Dimension fields available for runtime BI and agents."
        />
        <MetricCard
          icon={Sparkles}
          label="Measures"
          value={formatValue(detail?.measure_count || semanticFields.measures.length)}
          detail="Measures available for semantic query and visualization."
        />
      </section>

      <section className="toolbar">
        <input
          className="text-input search-input"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Filter semantic models by name or dataset"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh semantic models"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Semantic Models" eyebrow="Runtime" className="list-panel">
          {filteredModels.length > 0 ? (
            <div className="stack-list">
              {filteredModels.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/semantic-models/${buildItemRef(item)}`}
                >
                  <strong>{item.name}</strong>
                  <span>
                    {[`${item.dataset_count || 0} datasets`, `${item.measure_count || 0} measures`, item.default ? "default" : null]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No semantic models" message="This runtime does not expose semantic model metadata yet." />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel title={selected.name} eyebrow="Model Detail">
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading semantic model detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.default ? "Default runtime model" : "Secondary model"}</span>
                      <span>{detail.dataset_count || semanticDatasets.length} semantic datasets</span>
                      <span>{detail.measure_count || semanticFields.measures.length} measures</span>
                    </div>
                    <DetailList
                      items={[
                        { label: "Description", value: formatValue(detail.description) },
                        { label: "Default", value: formatValue(detail.default) },
                        { label: "Datasets", value: formatList(detail.dataset_names) },
                        { label: "Dimension count", value: formatValue(detail.dimension_count) },
                        { label: "Measure count", value: formatValue(detail.measure_count) },
                      ]}
                    />
                    <div className="panel-actions-inline">
                      <button className="ghost-button" type="button" onClick={() => navigate("/bi")}>
                        Open BI
                      </button>
                      <button className="ghost-button" type="button" onClick={() => navigate("/chat")}>
                        Open chat
                      </button>
                    </div>
                  </>
                ) : (
                  <PageEmpty title="No detail" message="The runtime did not return semantic model detail." />
                )}
              </Panel>

              <section className="summary-grid">
                <Panel title="Dataset Explorer" eyebrow="Model Structure">
                  {semanticDatasets.length > 0 ? (
                    <div className="detail-card-grid">
                      {semanticDatasets.map((item) => (
                        <article key={item.name} className="detail-card">
                          <strong>{item.name}</strong>
                          <span>{item.relationName || "No explicit relation name"}</span>
                          <div className="tag-list">
                            <span className="tag">{item.dimensions.length} dimensions</span>
                            <span className="tag">{item.measures.length} measures</span>
                          </div>
                          <small>
                            {[item.dimensions.slice(0, 3).map((field) => field.name).join(", "), item.measures.slice(0, 3).map((field) => field.name).join(", ")]
                              .filter(Boolean)
                              .join(" | ")}
                          </small>
                        </article>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty title="No semantic datasets" message="This model did not expose semantic dataset groups." />
                  )}
                </Panel>

                <Panel title="Field Inventory" eyebrow="Dimensions and Measures">
                  {semanticFields.dimensions.length > 0 || semanticFields.measures.length > 0 ? (
                    <div className="field-section-list">
                      <div className="field-group">
                        <div className="field-group-header">
                          <strong>Dimensions</strong>
                          <span>{semanticFields.dimensions.length}</span>
                        </div>
                        <div className="field-pill-list">
                          {semanticFields.dimensions.map((item) => (
                            <span key={item.value} className="field-pill static">
                              {item.label}
                            </span>
                          ))}
                        </div>
                      </div>
                      <div className="field-group">
                        <div className="field-group-header">
                          <strong>Measures</strong>
                          <span>{semanticFields.measures.length}</span>
                        </div>
                        <div className="field-pill-list">
                          {semanticFields.measures.map((item) => (
                            <span key={item.value} className="field-pill static">
                              {item.label}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <PageEmpty title="No fields exposed" message="This model did not expose dimensions or measures." />
                  )}
                </Panel>
              </section>

              <Panel title="Semantic Workspace" eyebrow="Inspect">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "datasets", label: "Datasets" },
                    { value: "fields", label: "Fields" },
                    { value: "yaml", label: "YAML" },
                    { value: "json", label: "JSON" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="detail-card-grid">
                    {semanticDatasets.map((item) => (
                      <article key={item.name} className="detail-card">
                        <strong>{item.name}</strong>
                        <span>{item.relationName || "No relation name provided"}</span>
                        <div className="tag-list">
                          <span className="tag">{item.dimensions.length} dimensions</span>
                          <span className="tag">{item.measures.length} measures</span>
                        </div>
                        <small>
                          {[item.dimensions.slice(0, 3).map((field) => field.name).join(", "), item.measures.slice(0, 3).map((field) => field.name).join(", ")]
                            .filter(Boolean)
                            .join(" | ")}
                        </small>
                      </article>
                    ))}
                  </div>
                ) : null}

                {activeTab === "datasets" ? (
                  filteredSemanticDatasets.length > 0 ? (
                    <div className="field-section-list">
                      {filteredSemanticDatasets.map((dataset) => (
                        <div key={dataset.name} className="field-group">
                          <div className="field-group-header">
                            <strong>{dataset.name}</strong>
                            <span>{dataset.relationName || "semantic dataset"}</span>
                          </div>
                          <div className="tag-list">
                            <span className="tag">{dataset.dimensions.length} dimensions</span>
                            <span className="tag">{dataset.measures.length} measures</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty title="No semantic datasets" message="This model did not expose semantic dataset groups." />
                  )
                ) : null}

                {activeTab === "fields" ? (
                  <div className="page-stack">
                    <label className="field">
                      <span>Find fields</span>
                      <input
                        className="text-input"
                        type="search"
                        value={fieldSearch}
                        onChange={(event) => setFieldSearch(event.target.value)}
                        placeholder="Filter datasets, dimensions, or measures"
                      />
                    </label>
                    {filteredSemanticDatasets.length > 0 ? (
                      <div className="field-section-list">
                        {filteredSemanticDatasets.map((dataset) => (
                          <div key={`${dataset.name}-fields`} className="field-group">
                            <div className="field-group-header">
                              <strong>{dataset.name}</strong>
                              <span>{dataset.relationName || "semantic dataset"}</span>
                            </div>
                            <div className="field-pill-list">
                              {dataset.dimensions.map((item) => (
                                <span key={`${dataset.name}-${item.name}-dimension`} className="field-pill static">
                                  {item.name}
                                </span>
                              ))}
                              {dataset.measures.map((item) => (
                                <span key={`${dataset.name}-${item.name}-measure`} className="field-pill static">
                                  {item.name}
                                </span>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <PageEmpty title="No fields found" message="Adjust the filter or switch models." />
                    )}
                  </div>
                ) : null}

                {activeTab === "yaml" ? (
                  detail?.content_yaml ? (
                    <pre className="code-block">{detail.content_yaml}</pre>
                  ) : (
                    <PageEmpty title="No YAML available" message="This semantic model did not expose YAML content." />
                  )
                ) : null}

                {activeTab === "json" ? (
                  detail?.content_json ? (
                    <pre className="code-block">{renderJson(detail.content_json)}</pre>
                  ) : (
                    <PageEmpty title="No JSON payload" message="This semantic model did not expose a JSON representation." />
                  )
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Semantic Model Detail" eyebrow="Runtime">
              <PageEmpty title="No model selected" message="Pick a semantic model to inspect its runtime definition." />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}

function SqlWorkspacePage() {
  const connectorsState = useAsyncData(fetchConnectors);
  const datasetsState = useAsyncData(fetchDatasets);
  const [activeTab, setActiveTab] = useState("results");
  const [form, setForm] = useState({
    query: DEFAULT_SQL_QUERY,
    connectionName: "",
    requestedLimit: "200",
  });
  const [result, setResult] = useState(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [workspaceNotice, setWorkspaceNotice] = useState("");
  const [selectedSavedId, setSelectedSavedId] = useState("");
  const [savedName, setSavedName] = useState("");
  const [savedTags, setSavedTags] = useState("");
  const [savedQueries, setSavedQueries] = usePersistentState(SQL_SAVED_STORAGE_KEY, []);
  const [historyItems, setHistoryItems] = usePersistentState(SQL_HISTORY_STORAGE_KEY, []);

  async function handleSubmit(event) {
    event.preventDefault();
    setRunning(true);
    setError("");
    setWorkspaceNotice("");
    try {
      const payload = {
        query: form.query,
        requested_limit: Number(form.requestedLimit) > 0 ? Number(form.requestedLimit) : undefined,
      };
      if (form.connectionName) {
        payload.connection_name = form.connectionName;
      }
      const response = await querySql(payload);
      setResult(response);
      setActiveTab("results");
      setHistoryItems((current) => [
        {
          id: createLocalId("sql-run"),
          createdAt: new Date().toISOString(),
          connectionName: form.connectionName,
          requestedLimit: form.requestedLimit,
          query: form.query,
          rowCount: response?.rowCount || response?.row_count_preview || 0,
          durationMs: response?.duration_ms || null,
          status: response?.status || "succeeded",
        },
        ...current,
      ].slice(0, 20));
    } catch (caughtError) {
      setResult(null);
      setError(getErrorMessage(caughtError));
      setHistoryItems((current) => [
        {
          id: createLocalId("sql-run"),
          createdAt: new Date().toISOString(),
          connectionName: form.connectionName,
          requestedLimit: form.requestedLimit,
          query: form.query,
          rowCount: 0,
          durationMs: null,
          status: "failed",
        },
        ...current,
      ].slice(0, 20));
    } finally {
      setRunning(false);
    }
  }

  const connectors = Array.isArray(connectorsState.data?.items) ? connectorsState.data.items : [];
  const datasets = Array.isArray(datasetsState.data?.items) ? datasetsState.data.items : [];
  const queryModeLabel = form.connectionName ? "Direct connector SQL" : "Federated runtime SQL";
  const warnings = useMemo(() => detectSqlWarnings(form.query), [form.query]);
  const normalizedResult = result ? normalizeTabularResult(result) : null;

  function resetWorkbench() {
    setForm({
      query: DEFAULT_SQL_QUERY,
      connectionName: "",
      requestedLimit: "200",
    });
    setWorkspaceNotice("");
    setSelectedSavedId("");
    setSavedName("");
    setSavedTags("");
  }

  function saveCurrentQuery() {
    const nextEntry = {
      id: selectedSavedId || createLocalId("sql"),
      name: String(savedName || "").trim() || `Saved query ${savedQueries.length + 1}`,
      tags: splitCsv(savedTags),
      query: form.query,
      connectionName: form.connectionName,
      requestedLimit: form.requestedLimit,
      updatedAt: new Date().toISOString(),
    };
    setSavedQueries((current) => {
      const next = [nextEntry, ...current.filter((item) => item.id !== nextEntry.id)];
      next.sort((left, right) => String(right.updatedAt || "").localeCompare(String(left.updatedAt || "")));
      return next;
    });
    setSelectedSavedId(nextEntry.id);
    setSavedName(nextEntry.name);
    setSavedTags(nextEntry.tags.join(", "));
    setWorkspaceNotice(`Saved "${nextEntry.name}" to local workspace storage.`);
    setActiveTab("saved");
  }

  function loadSavedQuery(entry) {
    setSelectedSavedId(entry.id);
    setSavedName(entry.name || "");
    setSavedTags(Array.isArray(entry.tags) ? entry.tags.join(", ") : "");
    setForm({
      query: entry.query || DEFAULT_SQL_QUERY,
      connectionName: entry.connectionName || "",
      requestedLimit: entry.requestedLimit || "200",
    });
    setWorkspaceNotice(`Loaded "${entry.name}" into the SQL workbench.`);
  }

  function deleteSavedQueryById(id) {
    setSavedQueries((current) => current.filter((item) => item.id !== id));
    if (selectedSavedId === id) {
      setSelectedSavedId("");
      setSavedName("");
      setSavedTags("");
    }
    setWorkspaceNotice("Removed saved query from local workspace storage.");
  }

  async function handleCopySql() {
    try {
      await copyTextToClipboard(form.query);
      setWorkspaceNotice("SQL copied to clipboard.");
    } catch (caughtError) {
      setWorkspaceNotice(getErrorMessage(caughtError));
    }
  }

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">SQL workspace</p>
          <h2>Bring back a more capable workbench without cloud orchestration weight</h2>
          <p className="hero-copy">
            Keep the runtime SQL surface lightweight, but restore better query setup, starter templates,
            reference panels, and execution feedback for everyday operator workflows.
          </p>
        </div>
        <div className="hint-list">
          <span>Mode: {queryModeLabel}</span>
          <span>Connectors: {connectors.length}</span>
          <span>Datasets: {datasets.length}</span>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={Table2}
          label="Query mode"
          value={queryModeLabel}
          detail="Blank connection runs federated runtime SQL."
        />
        <MetricCard
          icon={Cable}
          label="Connector targets"
          value={formatValue(connectors.length)}
          detail="Connections available for direct connector SQL."
        />
        <MetricCard
          icon={Database}
          label="Dataset aliases"
          value={formatValue(datasets.length)}
          detail="Dataset aliases available for federated runtime SQL."
        />
        <MetricCard
          icon={Activity}
          label="Requested limit"
          value={formatValue(form.requestedLimit)}
          detail="Client-side limit sent to the runtime query API."
        />
      </section>

      <section className="workspace-grid">
        <Panel title="Runtime SQL Workspace" eyebrow="Operational Query Surface">
          <div className="inline-notes">
            <span>Blank connection = federated runtime SQL</span>
            <span>Selected connection = direct SQL against that connector</span>
            <span>No organization or project scope is applied in this shell</span>
          </div>
          {workspaceNotice ? <div className="callout"><strong>Workspace note</strong><span>{workspaceNotice}</span></div> : null}
          <form className="form-grid" onSubmit={handleSubmit}>
            <label className="field">
              <span>Connection override</span>
              <select
                className="select-input"
                value={form.connectionName}
                onChange={(event) =>
                  setForm((current) => ({
                    ...current,
                    connectionName: event.target.value,
                  }))
                }
                disabled={running}
              >
                <option value="">Federated runtime query</option>
                {connectors.map((item) => (
                  <option key={item.id || item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span>Row limit</span>
              <input
                className="text-input"
                type="number"
                min="1"
                value={form.requestedLimit}
                onChange={(event) =>
                  setForm((current) => ({
                    ...current,
                    requestedLimit: event.target.value,
                  }))
                }
                disabled={running}
              />
            </label>
            <label className="field field-full">
              <span>SQL query</span>
              <textarea
                className="textarea-input"
                value={form.query}
                onChange={(event) =>
                  setForm((current) => ({
                    ...current,
                    query: event.target.value,
                  }))
                }
                disabled={running}
                rows={10}
              />
            </label>
            <div className="page-actions">
                <button className="primary-button" type="submit" disabled={running}>
                  {running ? "Running query..." : "Run query"}
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={resetWorkbench}
                  disabled={running}
                >
                  Reset
                </button>
                <button className="ghost-button" type="button" onClick={saveCurrentQuery} disabled={!form.query.trim()}>
                  Save locally
                </button>
                <button className="ghost-button" type="button" onClick={() => void handleCopySql()} disabled={!form.query.trim()}>
                  Copy SQL
                </button>
              </div>
            </form>
            {warnings.length > 0 ? (
              <div className="warning-list">
                {warnings.map((warning) => (
                  <div key={warning} className="callout warning">
                    <strong>Query warning</strong>
                    <span>{warning}</span>
                  </div>
                ))}
              </div>
            ) : null}
            {error ? <div className="error-banner">{error}</div> : null}
        </Panel>

        <div className="sidebar-stack">
          <Panel title="SQL Templates" eyebrow="Starters">
            <div className="template-grid">
              {SQL_TEMPLATES.map((template) => (
                <button
                  key={template.label}
                  className="template-card"
                  type="button"
                  onClick={() =>
                    setForm((current) => ({
                      ...current,
                      query: template.query,
                      connectionName: template.label === "Connector direct SQL" ? connectors[0]?.name || "" : "",
                    }))
                  }
                  disabled={running}
                >
                  <strong>{template.label}</strong>
                  <span>{template.description}</span>
                </button>
              ))}
            </div>
          </Panel>

          <Panel title="Dataset Aliases" eyebrow="Reference">
            {datasets.length > 0 ? (
              <div className="stack-list">
                {datasets.map((item) => (
                  <button
                    key={item.id || item.name}
                    className="list-card"
                    type="button"
                    onClick={() =>
                      setForm((current) => ({
                        ...current,
                        query: `${current.query.trim()}\n-- ${toSqlAlias(item.name)}`.trim(),
                      }))
                    }
                  >
                    <strong>{toSqlAlias(item.name)}</strong>
                    <span>{[item.connector, item.semantic_model].filter(Boolean).join(" | ") || "runtime dataset"}</span>
                  </button>
                ))}
              </div>
            ) : (
              <PageEmpty title="No datasets" message="Add runtime datasets to query them here." />
            )}
          </Panel>

          <Panel title="Connector Targets" eyebrow="Reference">
            {connectors.length > 0 ? (
              <div className="stack-list">
                {connectors.map((item) => (
                  <div key={item.id || item.name} className="list-card static">
                    <strong>{item.name}</strong>
                    <span>{[item.connector_type, item.supports_sync ? "sync enabled" : "query only"].filter(Boolean).join(" | ")}</span>
                  </div>
                ))}
              </div>
            ) : (
              <PageEmpty title="No connectors" message="Define connectors in runtime config to use direct SQL." />
            )}
          </Panel>
        </div>
      </section>

      <Panel title="SQL Console" eyebrow="Results and Workspace Memory">
        <SectionTabs
          tabs={[
            { value: "results", label: "Results" },
            { value: "history", label: "History" },
            { value: "saved", label: "Saved" },
            { value: "reference", label: "Reference" },
          ]}
          value={activeTab}
          onChange={setActiveTab}
        />

        {activeTab === "results" ? (
          normalizedResult ? (
            <>
              <div className="inline-notes">
                <span>Rows: {formatValue(result.rowCount || result.row_count_preview)}</span>
                <span>Duration: {formatValue(result.duration_ms)}</span>
                <span>Redaction: {formatValue(result.redaction_applied)}</span>
              </div>
              <div className="panel-actions-inline">
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() =>
                    downloadTextFile(
                      "runtime-sql-results.csv",
                      toCsvText(normalizedResult),
                      "text/csv;charset=utf-8",
                    )
                  }
                >
                  <Download className="button-icon" aria-hidden="true" />
                  Download CSV
                </button>
                {result.generated_sql ? (
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => void copyTextToClipboard(result.generated_sql).then(() => setWorkspaceNotice("Generated SQL copied to clipboard.")).catch((caughtError) => setWorkspaceNotice(getErrorMessage(caughtError)))}
                  >
                    <Copy className="button-icon" aria-hidden="true" />
                    Copy generated SQL
                  </button>
                ) : null}
              </div>
              <ResultTable result={normalizedResult} maxPreviewRows={16} />
              {result.generated_sql ? <pre className="code-block">{result.generated_sql}</pre> : null}
            </>
          ) : (
            <PageEmpty title="No SQL result yet" message="Run a federated or direct SQL query to inspect runtime results." />
          )
        ) : null}

        {activeTab === "history" ? (
          historyItems.length > 0 ? (
            <div className="stack-list">
              {historyItems.map((item) => (
                <div key={item.id} className="list-card static">
                  <strong>{item.connectionName ? `Direct SQL · ${item.connectionName}` : "Federated runtime SQL"}</strong>
                  <span>{[formatDateTime(item.createdAt), item.status, `${item.rowCount || 0} rows`].filter(Boolean).join(" | ")}</span>
                  <small>{item.query}</small>
                  <div className="panel-actions-inline">
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() =>
                        setForm({
                          query: item.query || DEFAULT_SQL_QUERY,
                          connectionName: item.connectionName || "",
                          requestedLimit: item.requestedLimit || "200",
                        })
                      }
                    >
                      Load
                    </button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <PageEmpty title="No local history" message="Executed runtime queries will appear here for this browser." />
          )
        ) : null}

        {activeTab === "saved" ? (
          <div className="summary-grid">
            <div className="page-stack">
              <label className="field">
                <span>Saved query name</span>
                <input
                  className="text-input"
                  type="text"
                  value={savedName}
                  onChange={(event) => setSavedName(event.target.value)}
                  placeholder="Revenue by region"
                />
              </label>
              <label className="field">
                <span>Tags</span>
                <input
                  className="text-input"
                  type="text"
                  value={savedTags}
                  onChange={(event) => setSavedTags(event.target.value)}
                  placeholder="finance, weekly"
                />
              </label>
              <div className="page-actions">
                <button className="primary-button" type="button" onClick={saveCurrentQuery}>
                  <Save className="button-icon" aria-hidden="true" />
                  {selectedSavedId ? "Update saved query" : "Save query"}
                </button>
              </div>
            </div>

            {savedQueries.length > 0 ? (
              <div className="stack-list">
                {savedQueries.map((item) => (
                  <div key={item.id} className={`list-card static ${selectedSavedId === item.id ? "active" : ""}`}>
                    <strong>{item.name}</strong>
                    <span>{[formatDateTime(item.updatedAt), ...(Array.isArray(item.tags) ? item.tags : [])].filter(Boolean).join(" | ")}</span>
                    <small>{item.query}</small>
                    <div className="panel-actions-inline">
                      <button className="ghost-button" type="button" onClick={() => loadSavedQuery(item)}>
                        Load
                      </button>
                      <button className="ghost-button" type="button" onClick={() => deleteSavedQueryById(item.id)}>
                        Delete
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <PageEmpty title="No saved queries" message="Save the current SQL to keep a local runtime workbench library." />
            )}
          </div>
        ) : null}

        {activeTab === "reference" ? (
          <div className="summary-grid">
            <Panel title="Datasets" eyebrow="Federated">
              {datasets.length > 0 ? (
                <div className="stack-list">
                  {datasets.map((item) => (
                    <div key={`ref-${item.id || item.name}`} className="list-card static">
                      <strong>{item.name}</strong>
                      <span>{toSqlAlias(item.name)}</span>
                      <small>{[item.connector, item.semantic_model].filter(Boolean).join(" | ") || "runtime dataset"}</small>
                    </div>
                  ))}
                </div>
              ) : (
                <PageEmpty title="No datasets" message="Runtime datasets appear here as federated SQL aliases." />
              )}
            </Panel>
            <Panel title="Connectors" eyebrow="Direct SQL">
              {connectors.length > 0 ? (
                <div className="stack-list">
                  {connectors.map((item) => (
                    <div key={`connector-${item.id || item.name}`} className="list-card static">
                      <strong>{item.name}</strong>
                      <span>{[item.connector_type, item.supports_sync ? "sync enabled" : "query only"].filter(Boolean).join(" | ")}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <PageEmpty title="No connectors" message="Define connectors in runtime config to use direct SQL." />
              )}
            </Panel>
          </div>
        ) : null}
      </Panel>
    </div>
  );
}

function AgentsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const [trialMessage, setTrialMessage] = useState(
    "Summarize the most relevant runtime signals for this workspace.",
  );
  const [trialResponse, setTrialResponse] = useState(null);
  const [trialError, setTrialError] = useState("");
  const [trialRunning, setTrialRunning] = useState(false);
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload } = useAsyncData(fetchAgents);
  const agents = Array.isArray(data?.items) ? data.items : [];
  const selected = resolveItemByRef(agents, params.id);
  const filteredAgents = agents.filter((item) => {
    const haystack = [
      item.name,
      item.description,
      item.llm_connection,
      ...(Array.isArray(item.tools) ? item.tools.map((tool) => tool.name) : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const totalTools = agents.reduce((sum, item) => sum + Number(item.tool_count || 0), 0);
  const enabledFeatureFlags = readAgentFeatureFlags(detail);
  const allowedConnectors = readAgentAllowedConnectors(detail);
  const systemPrompt = readAgentSystemPrompt(detail);
  const trialResult = trialResponse?.result ? normalizeTabularResult(trialResponse.result) : null;
  const trialVisualization = normalizeVisualizationSpec(trialResponse?.visualization);

  useEffect(() => {
    let cancelled = false;

    async function loadDetail() {
      if (!selected) {
        setDetail(null);
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchAgent(String(selected.id || selected.name));
        if (!cancelled) {
          setDetail(payload);
        }
      } catch (caughtError) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadDetail();

    return () => {
      cancelled = true;
    };
  }, [selected?.id, selected?.name]);

  async function handleQuickAsk(event) {
    event.preventDefault();
    if (!selected?.name || !trialMessage.trim()) {
      return;
    }
    setTrialRunning(true);
    setTrialError("");
    setTrialResponse(null);
    try {
      const payload = await askAgent({
        message: trialMessage.trim(),
        agent_name: selected.name,
        title: `Quick run · ${selected.name}`,
      });
      setTrialResponse(payload);
    } catch (caughtError) {
      setTrialError(getErrorMessage(caughtError));
    } finally {
      setTrialRunning(false);
    }
  }

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Agent management</p>
          <h2>Restore more of the runtime agent depth without turning this into cloud orchestration</h2>
          <p className="hero-copy">
            Agent surfaces again show prompt posture, tool bindings, access policy, and execution shape
            instead of stopping at a minimal inventory page.
          </p>
        </div>
        <div className="hint-list">
          <span>Agents: {agents.length}</span>
          <span>Total tools: {totalTools}</span>
          <span>Selected: {selected?.name || "none"}</span>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={Bot}
          label="Agents"
          value={formatValue(agents.length)}
          detail="Runtime agent definitions available to chat and BI."
        />
        <MetricCard
          icon={Workflow}
          label="Tool bindings"
          value={formatValue(totalTools)}
          detail="Tools exposed by the listed runtime agents."
        />
        <MetricCard
          icon={BrainCircuit}
          label="Selected tools"
          value={formatValue(detail?.tools?.length || 0)}
          detail="Tools attached to the active agent."
        />
        <MetricCard
          icon={ShieldCheck}
          label="Allowed connectors"
          value={formatValue(allowedConnectors.length)}
          detail="Connector allow-list entries on the active agent."
        />
      </section>

      <section className="toolbar">
        <input
          className="text-input search-input"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Filter agents by name, connection, or tool"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh agents"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Agent Inventory" eyebrow="Runtime" className="list-panel">
          {filteredAgents.length > 0 ? (
            <div className="stack-list">
              {filteredAgents.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/agents/${buildItemRef(item)}`}
                >
                  <strong>{item.name}</strong>
                  <span>
                    {[item.llm_connection, `${item.tool_count || 0} tools`, item.default ? "default" : null]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty title="No agents" message="Define runtime agents to use local chat and BI copilots." />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel
                title={selected.name}
                eyebrow="Agent Detail"
                actions={
                  <div className="panel-actions-inline">
                    <button className="ghost-button" type="button" onClick={() => navigate("/chat")}>
                      Open chat
                    </button>
                    <button className="ghost-button" type="button" onClick={() => setActiveTab("definition")}>
                      View definition
                    </button>
                  </div>
                }
              >
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading agent detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.default ? "Default agent" : "Runtime agent"}</span>
                      <span>{detail.llm_connection || "No LLM connection set"}</span>
                      <span>{detail.tools?.length || 0} tools</span>
                    </div>
                    <DetailList
                      items={[
                        { label: "Description", value: formatValue(detail.description) },
                        { label: "LLM connection", value: formatValue(detail.llm_connection) },
                        { label: "Semantic model", value: formatValue(detail.semantic_model) },
                        { label: "Dataset", value: formatValue(detail.dataset) },
                        { label: "Default", value: formatValue(detail.default) },
                      ]}
                    />
                  </>
                ) : (
                  <PageEmpty title="No detail" message="The runtime did not return agent detail." />
                )}
              </Panel>

              <section className="summary-grid">
                <Panel title="Prompt and Execution" eyebrow="Behavior">
                  {detail ? (
                    <>
                      <div className="callout">
                        <strong>System prompt</strong>
                        <span>{systemPrompt || "No explicit system prompt exposed by the runtime."}</span>
                      </div>
                      <DetailList
                        items={[
                          { label: "Execution mode", value: formatValue(detail.definition?.execution?.mode) },
                          { label: "Response mode", value: formatValue(detail.definition?.execution?.response_mode) },
                          { label: "Max iterations", value: formatValue(detail.definition?.execution?.max_iterations) },
                          { label: "Output format", value: formatValue(detail.definition?.output?.format) },
                        ]}
                      />
                    </>
                  ) : (
                    <PageEmpty title="No behavior detail" message="Select an agent to inspect prompt and execution posture." />
                  )}
                </Panel>

                <Panel title="Access Policy" eyebrow="Guardrails">
                  {detail ? (
                    <>
                      {enabledFeatureFlags.length > 0 ? (
                        <div className="tag-list">
                          {enabledFeatureFlags.map((item) => (
                            <span key={item} className="tag">
                              {item}
                            </span>
                          ))}
                        </div>
                      ) : null}
                      <DetailList
                        items={[
                          { label: "Allowed connectors", value: formatList(allowedConnectors) },
                          {
                            label: "Denied connectors",
                            value: formatList(detail.definition?.access_policy?.denied_connectors),
                          },
                          {
                            label: "Moderation enabled",
                            value: formatValue(detail.definition?.guardrails?.moderation_enabled),
                          },
                          {
                            label: "Parallel tools",
                            value: formatValue(detail.definition?.execution?.allow_parallel_tools),
                          },
                        ]}
                      />
                    </>
                  ) : (
                    <PageEmpty title="No policy detail" message="Select an agent to inspect access policy." />
                  )}
                </Panel>
              </section>

              <Panel title="Agent Workspace" eyebrow="Inspect and Try">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "tools", label: "Tools" },
                    { value: "definition", label: "Definition" },
                    { value: "try", label: "Quick Ask" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="detail-card-grid">
                    <article className="detail-card">
                      <strong>Semantic context</strong>
                      <span>{detail?.semantic_model || "No semantic model attached"}</span>
                      <small>{detail?.dataset || "No dataset shortcut configured"}</small>
                    </article>
                    <article className="detail-card">
                      <strong>Tool posture</strong>
                      <span>{detail?.tools?.length || 0} attached tools</span>
                      <small>{enabledFeatureFlags.length > 0 ? enabledFeatureFlags.join(", ") : "No feature flags exposed"}</small>
                    </article>
                    <article className="detail-card">
                      <strong>Connector access</strong>
                      <span>{allowedConnectors.length > 0 ? allowedConnectors.join(", ") : "Unspecified"}</span>
                      <small>Runtime UI does not restore cloud agent-definition editing workflows here.</small>
                    </article>
                  </div>
                ) : null}

                {activeTab === "tools" ? (
                  detail?.tools && detail.tools.length > 0 ? (
                    <div className="detail-card-grid">
                      {detail.tools.map((tool) => (
                        <article key={`${tool.name}-${tool.tool_type}`} className="detail-card">
                          <strong>{tool.name}</strong>
                          <span>{tool.tool_type || "runtime tool"}</span>
                          {tool.description ? <small>{tool.description}</small> : null}
                          {tool.config ? <pre className="code-block compact">{renderJson(tool.config)}</pre> : null}
                        </article>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty title="No tools exposed" message="This agent does not currently expose runtime tool metadata." />
                  )
                ) : null}

                {activeTab === "definition" ? (
                  detail?.definition ? (
                    <pre className="code-block">{renderJson(detail.definition)}</pre>
                  ) : (
                    <PageEmpty title="No definition payload" message="The runtime did not expose a definition snapshot for this agent." />
                  )
                ) : null}

                {activeTab === "try" ? (
                  <div className="page-stack">
                    <form className="form-grid" onSubmit={handleQuickAsk}>
                      <label className="field field-full">
                        <span>Prompt</span>
                        <textarea
                          className="textarea-input"
                          value={trialMessage}
                          onChange={(event) => setTrialMessage(event.target.value)}
                          rows={5}
                          disabled={trialRunning}
                        />
                      </label>
                      <div className="page-actions field-full">
                        <button className="primary-button" type="submit" disabled={trialRunning || !trialMessage.trim()}>
                          {trialRunning ? "Running agent..." : "Run agent"}
                        </button>
                        {trialResponse?.thread_id ? (
                          <button
                            className="ghost-button"
                            type="button"
                            onClick={() => navigate(`/chat/${encodeURIComponent(String(trialResponse.thread_id))}`)}
                          >
                            Open thread
                          </button>
                        ) : null}
                      </div>
                    </form>
                    {trialError ? <div className="error-banner">{trialError}</div> : null}
                    {trialResponse ? (
                      <>
                        <div className="callout">
                          <strong>{trialResponse.summary || "Run completed"}</strong>
                          <span>
                            {trialResponse.thread_id
                              ? `Thread ${trialResponse.thread_id} was created for this quick run.`
                              : "The runtime returned a direct response."}
                          </span>
                        </div>
                        {trialResult ? (
                          <>
                            {trialVisualization && hasRenderableVisualization(trialResponse.visualization) ? (
                              <ChartPreview
                                title={trialVisualization.title}
                                result={trialResult}
                                visualization={trialVisualization}
                                preferredDimension={trialVisualization.x}
                                preferredMeasure={trialVisualization.y?.[0]}
                              />
                            ) : null}
                            <ResultTable result={trialResult} maxPreviewRows={12} />
                          </>
                        ) : null}
                      </>
                    ) : (
                      <PageEmpty title="No quick run yet" message="Run the selected agent here to inspect its current runtime behavior." />
                    )}
                  </div>
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Agent Detail" eyebrow="Runtime">
              <PageEmpty title="No agent selected" message="Pick an agent to inspect its runtime bindings and definition." />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}

function LegacyChatPage() {
  const navigate = useNavigate();
  const params = useParams();
  const threadId = String(params.threadId || "").trim();
  const threadsState = useAsyncData(fetchThreads);
  const agentsState = useAsyncData(fetchAgents);
  const threads = Array.isArray(threadsState.data?.items) ? threadsState.data.items : [];
  const agents = Array.isArray(agentsState.data?.items) ? agentsState.data.items : [];

  const [selectedAgentName, setSelectedAgentName] = useState("");
  const [message, setMessage] = useState(DEFAULT_CHAT_MESSAGE);
  const [draftTitle, setDraftTitle] = useState("Runtime thread");
  const [thread, setThread] = useState(null);
  const [messages, setMessages] = useState([]);
  const [threadLoading, setThreadLoading] = useState(false);
  const [threadError, setThreadError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const [threadMutationError, setThreadMutationError] = useState("");
  const [renaming, setRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState("");
  const [renamingOpen, setRenamingOpen] = useState(false);
  const [creatingThread, setCreatingThread] = useState(false);
  const [deletingThreadId, setDeletingThreadId] = useState("");
  const selectedAgent = agents.find((item) => item.name === selectedAgentName) || null;
  const turns = useMemo(() => buildConversationTurns(messages, agents), [messages, agents]);
  const timelineEndRef = useRef(null);

  useEffect(() => {
    if (!selectedAgentName && agents.length > 0) {
      setSelectedAgentName(agents.find((item) => item.default)?.name || agents[0].name);
    }
  }, [agents, selectedAgentName]);

  useEffect(() => {
    let cancelled = false;

    async function loadThreadState() {
      if (!threadId) {
        setThread(null);
        setMessages([]);
        setThreadError("");
        return;
      }
      setThreadLoading(true);
      setThreadError("");
      try {
        const [threadPayload, messagePayload] = await Promise.all([
          fetchThread(threadId),
          fetchThreadMessages(threadId),
        ]);
        if (cancelled) {
          return;
        }
        setThread(threadPayload);
        setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
        setRenameValue(threadPayload?.title || "");
      } catch (caughtError) {
        if (cancelled) {
          return;
        }
        setThread(null);
        setMessages([]);
        setThreadError(getErrorMessage(caughtError));
      } finally {
        if (!cancelled) {
          setThreadLoading(false);
        }
      }
    }

    void loadThreadState();

    return () => {
      cancelled = true;
    };
  }, [threadId]);

  useEffect(() => {
    timelineEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [turns.length, submitting]);

  async function handleSubmit(event) {
    event.preventDefault();
    if (!selectedAgentName || !message.trim()) {
      return;
    }
    setSubmitting(true);
    setSubmitError("");
    setThreadMutationError("");
    try {
      let activeThreadId = threadId;
      let activeThread = thread;

      if (!activeThreadId) {
        const createdThread = await createThread({
          title: draftTitle.trim() || undefined,
        });
        activeThreadId = String(createdThread?.id || "").trim();
        activeThread = createdThread;
        setThread(createdThread);
        setMessages([]);
        setRenameValue(createdThread?.title || "");
        await threadsState.reload();
        if (activeThreadId) {
          navigate(`/chat/${activeThreadId}`);
        }
      }

      const response = await askAgent({
        message: message.trim(),
        agent_name: selectedAgentName,
        thread_id: activeThreadId || undefined,
      });
      setMessage("");
      setDraftTitle("Runtime thread");
      await threadsState.reload();
      const resolvedThreadId = String(response.thread_id || activeThreadId || "").trim();
      if (resolvedThreadId) {
        const messagePayload = await fetchThreadMessages(resolvedThreadId);
        setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
        const threadPayload = await fetchThread(resolvedThreadId);
        setThread(threadPayload);
        setRenameValue(threadPayload?.title || "");
        if (resolvedThreadId !== threadId) {
          navigate(`/chat/${resolvedThreadId}`);
        }
      } else if (activeThread) {
        setThread(activeThread);
      }
    } catch (caughtError) {
      setSubmitError(getErrorMessage(caughtError));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleCreateThread() {
    setCreatingThread(true);
    setThreadMutationError("");
    try {
      const createdThread = await createThread({
        title: draftTitle.trim() || undefined,
      });
      await threadsState.reload();
      navigate(`/chat/${createdThread.id}`);
    } catch (caughtError) {
      setThreadMutationError(getErrorMessage(caughtError));
    } finally {
      setCreatingThread(false);
    }
  }

  async function handleRenameThread() {
    if (!threadId) {
      return;
    }
    setRenaming(true);
    setThreadMutationError("");
    try {
      const updated = await updateThread(threadId, {
        title: renameValue.trim() || undefined,
      });
      setThread(updated);
      setRenameValue(updated?.title || "");
      setRenamingOpen(false);
      await threadsState.reload();
    } catch (caughtError) {
      setThreadMutationError(getErrorMessage(caughtError));
    } finally {
      setRenaming(false);
    }
  }

  async function handleDeleteThread(targetThreadId) {
    setDeletingThreadId(String(targetThreadId));
    setThreadMutationError("");
    try {
      await deleteThread(targetThreadId);
      await threadsState.reload();
      if (String(targetThreadId) === threadId) {
        navigate("/chat");
        setThread(null);
        setMessages([]);
      } else if (threadId) {
        const messagePayload = await fetchThreadMessages(threadId);
        setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
      }
    } catch (caughtError) {
      setThreadMutationError(getErrorMessage(caughtError));
    } finally {
      setDeletingThreadId("");
    }
  }

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Runtime chat</p>
          <h2>Threaded operational chat with starter prompts and better context framing</h2>
          <p className="hero-copy">
            Keep the runtime chat surface lightweight, but restore more of the product quality around
            thread history, agent selection, prompt starters, and result inspection.
          </p>
        </div>
        <div className="hint-list">
          <span>Threads: {threads.length}</span>
          <span>Agents: {agents.length}</span>
          <span>Turns in view: {turns.length}</span>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={MessageSquareText}
          label="Threads"
          value={formatValue(threads.length)}
          detail="Persisted runtime thread history."
        />
        <MetricCard
          icon={Bot}
          label="Agents"
          value={formatValue(agents.length)}
          detail="Agent definitions available to chat."
        />
        <MetricCard
          icon={Activity}
          label="Active messages"
          value={formatValue(messages.length)}
          detail="Messages loaded for the current thread."
        />
        <MetricCard
          icon={BrainCircuit}
          label="Selected agent"
          value={selectedAgent?.name || "None"}
          detail="Agent currently targeted for the next ask."
        />
      </section>

      <section className="chat-layout">
        <Panel
          title="Threads"
          eyebrow="Runtime History"
          className="thread-panel"
          actions={
            <div className="panel-actions-inline">
              <button className="ghost-button" type="button" onClick={() => void threadsState.reload()} disabled={threadsState.loading}>
                <RefreshCw className="button-icon" aria-hidden="true" />
                Refresh
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={() => void handleCreateThread()}
                disabled={creatingThread}
              >
                <Plus className="button-icon" aria-hidden="true" />
                {creatingThread ? "Creating..." : "New thread"}
              </button>
            </div>
          }
        >
          {threadsState.error ? <div className="error-banner">{threadsState.error}</div> : null}
          {threadMutationError ? <div className="error-banner">{threadMutationError}</div> : null}
          {threads.length > 0 ? (
            <div className="stack-list">
              {threads.map((item) => (
                <div key={item.id} className={`list-card static ${threadId === String(item.id) ? "active" : ""}`}>
                  <button
                    className="thread-link-button"
                    type="button"
                    onClick={() => navigate(`/chat/${item.id}`)}
                  >
                    <span className="thread-link-avatar">
                      {String(item.title || item.id || "th")
                        .slice(0, 2)
                        .toUpperCase()}
                    </span>
                    <span className="thread-link-copy">
                      <strong>{item.title || "Untitled thread"}</strong>
                      <span>{[formatValue(item.state), formatDateTime(item.updated_at)].filter(Boolean).join(" | ")}</span>
                    </span>
                    <ArrowRight className="thread-link-arrow" aria-hidden="true" />
                  </button>
                  <div className="panel-actions-inline">
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => void handleDeleteThread(item.id)}
                      disabled={deletingThreadId === String(item.id)}
                    >
                      <Trash2 className="button-icon" aria-hidden="true" />
                      {deletingThreadId === String(item.id) ? "Deleting..." : "Delete"}
                    </button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <PageEmpty title="No threads" message="Start a conversation to create a persisted runtime thread." />
          )}
        </Panel>

        <div className="detail-stack">
          <section className="summary-grid">
            <Panel title={thread?.title || draftTitle || "Runtime Chat"} eyebrow="Thread Workspace">
              <DetailList
                items={[
                  { label: "Thread title", value: formatValue(thread?.title || draftTitle || "New thread") },
                  { label: "State", value: formatValue(thread?.state || "draft") },
                  { label: "Updated", value: formatDateTime(thread?.updated_at) },
                  { label: "Turns", value: formatValue(turns.length) },
                  { label: "Selected agent", value: formatValue(selectedAgent?.name) },
                ]}
              />
              {threadId ? (
                <div className="page-actions">
                  <button className="ghost-button" type="button" onClick={() => setRenamingOpen((current) => !current)}>
                    <Edit3 className="button-icon" aria-hidden="true" />
                    {renamingOpen ? "Close rename" : "Rename thread"}
                  </button>
                </div>
              ) : (
                <div className="page-stack">
                  <label className="field">
                    <span>Thread title</span>
                    <input
                      className="text-input"
                      type="text"
                      value={draftTitle}
                      onChange={(event) => setDraftTitle(event.target.value)}
                      placeholder="Runtime thread"
                      disabled={creatingThread || submitting}
                    />
                  </label>
                  <div className="page-actions">
                    <button className="ghost-button" type="button" onClick={() => void handleCreateThread()} disabled={creatingThread}>
                      <Plus className="button-icon" aria-hidden="true" />
                      {creatingThread ? "Creating..." : "Create empty thread"}
                    </button>
                  </div>
                </div>
              )}
              {renamingOpen && threadId ? (
                <div className="page-stack">
                  <label className="field">
                    <span>Rename thread</span>
                    <input
                      className="text-input"
                      type="text"
                      value={renameValue}
                      onChange={(event) => setRenameValue(event.target.value)}
                      disabled={renaming}
                    />
                  </label>
                  <div className="page-actions">
                    <button className="primary-button" type="button" onClick={() => void handleRenameThread()} disabled={renaming}>
                      {renaming ? "Saving..." : "Save title"}
                    </button>
                  </div>
                </div>
              ) : null}
            </Panel>

            <Panel title="Agent Ask Surface" eyebrow="Composer">
              <form className="form-grid" onSubmit={handleSubmit}>
                <label className="field">
                  <span>Agent</span>
                  <select
                    className="select-input"
                    value={selectedAgentName}
                    onChange={(event) => setSelectedAgentName(event.target.value)}
                    disabled={submitting || agents.length === 0}
                  >
                    {agents.map((item) => (
                      <option key={item.id || item.name} value={item.name}>
                        {item.name}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="field field-full">
                  <span>Message</span>
                  <textarea
                    className="textarea-input"
                    value={message}
                    onChange={(event) => setMessage(event.target.value)}
                    disabled={submitting}
                    rows={6}
                  />
                </label>
                <div className="page-actions">
                  <button className="primary-button" type="submit" disabled={submitting || !selectedAgentName}>
                    {submitting ? "Sending..." : threadId ? "Reply in thread" : "Start thread"}
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => setMessage(DEFAULT_CHAT_MESSAGE)}
                    disabled={submitting}
                  >
                    Load default prompt
                  </button>
                </div>
              </form>
              <div className="starter-grid">
                {CHAT_STARTERS.map((starter) => (
                  <button
                    key={starter}
                    className="starter-button"
                    type="button"
                    onClick={() => setMessage(starter)}
                    disabled={submitting}
                  >
                    {starter}
                  </button>
                ))}
              </div>
              {selectedAgent ? (
                <div className="callout">
                  <strong>{selectedAgent.name}</strong>
                  <span>
                    {[selectedAgent.description, selectedAgent.llm_connection, `${selectedAgent.tool_count || 0} tools`]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </div>
              ) : null}
              {submitError ? <div className="error-banner">{submitError}</div> : null}
            </Panel>
          </section>

          <Panel title="Conversation Timeline" eyebrow="Turns">
            {threadError ? <div className="error-banner">{threadError}</div> : null}
            {threadLoading ? (
              <div className="empty-box">Loading thread messages...</div>
            ) : turns.length > 0 ? (
              <div className="conversation-stack">
                {turns.map((turn) => {
                  const visualization = normalizeVisualizationSpec(turn.assistantVisualization);
                  return (
                    <article key={turn.id} className="conversation-turn">
                      <div className="message-card user">
                        <header className="message-header">
                          <strong>User</strong>
                          <span>{formatDateTime(turn.createdAt)}</span>
                        </header>
                        <p className="message-copy">{turn.prompt}</p>
                      </div>
                      <div className="message-card assistant">
                        <header className="message-header">
                          <strong>{turn.agentLabel || "Assistant"}</strong>
                          <span>{turn.status}</span>
                        </header>
                        {turn.status === "pending" ? (
                          <div className="empty-box">Waiting for the runtime to finish this turn...</div>
                        ) : turn.status === "error" ? (
                          <div className="error-banner">{turn.errorMessage || "The runtime failed to complete this request."}</div>
                        ) : (
                          <div className="page-stack">
                            <p className="message-copy">{turn.assistantSummary || "No summary returned."}</p>
                            {turn.assistantTable ? (
                              <>
                                {visualization && hasRenderableVisualization(turn.assistantVisualization) ? (
                                  <ChartPreview
                                    title={visualization.title}
                                    result={turn.assistantTable}
                                    visualization={visualization}
                                    preferredDimension={visualization.x}
                                    preferredMeasure={visualization.y?.[0]}
                                  />
                                ) : null}
                                <ResultTable result={turn.assistantTable} maxPreviewRows={10} />
                              </>
                            ) : null}
                            {turn.diagnostics ? (
                              <details className="diagnostics-disclosure">
                                <summary>Execution diagnostics</summary>
                                <pre className="code-block compact">{renderJson(turn.diagnostics)}</pre>
                              </details>
                            ) : null}
                          </div>
                        )}
                      </div>
                    </article>
                  );
                })}
                <div ref={timelineEndRef} />
              </div>
            ) : (
              <PageEmpty
                title="No conversation yet"
                message={
                  threadId
                    ? "This thread does not have turns yet."
                    : "Send a message to create the first runtime thread."
                }
              />
            )}
          </Panel>
        </div>
      </section>
    </div>
  );
}

function ChatIndexPage() {
  const navigate = useNavigate();
  const threadsState = useAsyncData(fetchThreads);
  const agentsState = useAsyncData(fetchAgents);
  const threads = Array.isArray(threadsState.data?.items) ? threadsState.data.items : [];
  const agents = Array.isArray(agentsState.data?.items) ? agentsState.data.items : [];
  const sortedThreads = [...threads].sort((left, right) => {
    const leftTime = new Date(left.updated_at || left.created_at || 0).getTime();
    const rightTime = new Date(right.updated_at || right.created_at || 0).getTime();
    return rightTime - leftTime;
  });
  const latestThread = sortedThreads[0] || null;
  const [creatingThread, setCreatingThread] = useState(false);
  const [deletingThreadId, setDeletingThreadId] = useState("");
  const [mutationError, setMutationError] = useState("");

  async function handleCreateThread(seedMessage = "") {
    setCreatingThread(true);
    setMutationError("");
    try {
      const createdThread = await createThread({});
      if (seedMessage && typeof window !== "undefined") {
        window.sessionStorage.setItem(`runtime-thread-draft:${createdThread.id}`, seedMessage);
      }
      await threadsState.reload();
      navigate(`/chat/${createdThread.id}`);
    } catch (caughtError) {
      setMutationError(getErrorMessage(caughtError));
    } finally {
      setCreatingThread(false);
    }
  }

  async function handleDeleteThread(threadId) {
    setDeletingThreadId(String(threadId));
    setMutationError("");
    try {
      await deleteThread(threadId);
      await threadsState.reload();
    } catch (caughtError) {
      setMutationError(getErrorMessage(caughtError));
    } finally {
      setDeletingThreadId("");
    }
  }

  return (
    <div className="chat-index-shell">
      <section className="surface-panel thread-index-header">
        <div className="thread-index-copy">
          <p className="eyebrow">Threads</p>
          <h2>Your thread workspace</h2>
          <p className="hero-copy">
            Review recent runtime threads, resume an investigation, or create a new thread before
            handing work to an agent.
          </p>
          <div className="inline-notes">
            <span>{threads.length} persisted threads</span>
            <span>Single-workspace runtime history</span>
          </div>
        </div>
        <div className="thread-index-actions">
          <button className="primary-button" type="button" onClick={() => void handleCreateThread()} disabled={creatingThread}>
            <Plus className="button-icon" aria-hidden="true" />
            {creatingThread ? "Creating..." : "New thread"}
          </button>
        </div>
      </section>

      <div className="thread-overview-strip">
        <article className="thread-overview-card">
          <span>Persisted threads</span>
          <strong>{formatValue(threads.length)}</strong>
          <small>Runtime-local history with no cloud workspace dependency.</small>
        </article>
        <article className="thread-overview-card">
          <span>Available agents</span>
          <strong>{formatValue(agents.length)}</strong>
          <small>Agents from the runtime are available directly in the thread composer.</small>
        </article>
        <article className="thread-overview-card">
          <span>Latest activity</span>
          <strong>{latestThread ? formatRelativeTime(latestThread.updated_at || latestThread.created_at) : "No activity"}</strong>
          <small>{latestThread ? latestThread.title || `Thread ${String(latestThread.id).slice(0, 8)}` : "Create the first thread to start the workspace timeline."}</small>
        </article>
      </div>

      <div className="thread-index-layout">
        <section className="surface-panel thread-index-list">
          <div className="thread-section-head">
            <div>
              <h3>Recent threads</h3>
              <p>Persisted runtime chat history, scoped to this single-workspace runtime.</p>
            </div>
            <button className="ghost-button" type="button" onClick={() => void threadsState.reload()} disabled={threadsState.loading}>
              <RefreshCw className="button-icon" aria-hidden="true" />
              Refresh
            </button>
          </div>

          {threadsState.error ? <div className="error-banner">{threadsState.error}</div> : null}
          {mutationError ? <div className="error-banner">{mutationError}</div> : null}

          {threadsState.loading ? (
            <div className="empty-box">Loading threads...</div>
          ) : sortedThreads.length > 0 ? (
            <div className="thread-index-cards">
              {sortedThreads.map((thread) => (
                <article key={thread.id} className="thread-index-card">
                  <button
                    className="thread-index-card-main"
                    type="button"
                    onClick={() => navigate(`/chat/${thread.id}`)}
                  >
                    <span className="thread-link-avatar">
                      {String(thread.title || thread.id || "th")
                        .slice(0, 2)
                        .toUpperCase()}
                    </span>
                    <span className="thread-link-copy">
                      <strong>{thread.title || `Thread ${String(thread.id).slice(0, 8)}`}</strong>
                      <span>
                        {formatValue(thread.state)} | {thread.updated_at ? `Updated ${formatRelativeTime(thread.updated_at)}` : `Created ${formatRelativeTime(thread.created_at)}`}
                      </span>
                    </span>
                    <ArrowRight className="thread-link-arrow" aria-hidden="true" />
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => void handleDeleteThread(thread.id)}
                    disabled={deletingThreadId === String(thread.id)}
                  >
                    <Trash2 className="button-icon" aria-hidden="true" />
                    {deletingThreadId === String(thread.id) ? "Deleting..." : "Delete"}
                  </button>
                </article>
              ))}
            </div>
          ) : (
            <div className="thread-empty-state">
              <MessageSquareText className="thread-empty-icon" aria-hidden="true" />
              <div>
                <strong>No threads found</strong>
                <p>Start a new thread to see it appear here.</p>
              </div>
              <button className="primary-button" type="button" onClick={() => void handleCreateThread()} disabled={creatingThread}>
                <Plus className="button-icon" aria-hidden="true" />
                Start a thread
              </button>
            </div>
          )}
        </section>

        <aside className="surface-panel thread-index-rail">
          <div className="thread-section-head">
            <div>
              <h3>Start from a richer prompt</h3>
              <p>Seed the first message before entering the thread detail workspace.</p>
            </div>
          </div>
          <div className="callout">
            <strong>Runtime-first chat</strong>
            <span>Threads, agent choice, and history stay local to this runtime. No organization or project state is required.</span>
          </div>
          <div className="thread-rail-starters">
            {CHAT_STARTERS.map((starter) => (
              <button
                key={starter}
                className="starter-button"
                type="button"
                onClick={() => void handleCreateThread(starter)}
                disabled={creatingThread}
              >
                <strong>Start with this prompt</strong>
                <span>{starter}</span>
              </button>
            ))}
          </div>
        </aside>
      </div>
    </div>
  );
}

function ChatPage() {
  const navigate = useNavigate();
  const params = useParams();
  const threadId = String(params.threadId || "").trim();
  const agentsState = useAsyncData(fetchAgents);
  const agents = Array.isArray(agentsState.data?.items) ? agentsState.data.items : [];

  const [selectedAgentName, setSelectedAgentName] = useState("");
  const [message, setMessage] = useState(DEFAULT_CHAT_MESSAGE);
  const [thread, setThread] = useState(null);
  const [messages, setMessages] = useState([]);
  const [threadLoading, setThreadLoading] = useState(false);
  const [threadError, setThreadError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const [threadMutationError, setThreadMutationError] = useState("");
  const [renaming, setRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState("");
  const [renamingOpen, setRenamingOpen] = useState(false);
  const [transientTurn, setTransientTurn] = useState(null);
  const selectedAgent = agents.find((item) => item.name === selectedAgentName) || null;
  const turns = useMemo(() => buildConversationTurns(messages, agents), [messages, agents]);
  const displayTurns = useMemo(() => {
    if (!transientTurn) {
      return turns;
    }
    if (turns.some((turn) => String(turn.id) === String(transientTurn.id))) {
      return turns;
    }
    return [...turns, transientTurn];
  }, [turns, transientTurn]);
  const timelineEndRef = useRef(null);
  const readyTurns = displayTurns.filter((turn) => turn.status === "ready");
  const latestArtifactTurn =
    [...readyTurns].reverse().find((turn) => turn.assistantTable || turn.assistantVisualization) || null;
  const lastUpdated = readyTurns.length > 0 ? readyTurns[readyTurns.length - 1].createdAt : transientTurn?.createdAt || thread?.updated_at || null;
  const isPending = submitting || displayTurns.some((turn) => turn.status === "pending");

  useEffect(() => {
    if (!threadId) {
      return;
    }
    const storageKey = `runtime-thread-agent:${threadId}`;
    try {
      const stored = window.localStorage.getItem(storageKey);
      if (stored) {
        setSelectedAgentName(stored);
      }
    } catch {}
  }, [threadId]);

  useEffect(() => {
    if (!threadId) {
      return;
    }
    const storageKey = `runtime-thread-agent:${threadId}`;
    try {
      if (selectedAgentName) {
        window.localStorage.setItem(storageKey, selectedAgentName);
      } else {
        window.localStorage.removeItem(storageKey);
      }
    } catch {}
  }, [selectedAgentName, threadId]);

  useEffect(() => {
    if (!selectedAgentName && agents.length > 0) {
      setSelectedAgentName(agents.find((item) => item.default)?.name || agents[0].name);
    }
  }, [agents, selectedAgentName]);

  useEffect(() => {
    if (!threadId || typeof window === "undefined") {
      return;
    }
    const draftKey = `runtime-thread-draft:${threadId}`;
    const storedDraft = window.sessionStorage.getItem(draftKey);
    if (!storedDraft) {
      return;
    }
    setMessage(storedDraft);
    window.sessionStorage.removeItem(draftKey);
  }, [threadId]);

  useEffect(() => {
    let cancelled = false;

    async function loadThreadState() {
      if (!threadId) {
        return;
      }
      setThreadLoading(true);
      setThreadError("");
      try {
        const [threadPayload, messagePayload] = await Promise.all([
          fetchThread(threadId),
          fetchThreadMessages(threadId),
        ]);
        if (cancelled) {
          return;
        }
        setThread(threadPayload);
        setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
        setTransientTurn(null);
        setRenameValue(threadPayload?.title || "");
      } catch (caughtError) {
        if (!cancelled) {
          setThread(null);
          setMessages([]);
          setThreadError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setThreadLoading(false);
        }
      }
    }

    void loadThreadState();

    return () => {
      cancelled = true;
    };
  }, [threadId]);

  useEffect(() => {
    timelineEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [displayTurns.length, submitting]);

  async function handleSubmit(event) {
    event.preventDefault();
    if (!threadId || !selectedAgentName || !message.trim()) {
      return;
    }
    setSubmitting(true);
    setSubmitError("");
    const pendingPrompt = message.trim();
    const pendingTurn = {
      id: createLocalId("pending-turn"),
      prompt: pendingPrompt,
      createdAt: new Date().toISOString(),
      assistantSummary: "",
      assistantTable: null,
      assistantVisualization: null,
      diagnostics: null,
      errorMessage: "",
      agentId: String(selectedAgent?.id || ""),
      agentLabel: selectedAgent?.name || selectedAgentName,
      status: "pending",
    };
    setTransientTurn(pendingTurn);
    try {
      const response = await askAgent({
        message: pendingPrompt,
        agent_name: selectedAgentName,
        thread_id: threadId,
      });
      const resolvedThreadId = String(response.thread_id || threadId).trim();
      setMessage("");
      const [threadPayload, messagePayload] = await Promise.all([
        fetchThread(resolvedThreadId),
        fetchThreadMessages(resolvedThreadId),
      ]);
      setThread(threadPayload);
      setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
      setTransientTurn(null);
      setRenameValue(threadPayload?.title || "");
      if (resolvedThreadId !== threadId) {
        navigate(`/chat/${resolvedThreadId}`);
      }
    } catch (caughtError) {
      setTransientTurn({
        ...pendingTurn,
        status: "error",
        errorMessage: getErrorMessage(caughtError),
      });
      setSubmitError(getErrorMessage(caughtError));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleRenameThread() {
    if (!threadId) {
      return;
    }
    setRenaming(true);
    setThreadMutationError("");
    try {
      const updated = await updateThread(threadId, {
        title: renameValue.trim() || undefined,
      });
      setThread(updated);
      setRenameValue(updated?.title || "");
      setRenamingOpen(false);
    } catch (caughtError) {
      setThreadMutationError(getErrorMessage(caughtError));
    } finally {
      setRenaming(false);
    }
  }

  function handleReuseLastPrompt() {
    const lastPrompt = [...displayTurns].reverse().find((turn) => turn.prompt)?.prompt;
    if (lastPrompt) {
      setMessage(lastPrompt);
    }
  }

  function handleComposerKeyDown(event) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      if (!submitting && selectedAgentName && message.trim()) {
        void handleSubmit(event);
      }
    }
  }

  if (!threadId) {
    return <Navigate to="/chat" replace />;
  }

  const snapshotItems = [
    { label: "Thread title", value: formatValue(thread?.title || `Thread ${threadId.slice(0, 8)}`) },
    { label: "State", value: formatValue(thread?.state || (isPending ? "pending" : "ready")) },
    { label: "Messages", value: formatValue(messages.length) },
    { label: "Updated", value: lastUpdated ? formatRelativeTime(lastUpdated) : "Awaiting first prompt" },
  ];

  return (
    <div className="thread-detail-shell">
      <section className="surface-panel thread-detail-header">
        <div className="thread-detail-copy">
          <div className="thread-detail-meta">
            <span className="tag">Thread</span>
            <span className="thread-detail-id">{threadId}</span>
            <span className={`thread-status-pill ${isPending ? "pending" : "ready"}`}>
              {isPending ? "Generating response" : "Standing by"}
            </span>
          </div>
          <h2>{thread?.title?.trim() || `Thread ${threadId.slice(0, 8)}`}</h2>
          <p className="hero-copy">
            Messages, summaries, charts, and tables for this runtime thread. The layout follows the older
            cloud thread workspace more closely, but remains runtime-local.
          </p>
        </div>
        <div className="thread-detail-actions">
          <button className="ghost-button" type="button" onClick={() => navigate("/chat")}>
            <History className="button-icon" aria-hidden="true" />
            Thread list
          </button>
          <button className="ghost-button" type="button" onClick={handleReuseLastPrompt} disabled={turns.length === 0}>
            <RefreshCw className="button-icon" aria-hidden="true" />
            Reuse last prompt
          </button>
          <button className="ghost-button" type="button" onClick={() => setRenamingOpen((current) => !current)}>
            <Edit3 className="button-icon" aria-hidden="true" />
            {renamingOpen ? "Close rename" : "Rename"}
          </button>
        </div>
      </section>

      {renamingOpen ? (
        <section className="surface-panel thread-rename-panel">
          <div className="thread-section-head">
            <div>
              <h3>Rename thread</h3>
              <p>Update the working title shown across the runtime chat surface.</p>
            </div>
          </div>
          <div className="form-grid compact">
            <label className="field">
              <span>Thread title</span>
              <input
                className="text-input"
                type="text"
                value={renameValue}
                onChange={(event) => setRenameValue(event.target.value)}
                disabled={renaming}
              />
            </label>
            <div className="page-actions">
              <button className="primary-button" type="button" onClick={() => void handleRenameThread()} disabled={renaming}>
                {renaming ? "Saving..." : "Save title"}
              </button>
            </div>
          </div>
          {threadMutationError ? <div className="error-banner">{threadMutationError}</div> : null}
        </section>
      ) : null}

      <div className="thread-detail-grid">
        <section className="surface-panel thread-workspace-panel">
          <div className="thread-section-head thread-workspace-head">
            <div>
              <h3>Thread timeline</h3>
              <p>Messages, summaries, tables, and visual artifacts generated for this thread.</p>
            </div>
            <div className="thread-section-status">
              <span className={`thread-live-indicator ${isPending ? "pending" : "ready"}`}>
                <span className="thread-live-dot" aria-hidden="true" />
                {isPending ? "Generating response" : "Standing by"}
              </span>
              <span>{lastUpdated ? `Updated ${formatRelativeTime(lastUpdated)}` : "Awaiting first prompt"}</span>
            </div>
          </div>

          {threadError ? <div className="error-banner">{threadError}</div> : null}
          {threadLoading ? (
            <div className="empty-box">Loading thread messages...</div>
          ) : displayTurns.length > 0 ? (
            <div className="thread-transcript-scroll">
              <div className="conversation-stack thread-conversation-stack">
                {displayTurns.map((turn) => {
                  const visualization = normalizeVisualizationSpec(turn.assistantVisualization);
                  return (
                    <article key={turn.id} className="conversation-turn-shell">
                      <div className="thread-user-row">
                        <div className="thread-user-bubble">
                          <p>{turn.prompt}</p>
                          <span>{formatRelativeTime(turn.createdAt)}</span>
                        </div>
                      </div>

                      <div className="thread-assistant-row">
                        <div className="thread-assistant-shell">
                          <header className="thread-assistant-meta">
                            <div>
                              <strong>{turn.agentLabel || "Assistant"}</strong>
                              <span>{turn.agentId ? `Agent run` : "Runtime response"}</span>
                            </div>
                            <span className={`message-status-badge ${turn.status}`}>{turn.status}</span>
                          </header>

                          {turn.status === "pending" ? (
                            <div className="thread-runtime-pending">Waiting for the runtime to finish this turn...</div>
                          ) : turn.status === "error" ? (
                            <div className="error-banner">{turn.errorMessage || "The runtime failed to complete this request."}</div>
                          ) : (
                            <div className="thread-assistant-body">
                              <p className="assistant-summary-card">{turn.assistantSummary || "No summary returned."}</p>
                              {turn.assistantTable ? (
                                <div className="assistant-artifact-stack">
                                  {visualization && hasRenderableVisualization(turn.assistantVisualization) ? (
                                    <ChartPreview
                                      title={visualization.title}
                                      result={turn.assistantTable}
                                      visualization={visualization}
                                      preferredDimension={visualization.x}
                                      preferredMeasure={visualization.y?.[0]}
                                    />
                                  ) : null}
                                  <ResultTable result={turn.assistantTable} maxPreviewRows={10} />
                                </div>
                              ) : null}
                              {turn.diagnostics ? (
                                <details className="diagnostics-disclosure">
                                  <summary>Execution diagnostics</summary>
                                  <pre className="code-block compact">{renderJson(turn.diagnostics)}</pre>
                                </details>
                              ) : null}
                            </div>
                          )}
                        </div>
                      </div>
                    </article>
                  );
                })}
                <div ref={timelineEndRef} />
              </div>
            </div>
          ) : (
            <div className="thread-empty-state">
              <Sparkles className="thread-empty-icon" aria-hidden="true" />
              <div>
                <strong>Start the thread</strong>
                <p>Pick an agent and send the first prompt to generate summaries, tables, and charts.</p>
              </div>
            </div>
          )}

          <form className="thread-composer-form" onSubmit={handleSubmit}>
            <div className="thread-composer-topbar">
              <div>
                <h3>Composer</h3>
                <p>Choose an agent, send a prompt, and keep the thread moving.</p>
              </div>
              <div className="thread-composer-actions">
                <select
                  className="select-input thread-agent-select"
                  value={selectedAgentName}
                  onChange={(event) => setSelectedAgentName(event.target.value)}
                  disabled={submitting || agents.length === 0}
                >
                  {agents.map((item) => (
                    <option key={item.id || item.name} value={item.name}>
                      {item.name}
                    </option>
                  ))}
                </select>
                <button className="ghost-button" type="button" onClick={() => navigate("/agents")}>
                  <Bot className="button-icon" aria-hidden="true" />
                  Manage agents
                </button>
              </div>
            </div>

            <label className="field">
              <span>Message</span>
              <textarea
                className="textarea-input thread-composer-input"
                value={message}
                onChange={(event) => setMessage(event.target.value)}
                onKeyDown={handleComposerKeyDown}
                disabled={submitting}
                rows={5}
                placeholder="Shift + Enter for a new line. Describe what you need from this runtime thread..."
              />
            </label>
            <div className="thread-composer-footer">
              <p className="composer-note">Press Enter to send. Use Shift+Enter for a newline.</p>
              <div className="page-actions">
                <button className="ghost-button" type="button" onClick={handleReuseLastPrompt} disabled={turns.length === 0 || submitting}>
                  <RefreshCw className="button-icon" aria-hidden="true" />
                  Reuse last prompt
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => setMessage(DEFAULT_CHAT_MESSAGE)}
                  disabled={submitting}
                >
                  Load default prompt
                </button>
                <button className="primary-button" type="submit" disabled={submitting || !selectedAgentName}>
                  <ArrowRight className="button-icon" aria-hidden="true" />
                  {submitting ? "Sending..." : "Send prompt"}
                </button>
              </div>
            </div>
          </form>
          {submitError ? <div className="error-banner">{submitError}</div> : null}
        </section>

        <aside className="surface-panel thread-context-rail">
          <div className="thread-rail-section">
            <div className="thread-section-head">
              <div>
                <h3>Thread snapshot</h3>
                <p>Current runtime state for this thread.</p>
              </div>
            </div>
            <DetailList items={snapshotItems} />
          </div>

          <div className="thread-rail-section">
            <div className="thread-section-head">
              <div>
                <h3>Active agent</h3>
                <p>The selected runtime agent for the next turn.</p>
              </div>
            </div>
            {selectedAgent ? (
              <div className="callout">
                <strong>{selectedAgent.name}</strong>
                <span>
                  {[selectedAgent.description, selectedAgent.llm_connection, `${selectedAgent.tool_count || 0} tools`]
                    .filter(Boolean)
                    .join(" | ")}
                </span>
              </div>
            ) : (
              <PageEmpty title="No agent selected" message="Choose a runtime agent to send the next prompt." />
            )}
          </div>

          <div className="thread-rail-section">
            <div className="thread-section-head">
              <div>
                <h3>Starter prompts</h3>
                <p>Load a richer thread prompt without leaving the detail view.</p>
              </div>
            </div>
            <div className="thread-rail-starters">
              {CHAT_STARTERS.map((starter) => (
                <button
                  key={starter}
                  className="starter-button"
                  type="button"
                  onClick={() => setMessage(starter)}
                  disabled={submitting}
                >
                  {starter}
                </button>
              ))}
            </div>
          </div>

          {latestArtifactTurn ? (
            <div className="thread-rail-section">
              <div className="thread-section-head">
                <div>
                  <h3>Latest artifact</h3>
                  <p>Most recent turn that returned structured output.</p>
                </div>
              </div>
              <div className="callout">
                <strong>{latestArtifactTurn.agentLabel || "Assistant"}</strong>
                <span>
                  {[latestArtifactTurn.assistantVisualization ? "Chart available" : null, latestArtifactTurn.assistantTable ? `${latestArtifactTurn.assistantTable.rowCount || 0} rows` : null]
                    .filter(Boolean)
                    .join(" | ")}
                </span>
              </div>
            </div>
          ) : null}
        </aside>
      </div>
    </div>
  );
}

function LegacyBiPage() {
  const modelsState = useAsyncData(fetchSemanticModels);
  const models = Array.isArray(modelsState.data?.items) ? modelsState.data.items : [];
  const [selectedModel, setSelectedModel] = useState("");
  const [detail, setDetail] = useState(null);
  const [detailError, setDetailError] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);
  const [fieldSearch, setFieldSearch] = useState("");
  const deferredFieldSearch = useDeferredValue(fieldSearch);
  const [form, setForm] = useState({
    dimension: "",
    measure: "",
    limit: "12",
    chartType: "bar",
  });
  const [result, setResult] = useState(null);
  const [queryError, setQueryError] = useState("");
  const [running, setRunning] = useState(false);

  useEffect(() => {
    if (!selectedModel && models.length > 0) {
      setSelectedModel(models.find((item) => item.default)?.name || models[0].name);
    }
  }, [models, selectedModel]);

  useEffect(() => {
    let cancelled = false;

    async function loadModelDetail() {
      if (!selectedModel) {
        setDetail(null);
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchSemanticModel(selectedModel);
        if (cancelled) {
          return;
        }
        const fields = extractSemanticFields(payload);
        setDetail(payload);
        setForm((current) => ({
          ...current,
          dimension: current.dimension || fields.dimensions[0]?.value || "",
          measure: current.measure || fields.measures[0]?.value || "",
        }));
      } catch (caughtError) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadModelDetail();

    return () => {
      cancelled = true;
    };
  }, [selectedModel]);

  const fields = extractSemanticFields(detail);

  async function handleSubmit(event) {
    event.preventDefault();
    if (!selectedModel || !form.measure || !form.dimension) {
      return;
    }
    setRunning(true);
    setQueryError("");
    try {
      const response = await querySemantic({
        semantic_models: [selectedModel],
        measures: [form.measure],
        dimensions: [form.dimension],
        limit: Number(form.limit) > 0 ? Number(form.limit) : 12,
        order: {
          [form.measure]: "desc",
        },
      });
      setResult(response);
    } catch (caughtError) {
      setResult(null);
      setQueryError(getErrorMessage(caughtError));
    } finally {
      setRunning(false);
    }
  }

  const normalizedResult = result
    ? normalizeTabularResult({
        columns: buildColumnsFromRows(result.data),
        rows: Array.isArray(result.data) ? result.data : [],
        rowCount: Array.isArray(result.data) ? result.data.length : 0,
      })
    : null;
  const semanticDatasets = extractSemanticDatasets(detail);
  const filteredSemanticDatasets = semanticDatasets
    .map((dataset) => {
      const search = String(deferredFieldSearch || "").trim().toLowerCase();
      if (!search) {
        return dataset;
      }
      return {
        ...dataset,
        dimensions: dataset.dimensions.filter((item) =>
          String(item?.name || "").toLowerCase().includes(search),
        ),
        measures: dataset.measures.filter((item) =>
          String(item?.name || "").toLowerCase().includes(search),
        ),
      };
    })
    .filter(
      (dataset) =>
        !deferredFieldSearch ||
        String(dataset.name).toLowerCase().includes(String(deferredFieldSearch).toLowerCase()) ||
        dataset.dimensions.length > 0 ||
        dataset.measures.length > 0,
    );

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Lightweight BI</p>
          <h2>Semantic exploration with a stronger field browser and chart workflow</h2>
          <p className="hero-copy">
            The runtime BI surface keeps a compact footprint, but restores the cloud UI&apos;s stronger
            field hierarchy and preview rhythm around semantic dimensions, measures, and chart output.
          </p>
        </div>
        <div className="hint-list">
          <span>Models: {models.length}</span>
          <span>Active model: {selectedModel || "none"}</span>
          <span>Chart: {form.chartType}</span>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={Layers3}
          label="Semantic models"
          value={formatValue(models.length)}
          detail="Models available to the runtime BI surface."
        />
        <MetricCard
          icon={Database}
          label="Datasets"
          value={formatValue(detail?.dataset_count || semanticDatasets.length)}
          detail="Semantic datasets in the active model."
        />
        <MetricCard
          icon={SearchCheck}
          label="Dimensions"
          value={formatValue(fields.dimensions.length)}
          detail="Browsable dimensions available to query."
        />
        <MetricCard
          icon={Sparkles}
          label="Measures"
          value={formatValue(fields.measures.length)}
          detail="Measures available for charting and ranking."
        />
      </section>

      <section className="workspace-grid">
        <Panel title="Field Explorer" eyebrow="Semantic Model">
          <label className="field">
            <span>Find fields</span>
            <input
              className="text-input"
              type="search"
              value={fieldSearch}
              onChange={(event) => setFieldSearch(event.target.value)}
              placeholder="Filter datasets, dimensions, or measures"
            />
          </label>
          {detailError ? <div className="error-banner">{detailError}</div> : null}
          {detailLoading ? (
            <div className="empty-box">Loading semantic model...</div>
          ) : filteredSemanticDatasets.length > 0 ? (
            <div className="field-section-list">
              {filteredSemanticDatasets.map((dataset) => (
                <div key={dataset.name} className="field-group">
                  <div className="field-group-header">
                    <strong>{dataset.name}</strong>
                    <span>{dataset.relationName || "semantic dataset"}</span>
                  </div>
                  <div className="field-pill-list">
                    {dataset.dimensions.map((item) => {
                      const value = `${dataset.name}.${item.name}`;
                      return (
                        <button
                          key={value}
                          className={`field-pill ${form.dimension === value ? "active" : ""}`}
                          type="button"
                          onClick={() =>
                            setForm((current) => ({
                              ...current,
                              dimension: value,
                            }))
                          }
                        >
                          {item.name}
                        </button>
                      );
                    })}
                    {dataset.measures.map((item) => {
                      const value = `${dataset.name}.${item.name}`;
                      return (
                        <button
                          key={value}
                          className={`field-pill ${form.measure === value ? "active" : ""}`}
                          type="button"
                          onClick={() =>
                            setForm((current) => ({
                              ...current,
                              measure: value,
                            }))
                          }
                        >
                          {item.name}
                        </button>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <PageEmpty title="No fields found" message="Adjust the search or select a semantic model with exposed fields." />
          )}
        </Panel>

        <div className="detail-stack">
          <Panel title="Lightweight BI" eyebrow="Semantic Query Surface">
            <form className="form-grid" onSubmit={handleSubmit}>
              <label className="field">
                <span>Semantic model</span>
                <select
                  className="select-input"
                  value={selectedModel}
                  onChange={(event) => setSelectedModel(event.target.value)}
                  disabled={running || detailLoading}
                >
                  {models.map((item) => (
                    <option key={item.id || item.name} value={item.name}>
                      {item.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Dimension</span>
                <select
                  className="select-input"
                  value={form.dimension}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      dimension: event.target.value,
                    }))
                  }
                  disabled={running || detailLoading}
                >
                  <option value="">Select dimension</option>
                  {fields.dimensions.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Measure</span>
                <select
                  className="select-input"
                  value={form.measure}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      measure: event.target.value,
                    }))
                  }
                  disabled={running || detailLoading}
                >
                  <option value="">Select measure</option>
                  {fields.measures.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Chart type</span>
                <select
                  className="select-input"
                  value={form.chartType}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      chartType: event.target.value,
                    }))
                  }
                  disabled={running}
                >
                  <option value="bar">Bar</option>
                  <option value="line">Line</option>
                  <option value="pie">Pie</option>
                </select>
              </label>
              <label className="field">
                <span>Limit</span>
                <input
                  className="text-input"
                  type="number"
                  min="1"
                  value={form.limit}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      limit: event.target.value,
                    }))
                  }
                  disabled={running}
                />
              </label>
              <div className="page-actions">
                <button
                  className="primary-button"
                  type="submit"
                  disabled={running || !selectedModel || !form.measure || !form.dimension}
                >
                  {running ? "Running semantic query..." : "Run semantic query"}
                </button>
              </div>
            </form>
            {queryError ? <div className="error-banner">{queryError}</div> : null}
          </Panel>

          <section className="summary-grid">
            <Panel title="Model Summary" eyebrow="Metadata">
              {detailLoading ? (
                <div className="empty-box">Loading semantic model...</div>
              ) : detail ? (
                <DetailList
                  items={[
                    { label: "Description", value: formatValue(detail.description) },
                    { label: "Datasets", value: formatList(detail.dataset_names) },
                    { label: "Dimensions", value: formatValue(detail.dimension_count) },
                    { label: "Measures", value: formatValue(detail.measure_count) },
                  ]}
                />
              ) : (
                <PageEmpty title="No semantic model" message="Select a semantic model to drive the BI surface." />
              )}
            </Panel>

            <Panel title="Chart Preview" eyebrow="Visualization">
              {normalizedResult ? (
                <ChartPreview
                  title="Runtime BI preview"
                  result={normalizedResult}
                  metadata={Array.isArray(result?.metadata) ? result.metadata : []}
                  visualization={{
                    chartType: form.chartType,
                    x: form.dimension,
                    y: [form.measure],
                  }}
                  preferredDimension={form.dimension}
                  preferredMeasure={form.measure}
                />
              ) : (
                <PageEmpty title="No chart yet" message="Run a semantic query to render the lightweight BI preview." />
              )}
            </Panel>
          </section>
        </div>
      </section>

      <Panel title="Semantic Query Result" eyebrow="Rows">
        {normalizedResult ? (
          <>
            <div className="inline-notes">
              <span>Rows: {formatValue(normalizedResult.rowCount)}</span>
              <span>Dimension: {form.dimension || "none"}</span>
              <span>Measure: {form.measure || "none"}</span>
            </div>
            <ResultTable result={normalizedResult} maxPreviewRows={16} />
            {result?.generated_sql ? <pre className="code-block">{result.generated_sql}</pre> : null}
          </>
        ) : (
          <PageEmpty title="No BI result yet" message="Run a semantic query to inspect runtime BI output." />
        )}
      </Panel>
    </div>
  );
}

function BiPage() {
  const navigate = useNavigate();
  const modelsState = useAsyncData(fetchSemanticModels);
  const models = Array.isArray(modelsState.data?.items) ? modelsState.data.items : [];
  const [studioState, setStudioState] = useState(() => loadBiStudioState());
  const [activeWidgetId, setActiveWidgetId] = useState("");
  const [detail, setDetail] = useState(null);
  const [detailError, setDetailError] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);
  const [fieldSearch, setFieldSearch] = useState("");
  const [studioNotice, setStudioNotice] = useState(
    "Dashboards are autosaved locally in this browser. The runtime BI surface does not depend on cloud dashboard services.",
  );
  const [biEditMode, setBiEditMode] = useState(true);
  const deferredFieldSearch = useDeferredValue(fieldSearch);

  const boards = studioState.boards;
  const defaultModelName = models.find((item) => item.default)?.name || models[0]?.name || "";
  const activeBoard = boards.find((board) => board.id === studioState.activeBoardId) || boards[0] || null;
  const activeWidget =
    activeBoard?.widgets.find((widget) => widget.id === activeWidgetId) || activeBoard?.widgets[0] || null;
  const selectedModel = activeBoard?.selectedModel || "";
  const fields = extractSemanticFields(detail);
  const semanticDatasets = extractSemanticDatasets(detail);
  const runnableCount = activeBoard?.widgets.filter((widget) => Boolean(widget.measure)).length || 0;
  const totalWidgets = boards.reduce((count, board) => count + board.widgets.length, 0);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const snapshot = {
      activeBoardId: studioState.activeBoardId,
      boards: studioState.boards.map((board) => ({
        ...board,
        widgets: board.widgets.map(({ result, running, error, ...widget }) => widget),
      })),
    };
    window.localStorage.setItem(BI_STUDIO_STORAGE_KEY, JSON.stringify(snapshot));
  }, [studioState]);

  useEffect(() => {
    if (boards.length === 0) {
      const board = createBiBoard({ selectedModel: defaultModelName });
      setStudioState({ boards: [board], activeBoardId: board.id });
      setActiveWidgetId(board.widgets[0]?.id || "");
      return;
    }
    if (!boards.some((board) => board.id === studioState.activeBoardId)) {
      setStudioState((current) => ({ ...current, activeBoardId: current.boards[0]?.id || "" }));
    }
  }, [boards, defaultModelName, studioState.activeBoardId]);

  useEffect(() => {
    if (!activeBoard) {
      setActiveWidgetId("");
      return;
    }
    if (!activeBoard.selectedModel || !models.some((item) => item.name === activeBoard.selectedModel)) {
      updateBoard(activeBoard.id, { selectedModel: defaultModelName });
    }
    if (activeBoard.widgets.length === 0) {
      setActiveWidgetId("");
      return;
    }
    if (!activeBoard.widgets.some((widget) => widget.id === activeWidgetId)) {
      setActiveWidgetId(activeBoard.widgets[0].id);
    }
  }, [activeBoard, activeWidgetId, defaultModelName, models]);

  useEffect(() => {
    let cancelled = false;

    async function loadModelDetail() {
      if (!selectedModel) {
        setDetail(null);
        setDetailError("");
        setDetailLoading(false);
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchSemanticModel(selectedModel);
        if (!cancelled) {
          setDetail(payload);
        }
      } catch (caughtError) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadModelDetail();
    return () => {
      cancelled = true;
    };
  }, [selectedModel]);

  const filteredSemanticDatasets = semanticDatasets
    .map((dataset) => {
      const search = String(deferredFieldSearch || "").trim().toLowerCase();
      if (!search) {
        return dataset;
      }
      return {
        ...dataset,
        dimensions: dataset.dimensions.filter((item) =>
          String(item?.name || "").toLowerCase().includes(search),
        ),
        measures: dataset.measures.filter((item) =>
          String(item?.name || "").toLowerCase().includes(search),
        ),
      };
    })
    .filter(
      (dataset) =>
        !deferredFieldSearch ||
        String(dataset.name).toLowerCase().includes(String(deferredFieldSearch).toLowerCase()) ||
        dataset.dimensions.length > 0 ||
        dataset.measures.length > 0,
    );

  function updateBoard(boardId, updates) {
    setStudioState((current) => ({
      ...current,
      boards: current.boards.map((board) => (board.id === boardId ? { ...board, ...updates } : board)),
    }));
  }

  function updateWidget(boardId, widgetId, updates) {
    setStudioState((current) => ({
      ...current,
      boards: current.boards.map((board) =>
        board.id === boardId
          ? {
              ...board,
              widgets: board.widgets.map((widget) => (widget.id === widgetId ? { ...widget, ...updates } : widget)),
            }
          : board,
      ),
    }));
  }

  function createBoard() {
    const board = createBiBoard({
      name: `Runtime dashboard ${boards.length + 1}`,
      selectedModel: selectedModel || defaultModelName,
    });
    setStudioState((current) => ({ boards: [board, ...current.boards], activeBoardId: board.id }));
    setActiveWidgetId(board.widgets[0]?.id || "");
    setStudioNotice("Created a local dashboard draft.");
  }

  function removeBoard() {
    if (!activeBoard) {
      return;
    }
    const remaining = boards.filter((board) => board.id !== activeBoard.id);
    if (remaining.length === 0) {
      const freshBoard = createBiBoard({ selectedModel: defaultModelName });
      setStudioState({ boards: [freshBoard], activeBoardId: freshBoard.id });
      setActiveWidgetId(freshBoard.widgets[0]?.id || "");
      setStudioNotice("Reset the BI studio to a fresh local dashboard.");
      return;
    }
    setStudioState({ boards: remaining, activeBoardId: remaining[0].id });
    setActiveWidgetId(remaining[0].widgets[0]?.id || "");
    setStudioNotice("Removed the selected dashboard.");
  }

  function duplicateBoard() {
    if (!activeBoard) {
      return;
    }
    const board = createBiBoard({
      name: `${activeBoard.name} copy`,
      description: activeBoard.description,
      selectedModel: activeBoard.selectedModel,
      lastRefreshedAt: activeBoard.lastRefreshedAt,
      widgets: activeBoard.widgets.map((widget) => ({
        ...widget,
        id: createLocalId("widget"),
      })),
    });
    setStudioState((current) => ({ boards: [board, ...current.boards], activeBoardId: board.id }));
    setActiveWidgetId(board.widgets[0]?.id || "");
    setStudioNotice("Duplicated the active dashboard into a new local draft.");
  }

  function addWidget() {
    if (!activeBoard) {
      return;
    }
    const widget = createBiWidget({
      title: `Widget ${activeBoard.widgets.length + 1}`,
      description: "Local runtime widget powered by semantic query execution.",
      dimension: activeWidget?.dimension || fields.dimensions[0]?.value || "",
      measure: activeWidget?.measure || fields.measures[0]?.value || "",
    });
    updateBoard(activeBoard.id, { widgets: [...activeBoard.widgets, widget] });
    setActiveWidgetId(widget.id);
    setStudioNotice("Added a widget to the dashboard canvas.");
  }

  function removeWidget() {
    if (!activeBoard || !activeWidget) {
      return;
    }
    updateBoard(activeBoard.id, {
      widgets: activeBoard.widgets.filter((widget) => widget.id !== activeWidget.id),
    });
    setStudioNotice("Removed the active widget.");
  }

  function assignField(value, kind) {
    if (!activeBoard) {
      return;
    }
    const target = activeWidget || activeBoard.widgets[0];
    if (!target) {
      const widget = createBiWidget({
        title: "Widget 1",
        description: "Created from the semantic field library.",
        dimension: kind === "dimension" ? value : fields.dimensions[0]?.value || "",
        measure: kind === "measure" ? value : fields.measures[0]?.value || "",
      });
      updateBoard(activeBoard.id, { widgets: [...activeBoard.widgets, widget] });
      setActiveWidgetId(widget.id);
      setStudioNotice(`Created a widget from the selected ${kind}.`);
      return;
    }
    setActiveWidgetId(target.id);
    updateWidget(activeBoard.id, target.id, {
      [kind === "dimension" ? "dimension" : "measure"]: value,
    });
    setStudioNotice(`Assigned ${kind} to ${target.title}.`);
  }

  async function runWidget(widget) {
    if (!activeBoard || !selectedModel || !widget?.measure) {
      return;
    }
    updateWidget(activeBoard.id, widget.id, { running: true, error: "" });
    try {
      const response = await querySemantic({
        semantic_models: [selectedModel],
        measures: [widget.measure],
        dimensions: widget.dimension ? [widget.dimension] : [],
        limit: Number(widget.limit) > 0 ? Number(widget.limit) : 12,
        order: { [widget.measure]: "desc" },
      });
      const rows = Array.isArray(response?.data) ? response.data : [];
      updateWidget(activeBoard.id, widget.id, {
        running: false,
        error: "",
        lastRunAt: new Date().toISOString(),
        result: {
          columns: buildColumnsFromRows(rows),
          rows,
          rowCount: rows.length,
          metadata: Array.isArray(response?.metadata) ? response.metadata : [],
          generated_sql: response?.generated_sql || "",
        },
      });
      updateBoard(activeBoard.id, { lastRefreshedAt: new Date().toISOString() });
    } catch (caughtError) {
      updateWidget(activeBoard.id, widget.id, {
        running: false,
        result: null,
        error: getErrorMessage(caughtError),
      });
    }
  }

  async function runAllWidgets() {
    const widgets = activeBoard?.widgets.filter((widget) => Boolean(widget.measure)) || [];
    if (widgets.length === 0) {
      setStudioNotice("Add a measure to at least one widget before refreshing the dashboard.");
      return;
    }
    await Promise.all(widgets.map((widget) => runWidget(widget)));
    setStudioNotice("Refreshed all runnable widgets against the local runtime.");
  }

  async function copyGeneratedSql() {
    if (!activeWidget?.result?.generated_sql) {
      return;
    }
    try {
      await copyTextToClipboard(activeWidget.result.generated_sql);
      setStudioNotice("Copied generated SQL to the clipboard.");
    } catch (caughtError) {
      setStudioNotice(getErrorMessage(caughtError));
    }
  }

  function exportWidget() {
    if (!activeWidget?.result) {
      return;
    }
    downloadTextFile(
      `${activeWidget.title.toLowerCase().replaceAll(/\s+/g, "-") || "runtime-widget"}.csv`,
      toCsvText(activeWidget.result),
      "text/csv;charset=utf-8",
    );
    setStudioNotice("Downloaded the active widget as CSV.");
  }

  function exportBoard() {
    if (!activeBoard) {
      return;
    }
    downloadTextFile(
      `${activeBoard.name.toLowerCase().replaceAll(/\s+/g, "-") || "runtime-dashboard"}.json`,
      renderJson({
        exported_at: new Date().toISOString(),
        dashboard: activeBoard,
      }),
      "application/json;charset=utf-8",
    );
    setStudioNotice("Exported the active dashboard as local JSON.");
  }

  return (
    <div className="page-stack bi-shell">
      <section className="surface-panel bi-workspace-header">
        <div className="bi-workspace-copy">
          <p className="eyebrow">Workspace / BI Studio</p>
          <h2>{activeBoard?.name || "Runtime dashboard"}</h2>
          <p className="hero-copy">
            Richer cloud-style dashboard composition, but still single-workspace and runtime-local:
            browser autosave, runtime semantic execution, and no control-plane dashboard services.
          </p>
          <div className="bi-workspace-tags">
            <span className="tag">Autosaved locally</span>
            <span className="tag">{biEditMode ? "Edit mode" : "View mode"}</span>
            <span className="tag">Model: {selectedModel || "none"}</span>
          </div>
        </div>

        <div className="bi-workspace-actions">
          <label className="field bi-dashboard-picker">
            <span>Dashboard</span>
            <select
              className="select-input bi-dashboard-select"
              value={activeBoard?.id || ""}
              onChange={(event) =>
                setStudioState((current) => ({ ...current, activeBoardId: event.target.value }))
              }
            >
              {boards.map((board) => (
                <option key={board.id} value={board.id}>
                  {board.name}
                </option>
              ))}
            </select>
          </label>
          <div className="bi-header-actions">
            <button
              className={`ghost-button bi-mode-toggle ${biEditMode ? "active" : ""}`}
              type="button"
              onClick={() => setBiEditMode((current) => !current)}
            >
              {biEditMode ? <Edit3 className="button-icon" aria-hidden="true" /> : <Activity className="button-icon" aria-hidden="true" />}
              {biEditMode ? "Edit mode" : "View mode"}
            </button>
            <button className="ghost-button" type="button" onClick={createBoard}>
              <Plus className="button-icon" aria-hidden="true" />
              New
            </button>
            <button className="ghost-button" type="button" onClick={duplicateBoard} disabled={!activeBoard}>
              <Copy className="button-icon" aria-hidden="true" />
              Duplicate
            </button>
            <button className="ghost-button" type="button" onClick={removeBoard} disabled={!activeBoard}>
              <Trash2 className="button-icon" aria-hidden="true" />
              Delete
            </button>
            <button
              className="ghost-button"
              type="button"
              onClick={() => (activeWidget ? void runWidget(activeWidget) : undefined)}
              disabled={!activeWidget || !selectedModel || !activeWidget.measure}
            >
              <Activity className="button-icon" aria-hidden="true" />
              Run active
            </button>
            <button
              className="ghost-button"
              type="button"
              onClick={() => void runAllWidgets()}
              disabled={!activeBoard || !selectedModel || runnableCount === 0}
            >
              <RefreshCw className="button-icon" aria-hidden="true" />
              Run all
            </button>
            <button className="ghost-button" type="button" onClick={exportBoard} disabled={!activeBoard}>
              <Download className="button-icon" aria-hidden="true" />
              Export
            </button>
            <button className="ghost-button" type="button" onClick={() => navigate("/chat")}>
              <MessageSquareText className="button-icon" aria-hidden="true" />
              Open chat
            </button>
          </div>
        </div>

        <div className="bi-header-metrics">
          <MetricCard icon={LayoutGrid} label="Dashboards" value={formatValue(boards.length)} detail="Local dashboard drafts in the runtime UI." />
          <MetricCard icon={Layers3} label="Semantic models" value={formatValue(models.length)} detail="Models available to the BI studio." />
          <MetricCard icon={SearchCheck} label={biEditMode ? "Editable dimensions" : "Dimensions"} value={formatValue(fields.dimensions.length)} detail="Browsable dimensions in the selected model." />
          <MetricCard icon={Sparkles} label="Measures" value={formatValue(fields.measures.length)} detail="Measures available for widgets and charts." />
        </div>
      </section>

      {studioNotice ? (
        <div className="callout bi-studio-notice">
          <strong>Studio note</strong>
          <span>{studioNotice}</span>
        </div>
      ) : null}

      <section className="bi-studio-grid bi-cloud-grid">
        <div className="detail-stack bi-sidebar-stack">
          <Panel title="Dashboards" eyebrow="Local Collection" className="bi-sidebar-panel">
            <label className="field">
              <span>Semantic model</span>
              <select
                className="select-input"
                value={selectedModel}
                onChange={(event) =>
                  activeBoard
                    ? updateBoard(activeBoard.id, {
                        selectedModel: event.target.value,
                        lastRefreshedAt: null,
                      })
                    : null
                }
                disabled={!activeBoard || !biEditMode}
              >
                {models.map((item) => (
                  <option key={item.id || item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
            </label>
            <div className="inline-notes bi-inline-notes">
              <span>Dashboards: {boards.length}</span>
              <span>Widgets: {totalWidgets}</span>
              <span>Refresh: {formatValue(activeBoard?.lastRefreshedAt || "Not run yet")}</span>
            </div>
            <div className="board-list">
              {boards.map((board) => (
                <button
                  key={board.id}
                  className={`list-card ${board.id === activeBoard?.id ? "active" : ""}`}
                  type="button"
                  onClick={() => setStudioState((current) => ({ ...current, activeBoardId: board.id }))}
                >
                  <strong>{board.name}</strong>
                  <span>{board.description}</span>
                  <small>{board.selectedModel || "No semantic model"} - {board.widgets.length} widgets</small>
                </button>
              ))}
            </div>
            <div className="panel-actions-inline">
              <button className="ghost-button" type="button" onClick={removeBoard} disabled={!activeBoard}>
                <Trash2 className="button-icon" aria-hidden="true" />
                Delete dashboard
              </button>
            </div>
          </Panel>

          <Panel title="Field Library" eyebrow="Semantic Model" className="bi-sidebar-panel">
            <label className="field">
              <span>Find fields</span>
              <input
                className="text-input"
                type="search"
                value={fieldSearch}
                onChange={(event) => setFieldSearch(event.target.value)}
                placeholder="Filter datasets, dimensions, or measures"
              />
            </label>
            <div className="inline-notes bi-inline-notes">
              <span>Datasets: {semanticDatasets.length}</span>
              <span>Dimensions: {fields.dimensions.length}</span>
              <span>Measures: {fields.measures.length}</span>
            </div>
            {detailError ? <div className="error-banner">{detailError}</div> : null}
            {detailLoading ? (
              <div className="empty-box">Loading semantic model...</div>
            ) : filteredSemanticDatasets.length > 0 ? (
              <div className="field-section-list">
                {filteredSemanticDatasets.map((dataset) => (
                  <div key={dataset.name} className="field-group">
                    <div className="field-group-header">
                      <strong>{dataset.name}</strong>
                      <span>{dataset.relationName || "semantic dataset"}</span>
                    </div>
                    <div className="field-pill-list">
                      {dataset.dimensions.map((item) => {
                        const value = `${dataset.name}.${item.name}`;
                        return (
                          <button
                            key={value}
                            className={`field-pill ${activeWidget?.dimension === value ? "active" : ""} ${!biEditMode ? "static" : ""}`}
                            type="button"
                            onClick={() => assignField(value, "dimension")}
                            disabled={!biEditMode}
                          >
                            {item.name}
                          </button>
                        );
                      })}
                      {dataset.measures.map((item) => {
                        const value = `${dataset.name}.${item.name}`;
                        return (
                          <button
                            key={value}
                            className={`field-pill ${activeWidget?.measure === value ? "active" : ""} ${!biEditMode ? "static" : ""}`}
                            type="button"
                            onClick={() => assignField(value, "measure")}
                            disabled={!biEditMode}
                          >
                            {item.name}
                          </button>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <PageEmpty title="No fields found" message="Adjust the search or select a semantic model with exposed fields." />
            )}
          </Panel>
        </div>
        <Panel
          title={activeBoard?.name || "Dashboard Canvas"}
          eyebrow="Widget Canvas"
          className="bi-canvas-panel"
          actions={
            <div className="panel-actions-inline">
              <button className="ghost-button" type="button" onClick={addWidget} disabled={!activeBoard || !biEditMode}>
                <Plus className="button-icon" aria-hidden="true" />
                Add widget
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={() => navigate(`/semantic-models/${buildItemRef({ name: selectedModel })}`)}
                disabled={!selectedModel}
              >
                <ArrowRight className="button-icon" aria-hidden="true" />
                Open model
              </button>
            </div>
          }
        >
          {activeBoard ? (
            <div className="detail-stack">
              <div className="board-editor">
                <div className="form-grid compact">
                  <label className="field">
                    <span>Dashboard name</span>
                    <input
                      className="text-input"
                      type="text"
                      value={activeBoard.name}
                      onChange={(event) => updateBoard(activeBoard.id, { name: event.target.value })}
                      disabled={!biEditMode}
                    />
                  </label>
                  <label className="field">
                    <span>Semantic model</span>
                    <select
                      className="select-input"
                      value={selectedModel}
                      onChange={(event) =>
                        updateBoard(activeBoard.id, {
                          selectedModel: event.target.value,
                          lastRefreshedAt: null,
                        })
                      }
                      disabled={!biEditMode}
                    >
                      {models.map((item) => (
                        <option key={item.id || item.name} value={item.name}>
                          {item.name}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="field field-full">
                    <span>Description</span>
                    <textarea
                      className="textarea-input"
                      value={activeBoard.description}
                      onChange={(event) => updateBoard(activeBoard.id, { description: event.target.value })}
                      disabled={!biEditMode}
                    />
                  </label>
                </div>
                <div className="inline-notes bi-inline-notes">
                  <span>Widgets: {activeBoard.widgets.length}</span>
                  <span>Runnable: {runnableCount}</span>
                  <span>Storage: Browser local storage</span>
                  <span>Mode: {biEditMode ? "Editable" : "Preview"}</span>
                </div>
              </div>
              {activeBoard.widgets.length > 0 ? (
                <div className="widget-canvas bi-widget-canvas">
                  {activeBoard.widgets.map((widget) => (
                    <article
                      key={widget.id}
                      className={`widget-tile bi-widget-tile ${widget.id === activeWidget?.id ? "active" : ""}`}
                    >
                      <button className="widget-tile-header" type="button" onClick={() => setActiveWidgetId(widget.id)}>
                        <div>
                          <strong>{widget.title}</strong>
                          <span>{widget.description || "Configure fields to shape the runtime query."}</span>
                        </div>
                        <span className="chart-kind">{widget.chartType}</span>
                      </button>
                      <div className="inline-notes">
                        <span>Dimension: {widget.dimension || "none"}</span>
                        <span>Measure: {widget.measure || "none"}</span>
                        <span>Rows: {formatValue(widget.result?.rowCount || 0)}</span>
                      </div>
                      {widget.error ? <div className="error-banner">{widget.error}</div> : null}
                      {widget.running ? (
                        <div className="empty-box">Running semantic query...</div>
                      ) : widget.result ? (
                        <div className="detail-stack">
                          <ChartPreview
                            title={widget.title}
                            result={widget.result}
                            metadata={Array.isArray(widget.result?.metadata) ? widget.result.metadata : []}
                            visualization={{
                              chartType: widget.chartType,
                              x: widget.dimension,
                              y: [widget.measure],
                            }}
                            preferredDimension={widget.dimension}
                            preferredMeasure={widget.measure}
                          />
                          <ResultTable result={widget.result} maxPreviewRows={6} />
                        </div>
                      ) : (
                        <PageEmpty
                          title="No widget result"
                          message="Assign a measure and refresh the widget to render chart and table output."
                        />
                      )}
                      <div className="panel-actions-inline bi-widget-actions">
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => void runWidget(widget)}
                          disabled={!selectedModel || !widget.measure}
                        >
                          <RefreshCw className="button-icon" aria-hidden="true" />
                          Run
                        </button>
                      </div>
                    </article>
                  ))}
                </div>
              ) : (
                <PageEmpty
                  title="No widgets yet"
                  message="Add a widget to begin composing a runtime dashboard."
                  action={
                    <button className="primary-button" type="button" onClick={addWidget} disabled={!biEditMode}>
                      Add widget
                    </button>
                  }
                />
              )}
            </div>
          ) : (
            <PageEmpty title="No dashboard selected" message="Create or select a dashboard to continue." />
          )}
        </Panel>

        <div className="detail-stack bi-inspector-stack">
          <Panel title="Widget Studio" eyebrow="Config" className="bi-inspector-panel">
            {activeWidget && activeBoard ? (
              <div className="detail-stack">
                <div className="form-grid compact">
                  <label className="field">
                    <span>Widget title</span>
                    <input
                      className="text-input"
                      type="text"
                      value={activeWidget.title}
                      onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { title: event.target.value })}
                      disabled={!biEditMode}
                    />
                  </label>
                  <label className="field">
                    <span>Chart type</span>
                    <select
                      className="select-input"
                      value={activeWidget.chartType}
                      onChange={(event) =>
                        updateWidget(activeBoard.id, activeWidget.id, { chartType: event.target.value })
                      }
                      disabled={!biEditMode}
                    >
                      <option value="bar">Bar</option>
                      <option value="line">Line</option>
                      <option value="pie">Pie</option>
                    </select>
                  </label>
                  <label className="field field-full">
                    <span>Widget description</span>
                    <textarea
                      className="textarea-input"
                      value={activeWidget.description}
                      onChange={(event) =>
                        updateWidget(activeBoard.id, activeWidget.id, { description: event.target.value })
                      }
                      disabled={!biEditMode}
                    />
                  </label>
                  <label className="field">
                    <span>Dimension</span>
                    <select
                      className="select-input"
                      value={activeWidget.dimension}
                      onChange={(event) =>
                        updateWidget(activeBoard.id, activeWidget.id, { dimension: event.target.value })
                      }
                      disabled={detailLoading || !biEditMode}
                    >
                      <option value="">No dimension</option>
                      {fields.dimensions.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="field">
                    <span>Measure</span>
                    <select
                      className="select-input"
                      value={activeWidget.measure}
                      onChange={(event) =>
                        updateWidget(activeBoard.id, activeWidget.id, { measure: event.target.value })
                      }
                      disabled={detailLoading || !biEditMode}
                    >
                      <option value="">Select measure</option>
                      {fields.measures.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="field">
                    <span>Row limit</span>
                    <input
                      className="text-input"
                      type="number"
                      min="1"
                      value={activeWidget.limit}
                      onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { limit: event.target.value })}
                      disabled={!biEditMode}
                    />
                  </label>
                </div>
                <div className="panel-actions-inline">
                  <button
                    className="primary-button"
                    type="button"
                    onClick={() => runWidget(activeWidget)}
                    disabled={!selectedModel || !activeWidget.measure}
                  >
                    <RefreshCw className="button-icon" aria-hidden="true" />
                    Run widget
                  </button>
                  <button className="ghost-button" type="button" onClick={exportWidget} disabled={!activeWidget.result}>
                    <Download className="button-icon" aria-hidden="true" />
                    Export CSV
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={copyGeneratedSql}
                      disabled={!activeWidget.result?.generated_sql}
                    >
                      <Copy className="button-icon" aria-hidden="true" />
                      Copy SQL
                    </button>
                  <button className="ghost-button" type="button" onClick={removeWidget} disabled={!biEditMode}>
                    <Trash2 className="button-icon" aria-hidden="true" />
                    Delete widget
                  </button>
                </div>
                <div className="inline-notes bi-inline-notes">
                  <span>Last run: {formatValue(activeWidget.lastRunAt || "Not run yet")}</span>
                  <span>Dimension: {activeWidget.dimension || "none"}</span>
                  <span>Measure: {activeWidget.measure || "none"}</span>
                </div>
                {activeWidget.result?.generated_sql ? (
                  <details className="diagnostics-disclosure">
                    <summary>Generated SQL</summary>
                    <pre className="code-block compact">{activeWidget.result.generated_sql}</pre>
                  </details>
                ) : null}
              </div>
            ) : (
              <PageEmpty title="No active widget" message="Select or create a widget to configure the dashboard." />
            )}
          </Panel>

          <Panel title="Model Summary" eyebrow="Semantic Runtime" className="bi-inspector-panel">
            {detailLoading ? (
              <div className="empty-box">Loading semantic model...</div>
            ) : detail ? (
              <div className="detail-stack">
                <DetailList
                  items={[
                    { label: "Semantic model", value: formatValue(detail.name || selectedModel) },
                    { label: "Description", value: formatValue(detail.description) },
                    { label: "Datasets", value: formatValue(detail.dataset_count || semanticDatasets.length) },
                    { label: "Dimensions", value: formatValue(detail.dimension_count || fields.dimensions.length) },
                    { label: "Measures", value: formatValue(detail.measure_count || fields.measures.length) },
                  ]}
                />
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => navigate(`/semantic-models/${buildItemRef({ name: selectedModel })}`)}
                >
                  <ArrowRight className="button-icon" aria-hidden="true" />
                  Open semantic model
                </button>
              </div>
            ) : (
              <PageEmpty title="No semantic model" message="Select a semantic model to populate the BI studio." />
            )}
          </Panel>
        </div>
      </section>
    </div>
  );
}

function SettingsPage({ authStatus, session }) {
  const { data, loading, error, reload } = useAsyncData(fetchRuntimeInfo);
  const info = data || {};

  return (
    <div className="page-stack">
      <Panel
        title="Runtime Settings"
        eyebrow="Identity and Capabilities"
        actions={
          <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh runtime info"}
          </button>
        }
      >
        {error ? <div className="error-banner">{error}</div> : null}
        {loading ? (
          <div className="empty-box">Loading runtime info...</div>
        ) : (
          <DetailList
            items={[
              { label: "Runtime mode", value: formatValue(info.runtime_mode) },
              { label: "Config path", value: formatValue(info.config_path) },
              { label: "Workspace ID", value: formatValue(info.workspace_id) },
              { label: "Actor ID", value: formatValue(info.actor_id) },
              { label: "Default semantic model", value: formatValue(info.default_semantic_model) },
              { label: "Default agent", value: formatValue(info.default_agent) },
            ]}
          />
        )}
      </Panel>

      <section className="summary-grid">
        <Panel title="Session" eyebrow="Auth">
          <DetailList
            items={[
              { label: "Auth enabled", value: formatValue(authStatus?.auth_enabled) },
              { label: "Auth mode", value: formatValue(authStatus?.auth_mode) },
              { label: "Bootstrap required", value: formatValue(authStatus?.bootstrap_required) },
              { label: "Has admin", value: formatValue(authStatus?.has_admin) },
              { label: "Login allowed", value: formatValue(authStatus?.login_allowed) },
              { label: "User", value: formatValue(session?.username || "runtime") },
              { label: "Email", value: formatValue(session?.email) },
              { label: "Roles", value: formatList(session?.roles) },
            ]}
          />
        </Panel>

        <Panel title="Capabilities" eyebrow="Runtime API">
          {Array.isArray(info.capabilities) && info.capabilities.length > 0 ? (
            <div className="tag-list">
              {info.capabilities.map((item) => (
                <span key={item} className="tag">
                  {item}
                </span>
              ))}
            </div>
          ) : (
            <PageEmpty title="No capabilities" message="The runtime did not expose capability metadata." />
          )}
        </Panel>
      </section>
    </div>
  );
}

function RuntimeRoutes({ authStatus, session, onLogout }) {
  return (
    <AppShell session={session} authStatus={authStatus} onLogout={onLogout}>
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/connectors" element={<ConnectorsPage />} />
        <Route path="/connectors/:id" element={<ConnectorsPage />} />
        <Route path="/datasets" element={<DatasetsPage />} />
        <Route path="/datasets/:id" element={<DatasetsPage />} />
        <Route path="/semantic-models" element={<SemanticModelsPage />} />
        <Route path="/semantic-models/:id" element={<SemanticModelsPage />} />
        <Route path="/sql" element={<SqlWorkspacePage />} />
        <Route path="/agents" element={<AgentsPage />} />
        <Route path="/agents/:id" element={<AgentsPage />} />
        <Route path="/chat" element={<ChatIndexPage />} />
        <Route path="/chat/:threadId" element={<ChatPage />} />
        <Route path="/bi" element={<BiPage />} />
        <Route path="/settings" element={<SettingsPage authStatus={authStatus} session={session} />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}

function App() {
  const [state, setState] = useState({
    phase: "loading",
    authStatus: null,
    session: null,
    error: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");

  async function loadAuthState() {
    startTransition(() => {
      setState((current) => ({
        ...current,
        phase: "loading",
        error: "",
      }));
    });

    try {
      const authStatus = await fetchAuthBootstrapStatus();

      if (!authStatus.auth_enabled) {
        const me = await fetchAuthMe();
        startTransition(() => {
          setState({
            phase: "ready",
            authStatus,
            session: me.user || null,
            error: "",
          });
        });
        return;
      }

      if (authStatus.auth_mode !== "local") {
        startTransition(() => {
          setState({
            phase: "unsupported",
            authStatus,
            session: null,
            error: "",
          });
        });
        return;
      }

      if (authStatus.bootstrap_required) {
        startTransition(() => {
          setState({
            phase: "bootstrap",
            authStatus,
            session: null,
            error: "",
          });
        });
        return;
      }

      try {
        const me = await fetchAuthMe();
        startTransition(() => {
          setState({
            phase: "ready",
            authStatus,
            session: me.user || null,
            error: "",
          });
        });
      } catch (caughtError) {
        if (caughtError?.status === 401) {
          startTransition(() => {
            setState({
              phase: "login",
              authStatus,
              session: null,
              error: "",
            });
          });
          return;
        }
        throw caughtError;
      }
    } catch (caughtError) {
      startTransition(() => {
        setState({
          phase: "error",
          authStatus: null,
          session: null,
          error: getErrorMessage(caughtError),
        });
      });
    }
  }

  useEffect(() => {
    void loadAuthState();
  }, []);

  async function handleBootstrap(form) {
    setSubmitting(true);
    setSubmitError("");
    try {
      await bootstrapAdmin(form);
      await loadAuthState();
    } catch (caughtError) {
      setSubmitError(getErrorMessage(caughtError));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleLogin(form) {
    setSubmitting(true);
    setSubmitError("");
    try {
      await login(form);
      await loadAuthState();
    } catch (caughtError) {
      setSubmitError(getErrorMessage(caughtError));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleLogout() {
    await logout();
    await loadAuthState();
  }

  if (state.phase === "loading") {
    return <LoadingScreen />;
  }

  if (state.phase === "error") {
    return <ErrorScreen error={state.error} onRetry={() => void loadAuthState()} />;
  }

  if (state.phase === "bootstrap") {
    return <BootstrapScreen error={submitError} submitting={submitting} onSubmit={handleBootstrap} />;
  }

  if (state.phase === "login") {
    return <LoginScreen error={submitError} submitting={submitting} onSubmit={handleLogin} />;
  }

  if (state.phase === "unsupported") {
    return <UnsupportedAuthScreen authStatus={state.authStatus} onRetry={() => void loadAuthState()} />;
  }

  return (
    <RuntimeRoutes
      authStatus={state.authStatus}
      session={state.session}
      onLogout={() => void handleLogout()}
    />
  );
}

export default App;
