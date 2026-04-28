import {
  Activity,
  Blocks,
  Bot,
  MessageSquareText,
  Table2,
} from "lucide-react";
import { Link } from "react-router-dom";

import { MetricCard, PageEmpty } from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { readStoredJson } from "../hooks/usePersistentState";
import {
  fetchAgents,
  fetchConnectors,
  fetchDatasets,
  fetchRuntimeSummary,
  fetchSemanticModels,
  fetchThreads,
} from "../lib/runtimeApi";
import {
  DASHBOARD_BUILDER_STORAGE_KEY,
  SQL_HISTORY_STORAGE_KEY,
  SQL_SAVED_STORAGE_KEY,
  buildActivityFeed,
  formatRelativeTime,
} from "../lib/runtimeUi";
import { formatValue, getRuntimeTimestamp } from "../lib/format";
import { ActivityPanel, QuickActionPanel } from "../components/overview/CommandCenterPanels";

function trimText(value, maxLength = 96) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}...`;
}

function buildThreadTitle(thread) {
  return thread?.title?.trim() || `Thread ${String(thread?.id || "").slice(0, 8)}`;
}

function buildQueryModeLabel(queryScope, connectionName = "") {
  if (queryScope === "semantic") {
    return "Semantic query";
  }
  if (queryScope === "source") {
    return connectionName ? `Source SQL on ${connectionName}` : "Source SQL";
  }
  return "Dataset SQL";
}

function sortByTimestampDesc(items) {
  return [...items].sort((left, right) => {
    const leftTime = getRuntimeTimestamp(left.timestamp || 0);
    const rightTime = getRuntimeTimestamp(right.timestamp || 0);
    return rightTime - leftTime;
  });
}

async function loadOverviewData() {
  const [summary, connectors, datasets, models, agents, threads] = await Promise.all([
    fetchRuntimeSummary(),
    fetchConnectors(),
    fetchDatasets(),
    fetchSemanticModels(),
    fetchAgents(),
    fetchThreads(),
  ]);

  return {
    summary: summary || {},
    connectors: Array.isArray(connectors?.items) ? connectors.items : [],
    datasets: Array.isArray(datasets?.items) ? datasets.items : [],
    models: Array.isArray(models?.items) ? models.items : [],
    agents: Array.isArray(agents?.items) ? agents.items : [],
    threads: Array.isArray(threads?.items) ? threads.items : [],
  };
}

export function OverviewPage() {
  const { data, loading, error, reload } = useAsyncData(loadOverviewData);
  const summary = data?.summary || {};
  const counts = summary.counts || {};
  const connectors = data?.connectors || [];
  const datasets = data?.datasets || [];
  const models = data?.models || [];
  const agents = data?.agents || [];
  const threads = data?.threads || [];
  const sortedThreads = sortByTimestampDesc(
    threads.map((thread) => ({
      ...thread,
      timestamp: thread.updated_at || thread.created_at,
    })),
  );
  const latestThread = sortedThreads[0] || null;
  const storedSqlHistory = readStoredJson(SQL_HISTORY_STORAGE_KEY, []);
  const sqlHistory = Array.isArray(storedSqlHistory) ? storedSqlHistory : [];
  const storedSavedQueries = readStoredJson(SQL_SAVED_STORAGE_KEY, []);
  const savedQueries = Array.isArray(storedSavedQueries) ? storedSavedQueries : [];
  const dashboardState = readStoredJson(DASHBOARD_BUILDER_STORAGE_KEY, { boards: [] });
  const boards = Array.isArray(dashboardState?.boards) ? dashboardState.boards : [];

  const activityItems = buildActivityFeed({ connectors, datasets, models, agents, threads });
  const recentExecutionItems = sortByTimestampDesc([
    ...sortedThreads.slice(0, 3).map((thread) => ({
      id: `thread-${thread.id}`,
      href: `/chat/${encodeURIComponent(String(thread.id))}`,
      title: buildThreadTitle(thread),
      kind: "Thread",
      description: "Continue an existing analytical thread.",
      timestamp: thread.updated_at || thread.created_at,
    })),
    ...sqlHistory.slice(0, 3).map((entry) => ({
      id: `query-${entry.id || entry.createdAt}`,
      href: "/query-workspace",
      title: buildQueryModeLabel(entry.queryScope, entry.connectionName),
      kind: "Query run",
      description: trimText(entry.query, 110) || "Open Query Workspace to continue this run.",
      timestamp: entry.createdAt,
    })),
    ...boards
      .filter((board) => board?.lastRefreshedAt)
      .slice(0, 2)
      .map((board) => ({
        id: `dashboard-${board.id}`,
        href: "/dashboards",
        title: board.name || "Runtime dashboard",
        kind: "Dashboard",
        description: "Local semantic dashboard refreshed against the runtime.",
        timestamp: board.lastRefreshedAt,
      })),
  ]).slice(0, 8);

  const quickActions = [
    {
      to: latestThread ? `/chat/${encodeURIComponent(String(latestThread.id))}` : "/chat",
      label: latestThread ? "Continue analysis" : "Ask the runtime",
      description: latestThread
        ? `Resume ${buildThreadTitle(latestThread)}.`
        : "Start a new runtime thread.",
      icon: MessageSquareText,
      emphasis: "primary",
    },
    {
      to: "/query-workspace",
      label: "Run a query",
      description: "Move straight into semantic or SQL analysis.",
      icon: Table2,
    },
    {
      to: "/runs",
      label: "Review runs",
      description: "Pick up recent executions and failures.",
      icon: Activity,
    },
    {
      to: "/semantic-models",
      label: "Refine models",
      description: "Keep governed metrics and dimensions aligned.",
      icon: Blocks,
    },
  ];
  const runtimeStatusItems = [
    {
      label: "Default agent",
      value: summary.runtime?.default_agent || "Not set",
      detail: "Current ask path.",
    },
    {
      label: "Default semantic model",
      value: summary.runtime?.default_semantic_model || "Not set",
      detail: "Primary governed model.",
    },
    {
      label: "Saved queries",
      value: formatValue(savedQueries.length),
      detail: "Local query history kept in this browser.",
    },
    {
      label: "Dashboard drafts",
      value: formatValue(boards.length),
      detail: "Local dashboard work still available.",
    },
  ];
  const buildLinks = [
    { to: "/semantic-models", label: "Semantic models" },
    { to: "/datasets", label: "Datasets" },
    { to: "/connectors", label: "Connectors" },
    { to: "/dashboards", label: "Dashboards" },
  ];

  return (
    <div className="page-stack command-center-shell">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Command Center</p>
            <h2>Keep runtime work moving</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(counts.connectors ?? connectors.length)} connectors</span>
              <span className="chip">{formatValue(counts.datasets ?? datasets.length)} datasets</span>
              <span className="chip">{formatValue(counts.semantic_models ?? models.length)} models</span>
              <span className="chip">{formatValue(counts.threads ?? threads.length)} threads</span>
              <span className="chip">{summary.auth?.auth_enabled ? "Session scoped" : "Direct access"}</span>
            </div>
          </div>
          <div className="product-command-bar-actions">
            <Link className="primary-button" to="/chat">
              Ask runtime
            </Link>
            <Link className="ghost-button" to="/runs">
              View runs
            </Link>
            <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
              {loading ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>
      </section>

      <section className="metric-grid metric-grid--compact">
        <MetricCard
          icon={MessageSquareText}
          label="Threads"
          value={formatValue(threads.length)}
          detail={
            latestThread
              ? `Latest activity ${formatRelativeTime(latestThread.updated_at || latestThread.created_at)}`
              : "No active thread yet"
          }
        />
        <MetricCard
          icon={Table2}
          label="Local runs"
          value={formatValue(sqlHistory.length + boards.length)}
          detail="Saved query and dashboard work."
        />
        <MetricCard
          icon={Blocks}
          label="Governed assets"
          value={formatValue(models.length + datasets.length)}
          detail="Semantic models and datasets in play."
        />
        <MetricCard
          icon={Bot}
          label="Agents"
          value={formatValue(agents.length)}
          detail={summary.runtime?.default_agent ? `Default: ${summary.runtime.default_agent}` : "No default agent"}
        />
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="command-center-grid command-center-grid--balanced">
        <section className="surface-panel command-primary-panel">
          <div className="command-panel-head-row">
            <div>
              <p className="command-panel-eyebrow">Start</p>
              <h3>Move from question to result</h3>
            </div>
          </div>
          <QuickActionPanel actions={quickActions} showHeader={false} />
        </section>

        <section className="surface-panel command-state-panel">
          <div className="command-panel-heading">
            <div>
              <p className="command-panel-eyebrow">Runtime status</p>
              <h3>Keep the main context in view</h3>
            </div>
          </div>
          <div className="command-status-list">
            {runtimeStatusItems.map((item) => (
              <article key={item.label} className="command-status-item">
                <span>{item.label}</span>
                <strong>{item.value}</strong>
                <p>{item.detail}</p>
              </article>
            ))}
          </div>
          <div className="command-link-row">
            {buildLinks.map((item) => (
              <Link key={item.to} className="ghost-button command-link-chip" to={item.to}>
                {item.label}
              </Link>
            ))}
          </div>
        </section>
      </section>

      <section className="command-center-grid secondary">
        <ActivityPanel
          title="Recent activity"
          eyebrow="Resources"
          items={activityItems}
          emptyTitle="No recent activity"
          emptyMessage="Recent runtime changes will appear here."
        />
        <ActivityPanel
          title="Recent runs"
          eyebrow="Execution"
          items={recentExecutionItems}
          emptyTitle="No recent run"
          emptyMessage="Recent runtime execution will appear here."
        />
      </section>

      {!loading &&
      connectors.length === 0 &&
      datasets.length === 0 &&
      models.length === 0 &&
      agents.length === 0 ? (
        <PageEmpty
          title="Runtime onboarding ready"
          message="The runtime is empty but healthy. Start with connectors, datasets, or semantic models to build the question-answering surface."
        />
      ) : null}
    </div>
  );
}
