import {
  Bot,
  Cable,
  Database,
  LayoutGrid,
  MessageSquareText,
  Sparkles,
  Table2,
} from "lucide-react";

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
  BI_STUDIO_STORAGE_KEY,
  SQL_HISTORY_STORAGE_KEY,
  SQL_SAVED_STORAGE_KEY,
  buildActivityFeed,
} from "../lib/runtimeUi";
import { formatValue } from "../lib/format";
import { FeatureCard, PageEmpty } from "../components/PagePrimitives";
import {
  ActivityPanel,
  QuickActionPanel,
  RuntimeMemoryPanel,
} from "../components/overview/CommandCenterPanels";

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

  const memory = (() => {
    const sqlHistory = readStoredJson(SQL_HISTORY_STORAGE_KEY, []);
    const savedQueries = readStoredJson(SQL_SAVED_STORAGE_KEY, []);
    const studio = readStoredJson(BI_STUDIO_STORAGE_KEY, { boards: [] });
    const boards = Array.isArray(studio?.boards) ? studio.boards : [];
    return [
      {
        label: "Saved queries",
        value: formatValue(savedQueries.length),
        detail: "SQL workbench snippets persisted to this browser.",
      },
      {
        label: "Recent SQL runs",
        value: formatValue(sqlHistory.length),
        detail: "Local execution history captured from the runtime SQL workspace.",
      },
      {
        label: "BI dashboards",
        value: formatValue(boards.length),
        detail: "Browser-local dashboards replacing cloud dashboard services.",
      },
    ];
  })();

  const activityItems = buildActivityFeed({ connectors, datasets, models, agents, threads });

  const quickActions = [
    {
      to: "/connectors",
      label: "Add or inspect connectors",
      description: "Review ingress posture, sync scope, and runtime source inventory.",
      icon: Cable,
      emphasis: "primary",
    },
    {
      to: "/datasets",
      label: "Inspect datasets",
      description: "Open governed dataset bindings, schema, and preview flows.",
      icon: Database,
    },
    {
      to: "/sql",
      label: "Open SQL workspace",
      description: "Run federated or direct SQL with local saved query memory.",
      icon: Table2,
    },
    {
      to: "/bi",
      label: "Launch BI studio",
      description: "Compose runtime-local dashboards on top of semantic models.",
      icon: Sparkles,
    },
    {
      to: "/chat",
      label: "Resume threads",
      description: "Jump into runtime agent threads and continue analysis.",
      icon: MessageSquareText,
    },
    {
      to: "/agents",
      label: "Review agents",
      description: "Inspect prompts, tools, and runtime execution posture.",
      icon: Bot,
    },
  ];

  return (
    <div className="page-stack command-center-shell">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Command Center</p>
            <h2>Runtime workspace</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(counts.connectors ?? connectors.length)} connectors</span>
              <span className="chip">{formatValue(counts.datasets ?? datasets.length)} datasets</span>
              <span className="chip">{formatValue(counts.semantic_models ?? models.length)} models</span>
              <span className="chip">{formatValue(counts.agents ?? agents.length)} agents</span>
              <span className="chip">{summary.auth?.auth_enabled ? "Session scoped" : "Direct access"}</span>
            </div>
          </div>
          <div className="product-command-bar-actions">
            <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
              {loading ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>
      </section>

      <section className="command-center-grid">
        <section className="surface-panel command-primary-panel">
          <div className="command-panel-head-row">
            <div>
              <p className="command-panel-eyebrow">Workspace</p>
              <h3>Move through the runtime stack</h3>
            </div>
          </div>
          {error ? <div className="error-banner">{error}</div> : null}
          <QuickActionPanel actions={quickActions} />
        </section>

        <RuntimeMemoryPanel items={memory} />
      </section>

      <section className="command-center-grid secondary">
        <ActivityPanel
          title="Resume active work"
          eyebrow="Recent activity"
          items={activityItems}
          emptyTitle="No recent activity"
          emptyMessage="Datasets, models, agents, connectors, and threads will appear here as the runtime fills out."
        />
        <section className="command-activity-panel surface-panel">
          <div className="command-panel-heading">
            <div>
              <p className="command-panel-eyebrow">Feature surfaces</p>
              <h3>Runtime entry points</h3>
            </div>
          </div>
          <div className="feature-grid">
            <FeatureCard
              to="/connectors"
              icon={Cable}
              metric={`${formatValue(connectors.length)} connectors`}
              title="Connector management"
              description="Inspect runtime connector inventory, sync resources, and state."
              cta="Open connectors"
            />
            <FeatureCard
              to="/sql"
              icon={Table2}
              metric={`${formatValue(memory[0].value)} saved queries`}
              title="SQL workspace"
              description="Run federated and direct SQL with local saved query memory and history."
              cta="Open SQL"
            />
            <FeatureCard
              to="/bi"
              icon={LayoutGrid}
              metric={`${formatValue(memory[2].value)} dashboards`}
              title="BI studio"
              description="Compose runtime-local dashboards with semantic fields, filters, and widgets."
              cta="Launch BI"
            />
          </div>
        </section>
      </section>

      {!loading &&
      connectors.length === 0 &&
      datasets.length === 0 &&
      models.length === 0 &&
      agents.length === 0 ? (
        <PageEmpty
          title="Runtime onboarding ready"
          message="The runtime is empty but healthy. Start with connectors or datasets to rebuild the working surface."
        />
      ) : null}
    </div>
  );
}
