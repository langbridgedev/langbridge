import { useEffect, useMemo, useState } from "react";
import {
  LayoutGrid,
  MessageSquareText,
  Table2,
} from "lucide-react";
import { Link } from "react-router-dom";

import { RuntimeResultPanel } from "../components/RuntimeResultPanel";
import {
  DetailList,
  PageEmpty,
  Panel,
  SectionTabs,
} from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { readStoredJson } from "../hooks/usePersistentState";
import { loadDashboardBuilderState } from "../lib/dashboardBuilder";
import { formatDateTime, formatValue, getErrorMessage, getRuntimeTimestamp } from "../lib/format";
import { fetchAgents, fetchThreadMessages, fetchThreads } from "../lib/runtimeApi";
import {
  DASHBOARD_BUILDER_STORAGE_KEY,
  SQL_HISTORY_STORAGE_KEY,
  buildConversationTurns,
  formatRelativeTime,
} from "../lib/runtimeUi";

const RUN_FILTERS = [
  { value: "all", label: "All runs" },
  { value: "agent", label: "Agent turns" },
  { value: "query", label: "Queries" },
  { value: "dashboard", label: "Dashboards" },
];

const RUN_KIND_META = {
  agent: {
    icon: MessageSquareText,
    label: "Agent turn",
    emptyMessage: "Ask the runtime to generate agent runs and thread history.",
  },
  query: {
    icon: Table2,
    label: "Query run",
    emptyMessage: "Run semantic, dataset, or source SQL to capture query executions.",
  },
  dashboard: {
    icon: LayoutGrid,
    label: "Dashboard widget",
    emptyMessage: "Run Dashboard Builder widgets to capture semantic execution history.",
  },
};

function trimText(value, maxLength = 160) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}...`;
}

function sortByTimestampDesc(items) {
  return [...items].sort((left, right) => {
    const leftTime = getRuntimeTimestamp(left.timestamp || 0);
    const rightTime = getRuntimeTimestamp(right.timestamp || 0);
    return rightTime - leftTime;
  });
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

function buildQueryRunTitle(entry) {
  const firstLine = String(entry?.query || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find(Boolean);
  return trimText(firstLine || buildQueryModeLabel(entry?.queryScope, entry?.connectionName), 72);
}

function buildDashboardRunVisualization(widget) {
  const x = widget?.chartX || (Array.isArray(widget?.dimensions) ? widget.dimensions[0] : "");
  const y =
    widget?.chartY || (Array.isArray(widget?.measures) ? widget.measures[0] : "");

  if (!widget?.chartType && !x && !y) {
    return null;
  }

  return {
    chartType: widget?.chartType || "table",
    x,
    y: y ? [y] : [],
  };
}

function buildRunStateLabel(run) {
  if (run.status === "pending") {
    return "Running";
  }
  if (run.status === "error") {
    return "Failed";
  }
  return "Completed";
}

async function loadRunsData() {
  const [agentsPayload, threadsPayload] = await Promise.all([fetchAgents(), fetchThreads()]);
  const agents = Array.isArray(agentsPayload?.items) ? agentsPayload.items : [];
  const threads = sortByTimestampDesc(
    Array.isArray(threadsPayload?.items) ? threadsPayload.items : [],
  ).slice(0, 8);

  const threadMessagePayloads = await Promise.all(
    threads.map(async (thread) => {
      try {
        const payload = await fetchThreadMessages(String(thread.id));
        return {
          ...thread,
          messages: Array.isArray(payload?.items) ? payload.items : [],
          messageError: "",
        };
      } catch (caughtError) {
        return {
          ...thread,
          messages: [],
          messageError: getErrorMessage(caughtError),
        };
      }
    }),
  );

  return {
    agents,
    threads,
    threadMessagePayloads,
  };
}

export function RunsPage() {
  const { data, loading, error, reload } = useAsyncData(loadRunsData);
  const [activeFilter, setActiveFilter] = useState("all");
  const [selectedRunId, setSelectedRunId] = useState("");

  const storedSqlHistory = readStoredJson(SQL_HISTORY_STORAGE_KEY, []);
  const sqlHistory = Array.isArray(storedSqlHistory) ? storedSqlHistory : [];
  const dashboardState = loadDashboardBuilderState(readStoredJson);

  const agentRuns = useMemo(() => {
    const threadPayloads = Array.isArray(data?.threadMessagePayloads) ? data.threadMessagePayloads : [];
    const agents = Array.isArray(data?.agents) ? data.agents : [];

    return sortByTimestampDesc(
      threadPayloads.flatMap((thread) => {
        const threadTitle = buildThreadTitle(thread);
        return buildConversationTurns(thread.messages, agents).map((turn) => ({
          id: `agent-${thread.id}-${turn.id}`,
          kind: "agent",
          title: trimText(turn.prompt || threadTitle, 76),
          subtitle: turn.agentLabel || "Runtime agent",
          description:
            trimText(turn.assistantSummary, 160) ||
            "This thread turn completed without a returned narrative summary.",
          timestamp: turn.createdAt || thread.updated_at || thread.created_at,
          href: `/chat/${encodeURIComponent(String(thread.id))}`,
          cta: "Open thread",
          status: turn.status,
          summary: turn.assistantSummary,
          result: turn.assistantTable,
          visualization: turn.assistantVisualization,
          diagnostics: turn.diagnostics,
          errorMessage: turn.errorMessage,
          errorStatus: turn.errorStatus,
          prompt: turn.prompt,
          threadTitle,
          detailItems: [
            { label: "Surface", value: "Ask" },
            { label: "Thread", value: threadTitle },
            { label: "Agent", value: formatValue(turn.agentLabel || "Runtime agent") },
            { label: "Updated", value: formatDateTime(turn.createdAt || thread.updated_at) },
          ],
        }));
      }),
    );
  }, [data?.agents, data?.threadMessagePayloads]);

  const queryRuns = useMemo(
    () =>
      sortByTimestampDesc(
        sqlHistory.map((entry) => {
          const response = entry?.response && typeof entry.response === "object" ? entry.response : null;
          return {
            id: `query-${entry.id || entry.createdAt}`,
            kind: "query",
            title: buildQueryRunTitle(entry),
            subtitle: buildQueryModeLabel(entry?.queryScope, entry?.connectionName),
            description:
              trimText(entry?.query, 160) ||
              "No query text was stored for this local workspace run.",
            timestamp: entry?.createdAt,
            href: "/query-workspace",
            cta: "Open workspace",
            status: entry?.status === "failed" ? "error" : "ready",
            summary: response?.summary || "",
            result:
              Array.isArray(response?.rows) || Array.isArray(response?.data) ? response : null,
            visualization: null,
            diagnostics:
              response?.federation_diagnostics && typeof response.federation_diagnostics === "object"
                ? response.federation_diagnostics
                : null,
            errorMessage: entry?.errorMessage || "",
            errorStatus: null,
            prompt: entry?.query || "",
            generatedSql: response?.generated_sql || "",
            detailItems: [
              { label: "Surface", value: "Query Workspace" },
              { label: "Mode", value: buildQueryModeLabel(entry?.queryScope, entry?.connectionName) },
              { label: "Rows", value: formatValue(entry?.rowCount || 0) },
              { label: "Duration", value: formatValue(entry?.durationMs) },
            ],
          };
        }),
      ),
    [sqlHistory],
  );

  const dashboardRuns = useMemo(
    () =>
      sortByTimestampDesc(
        (dashboardState?.boards || []).flatMap((board) =>
          (Array.isArray(board?.widgets) ? board.widgets : [])
            .filter((widget) => widget?.lastRunAt || widget?.result || widget?.error)
            .map((widget) => ({
              id: `dashboard-${board.id}-${widget.id}`,
              kind: "dashboard",
              title: trimText(widget?.title || "Dashboard widget", 76),
              subtitle: board?.name || "Runtime dashboard",
              description:
                trimText(widget?.description, 160) ||
                "Semantic dashboard widget backed by runtime query execution.",
              timestamp: widget?.lastRunAt || board?.lastRefreshedAt,
              href: "/dashboards",
              cta: "Open Dashboard Builder",
              status: widget?.error ? "error" : widget?.running ? "pending" : "ready",
              summary: `${board?.name || "Runtime dashboard"} on ${board?.selectedModel || "no semantic model selected"}`,
              result: widget?.result || null,
              visualization: buildDashboardRunVisualization(widget),
              diagnostics:
                widget?.result?.federation_diagnostics &&
                typeof widget.result.federation_diagnostics === "object"
                  ? widget.result.federation_diagnostics
                  : null,
              errorMessage: widget?.error || "",
              errorStatus: null,
              prompt: "",
              generatedSql: widget?.result?.generated_sql || "",
              detailItems: [
                { label: "Surface", value: "Dashboard Builder" },
                { label: "Dashboard", value: formatValue(board?.name) },
                { label: "Semantic model", value: formatValue(board?.selectedModel) },
                { label: "Rows", value: formatValue(widget?.result?.rowCount || 0) },
              ],
            })),
        ),
      ),
    [dashboardState],
  );

  const allRuns = useMemo(
    () => sortByTimestampDesc([...agentRuns, ...queryRuns, ...dashboardRuns]).slice(0, 24),
    [agentRuns, dashboardRuns, queryRuns],
  );

  const visibleRuns = useMemo(() => {
    if (activeFilter === "all") {
      return allRuns;
    }
    return allRuns.filter((run) => run.kind === activeFilter);
  }, [activeFilter, allRuns]);

  useEffect(() => {
    if (!visibleRuns.some((run) => run.id === selectedRunId)) {
      setSelectedRunId(visibleRuns[0]?.id || "");
    }
  }, [selectedRunId, visibleRuns]);

  const selectedRun =
    visibleRuns.find((run) => run.id === selectedRunId) ||
    allRuns.find((run) => run.id === selectedRunId) ||
    visibleRuns[0] ||
    allRuns[0] ||
    null;
  const failedRuns = allRuns.filter((run) => run.status === "error").length;
  const runSummaryItems = [
    {
      label: "Recent runs",
      value: formatValue(allRuns.length),
      detail: activeFilter === "all" ? "Across runtime surfaces." : `${RUN_KIND_META[activeFilter]?.label || "Run"} history.`,
    },
    {
      label: "Failures",
      value: formatValue(failedRuns),
      detail: failedRuns > 0 ? "Needs review." : "Nothing failing right now.",
      tone: failedRuns > 0 ? "warning" : "",
    },
    {
      label: "Latest activity",
      value: allRuns[0]?.timestamp ? formatRelativeTime(allRuns[0].timestamp) : "None yet",
      detail: selectedRun ? `${RUN_KIND_META[selectedRun.kind]?.label || "Run"} selected.` : "Pick a run to inspect.",
    },
  ];

  return (
    <div className="page-stack runs-shell">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Execution</p>
            <h2>Review recent runtime executions</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(allRuns.length)} recent runs</span>
              <span className="chip">{formatValue(agentRuns.length)} agent turns</span>
              <span className="chip">{formatValue(queryRuns.length)} query runs</span>
              <span className="chip">{formatValue(dashboardRuns.length)} dashboard runs</span>
            </div>
          </div>
          <div className="product-command-bar-actions">
            <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
              {loading ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>
      </section>

      <section className="runs-summary-grid">
        {runSummaryItems.map((item) => (
          <article key={item.label} className={`runs-summary-card ${item.tone || ""}`.trim()}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <p>{item.detail}</p>
          </article>
        ))}
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="runs-layout">
        <section className="surface-panel runs-feed-panel">
          <div className="thread-section-head">
            <div>
              <h3>Recent execution feed</h3>
              <p>Pick a run, inspect the result, and jump back in.</p>
            </div>
          </div>

          <SectionTabs tabs={RUN_FILTERS} value={activeFilter} onChange={setActiveFilter} />

          {loading && allRuns.length === 0 ? (
            <div className="empty-box">Loading runtime execution history...</div>
          ) : visibleRuns.length > 0 ? (
            <div className="runs-feed-list">
              {visibleRuns.map((run) => {
                const meta = RUN_KIND_META[run.kind];
                const Icon = meta.icon;
                return (
                  <article
                    key={run.id}
                    className={`runs-feed-card ${selectedRun?.id === run.id ? "active" : ""}`.trim()}
                  >
                    <button
                      className="runs-feed-card-main"
                      type="button"
                      onClick={() => setSelectedRunId(run.id)}
                    >
                      <span className="runs-feed-icon">
                        <Icon className="button-icon" aria-hidden="true" />
                      </span>
                      <span className="runs-feed-copy">
                        <span className="runs-feed-topline">
                          <strong>{run.title}</strong>
                          <span className={`message-status-badge ${run.status}`}>
                            {buildRunStateLabel(run)}
                          </span>
                        </span>
                        <span className="runs-feed-subcopy">
                          {[meta.label, run.subtitle, formatRelativeTime(run.timestamp)]
                            .filter(Boolean)
                            .join(" | ")}
                        </span>
                        <span className="runs-feed-description">{run.description}</span>
                      </span>
                    </button>
                    <div className="runs-feed-card-foot">
                      <span>{meta.label}</span>
                      <Link className="runs-feed-link" to={run.href}>
                        {run.cta}
                      </Link>
                    </div>
                  </article>
                );
              })}
            </div>
          ) : (
            <PageEmpty
              title="No runs in this filter"
              message={RUN_KIND_META[activeFilter]?.emptyMessage || "Run the runtime to populate this feed."}
            />
          )}
        </section>

        <div className="detail-stack">
          <Panel
            title={selectedRun ? selectedRun.title : "Execution preview"}
            eyebrow={selectedRun ? RUN_KIND_META[selectedRun.kind]?.label : "Execution"}
            className="compact-panel"
            actions={
              selectedRun ? (
                <Link className="ghost-button" to={selectedRun.href}>
                  {selectedRun.cta}
                </Link>
              ) : null
            }
          >
            {selectedRun ? (
              selectedRun.summary ||
              selectedRun.result ||
              selectedRun.visualization ||
              selectedRun.diagnostics ||
              selectedRun.errorMessage ? (
                <RuntimeResultPanel
                  summary={selectedRun.summary}
                  result={selectedRun.result}
                  visualization={selectedRun.visualization}
                  diagnostics={selectedRun.diagnostics}
                  status={selectedRun.status}
                  errorMessage={selectedRun.errorMessage}
                  errorStatus={selectedRun.errorStatus}
                  maxPreviewRows={10}
                />
              ) : (
                <PageEmpty
                  title="No structured artifact"
                  message="This execution did not persist a reusable result, chart, or diagnostic payload."
                />
              )
            ) : (
              <PageEmpty
                title="No execution selected"
                message="Select a run from the feed to inspect its current runtime output."
              />
            )}
          </Panel>

          <Panel title="Context" eyebrow="Selection" className="compact-panel">
            {selectedRun ? (
              <div className="page-stack">
                <DetailList items={selectedRun.detailItems || []} />
                {selectedRun.prompt ? (
                  <details className="inline-disclosure">
                    <summary>{selectedRun.kind === "query" ? "Submitted query" : "Submitted prompt"}</summary>
                    <div className="inline-disclosure-body">
                      <pre className="code-block compact">{selectedRun.prompt}</pre>
                    </div>
                  </details>
                ) : null}
                {selectedRun.generatedSql ? (
                  <details className="inline-disclosure">
                    <summary>Generated SQL</summary>
                    <div className="inline-disclosure-body">
                      <pre className="code-block compact">{selectedRun.generatedSql}</pre>
                    </div>
                  </details>
                ) : null}
              </div>
            ) : (
              <PageEmpty
                title="No execution detail"
                message="Select a run to inspect its surface, timing, and submitted input."
              />
            )}
          </Panel>
        </div>
      </section>
    </div>
  );
}
