import { useEffect, useId, useRef, useState } from "react";

import { AnalyticsArtifactModal } from "../../components/analytics/AnalyticsArtifactModal.jsx";
import { AnalyticsResultTable } from "../../components/analytics/AnalyticsResultTable.jsx";
import {
  copyText,
  csvForResult,
  downloadText,
  safeFileName,
} from "../../components/analytics/analyticsArtifacts.js";
import { usePersistentState } from "../../hooks/usePersistentState.js";
import { getErrorMessage } from "../../lib/format.js";
import { renderJson } from "../../lib/runtimeUi.js";
import {
  cancelQueryRun,
  executeQueryRun,
  listQueryResources,
  saveQueryDraft,
} from "../../services/queryService.js";
import {
  QUERY_WORKSPACE_STORAGE_KEYS,
  buildScopeResourceTree,
  buildSqlQueryPayload,
  buildSqlCompletionItems,
  createQueryArtifactBundle,
  defaultQueryForScope,
  findOptionByValue,
  getResourceLabel,
  isDefaultQuery,
  normalizePositiveInteger,
  normalizeQueryScope,
} from "./queryWorkspaceModel.js";
import { SqlCodeBlock, SqlEditor } from "./SqlCodeBlock.jsx";

const EMPTY_RESOURCES = {
  connectors: [],
  datasets: [],
  semanticModels: [],
};

const INITIAL_RUN_STATE = {
  status: "idle",
  jobId: "",
  events: [],
  message: "",
};

export function QueryWorkspacePage() {
  const [queryScope, setQueryScope] = usePersistentState(
    QUERY_WORKSPACE_STORAGE_KEYS.scope,
    "semantic",
  );
  const [query, setQuery] = usePersistentState(
    QUERY_WORKSPACE_STORAGE_KEYS.draft,
    defaultQueryForScope(queryScope),
  );
  const [connectorRef, setConnectorRef] = usePersistentState(
    QUERY_WORKSPACE_STORAGE_KEYS.connector,
    "",
  );
  const [limit, setLimit] = usePersistentState(QUERY_WORKSPACE_STORAGE_KEYS.limit, 100);
  const [timeoutSeconds, setTimeoutSeconds] = usePersistentState(
    QUERY_WORKSPACE_STORAGE_KEYS.timeout,
    30,
  );
  const [explain, setExplain] = usePersistentState(
    QUERY_WORKSPACE_STORAGE_KEYS.explain,
    false,
  );
  const [contextOpen, setContextOpen] = usePersistentState(
    QUERY_WORKSPACE_STORAGE_KEYS.contextOpen,
    true,
  );

  const [resources, setResources] = useState(EMPTY_RESOURCES);
  const [resourcesLoading, setResourcesLoading] = useState(true);
  const [resourcesError, setResourcesError] = useState("");
  const [runState, setRunState] = useState(INITIAL_RUN_STATE);
  const [result, setResult] = useState(null);
  const [runError, setRunError] = useState("");
  const [savingMessage, setSavingMessage] = useState("");
  const [copiedKey, setCopiedKey] = useState("");
  const [activeArtifact, setActiveArtifact] = useState(null);

  const abortControllerRef = useRef(null);
  const modalTitleId = useId();

  const normalizedScope = normalizeQueryScope(queryScope);
  const selectedConnector = findOptionByValue(resources.connectors, connectorRef);
  const selectedDatasets = [];
  const completionItems = buildSqlCompletionItems({ queryScope: normalizedScope, resources });
  const resourceTree = buildScopeResourceTree({ queryScope: normalizedScope, resources });
  const canRun =
    Boolean(String(query || "").trim()) &&
    !isRunning(runState.status) &&
    (normalizedScope !== "source" || Boolean(selectedConnector));
  const requestPreview = safeBuildPayload({
    queryScope: normalizedScope,
    query,
    connector: selectedConnector,
    selectedDatasets,
    requestedLimit: limit,
    requestedTimeoutSeconds: timeoutSeconds,
    explain,
  });

  useEffect(() => {
    let cancelled = false;
    setResourcesLoading(true);
    setResourcesError("");

    listQueryResources()
      .then((payload) => {
        if (cancelled) {
          return;
        }
        setResources(payload);
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setResources(EMPTY_RESOURCES);
        setResourcesError(getErrorMessage(error));
      })
      .finally(() => {
        if (!cancelled) {
          setResourcesLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  function handleScopeChange(nextScope) {
    const scope = normalizeQueryScope(nextScope);
    setQueryScope(scope);
    if (!String(query || "").trim() || isDefaultQuery(query)) {
      setQuery(defaultQueryForScope(scope));
    }
    setRunError("");
  }

  async function handleRun() {
    if (!canRun) {
      return;
    }

    const controller = new AbortController();
    abortControllerRef.current = controller;
    setRunError("");
    setSavingMessage("");
    setResult(null);
    setRunState({
      status: "queued",
      jobId: "",
      events: [],
      message: "Queueing SQL run.",
    });

    try {
      const run = await executeQueryRun(
        {
          queryScope: normalizedScope,
          query,
          connector: selectedConnector,
          selectedDatasets,
          requestedLimit: limit,
          requestedTimeoutSeconds: timeoutSeconds,
          explain,
        },
        {
          signal: controller.signal,
          onQueued: (queued) => {
            setRunState((current) => ({
              ...current,
              status: queued?.status || "queued",
              jobId: String(queued?.job_id || ""),
              message: "Runtime job queued.",
            }));
          },
          onEvent: (event) => {
            setRunState((current) => appendRunEvent(current, event));
          },
        },
      );

      setResult(run);
      setRunState((current) => ({
        ...current,
        status: run.status || "succeeded",
        jobId: run.diagnostics?.job_id || current.jobId,
        message: "Query completed.",
      }));
    } catch (error) {
      if (controller.signal.aborted) {
        setRunState((current) => ({
          ...current,
          status: "cancelled",
          message: "Query cancelled.",
        }));
        return;
      }
      setRunError(getErrorMessage(error));
      setRunState((current) => ({
        ...current,
        status: "failed",
        message: getErrorMessage(error),
      }));
    } finally {
      abortControllerRef.current = null;
    }
  }

  async function handleCancel() {
    const jobId = runState.jobId;
    setRunState((current) => ({
      ...current,
      status: "cancelling",
      message: "Cancelling query.",
    }));
    try {
      if (jobId) {
        await cancelQueryRun(jobId);
      }
    } catch (error) {
      setRunError(getErrorMessage(error));
    } finally {
      abortControllerRef.current?.abort();
      setRunState((current) => ({
        ...current,
        status: "cancelled",
        message: "Query cancelled.",
      }));
    }
  }

  function handleSaveQuery() {
    try {
      const record = saveQueryDraft({
        query,
        queryScope: normalizedScope,
        connector: selectedConnector,
      });
      setSavingMessage(`Saved "${record.title}".`);
      window.setTimeout(() => setSavingMessage(""), 1800);
    } catch (error) {
      setSavingMessage(getErrorMessage(error));
    }
  }

  async function handleCopy(key, value) {
    const copied = await copyText(value);
    if (!copied) {
      return;
    }
    setCopiedKey(key);
    window.setTimeout(() => setCopiedKey(""), 1200);
  }

  return (
    <section className="workspace-page workspace-page--query query-workspace-minimal">
      <div className="query-minimal-bar query-workspace-bar">
        <div className="query-title-block">
          <strong>Scratchpad</strong>
          <span>Run governed SQL against semantic, dataset, or source scope.</span>
        </div>
        <div className="query-control-strip">
          <label className="query-control">
            Scope
            <select value={normalizedScope} onChange={(event) => handleScopeChange(event.target.value)}>
              <option value="semantic">Semantic</option>
              <option value="dataset">Dataset</option>
              <option value="source">Source</option>
            </select>
          </label>

          {normalizedScope === "source" ? (
            <ResourceSelect
              label="Connector"
              value={connectorRef || selectedConnector?.value || ""}
              options={resources.connectors}
              emptyLabel="Choose connector"
              disabled={resourcesLoading}
              requireValue
              onChange={setConnectorRef}
            />
          ) : null}

          <label className="query-control query-control--small">
            Limit
            <input
              type="number"
              min="1"
              value={limit}
              onChange={(event) => setLimit(event.target.value)}
            />
          </label>

          <label className="query-control query-control--small">
            Timeout
            <input
              type="number"
              min="1"
              value={timeoutSeconds}
              onChange={(event) => setTimeoutSeconds(event.target.value)}
            />
          </label>

          <label className="query-toggle">
            <input
              type="checkbox"
              checked={Boolean(explain)}
              onChange={(event) => setExplain(event.target.checked)}
            />
            Explain
          </label>

          <button type="button" onClick={() => setContextOpen((open) => !open)}>
            {contextOpen ? "Hide context" : "Show context"}
          </button>

          {isRunning(runState.status) ? (
            <button type="button" onClick={() => void handleCancel()}>
              Cancel
            </button>
          ) : (
            <button className="primary-action" type="button" disabled={!canRun} onClick={() => void handleRun()}>
              Run
            </button>
          )}
        </div>
      </div>

      {resourcesError ? <div className="query-inline-error">{resourcesError}</div> : null}

      <div className={contextOpen ? "query-workspace-body" : "query-workspace-body query-workspace-body--context-hidden"}>
        <div className="query-workspace-main">
          <div className="editor-panel query-scratchpad-panel">
            <div className="editor-toolbar">
              <span>{workspaceLabel(normalizedScope, selectedConnector)}</span>
              <div>
                <button type="button" onClick={() => setQuery(defaultQueryForScope(normalizedScope))}>
                  Reset
                </button>
                <button type="button" onClick={() => void handleCopy("query", query)}>
                  {copiedKey === "query" ? "Copied" : "Copy"}
                </button>
                <button type="button" onClick={handleSaveQuery}>
                  Save
                </button>
              </div>
            </div>
            <SqlEditor
              value={query}
              disabled={isRunning(runState.status)}
              suggestions={completionItems}
              onChange={setQuery}
            />
            <div className="query-editor-meta">
              <span>{requestPreview.error || requestSummary(requestPreview.payload)}</span>
              {savingMessage ? <span>{savingMessage}</span> : null}
            </div>
          </div>

          {isRunning(runState.status) || runState.status === "cancelled" || runError || result ? (
            <QueryRunInspector
              runState={runState}
              result={result}
              error={runError}
              payload={requestPreview.payload}
              onCopy={(key, value) => void handleCopy(key, value)}
              copiedKey={copiedKey}
            />
          ) : null}

          {result ? (
            <QueryResultPreview
              result={result}
              copiedKey={copiedKey}
              onCopy={(key, value) => void handleCopy(key, value)}
              onOpenArtifact={setActiveArtifact}
            />
          ) : (
            <QueryEmptyState
              resourcesLoading={resourcesLoading}
              scope={normalizedScope}
              connectorCount={resources.connectors.length}
              datasetCount={resources.datasets.length}
            />
          )}
        </div>

        {contextOpen ? (
          <QueryResourceContext
            tree={resourceTree}
            loading={resourcesLoading}
            scope={normalizedScope}
            onClose={() => setContextOpen(false)}
            onInsert={(value) => {
              if (!value) {
                return;
              }
              setQuery((current) => `${current}${current.endsWith(" ") || current.endsWith("\n") ? "" : " "}${value}`);
            }}
          />
        ) : null}
      </div>

      {activeArtifact ? (
        <AnalyticsArtifactModal
          artifact={activeArtifact}
          titleId={modalTitleId}
          copiedKey={copiedKey}
          onCopyArtifact={(key, text) => void handleCopy(key, text)}
          onClose={() => setActiveArtifact(null)}
        />
      ) : null}
    </section>
  );
}

function ResourceSelect({
  label,
  value,
  options,
  emptyLabel,
  disabled = false,
  requireValue = false,
  onChange,
}) {
  return (
    <label className="query-control">
      {label}
      <select value={value || ""} disabled={disabled} onChange={(event) => onChange(event.target.value)}>
        {!requireValue ? <option value="">{emptyLabel}</option> : null}
        {requireValue && options.length === 0 ? <option value="">{emptyLabel}</option> : null}
        {options.map((item) => (
          <option key={item.value || item.id} value={item.value}>
            {item.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function QueryResourceContext({ tree, loading, scope, onClose, onInsert }) {
  const groups = Array.isArray(tree?.groups) ? tree.groups : [];
  const groupKey = groups.map(resourceNodeKey).join("|");
  const [expandedIds, setExpandedIds] = useState(() => defaultExpandedContextIds(groups));

  useEffect(() => {
    setExpandedIds(defaultExpandedContextIds(groups));
  }, [scope, groupKey]);

  function toggleGroup(nodeKey) {
    setExpandedIds((current) => {
      const next = new Set(current);
      if (next.has(nodeKey)) {
        next.delete(nodeKey);
      } else {
        next.add(nodeKey);
      }
      return next;
    });
  }

  return (
    <aside className="query-context-card">
      <div className="query-context-card-head">
        <div>
          <p className="eyebrow">Available context</p>
          <h3>{tree?.title || "Runtime context"}</h3>
        </div>
        <button type="button" onClick={onClose} aria-label="Hide context panel">
          Hide
        </button>
      </div>
      <p>{loading ? "Loading runtime resources." : tree?.description}</p>

      <div className="query-context-tree" aria-label={`${scope} resources`}>
        {groups.length > 0 ? (
          groups.map((group, index) => {
            const nodeKey = contextNodeKey(group, index);
            return (
              <QueryContextNode
                key={nodeKey}
                node={group}
                nodeKey={nodeKey}
                depth={0}
                expandedIds={expandedIds}
                onInsert={onInsert}
                onToggle={toggleGroup}
              />
            );
          })
        ) : (
          <span className="query-context-empty">{tree?.emptyLabel || "No resources returned yet."}</span>
        )}
      </div>

      <div className="query-context-foot">
        <span>Click names to insert into the scratchpad.</span>
      </div>
    </aside>
  );
}

function QueryContextNode({ node, nodeKey, depth, expandedIds, onInsert, onToggle }) {
  const children = Array.isArray(node?.children) ? node.children : [];
  const hasChildren = children.length > 0;
  const expanded = expandedIds.has(nodeKey);

  if (!hasChildren) {
    return (
      <button
        type="button"
        className={`query-context-leaf query-context-leaf--depth-${Math.min(depth, 3)}`}
        disabled={!node.insertText}
        onClick={() => onInsert(node.insertText)}
        title={node.meta || node.kind || node.label}
        style={{ "--context-depth": depth }}
      >
        <span>{node.label}</span>
        {node.kind ? <small>{node.kind}</small> : null}
      </button>
    );
  }

  return (
    <div className={`query-context-group query-context-group--depth-${Math.min(depth, 3)}`} style={{ "--context-depth": depth }}>
      <button
        type="button"
        className="query-context-group-head"
        aria-expanded={expanded}
        onClick={() => onToggle(nodeKey)}
        title={node.meta || node.label}
      >
        <span className={expanded ? "query-context-caret expanded" : "query-context-caret"} />
        <span className="query-context-group-title">{node.label}</span>
        <small>{children.length}</small>
      </button>
      {node.meta ? <span className="query-context-group-meta">{node.meta}</span> : null}
      {expanded ? (
        <div className="query-context-children">
          {children.map((child, index) => {
            const childNodeKey = contextNodeKey(child, index, nodeKey);
            return (
              <QueryContextNode
                key={childNodeKey}
                node={child}
                nodeKey={childNodeKey}
                depth={depth + 1}
                expandedIds={expandedIds}
                onInsert={onInsert}
                onToggle={onToggle}
              />
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function resourceNodeKey(node) {
  const children = Array.isArray(node?.children) ? node.children.map(resourceNodeKey).join(",") : "";
  return `${node?.id || node?.label || ""}[${children}]`;
}

function contextNodeKey(node, index, parentKey = "") {
  const localKey = String(node?.id || node?.label || node?.kind || `node-${index}`).trim();
  return parentKey ? `${parentKey}/${localKey}` : localKey;
}

function defaultExpandedContextIds(groups) {
  const ids = new Set();
  const first = Array.isArray(groups) ? groups[0] : null;
  let current = first;
  let currentKey = first ? contextNodeKey(first, 0) : "";
  let depth = 0;

  while (current && currentKey && depth < 3) {
    const children = Array.isArray(current.children) ? current.children : [];
    ids.add(currentKey);
    const nextIndex = children.findIndex((child) => Array.isArray(child?.children) && child.children.length > 0);
    if (nextIndex < 0) {
      break;
    }
    current = children[nextIndex];
    currentKey = contextNodeKey(current, nextIndex, currentKey);
    depth += 1;
  }

  return ids;
}

function QueryEmptyState({ resourcesLoading, scope, connectorCount, datasetCount }) {
  let message = "Run a query to preview rows, generated SQL, and diagnostics.";
  if (resourcesLoading) {
    message = "Loading runtime resources.";
  } else if (scope === "source" && connectorCount === 0) {
    message = "No SQL-capable source connectors were returned by the runtime.";
  } else if (scope === "dataset" && datasetCount === 0) {
    message = "No datasets were returned by the runtime. Dataset scope can still run if the runtime supports federation without an explicit selector.";
  }

  return (
    <div className="query-empty-card">
      <strong>No result yet</strong>
      <span>{message}</span>
    </div>
  );
}

function QueryResultPreview({ result, copiedKey, onCopy, onOpenArtifact }) {
  const tableResult = {
    columns: result.columns,
    rows: result.rows,
    rowCount: result.rowCount,
    row_count_preview: result.rowCountPreview,
    duration_ms: result.durationMs,
  };
  const artifactBundle = result.artifactBundle || createQueryArtifactBundle(result);
  const tableArtifact = artifactBundle.artifacts.find((artifact) => artifact.type === "table");
  const sqlArtifact = artifactBundle.artifacts.find((artifact) => artifact.type === "sql");
  const csv = csvForResult(tableResult);
  const json = renderJson({
    status: result.status,
    query_scope: result.queryScope,
    row_count: result.rowCount,
    duration_ms: result.durationMs,
    generated_sql: result.generatedSql || null,
    result: tableResult,
    artifacts: artifactBundle.artifacts,
  });

  return (
    <div className="query-result-panel">
      <div className="query-result-toolbar">
        <div>
          <p className="eyebrow">Result</p>
          <h3>{result.title || "Query result"}</h3>
        </div>
        <div className="query-result-actions">
          <button type="button" onClick={() => onCopy("rows", csv)}>
            {copiedKey === "rows" ? "Copied" : "Copy rows"}
          </button>
          <button
            type="button"
            onClick={() =>
              downloadText({
                fileName: safeFileName(result.title || "query-result", "csv"),
                mimeType: "text/csv;charset=utf-8",
                text: csv,
              })
            }
          >
            CSV
          </button>
          <button
            type="button"
            onClick={() =>
              downloadText({
                fileName: safeFileName(result.title || "query-result", "json"),
                mimeType: "application/json",
                text: json,
              })
            }
          >
            JSON
          </button>
          <button type="button" onClick={() => onCopy("artifact-markdown", artifactBundle.answer_markdown)}>
            {copiedKey === "artifact-markdown" ? "Copied" : "Copy artifact markdown"}
          </button>
        </div>
      </div>

      <div className="query-result-summary">
        <span>{Number(result.rowCount || 0).toLocaleString()} rows</span>
        <span>{result.columns.length.toLocaleString()} columns</span>
        <span>{formatElapsed(result.durationMs)}</span>
        <span>Scope: {result.queryScope}</span>
        {result.redactionApplied ? <span>Redacted</span> : null}
      </div>

      <AnalyticsResultTable result={tableResult} maxPreviewRows={24} />

      {result.generatedSql ? (
        <details className="query-sql-details">
          <summary>Generated SQL</summary>
          <div className="query-sql-details-actions">
            <button type="button" onClick={() => onCopy("generated-sql", result.generatedSql)}>
              {copiedKey === "generated-sql" ? "Copied" : "Copy generated SQL"}
            </button>
            {sqlArtifact ? (
              <button type="button" onClick={() => onOpenArtifact(sqlArtifact)}>
                Open SQL artifact
              </button>
            ) : null}
          </div>
          <SqlCodeBlock sql={result.generatedSql} compact label="Generated SQL" />
        </details>
      ) : null}

      <div className="query-result-footer">
        <span>Result is normalized as a markdown artifact bundle for chat and dashboards.</span>
        <div>
          {tableArtifact ? (
            <button type="button" onClick={() => onOpenArtifact(tableArtifact)}>
              Open full result
            </button>
          ) : null}
          <button
            type="button"
            onClick={() =>
              downloadText({
                fileName: safeFileName(result.title || "query-artifacts", "json"),
                mimeType: "application/json",
                text: renderJson(artifactBundle),
              })
            }
          >
            Export artifacts
          </button>
        </div>
      </div>
    </div>
  );
}

function QueryRunInspector({ runState, result, error, payload, copiedKey, onCopy }) {
  const events = Array.isArray(runState.events) ? runState.events : [];
  const diagnostics = result?.diagnostics || {};
  const generatedSql = result?.generatedSql || diagnostics.generated_sql || "";
  const status = result?.status || runState.status;

  return (
    <details className="query-run-inspector" open={isRunning(runState.status) || Boolean(error)}>
      <summary>
        <span className={`query-run-status query-run-status--${statusClassName(status)}`} />
        <strong>{statusLabel(status)}</strong>
        <span>{runState.message || error || resultSummary(result)}</span>
      </summary>

      <div className="query-run-inspector-body">
        <div className="query-result-summary">
          <span>Scope: {payload?.query_scope || result?.queryScope || "n/a"}</span>
          {runState.jobId || diagnostics.job_id ? <span>Job: {shortId(runState.jobId || diagnostics.job_id)}</span> : null}
          {result?.durationMs ? <span>{formatElapsed(result.durationMs)}</span> : null}
          {result?.bytesScanned ? <span>{formatBytes(result.bytesScanned)}</span> : null}
        </div>

        {error ? <div className="query-inline-error">{error}</div> : null}

        {events.length > 0 ? (
          <div className="query-event-list">
            {events.slice(-6).map((event, index) => (
              <div key={eventKey(event, index)} className="query-event-row">
                <span>{formatEventName(event)}</span>
                <p>{event.message || event.status || event.stage || "Runtime event"}</p>
              </div>
            ))}
          </div>
        ) : null}

        {generatedSql ? (
          <div className="query-inspector-sql">
            <div className="query-sql-details-actions">
              <strong>Executed SQL</strong>
              <button type="button" onClick={() => onCopy("inspector-sql", generatedSql)}>
                {copiedKey === "inspector-sql" ? "Copied" : "Copy"}
              </button>
            </div>
            <SqlCodeBlock sql={generatedSql} compact label="Executed SQL" />
          </div>
        ) : null}

        {diagnostics.federation_diagnostics ? (
          <details className="query-diagnostic-json">
            <summary>Federation diagnostics</summary>
            <pre>{renderJson(diagnostics.federation_diagnostics)}</pre>
          </details>
        ) : null}
      </div>
    </details>
  );
}

function appendRunEvent(current, event) {
  const normalized = {
    sequence: Number(event?.sequence || current.events.length + 1),
    event: event?.event || event?.event_type || "runtime.event",
    stage: event?.stage || "",
    status: event?.status || "",
    message: event?.message || event?.details?.message || event?.details?.summary || "",
    timestamp: event?.timestamp || new Date().toISOString(),
    terminal: Boolean(event?.terminal),
  };
  const events = current.events.some((item) => item.sequence === normalized.sequence)
    ? current.events
    : [...current.events, normalized];
  return {
    ...current,
    status: normalized.terminal ? normalized.status || current.status : normalized.status || current.status,
    message: normalized.message || current.message,
    events,
  };
}

function safeBuildPayload(request) {
  try {
    return { payload: buildSqlQueryPayload(request), error: "" };
  } catch (error) {
    return { payload: null, error: getErrorMessage(error) };
  }
}

function workspaceLabel(scope, connector) {
  if (scope === "source") {
    return connector ? `Source SQL - ${getResourceLabel(connector)}` : "Source SQL";
  }
  if (scope === "dataset") {
    return "Dataset SQL - all datasets";
  }
  return "Semantic SQL";
}

function requestSummary(payload) {
  if (!payload) {
    return "Ready";
  }
  const limit = normalizePositiveInteger(payload.requested_limit);
  const timeout = normalizePositiveInteger(payload.requested_timeout_seconds);
  return [
    `${payload.query_scope} scope`,
    limit ? `${limit.toLocaleString()} row limit` : "",
    timeout ? `${timeout}s timeout` : "",
    payload.explain ? "explain on" : "",
  ]
    .filter(Boolean)
    .join(" | ");
}

function isRunning(status) {
  return ["queued", "running", "in_progress", "cancelling", "pending"].includes(
    String(status || "").toLowerCase(),
  );
}

function statusClassName(status) {
  const normalized = String(status || "").toLowerCase();
  if (["succeeded", "success", "completed"].includes(normalized)) {
    return "success";
  }
  if (["failed", "error"].includes(normalized)) {
    return "error";
  }
  if (["cancelled", "canceled"].includes(normalized)) {
    return "cancelled";
  }
  return "running";
}

function statusLabel(status) {
  const normalized = String(status || "idle").replaceAll("_", " ");
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function resultSummary(result) {
  if (!result) {
    return "No completed run yet.";
  }
  return `${Number(result.rowCount || 0).toLocaleString()} rows returned.`;
}

function formatElapsed(value) {
  const numeric = normalizePositiveInteger(value);
  if (!numeric) {
    return "n/a";
  }
  if (numeric < 1000) {
    return `${numeric} ms`;
  }
  return `${(numeric / 1000).toFixed(2)} s`;
}

function formatBytes(value) {
  const numeric = normalizePositiveInteger(value);
  if (!numeric) {
    return "n/a";
  }
  if (numeric < 1024) {
    return `${numeric} B`;
  }
  if (numeric < 1024 * 1024) {
    return `${(numeric / 1024).toFixed(1)} KB`;
  }
  return `${(numeric / 1024 / 1024).toFixed(1)} MB`;
}

function shortId(value) {
  const text = String(value || "").trim();
  return text.length > 12 ? `${text.slice(0, 8)}...${text.slice(-4)}` : text;
}

function eventKey(event, index) {
  return `${event.sequence || index}-${event.event || "event"}`;
}

function formatEventName(event) {
  return String(event.event || event.stage || "event").replaceAll(".", " ");
}
