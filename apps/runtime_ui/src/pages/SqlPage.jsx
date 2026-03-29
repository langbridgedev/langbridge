import { useMemo, useState } from "react";
import { Copy, Download, Save } from "lucide-react";

import { ResultTable } from "../components/ResultTable";
import { PageEmpty, Panel, SectionTabs } from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { usePersistentState } from "../hooks/usePersistentState";
import { fetchConnectors, fetchDatasets, querySql } from "../lib/runtimeApi";
import {
  formatDateTime,
  formatValue,
  getErrorMessage,
  splitCsv,
  toSqlAlias,
} from "../lib/format";
import {
  DEFAULT_SQL_QUERY,
  SQL_HISTORY_STORAGE_KEY,
  SQL_SAVED_STORAGE_KEY,
  SQL_TEMPLATES,
  copyTextToClipboard,
  createLocalId,
  detectSqlWarnings,
  downloadTextFile,
  normalizeTabularResult,
  toCsvText,
} from "../lib/runtimeUi";

export function SqlPage() {
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

  const connectors = Array.isArray(connectorsState.data?.items)
    ? connectorsState.data.items
    : [];
  const datasets = Array.isArray(datasetsState.data?.items) ? datasetsState.data.items : [];
  const queryModeLabel = form.connectionName ? "Direct connector SQL" : "Federated runtime SQL";
  const warnings = useMemo(() => detectSqlWarnings(form.query), [form.query]);
  const normalizedResult = result ? normalizeTabularResult(result) : null;

  async function handleSubmit(event) {
    event.preventDefault();
    setRunning(true);
    setError("");
    setWorkspaceNotice("");
    try {
      const payload = {
        query: form.query,
        requested_limit:
          Number(form.requestedLimit) > 0 ? Number(form.requestedLimit) : undefined,
      };
      if (form.connectionName) {
        payload.connection_name = form.connectionName;
      }
      const response = await querySql(payload);
      setResult(response);
      setActiveTab("results");
      setHistoryItems((current) =>
        [
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
        ].slice(0, 20),
      );
    } catch (caughtError) {
      setResult(null);
      setError(getErrorMessage(caughtError));
      setHistoryItems((current) =>
        [
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
        ].slice(0, 20),
      );
    } finally {
      setRunning(false);
    }
  }

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
      next.sort((left, right) =>
        String(right.updatedAt || "").localeCompare(String(left.updatedAt || "")),
      );
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

  async function handleCopyGeneratedSql() {
    if (!result?.generated_sql) {
      return;
    }
    try {
      await copyTextToClipboard(result.generated_sql);
      setWorkspaceNotice("Generated SQL copied to clipboard.");
    } catch (caughtError) {
      setWorkspaceNotice(getErrorMessage(caughtError));
    }
  }

  return (
    <div className="page-stack">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">SQL Workspace</p>
            <h2>{queryModeLabel}</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(connectors.length)} connectors</span>
              <span className="chip">{formatValue(datasets.length)} datasets</span>
              <span className="chip">{formatValue(form.requestedLimit)} row limit</span>
            </div>
          </div>
        </div>
      </section>

      <section className="workspace-grid">
        <Panel title="Runtime SQL workspace" className="compact-panel">
          <div className="product-panel-meta">
            <span>Blank connection = federated SQL</span>
            <span>Selected connection = direct SQL</span>
            <span>Single runtime scope</span>
          </div>
          {workspaceNotice ? (
            <div className="callout">
              <strong>Workspace note</strong>
              <span>{workspaceNotice}</span>
            </div>
          ) : null}
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
              <button
                className="ghost-button"
                type="button"
                onClick={saveCurrentQuery}
                disabled={!form.query.trim()}
              >
                Save locally
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={() => void handleCopySql()}
                disabled={!form.query.trim()}
              >
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
          <Panel title="SQL templates" eyebrow="Starters">
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
                      connectionName:
                        template.label === "Connector direct SQL"
                          ? connectors[0]?.name || ""
                          : "",
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

          <Panel title="Dataset aliases" eyebrow="Reference">
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
                    <span>
                      {[item.connector, item.semantic_model].filter(Boolean).join(" | ") ||
                        "runtime dataset"}
                    </span>
                  </button>
                ))}
              </div>
            ) : (
              <PageEmpty title="No datasets" message="Add runtime datasets to query them here." />
            )}
          </Panel>

          <Panel title="Connector targets" eyebrow="Reference">
            {connectors.length > 0 ? (
              <div className="stack-list">
                {connectors.map((item) => (
                  <div key={item.id || item.name} className="list-card static">
                    <strong>{item.name}</strong>
                    <span>
                      {[item.connector_type, item.supports_sync ? "sync enabled" : "query only"]
                        .filter(Boolean)
                        .join(" | ")}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <PageEmpty
                title="No connectors"
                message="Define connectors in runtime config to use direct SQL."
              />
            )}
          </Panel>
        </div>
      </section>

      <Panel title="SQL console" eyebrow="Results and workspace memory">
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
                    onClick={() => void handleCopyGeneratedSql()}
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
            <PageEmpty
              title="No SQL result yet"
              message="Run a federated or direct SQL query to inspect runtime results."
            />
          )
        ) : null}

        {activeTab === "history" ? (
          historyItems.length > 0 ? (
            <div className="stack-list">
              {historyItems.map((item) => (
                <div key={item.id} className="list-card static">
                  <strong>
                    {item.connectionName
                      ? `Direct SQL - ${item.connectionName}`
                      : "Federated runtime SQL"}
                  </strong>
                  <span>
                    {[formatDateTime(item.createdAt), item.status, `${item.rowCount || 0} rows`]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
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
            <PageEmpty
              title="No local history"
              message="Executed runtime queries will appear here for this browser."
            />
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
                  <div
                    key={item.id}
                    className={`list-card static ${selectedSavedId === item.id ? "active" : ""}`}
                  >
                    <strong>{item.name}</strong>
                    <span>
                      {[formatDateTime(item.updatedAt), ...(Array.isArray(item.tags) ? item.tags : [])]
                        .filter(Boolean)
                        .join(" | ")}
                    </span>
                    <small>{item.query}</small>
                    <div className="panel-actions-inline">
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => loadSavedQuery(item)}
                      >
                        Load
                      </button>
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => deleteSavedQueryById(item.id)}
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <PageEmpty
                title="No saved queries"
                message="Save the current SQL to keep a local runtime workbench library."
              />
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
                      <small>
                        {[item.connector, item.semantic_model].filter(Boolean).join(" | ") ||
                          "runtime dataset"}
                      </small>
                    </div>
                  ))}
                </div>
              ) : (
                <PageEmpty
                  title="No datasets"
                  message="Runtime datasets appear here as federated SQL aliases."
                />
              )}
            </Panel>
            <Panel title="Connectors" eyebrow="Direct SQL">
              {connectors.length > 0 ? (
                <div className="stack-list">
                  {connectors.map((item) => (
                    <div key={`connector-${item.id || item.name}`} className="list-card static">
                      <strong>{item.name}</strong>
                      <span>
                        {[item.connector_type, item.supports_sync ? "sync enabled" : "query only"]
                          .filter(Boolean)
                          .join(" | ")}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <PageEmpty
                  title="No connectors"
                  message="Define connectors in runtime config to use direct SQL."
                />
              )}
            </Panel>
          </div>
        ) : null}
      </Panel>
    </div>
  );
}
