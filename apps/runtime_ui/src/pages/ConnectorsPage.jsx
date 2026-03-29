import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Activity, Cable, Database, Workflow } from "lucide-react";

import { useAsyncData } from "../hooks/useAsyncData";
import {
  fetchConnectorResources,
  fetchConnectorStates,
  fetchConnectors,
  runConnectorSync,
} from "../lib/runtimeApi";
import { formatDateTime, formatList, formatValue, getErrorMessage } from "../lib/format";
import { buildItemRef, resolveItemByRef } from "../lib/runtimeUi";
import { DetailList, PageEmpty, Panel } from "../components/PagePrimitives";

export function ConnectorsPage() {
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
      resources.items.flatMap((item) =>
        Array.isArray(item?.dataset_names) ? item.dataset_names : [],
      ),
    ),
  ).length;
  const stateByResource = useMemo(
    () =>
      Object.fromEntries(
        (Array.isArray(states.items) ? states.items : []).map((item) => [
          String(item.resource_name),
          item,
        ]),
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
        const resourceItems = Array.isArray(resourcePayload?.items)
          ? resourcePayload.items
          : [];
        const stateItems = Array.isArray(statePayload?.items) ? statePayload.items : [];
        setResources({ items: resourceItems, loading: false, error: "" });
        setStates({ items: stateItems, loading: false, error: "" });
        setSelectedResources((current) => {
          const available = new Set(resourceItems.map((item) => item.name));
          const retained = current.filter((value) => available.has(value));
          return retained.length > 0
            ? retained
            : resourceItems.slice(0, 3).map((item) => item.name);
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
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Connectors</p>
            <h2>{selected?.name || "Connector inventory"}</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(connectors.length)} connectors</span>
              <span className="chip">{formatValue(syncEnabledCount)} sync-enabled</span>
              <span className="chip">{formatValue(resources.items.length)} resources</span>
              <span className="chip">{formatValue(states.items.length)} states</span>
            </div>
          </div>
        </div>
      </section>

      <section className="product-search-bar">
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
        <Panel title="Connector inventory" className="compact-panel">
          {filteredConnectors.length > 0 ? (
            <div className="stack-list">
              {filteredConnectors.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/connectors/${buildItemRef(item)}`}
                >
                  <strong>{item.name}</strong>
                  <span>
                    {[item.connector_type, item.supports_sync ? "sync enabled" : "query only"]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty
              title="No connectors found"
              message="Adjust the filter or add connectors to the runtime config."
            />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel
                title={selected.name}
                className="compact-panel"
                actions={
                  <div className="panel-actions-inline">
                    <span className="chip">{selected.connector_type || "runtime connector"}</span>
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => void reload()}
                      disabled={loading}
                    >
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
                <Panel title="Operational posture" eyebrow="Coverage">
                  <DetailList
                    items={[
                      { label: "Resource definitions", value: formatValue(resources.items.length) },
                      { label: "Sync state rows", value: formatValue(states.items.length) },
                      { label: "Dataset bindings", value: formatValue(selectedDatasetCount) },
                      {
                        label: "Primary route",
                        value: selected.supports_sync ? "Sync + query" : "Query only",
                      },
                    ]}
                  />
                </Panel>

                <Panel title="Supported resources" eyebrow="Catalog">
                  {Array.isArray(selected.supported_resources) &&
                  selected.supported_resources.length > 0 ? (
                    <div className="tag-list">
                      {selected.supported_resources.map((item) => (
                        <span key={item} className="tag">
                          {item}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty
                      title="No resource types"
                      message="This connector did not expose resource types."
                    />
                  )}
                </Panel>
              </section>

              {selected.supports_sync ? (
                <>
                  <Panel
                    title="Resource catalog"
                    eyebrow="Select sync scope"
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
                                    onChange={(event) =>
                                      handleToggleResource(item.name, event.target.checked)
                                    }
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
                                <span>
                                  {item.supports_incremental ? "incremental" : "full refresh"}
                                </span>
                                <span>{item.primary_key || "no primary key"}</span>
                              </div>
                              <p className="resource-card-copy">
                                Last sync: {formatDateTime(state?.last_sync_at || item.last_sync_at)} |
                                Records synced: {formatValue(state?.records_synced ?? item.records_synced ?? 0)}
                              </p>
                              {datasetPairs.length > 0 ? (
                                <div className="tag-list">
                                  {datasetPairs.map((dataset) =>
                                    dataset.id ? (
                                      <button
                                        key={`${item.name}-${dataset.id}`}
                                        className="tag-action"
                                        type="button"
                                        onClick={() =>
                                          navigate(`/datasets/${encodeURIComponent(String(dataset.id))}`)
                                        }
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
                      <PageEmpty
                        title="No resources"
                        message="This connector did not expose sync resources."
                      />
                    )}
                  </Panel>

                  <section className="summary-grid">
                    <Panel title="Sync control" eyebrow="Action">
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
                              Runtime sync stays local and single-workspace. Dataset materialization is
                              still runtime-owned, not a cloud orchestration flow.
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

                    <Panel title="Selected output" eyebrow="Scope">
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
                        <PageEmpty
                          title="No scope selected"
                          message="Pick one or more resources to define the next sync."
                        />
                      )}
                    </Panel>
                  </section>

                  <Panel title="Runtime sync state" eyebrow="History">
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
                              {[
                                `Datasets: ${formatList(item.dataset_names)}`,
                                `Last sync: ${formatDateTime(item.last_sync_at)}`,
                              ]
                                .filter(Boolean)
                                .join(" | ")}
                            </small>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <PageEmpty
                        title="No sync history"
                        message="Run a sync to create runtime sync state."
                      />
                    )}
                  </Panel>
                </>
              ) : (
                <Panel title="Sync" eyebrow="Operational">
                  <PageEmpty
                    title="Query-only connector"
                    message="This connector does not expose runtime sync resources, so the runtime UI keeps it read-only."
                  />
                </Panel>
              )}
            </>
          ) : (
            <Panel title="Connector detail" eyebrow="Runtime">
              <PageEmpty
                title="No connector selected"
                message="Pick a connector to inspect its runtime capabilities."
              />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}
