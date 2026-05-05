import { useEffect, useMemo, useRef, useState } from "react";

import { fetchConnectors } from "../../lib/runtimeApi.js";
import { getItems } from "../../services/langbridgeApiClient.js";
import { Modal } from "../ui/Modal.jsx";
import { DynamicFieldValue } from "../resources/DynamicFieldViewer.jsx";
import { ManagementPill } from "../resources/ManagementPill.jsx";
import { classNames } from "../../utils/classNames.js";
import {
  DATASET_SOURCE_MODES,
  buildDatasetCreateFormState,
  buildDatasetEditFormState,
  buildDatasetSubmitPayload,
  datasetConnectorFamilyOptions,
  describeDatasetSource,
  isSyncedDataset,
  normalizeDatasetConnectors,
  resolveDatasetSourceMode,
} from "../../features/configuration/datasetFormModel.js";
import { normalizeConnectorFamily } from "../../features/configuration/connectorFormModel.js";

const tabLabels = {
  overview: "Overview",
  definition: "Definition",
  actions: "Actions",
};

export function DatasetWorkspaceModal({
  mode,
  resource,
  detailLoading = false,
  detailError = "",
  capabilities,
  actions = [],
  onClose,
  onCreate,
  onUpdate,
  onDelete,
  onAction,
}) {
  const isCreateMode = mode === "create";
  const canMutate =
    isCreateMode
      ? Boolean(capabilities?.canCreate)
      : Boolean(capabilities?.canUpdate && resource?.management === "runtime_managed");
  const canDelete = Boolean(capabilities?.canDelete && resource?.management === "runtime_managed");
  const [activeTab, setActiveTab] = useState(isCreateMode ? "definition" : "overview");
  const [connectors, setConnectors] = useState([]);
  const [connectorsLoading, setConnectorsLoading] = useState(false);
  const [connectorsError, setConnectorsError] = useState("");
  const [form, setForm] = useState(() => buildDatasetCreateFormState());
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [actionResult, setActionResult] = useState(null);
  const editInitializedRef = useRef("");
  const visibleTabs = useMemo(
    () => (isCreateMode ? ["definition"] : ["overview", "definition", "actions"]),
    [isCreateMode],
  );
  const connectorFamilies = datasetConnectorFamilyOptions(connectors);
  const filteredConnectors = useMemo(
    () =>
      connectors.filter((connector) => {
        if (!form.connectorFamily) {
          return true;
        }
        return normalizeConnectorFamily(connector.family) === normalizeConnectorFamily(form.connectorFamily);
      }),
    [connectors, form.connectorFamily],
  );
  const selectedConnector = connectors.find((connector) => connector.value === form.connector) || null;
  const effectiveSourceMode = resolveDatasetSourceMode(form);
  const isUnsupportedSource = effectiveSourceMode === "unsupported";
  const title = isCreateMode ? "Create dataset" : resource?.name || "Dataset details";

  useEffect(() => {
    setActiveTab(isCreateMode ? "definition" : "overview");
    setError("");
    setActionResult(null);
    editInitializedRef.current = "";
    if (isCreateMode) {
      setForm(buildDatasetCreateFormState());
    }
  }, [isCreateMode, resource?.id]);

  useEffect(() => {
    let cancelled = false;
    setConnectorsLoading(true);
    setConnectorsError("");
    fetchConnectors()
      .then((payload) => {
        if (!cancelled) {
          setConnectors(normalizeDatasetConnectors(getItems(payload)));
        }
      })
      .catch((caughtError) => {
        if (!cancelled) {
          setConnectors([]);
          setConnectorsError(caughtError?.message || "Unable to load connectors.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setConnectorsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (isCreateMode || !resource) {
      return;
    }
    const key = stableEditKey(resource);
    if (editInitializedRef.current === key) {
      return;
    }
    setForm(buildDatasetEditFormState(resource));
    editInitializedRef.current = key;
  }, [isCreateMode, resource]);

  useEffect(() => {
    if (!form.connector || form.connectorFamily) {
      return;
    }
    const selected = connectors.find((connector) => connector.value === form.connector);
    if (!selected?.family) {
      return;
    }
    setForm((current) => ({ ...current, connectorFamily: selected.family }));
  }, [connectors, form.connector, form.connectorFamily]);

  function patchForm(patch) {
    setForm((current) => ({ ...current, ...patch }));
  }

  function handleConnectorFamilyChange(nextFamily) {
    const family = normalizeConnectorFamily(nextFamily);
    const nextConnector = connectors.find((connector) => !family || connector.family === family)?.value || "";
    setForm((current) => ({
      ...current,
      connectorFamily: family,
      connector: isCreateMode ? nextConnector : current.connector,
    }));
  }

  function handleConnectorChange(nextConnector) {
    const selected = connectors.find((connector) => connector.value === nextConnector);
    setForm((current) => ({
      ...current,
      connector: nextConnector,
      connectorFamily: selected?.family || current.connectorFamily,
    }));
  }

  function handleMaterializationChange(nextMode) {
    setForm((current) => ({
      ...current,
      materializationMode: nextMode,
      sourceMode: nextMode === "synced" ? "resource" : current.sourceMode === "resource" ? "table" : current.sourceMode,
    }));
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const payload = buildDatasetSubmitPayload({
        mode: isCreateMode ? "create" : "edit",
        form,
        originalResource: resource,
        connector: selectedConnector,
      });
      const nextResource = isCreateMode ? await onCreate(payload) : await onUpdate(payload);
      setActionResult({
        label: isCreateMode ? "Dataset created" : "Dataset updated",
        payload: nextResource,
      });
      setActiveTab(isCreateMode ? "overview" : "definition");
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to save dataset.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm(`Delete ${resource?.name || "this dataset"}? This cannot be undone.`)) {
      return;
    }
    setError("");
    setSubmitting(true);
    try {
      await onDelete();
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to delete dataset.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleAction(action) {
    if (action.disabled) {
      return;
    }
    setError("");
    setSubmitting(true);
    try {
      const payload = await onAction(action.id);
      setActionResult({ label: action.label, payload });
    } catch (caughtError) {
      setError(caughtError?.message || `Unable to run ${action.label}.`);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Modal title={title} onClose={onClose}>
      <article className="resource-workspace dataset-workspace">
        <header className="resource-workspace-head">
          <div>
            <p className="eyebrow">{isCreateMode ? "Create runtime dataset" : "Opened dataset"}</p>
            <h3>{title}</h3>
            <span>
              {isCreateMode
                ? "Create a runtime-managed dataset with guided source fields."
                : resource?.subtitle}
            </span>
          </div>
          {!isCreateMode && resource ? <ManagementPill mode={resource.management} /> : null}
        </header>

        <nav className="resource-workspace-tabs" aria-label="Dataset workspace tabs">
          {visibleTabs.map((tab) => (
            <button
              key={tab}
              className={classNames(activeTab === tab && "active")}
              type="button"
              onClick={() => setActiveTab(tab)}
            >
              {tabLabels[tab]}
            </button>
          ))}
        </nav>

        {error ? <div className="resource-error">{error}</div> : null}
        {detailError ? <div className="resource-error">{detailError}</div> : null}
        {connectorsError ? <div className="resource-error">{connectorsError}</div> : null}
        {detailLoading ? <div className="resource-loading">Loading runtime detail...</div> : null}

        {activeTab === "overview" && resource ? <DatasetOverview resource={resource} /> : null}

        {activeTab === "definition" ? (
          <section className="resource-editor-panel">
            {!canMutate && !isCreateMode ? <ReadOnlyNotice resource={resource} /> : null}
            <form className="connector-form dataset-form" onSubmit={handleSubmit}>
              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Dataset identity</h4>
                    <p>Name the governed dataset and add the metadata users see in analysis workflows.</p>
                  </div>
                </div>
                <div className="connector-field-grid">
                  <label className="connector-field">
                    <span>Name</span>
                    <input
                      type="text"
                      value={form.name}
                      disabled={!isCreateMode || !canMutate}
                      onChange={(event) => patchForm({ name: event.target.value })}
                      placeholder="sales_orders"
                    />
                  </label>
                  <label className="connector-field">
                    <span>Label</span>
                    <input
                      type="text"
                      value={form.label}
                      disabled={!canMutate}
                      onChange={(event) => patchForm({ label: event.target.value })}
                      placeholder="Sales orders"
                    />
                  </label>
                  <label className="connector-field connector-field--full">
                    <span>Description</span>
                    <input
                      type="text"
                      value={form.description}
                      disabled={!canMutate}
                      onChange={(event) => patchForm({ description: event.target.value })}
                      placeholder="Optional description"
                    />
                  </label>
                  <label className="connector-field connector-field--full">
                    <span>Tags</span>
                    <input
                      type="text"
                      value={form.tags}
                      disabled={!canMutate}
                      onChange={(event) => patchForm({ tags: event.target.value })}
                      placeholder="sales, finance"
                    />
                    <small>Separate tags with commas.</small>
                  </label>
                </div>
              </section>

              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Runtime binding</h4>
                    <p>Choose the connector, materialization mode, and source pattern.</p>
                  </div>
                </div>
                <div className="connector-field-grid">
                  <label className="connector-field">
                    <span>Connector family</span>
                    <select
                      value={form.connectorFamily}
                      disabled={!isCreateMode || !canMutate || connectorsLoading}
                      onChange={(event) => handleConnectorFamilyChange(event.target.value)}
                    >
                      <option value="">All families</option>
                      {connectorFamilies.map((family) => (
                        <option key={family.value} value={family.value}>{family.label}</option>
                      ))}
                    </select>
                  </label>
                  <label className="connector-field">
                    <span>Connector</span>
                    <select
                      value={form.connector}
                      disabled={!isCreateMode || !canMutate || connectorsLoading}
                      onChange={(event) => handleConnectorChange(event.target.value)}
                    >
                      <option value="">No connector</option>
                      {filteredConnectors.map((connector) => (
                        <option key={connector.value} value={connector.value}>
                          {connector.label}
                        </option>
                      ))}
                    </select>
                    <small>Connectorless datasets are only valid for file sources.</small>
                  </label>
                  <label className="connector-field">
                    <span>Materialization</span>
                    <select
                      value={form.materializationMode}
                      disabled={!canMutate || isUnsupportedSource}
                      onChange={(event) => handleMaterializationChange(event.target.value)}
                    >
                      <option value="live">Live</option>
                      <option value="synced">Synced</option>
                    </select>
                  </label>
                  <label className="connector-field">
                    <span>Source mode</span>
                    <select
                      value={effectiveSourceMode}
                      disabled={!canMutate || form.materializationMode === "synced" || isUnsupportedSource}
                      onChange={(event) => patchForm({ sourceMode: event.target.value })}
                    >
                      {isUnsupportedSource ? <option value="unsupported">Unsupported source</option> : null}
                      {DATASET_SOURCE_MODES.filter((item) => form.materializationMode === "synced" ? item.value === "resource" : item.value !== "resource").map((item) => (
                        <option key={item.value} value={item.value}>{item.label}</option>
                      ))}
                    </select>
                  </label>
                </div>
              </section>

              <DatasetSourceFields
                form={form}
                sourceMode={effectiveSourceMode}
                resource={resource}
                disabled={!canMutate}
                onPatch={patchForm}
              />

              <div className="resource-editor-actions">
                <button type="submit" disabled={!canMutate || submitting}>
                  {submitting ? "Saving..." : isCreateMode ? "Create dataset" : "Save dataset"}
                </button>
                {!isCreateMode && canDelete ? (
                  <button className="danger" type="button" disabled={submitting} onClick={handleDelete}>
                    Delete
                  </button>
                ) : null}
              </div>
            </form>
          </section>
        ) : null}

        {activeTab === "actions" && resource ? (
          <section className="resource-actions-panel">
            <ReadOnlyNotice resource={resource} compact />
            <div className="resource-action-grid">
              {actions.map((action) => (
                <button
                  key={action.id}
                  className="resource-action-card"
                  type="button"
                  disabled={submitting || action.disabled}
                  onClick={() => void handleAction(action)}
                >
                  <strong>{action.label}</strong>
                  <span>{action.description}</span>
                </button>
              ))}
            </div>
            <ActionResult result={actionResult} />
          </section>
        ) : null}
      </article>
    </Modal>
  );
}

function DatasetSourceFields({ form, sourceMode, resource, disabled, onPatch }) {
  if (sourceMode === "unsupported") {
    return (
      <section className="connector-form-section">
        <div className="connector-form-section-head">
          <div>
            <h4>Source</h4>
            <p>This source uses an advanced runtime shape. The guided editor will preserve it and only save metadata changes.</p>
          </div>
        </div>
        <div className="resource-action-payload">
          <DynamicFieldValue value={resource?.rawPayload?.source || {}} />
        </div>
      </section>
    );
  }

  return (
    <section className="connector-form-section">
      <div className="connector-form-section-head">
        <div>
          <h4>Source fields</h4>
          <p>Only fields needed for the selected source mode are shown.</p>
        </div>
      </div>
      <div className="connector-field-grid">
        {sourceMode === "table" ? (
          <label className="connector-field connector-field--full">
            <span>Source table</span>
            <input
              type="text"
              value={form.table}
              disabled={disabled}
              onChange={(event) => onPatch({ table: event.target.value })}
              placeholder="schema.table_name"
            />
          </label>
        ) : null}

        {sourceMode === "sql" ? (
          <label className="connector-field connector-field--full">
            <span>Source SQL</span>
            <textarea
              value={form.sql}
              disabled={disabled}
              onChange={(event) => onPatch({ sql: event.target.value })}
              spellCheck="false"
              placeholder="SELECT * FROM source_table"
            />
          </label>
        ) : null}

        {sourceMode === "file" ? (
          <>
            <label className="connector-field">
              <span>Location type</span>
              <select
                value={form.fileLocationField}
                disabled={disabled}
                onChange={(event) => onPatch({ fileLocationField: event.target.value })}
              >
                <option value="path">Path</option>
                <option value="storage_uri">Storage URI</option>
              </select>
            </label>
            <label className="connector-field connector-field--full">
              <span>{form.fileLocationField === "storage_uri" ? "Storage URI" : "Path"}</span>
              <input
                type="text"
                value={form.path}
                disabled={disabled}
                onChange={(event) => onPatch({ path: event.target.value })}
                placeholder={form.fileLocationField === "storage_uri" ? "s3://bucket/path.csv" : "/data/path.csv"}
              />
            </label>
            <label className="connector-field">
              <span>Format</span>
              <select
                value={form.format}
                disabled={disabled}
                onChange={(event) => onPatch({ format: event.target.value })}
              >
                <option value="csv">CSV</option>
                <option value="parquet">Parquet</option>
              </select>
            </label>
            <label className="connector-field">
              <span>Delimiter</span>
              <input
                type="text"
                value={form.delimiter}
                disabled={disabled}
                onChange={(event) => onPatch({ delimiter: event.target.value })}
              />
            </label>
            <label className="connector-field">
              <span>Quote</span>
              <input
                type="text"
                value={form.quote}
                disabled={disabled}
                onChange={(event) => onPatch({ quote: event.target.value })}
              />
            </label>
            <label className="dataset-checkbox-field">
              <input
                type="checkbox"
                checked={form.header}
                disabled={disabled}
                onChange={(event) => onPatch({ header: event.target.checked })}
              />
              <span>Header row</span>
            </label>
          </>
        ) : null}

        {sourceMode === "resource" ? (
          <label className="connector-field connector-field--full">
            <span>Connector resource</span>
            <input
              type="text"
              value={form.resource}
              disabled={disabled}
              onChange={(event) => onPatch({ resource: event.target.value })}
              placeholder="orders"
            />
            <small>Use a resource name exposed by the selected sync-capable connector.</small>
          </label>
        ) : null}
      </div>
    </section>
  );
}

function DatasetOverview({ resource }) {
  const raw = resource.rawPayload || {};
  const columns = Array.isArray(raw.columns) ? raw.columns : [];
  const policy = raw.policy && typeof raw.policy === "object" ? raw.policy : {};
  return (
    <div className="config-resource-detail">
      <div className="resource-meta-grid">
        <div><span>Status</span><strong>{resource.status}</strong></div>
        <div><span>Mode</span><strong>{isSyncedDataset(raw) ? "Synced" : "Live"}</strong></div>
        <div><span>Connector</span><strong>{raw.connector || "None"}</strong></div>
      </div>

      <div className="resource-state-grid">
        <ResourceSection title="Runtime state" rows={resource.runtimeState} />
        <ResourceSection title="Config definition" rows={resource.configDefinition} />
      </div>

      <div className="resource-detail-block">
        <h4>Source</h4>
        <div className="dataset-source-summary">
          <strong>{describeDatasetSource(raw)}</strong>
          <span>{raw.semantic_model || "No semantic model binding"}</span>
        </div>
      </div>

      <div className="resource-detail-block">
        <h4>Schema</h4>
        {columns.length > 0 ? (
          <div className="resource-result-table-wrap">
            <table className="resource-result-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Nullable</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                {columns.slice(0, 12).map((column) => (
                  <tr key={column.id || column.name}>
                    <td>{formatCell(column.name)}</td>
                    <td>{formatCell(column.data_type || column.type)}</td>
                    <td>{formatCell(column.nullable)}</td>
                    <td>{formatCell(column.description)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="connector-empty-note">No column metadata available.</div>
        )}
      </div>

      <div className="resource-detail-block">
        <h4>Runtime detail</h4>
        <dl className="resource-definition-list">
          <div>
            <dt>Materialization</dt>
            <dd><DynamicFieldValue value={raw.materialization || { mode: raw.materialization_mode }} /></dd>
          </div>
          <div>
            <dt>Source</dt>
            <dd><DynamicFieldValue value={raw.source || {}} /></dd>
          </div>
          <div>
            <dt>Policy</dt>
            <dd><DynamicFieldValue value={policy} /></dd>
          </div>
        </dl>
      </div>
    </div>
  );
}

function ResourceSection({ title, rows }) {
  return (
    <section className="resource-detail-block">
      <h4>{title}</h4>
      <dl className="resource-definition-list">
        {(rows || []).map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd><DynamicFieldValue value={value} /></dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

function ReadOnlyNotice({ resource, compact = false }) {
  if (!resource || resource.management === "runtime_managed") {
    return null;
  }
  return (
    <div className={classNames("resource-readonly-notice", compact && "compact")}>
      <strong>Config-managed dataset</strong>
      <span>This dataset is loaded from runtime configuration and is inspectable here, but edits and deletes are disabled.</span>
    </div>
  );
}

function ActionResult({ result }) {
  if (!result) {
    return null;
  }
  const rows = extractRows(result.payload);
  const columns = extractColumns(result.payload, rows);
  return (
    <section className="resource-action-result">
      <h4>{result.label}</h4>
      {rows.length > 0 && columns.length > 0 ? (
        <div className="resource-result-table-wrap">
          <table className="resource-result-table">
            <thead>
              <tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr>
            </thead>
            <tbody>
              {rows.slice(0, 12).map((row, index) => (
                <tr key={`${result.label}-${index}`}>
                  {columns.map((column, columnIndex) => (
                    <td key={column}>{formatCell(readCell(row, column, columnIndex))}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
      <div className="resource-action-payload">
        <DynamicFieldValue value={result.payload} />
      </div>
    </section>
  );
}

function stableEditKey(resource) {
  const raw = resource?.rawPayload || {};
  return JSON.stringify({
    id: resource?.id,
    label: raw.label,
    description: raw.description,
    connector: raw.connector,
    source: raw.source,
    materialization: raw.materialization,
    materialization_mode: raw.materialization_mode,
    tags: raw.tags,
  });
}

function extractRows(payload) {
  if (Array.isArray(payload?.items)) {
    return payload.items;
  }
  if (Array.isArray(payload?.rows)) {
    return payload.rows;
  }
  if (Array.isArray(payload?.data)) {
    return payload.data;
  }
  return [];
}

function extractColumns(payload, rows) {
  if (Array.isArray(payload?.columns) && payload.columns.length > 0) {
    return payload.columns.map((column) => column.name || column.label || String(column));
  }
  const firstRow = rows[0];
  if (firstRow && typeof firstRow === "object" && !Array.isArray(firstRow)) {
    return Object.keys(firstRow).slice(0, 8);
  }
  if (Array.isArray(firstRow)) {
    return firstRow.map((_, index) => `Column ${index + 1}`);
  }
  return [];
}

function readCell(row, column, index) {
  if (Array.isArray(row)) {
    return row[index];
  }
  return row?.[column];
}

function formatCell(value) {
  if (value === undefined || value === null) {
    return "n/a";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}
