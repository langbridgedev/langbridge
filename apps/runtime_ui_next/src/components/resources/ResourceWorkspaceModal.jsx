import { useEffect, useMemo, useState } from "react";

import { Modal } from "../ui/Modal.jsx";
import { DynamicFieldValue } from "./DynamicFieldViewer.jsx";
import { ManagementPill } from "./ManagementPill.jsx";
import { classNames } from "../../utils/classNames.js";

const tabLabels = {
  overview: "Overview",
  definition: "Definition",
  actions: "Actions",
};

export function ResourceWorkspaceModal({
  section,
  mode,
  resource,
  detailLoading = false,
  capabilities,
  createTemplate,
  updateTemplate,
  actions = [],
  onClose,
  onCreate,
  onUpdate,
  onDelete,
  onAction,
}) {
  const isCreateMode = mode === "create";
  const [activeTab, setActiveTab] = useState(isCreateMode ? "definition" : "overview");
  const [editorText, setEditorText] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [actionResult, setActionResult] = useState(null);
  const canMutate =
    isCreateMode
      ? Boolean(capabilities?.canCreate)
      : Boolean(capabilities?.canUpdate && resource?.management === "runtime_managed");
  const canDelete = Boolean(capabilities?.canDelete && resource?.management === "runtime_managed");
  const editorTitle = isCreateMode ? "Create payload" : "Update payload";
  const title = isCreateMode ? capabilities?.createLabel || "Add resource" : resource?.name || "Resource details";
  const visibleTabs = useMemo(
    () => (isCreateMode ? ["definition"] : ["overview", "definition", "actions"]),
    [isCreateMode],
  );

  useEffect(() => {
    setActiveTab(isCreateMode ? "definition" : "overview");
    setError("");
    setActionResult(null);
  }, [isCreateMode, resource?.id, section]);

  useEffect(() => {
    setEditorText(formatJson(isCreateMode ? createTemplate : updateTemplate));
  }, [createTemplate, isCreateMode, updateTemplate]);

  async function handleSubmit(event) {
    event.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const payload = parseJson(editorText, editorTitle);
      const nextResource = isCreateMode ? await onCreate(payload) : await onUpdate(payload);
      setActionResult({
        label: isCreateMode ? "Resource created" : "Resource updated",
        payload: nextResource,
      });
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to save resource.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm(`Delete ${resource?.name || "this resource"}? This cannot be undone.`)) {
      return;
    }
    setError("");
    setSubmitting(true);
    try {
      await onDelete();
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to delete resource.");
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
      <article className="resource-workspace">
        <header className="resource-workspace-head">
          <div>
            <p className="eyebrow">{isCreateMode ? "Create runtime resource" : "Opened resource"}</p>
            <h3>{title}</h3>
            <span>{isCreateMode ? "Create a runtime-managed resource with a JSON payload." : resource?.subtitle}</span>
          </div>
          {!isCreateMode && resource ? <ManagementPill mode={resource.management} /> : null}
        </header>

        <nav className="resource-workspace-tabs" aria-label="Resource workspace tabs">
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
        {detailLoading ? <div className="resource-loading">Loading runtime detail...</div> : null}

        {activeTab === "overview" && resource ? (
          <ResourceOverview resource={resource} />
        ) : null}

        {activeTab === "definition" ? (
          <section className="resource-editor-panel">
            {!canMutate && !isCreateMode ? (
              <ReadOnlyNotice resource={resource} />
            ) : null}
            <form className="resource-json-editor" onSubmit={handleSubmit}>
              <label htmlFor="resource-json-editor">{editorTitle}</label>
              <textarea
                id="resource-json-editor"
                value={editorText}
                onChange={(event) => setEditorText(event.target.value)}
                spellCheck="false"
                disabled={!canMutate}
              />
              <div className="resource-editor-actions">
                <button type="submit" disabled={!canMutate || submitting}>
                  {submitting ? "Saving..." : isCreateMode ? "Create resource" : "Save changes"}
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

function ResourceOverview({ resource }) {
  return (
    <div className="config-resource-detail">
      <div className="resource-meta-grid">
        <div><span>Status</span><strong>{resource.status}</strong></div>
        <div><span>Owner</span><strong>{resource.owner}</strong></div>
        <div><span>Updated</span><strong>{resource.lastUpdated}</strong></div>
      </div>

      <div className="resource-state-grid">
        <ResourceSection title="Runtime state" rows={resource.runtimeState} />
        <ResourceSection title="Config definition" rows={resource.configDefinition} />
      </div>

      <div className="resource-detail-block">
        <h4>Relationships</h4>
        <div className="resource-chip-row">
          {(resource.relationships || []).map((item, index) => (
            <span key={relationshipKey(item, index)}>{formatRelationship(item)}</span>
          ))}
        </div>
      </div>

      <div className="resource-detail-block">
        <h4>Resource detail</h4>
        <dl className="resource-definition-list">
          {Object.entries(resource.details || {}).map(([label, value]) => (
            <div key={label}>
              <dt>{label}</dt>
              <dd><DynamicFieldValue value={value} /></dd>
            </div>
          ))}
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

function relationshipKey(item, index) {
  if (item && typeof item === "object") {
    return String(item.id || item.name || item.label || item.tool_name || `relationship-${index}`);
  }
  return `${String(item)}-${index}`;
}

function formatRelationship(item) {
  if (item === undefined || item === null || item === "") {
    return "n/a";
  }
  if (typeof item === "object") {
    return String(item.name || item.label || item.id || item.tool_name || `${Object.keys(item).length} fields`);
  }
  return String(item);
}

function ReadOnlyNotice({ resource, compact = false }) {
  if (!resource || resource.management === "runtime_managed") {
    return null;
  }
  return (
    <div className={classNames("resource-readonly-notice", compact && "compact")}>
      <strong>Config-managed resource</strong>
      <span>This resource is loaded from runtime configuration and is inspectable here, but edits and deletes are disabled.</span>
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
              {rows.slice(0, 10).map((row, index) => (
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

function parseJson(text, label) {
  try {
    return JSON.parse(text || "{}");
  } catch (error) {
    throw new Error(`${label} must be valid JSON.`);
  }
}

function formatJson(value) {
  return JSON.stringify(value ?? {}, null, 2);
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
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}
