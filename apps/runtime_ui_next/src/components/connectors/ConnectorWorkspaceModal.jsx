import { useEffect, useMemo, useRef, useState } from "react";

import { getItems } from "../../services/langbridgeApiClient.js";
import { fetchConnectorTypeConfig, fetchConnectorTypes } from "../../lib/runtimeApi.js";
import { Modal } from "../ui/Modal.jsx";
import { DynamicFieldValue } from "../resources/DynamicFieldViewer.jsx";
import { ManagementPill } from "../resources/ManagementPill.jsx";
import { classNames } from "../../utils/classNames.js";
import {
  SECRET_PROVIDER_OPTIONS,
  buildConnectorConfigValues,
  buildConnectorCreateFormState,
  buildConnectorEditFormState,
  buildConnectorSubmitPayload,
  connectorFamilyOptions,
  createBlankMetadataRow,
  createBlankSecretRow,
  formatConnectorFamilyLabel,
  normalizeConnectorFamily,
  normalizeConnectorTypeName,
  normalizeConnectorTypes,
  schemaEntries,
} from "../../features/configuration/connectorFormModel.js";

const tabLabels = {
  overview: "Overview",
  connection: "Connection",
  actions: "Actions",
};

export function ConnectorWorkspaceModal({
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
  const [activeTab, setActiveTab] = useState(isCreateMode ? "connection" : "overview");
  const [connectorTypes, setConnectorTypes] = useState([]);
  const [connectorTypesLoading, setConnectorTypesLoading] = useState(false);
  const [connectorTypesError, setConnectorTypesError] = useState("");
  const [schemasByType, setSchemasByType] = useState({});
  const [schemaLoadingByType, setSchemaLoadingByType] = useState({});
  const [schemaErrorsByType, setSchemaErrorsByType] = useState({});
  const [form, setForm] = useState(() => buildConnectorCreateFormState());
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [actionResult, setActionResult] = useState(null);
  const createSchemaInitializedRef = useRef("");
  const editInitializedRef = useRef("");
  const schemaRequestsRef = useRef(new Map());
  const visibleTabs = useMemo(
    () => (isCreateMode ? ["connection"] : ["overview", "connection", "actions"]),
    [isCreateMode],
  );
  const rawResource = resource?.rawPayload || {};
  const currentType = normalizeConnectorTypeName(isCreateMode ? form.type : rawResource.connector_type || rawResource.type);
  const currentSchema = schemasByType[currentType] || null;
  const currentSchemaLoading = Boolean(schemaLoadingByType[currentType]);
  const currentSchemaError = schemaErrorsByType[currentType] || "";
  const families = connectorFamilyOptions(connectorTypes);
  const filteredConnectorTypes = useMemo(
    () =>
      connectorTypes.filter((item) => {
        if (!form.family) {
          return true;
        }
        return normalizeConnectorFamily(item.family) === normalizeConnectorFamily(form.family);
      }),
    [connectorTypes, form.family],
  );
  const targetFieldOptions = useMemo(
    () =>
      Array.from(
        new Set([
          ...schemaEntries(currentSchema).map((entry) => entry.field).filter(Boolean),
          ...form.secretRows.map((row) => row.field).filter(Boolean),
        ]),
      ),
    [currentSchema, form.secretRows],
  );
  const title = isCreateMode ? "Create connector" : resource?.name || "Connector details";

  useEffect(() => {
    setActiveTab(isCreateMode ? "connection" : "overview");
    setError("");
    setActionResult(null);
    createSchemaInitializedRef.current = "";
    editInitializedRef.current = "";
    if (isCreateMode) {
      setForm(buildConnectorCreateFormState({ connectorTypes }));
    }
  }, [isCreateMode, resource?.id]);

  useEffect(() => {
    let cancelled = false;
    setConnectorTypesLoading(true);
    setConnectorTypesError("");
    fetchConnectorTypes()
      .then((payload) => {
        if (!cancelled) {
          setConnectorTypes(normalizeConnectorTypes(getItems(payload)));
        }
      })
      .catch((caughtError) => {
        if (!cancelled) {
          setConnectorTypes([]);
          setConnectorTypesError(caughtError?.message || "Unable to load connector types.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setConnectorTypesLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!isCreateMode || connectorTypes.length === 0 || form.type) {
      return;
    }
    setForm(buildConnectorCreateFormState({ connectorTypes }));
  }, [connectorTypes, form.type, isCreateMode]);

  useEffect(() => {
    if (!currentType || schemasByType[currentType]) {
      return;
    }
    let cancelled = false;

    let request = schemaRequestsRef.current.get(currentType);
    if (!request) {
      setSchemaLoadingByType((current) => ({ ...current, [currentType]: true }));
      setSchemaErrorsByType((current) => ({ ...current, [currentType]: "" }));
      request = fetchConnectorTypeConfig(currentType).finally(() => {
        schemaRequestsRef.current.delete(currentType);
      });
      schemaRequestsRef.current.set(currentType, request);
    } else {
      setSchemaLoadingByType((current) => ({ ...current, [currentType]: true }));
    }

    request
      .then((schema) => {
        if (!cancelled) {
          setSchemasByType((current) => ({ ...current, [currentType]: schema }));
        }
      })
      .catch((caughtError) => {
        if (!cancelled) {
          setSchemaErrorsByType((current) => ({
            ...current,
            [currentType]: caughtError?.message || "Unable to load connector schema.",
          }));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setSchemaLoadingByType((current) => ({ ...current, [currentType]: false }));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [currentType, schemasByType]);

  useEffect(() => {
    if (!isCreateMode || !currentSchema || !form.type) {
      return;
    }
    if (createSchemaInitializedRef.current === form.type) {
      return;
    }
    setForm((current) => ({
      ...current,
      configValues: buildConnectorConfigValues(currentSchema),
    }));
    createSchemaInitializedRef.current = form.type;
  }, [currentSchema, form.type, isCreateMode]);

  useEffect(() => {
    if (isCreateMode || !resource || !currentSchema) {
      return;
    }
    const key = stableEditKey(resource, currentSchema);
    if (editInitializedRef.current === key) {
      return;
    }
    setForm(buildConnectorEditFormState(resource, currentSchema));
    editInitializedRef.current = key;
  }, [currentSchema, isCreateMode, resource]);

  function patchForm(patch) {
    setForm((current) => ({ ...current, ...patch }));
  }

  function patchConfigValue(field, value) {
    setForm((current) => ({
      ...current,
      configValues: {
        ...current.configValues,
        [field]: value,
      },
    }));
  }

  function handleFamilyChange(nextFamily) {
    const normalizedFamily = normalizeConnectorFamily(nextFamily);
    const nextType = connectorTypes.find((item) => !normalizedFamily || item.family === normalizedFamily)?.value || "";
    createSchemaInitializedRef.current = "";
    setForm((current) => ({
      ...current,
      family: normalizedFamily,
      type: nextType,
      configValues: {},
    }));
  }

  function handleTypeChange(nextType) {
    const normalizedType = normalizeConnectorTypeName(nextType);
    const selectedType = connectorTypes.find((item) => item.value === normalizedType);
    createSchemaInitializedRef.current = "";
    setForm((current) => ({
      ...current,
      family: selectedType?.family || current.family,
      type: normalizedType,
      configValues: {},
    }));
  }

  function patchMetadataRow(rowId, patch) {
    setForm((current) => ({
      ...current,
      metadataRows: current.metadataRows.map((row) => (row.id === rowId ? { ...row, ...patch } : row)),
    }));
  }

  function patchSecretRow(rowId, patch) {
    setForm((current) => ({
      ...current,
      secretRows: current.secretRows.map((row) => (row.id === rowId ? { ...row, ...patch } : row)),
    }));
  }

  function removeMetadataRow(rowId) {
    setForm((current) => ({
      ...current,
      metadataRows: current.metadataRows.filter((row) => row.id !== rowId),
    }));
  }

  function removeSecretRow(rowId) {
    setForm((current) => ({
      ...current,
      secretRows: current.secretRows.filter((row) => row.id !== rowId),
    }));
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const payload = buildConnectorSubmitPayload({
        mode: isCreateMode ? "create" : "edit",
        form,
        schema: currentSchema,
      });
      const nextResource = isCreateMode ? await onCreate(payload) : await onUpdate(payload);
      setActionResult({
        label: isCreateMode ? "Connector created" : "Connector updated",
        payload: nextResource,
      });
      setActiveTab(isCreateMode ? "overview" : "connection");
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to save connector.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm(`Delete ${resource?.name || "this connector"}? This cannot be undone.`)) {
      return;
    }
    setError("");
    setSubmitting(true);
    try {
      await onDelete();
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to delete connector.");
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
      <article className="resource-workspace connector-workspace">
        <header className="resource-workspace-head">
          <div>
            <p className="eyebrow">{isCreateMode ? "Create runtime connector" : "Opened connector"}</p>
            <h3>{title}</h3>
            <span>
              {isCreateMode
                ? "Configure a runtime-managed connector with guided fields."
                : resource?.subtitle}
            </span>
          </div>
          {!isCreateMode && resource ? <ManagementPill mode={resource.management} /> : null}
        </header>

        <nav className="resource-workspace-tabs" aria-label="Connector workspace tabs">
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
        {connectorTypesError ? <div className="resource-error">{connectorTypesError}</div> : null}
        {detailLoading ? <div className="resource-loading">Loading runtime detail...</div> : null}

        {activeTab === "overview" && resource ? <ConnectorOverview resource={resource} /> : null}

        {activeTab === "connection" ? (
          <section className="resource-editor-panel">
            {!canMutate && !isCreateMode ? <ReadOnlyNotice resource={resource} /> : null}
            <form className="connector-form" onSubmit={handleSubmit}>
              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Connector identity</h4>
                    <p>Name the connector and choose the runtime connector type.</p>
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
                      placeholder="warehouse"
                    />
                  </label>
                  <label className="connector-field">
                    <span>Family</span>
                    <select
                      value={form.family}
                      disabled={!isCreateMode || !canMutate || connectorTypesLoading}
                      onChange={(event) => handleFamilyChange(event.target.value)}
                    >
                      <option value="">All families</option>
                      {families.map((family) => (
                        <option key={family.value} value={family.value}>
                          {family.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="connector-field">
                    <span>Type</span>
                    <select
                      value={form.type}
                      disabled={!isCreateMode || !canMutate || connectorTypesLoading}
                      onChange={(event) => handleTypeChange(event.target.value)}
                    >
                      {filteredConnectorTypes.length === 0 ? <option value="">No connector types available</option> : null}
                      {filteredConnectorTypes.map((type) => (
                        <option key={type.value} value={type.value}>
                          {type.label || type.value}
                        </option>
                      ))}
                    </select>
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
                </div>
              </section>

              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Connection fields</h4>
                    <p>{currentSchema?.description || "Fields are generated from the connector runtime schema."}</p>
                  </div>
                </div>
                {currentSchemaLoading ? <div className="resource-loading">Loading connector fields...</div> : null}
                {currentSchemaError ? <div className="resource-error">{currentSchemaError}</div> : null}
                {!currentSchemaLoading && !currentSchemaError && schemaEntries(currentSchema).length === 0 ? (
                  <div className="config-resource-empty">
                    <strong>No connection schema</strong>
                    <span>Select a connector type to load connection fields.</span>
                  </div>
                ) : null}
                <div className="connector-field-grid">
                  {schemaEntries(currentSchema).map((entry) => (
                    <ConnectorSchemaField
                      key={entry.field}
                      entry={entry}
                      value={form.configValues[entry.field] ?? ""}
                      disabled={!canMutate}
                      usesSecret={form.secretRows.some((row) => row.field === entry.field)}
                      onChange={(value) => patchConfigValue(entry.field, value)}
                    />
                  ))}
                </div>
              </section>

              <MetadataRows
                rows={form.metadataRows}
                disabled={!canMutate}
                onAdd={() => patchForm({ metadataRows: [...form.metadataRows, createBlankMetadataRow()] })}
                onPatch={patchMetadataRow}
                onRemove={removeMetadataRow}
              />

              <SecretReferenceRows
                rows={form.secretRows}
                targetFieldOptions={targetFieldOptions}
                disabled={!canMutate}
                onAdd={() => patchForm({ secretRows: [...form.secretRows, createBlankSecretRow(targetFieldOptions[0] || "")] })}
                onPatch={patchSecretRow}
                onRemove={removeSecretRow}
              />

              <div className="resource-editor-actions">
                <button type="submit" disabled={!canMutate || submitting || currentSchemaLoading}>
                  {submitting ? "Saving..." : isCreateMode ? "Create connector" : "Save connector"}
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

function ConnectorSchemaField({ entry, value, disabled, usesSecret, onChange }) {
  const label = entry.label || entry.field;
  const fieldType = String(entry.type || "string").toLowerCase();
  const hasOptions = Array.isArray(entry.value_list) && entry.value_list.length > 0;
  return (
    <label className={classNames("connector-field", fieldType === "textarea" && "connector-field--full")}>
      <span>
        {label}
        {entry.required ? <i>Required</i> : null}
      </span>
      {hasOptions ? (
        <select value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)}>
          <option value="">Select {label}</option>
          {entry.value_list.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      ) : fieldType === "boolean" ? (
        <select value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)}>
          <option value="">Select {label}</option>
          <option value="true">True</option>
          <option value="false">False</option>
        </select>
      ) : fieldType === "textarea" ? (
        <textarea value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)} />
      ) : (
        <input
          type={fieldType === "password" ? "password" : fieldType === "number" ? "number" : "text"}
          value={value}
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
          placeholder={entry.default || ""}
        />
      )}
      <small>
        {entry.description || "Runtime connector field."}
        {usesSecret ? " A secret reference can provide this value at runtime." : ""}
      </small>
    </label>
  );
}

function MetadataRows({ rows, disabled, onAdd, onPatch, onRemove }) {
  return (
    <section className="connector-form-section">
      <div className="connector-form-section-head">
        <div>
          <h4>Metadata</h4>
          <p>Optional non-secret values merged into connector metadata.</p>
        </div>
        <button type="button" disabled={disabled} onClick={onAdd}>Add metadata</button>
      </div>
      {rows.length === 0 ? <div className="connector-empty-note">No metadata fields added.</div> : null}
      {rows.map((row) => (
        <div className="connector-repeat-row connector-repeat-row--metadata" key={row.id}>
          <label className="connector-field">
            <span>Key</span>
            <input value={row.key} disabled={disabled} onChange={(event) => onPatch(row.id, { key: event.target.value })} />
          </label>
          <label className="connector-field">
            <span>Value</span>
            {row.valueType === "boolean" ? (
              <select
                value={row.value}
                disabled={disabled}
                onChange={(event) => onPatch(row.id, { value: event.target.value })}
              >
                <option value="">Select value</option>
                <option value="true">True</option>
                <option value="false">False</option>
              </select>
            ) : (
              <input
                type={row.valueType === "number" ? "number" : "text"}
                value={row.value}
                disabled={disabled}
                onChange={(event) => onPatch(row.id, { value: event.target.value })}
              />
            )}
          </label>
          <label className="connector-field">
            <span>Type</span>
            <select value={row.valueType} disabled={disabled} onChange={(event) => onPatch(row.id, { valueType: event.target.value })}>
              <option value="string">Text</option>
              <option value="number">Number</option>
              <option value="boolean">Boolean</option>
            </select>
          </label>
          <button type="button" disabled={disabled} onClick={() => onRemove(row.id)}>Remove</button>
        </div>
      ))}
    </section>
  );
}

function SecretReferenceRows({ rows, targetFieldOptions, disabled, onAdd, onPatch, onRemove }) {
  const datalistId = "connector-secret-target-fields";
  return (
    <section className="connector-form-section">
      <div className="connector-form-section-head">
        <div>
          <h4>Secret references</h4>
          <p>Reference runtime secret providers without pasting secret values into configuration.</p>
        </div>
        <button type="button" disabled={disabled} onClick={onAdd}>Add secret</button>
      </div>
      <datalist id={datalistId}>
        {targetFieldOptions.map((field) => <option key={field} value={field} />)}
      </datalist>
      {rows.length === 0 ? <div className="connector-empty-note">No secret references added.</div> : null}
      {rows.map((row) => (
        <div className="connector-repeat-row connector-repeat-row--secret" key={row.id}>
          <label className="connector-field">
            <span>Target field</span>
            <input
              list={datalistId}
              value={row.field}
              disabled={disabled}
              onChange={(event) => onPatch(row.id, { field: event.target.value })}
              placeholder="password"
            />
          </label>
          <label className="connector-field">
            <span>Provider</span>
            <select
              value={row.provider_type}
              disabled={disabled}
              onChange={(event) => onPatch(row.id, { provider_type: event.target.value })}
            >
              {SECRET_PROVIDER_OPTIONS.map((provider) => (
                <option key={provider.value} value={provider.value}>{provider.label}</option>
              ))}
            </select>
          </label>
          <label className="connector-field">
            <span>Identifier</span>
            <input
              value={row.identifier}
              disabled={disabled}
              onChange={(event) => onPatch(row.id, { identifier: event.target.value })}
              placeholder="SECRET_NAME"
            />
          </label>
          <label className="connector-field">
            <span>Key</span>
            <input value={row.key} disabled={disabled} onChange={(event) => onPatch(row.id, { key: event.target.value })} />
          </label>
          <label className="connector-field">
            <span>Version</span>
            <input value={row.version} disabled={disabled} onChange={(event) => onPatch(row.id, { version: event.target.value })} />
          </label>
          <button type="button" disabled={disabled} onClick={() => onRemove(row.id)}>Remove</button>
        </div>
      ))}
    </section>
  );
}

function ConnectorOverview({ resource }) {
  return (
    <div className="config-resource-detail">
      <div className="resource-meta-grid">
        <div><span>Status</span><strong>{resource.status}</strong></div>
        <div><span>Type</span><strong>{resource.rawPayload?.connector_type || "n/a"}</strong></div>
        <div><span>Family</span><strong>{formatConnectorFamilyLabel(resource.rawPayload?.connector_family)}</strong></div>
      </div>

      <div className="resource-state-grid">
        <ResourceSection title="Runtime state" rows={resource.runtimeState} />
        <ResourceSection title="Config definition" rows={resource.configDefinition} />
      </div>

      <div className="resource-detail-block">
        <h4>Supported resources</h4>
        <div className="resource-chip-row">
          {(resource.relationships || []).map((item, index) => (
            <span key={`${String(item)}-${index}`}>{String(item)}</span>
          ))}
        </div>
      </div>

      <div className="resource-detail-block">
        <h4>Connector detail</h4>
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

function ReadOnlyNotice({ resource, compact = false }) {
  if (!resource || resource.management === "runtime_managed") {
    return null;
  }
  return (
    <div className={classNames("resource-readonly-notice", compact && "compact")}>
      <strong>Config-managed connector</strong>
      <span>This connector is loaded from runtime configuration and is inspectable here, but edits and deletes are disabled.</span>
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

function stableEditKey(resource, schema) {
  const raw = resource?.rawPayload || {};
  return JSON.stringify({
    id: resource?.id,
    type: raw.connector_type,
    schemaVersion: schema?.version,
    description: raw.description,
    connection: raw.connection,
    metadata: raw.metadata,
    secrets: raw.secrets,
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
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}
