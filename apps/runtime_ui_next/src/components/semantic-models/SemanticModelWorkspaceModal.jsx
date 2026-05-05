import { useEffect, useMemo, useRef, useState } from "react";

import { fetchDataset, fetchDatasets } from "../../lib/runtimeApi.js";
import { getItems } from "../../services/langbridgeApiClient.js";
import { Modal } from "../ui/Modal.jsx";
import { DynamicFieldValue } from "../resources/DynamicFieldViewer.jsx";
import { ManagementPill } from "../resources/ManagementPill.jsx";
import { classNames } from "../../utils/classNames.js";
import {
  SEMANTIC_DIMENSION_TYPES,
  SEMANTIC_MEASURE_AGGREGATIONS,
  SEMANTIC_MEASURE_TYPES,
  SEMANTIC_RELATIONSHIP_TYPES,
  buildSemanticDatasetFromRuntimeDataset,
  buildSemanticModelCreateFormState,
  buildSemanticModelDefinition,
  buildSemanticModelEditFormState,
  buildSemanticModelSubmitPayload,
  createEmptySemanticDimension,
  createEmptySemanticMeasure,
  createEmptySemanticMetric,
  createEmptySemanticRelationship,
  normalizeSemanticDatasetOptions,
  sanitizeSemanticKey,
  semanticModelStats,
} from "../../features/configuration/semanticModelFormModel.js";

const tabLabels = {
  overview: "Overview",
  definition: "Definition",
  actions: "Actions",
};

export function SemanticModelWorkspaceModal({
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
  const [datasets, setDatasets] = useState([]);
  const [datasetsLoading, setDatasetsLoading] = useState(false);
  const [datasetsError, setDatasetsError] = useState("");
  const [datasetSearch, setDatasetSearch] = useState("");
  const [loadingDatasetNames, setLoadingDatasetNames] = useState([]);
  const [form, setForm] = useState(() => buildSemanticModelCreateFormState());
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [actionResult, setActionResult] = useState(null);
  const editInitializedRef = useRef("");
  const visibleTabs = useMemo(
    () => (isCreateMode ? ["definition"] : ["overview", "definition", "actions"]),
    [isCreateMode],
  );
  const selectedDatasetNames = useMemo(
    () => new Set(form.semanticDatasets.map((dataset) => dataset.sourceDatasetName).filter(Boolean)),
    [form.semanticDatasets],
  );
  const filteredDatasets = useMemo(() => {
    const needle = datasetSearch.trim().toLowerCase();
    if (!needle) {
      return datasets;
    }
    return datasets.filter((dataset) =>
      [dataset.name, dataset.label, dataset.description, dataset.connector]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(needle),
    );
  }, [datasets, datasetSearch]);
  const relationshipDatasetOptions = useMemo(
    () =>
      form.semanticDatasets.map((dataset) => ({
        value: dataset.semanticKey,
        label: `${dataset.semanticKey} (${dataset.sourceDatasetLabel || dataset.sourceDatasetName})`,
        fields: [
          ...(Array.isArray(dataset.dimensions) ? dataset.dimensions : []),
          ...(Array.isArray(dataset.measures) ? dataset.measures : []),
        ].map((field) => field.name).filter(Boolean),
      })),
    [form.semanticDatasets],
  );
  const stats = semanticModelStats(form);
  const title = isCreateMode ? "Create semantic model" : resource?.name || "Semantic model details";
  const guidedEditorDisabled = !canMutate || form.unsupported;

  useEffect(() => {
    setActiveTab(isCreateMode ? "definition" : "overview");
    setError("");
    setActionResult(null);
    editInitializedRef.current = "";
    if (isCreateMode) {
      setForm(buildSemanticModelCreateFormState());
    }
  }, [isCreateMode, resource?.id]);

  useEffect(() => {
    let cancelled = false;
    setDatasetsLoading(true);
    setDatasetsError("");
    fetchDatasets()
      .then((payload) => {
        if (!cancelled) {
          setDatasets(normalizeSemanticDatasetOptions(getItems(payload)));
        }
      })
      .catch((caughtError) => {
        if (!cancelled) {
          setDatasets([]);
          setDatasetsError(caughtError?.message || "Unable to load datasets.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setDatasetsLoading(false);
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
    const key = stableEditKey(resource, datasets);
    if (editInitializedRef.current === key) {
      return;
    }
    setForm(buildSemanticModelEditFormState(resource, datasets));
    editInitializedRef.current = key;
  }, [isCreateMode, resource]);

  function patchForm(patch) {
    setForm((current) => ({ ...current, ...patch }));
  }

  async function handleToggleDataset(dataset) {
    const datasetName = String(dataset?.name || "").trim();
    if (!datasetName || guidedEditorDisabled) {
      return;
    }

    if (selectedDatasetNames.has(datasetName)) {
      setForm((current) => {
        const removed = current.semanticDatasets.find((item) => item.sourceDatasetName === datasetName);
        const removedKey = removed?.semanticKey;
        return {
          ...current,
          semanticDatasets: current.semanticDatasets.filter((item) => item.sourceDatasetName !== datasetName),
          relationships: current.relationships.filter(
            (relationship) =>
              relationship.sourceDataset !== removedKey && relationship.targetDataset !== removedKey,
          ),
        };
      });
      return;
    }

    setLoadingDatasetNames((current) => [...current, datasetName]);
    setError("");
    try {
      const detail = await fetchDataset(datasetName);
      setForm((current) => ({
        ...current,
        semanticDatasets: [
          ...current.semanticDatasets,
          buildSemanticDatasetFromRuntimeDataset(detail, current.semanticDatasets.map((item) => item.semanticKey)),
        ],
      }));
    } catch (caughtError) {
      setError(caughtError?.message || `Unable to load ${datasetName}.`);
    } finally {
      setLoadingDatasetNames((current) => current.filter((item) => item !== datasetName));
    }
  }

  function patchDataset(datasetId, patchOrUpdater) {
    setForm((current) => {
      let previousKey = "";
      let nextKey = "";
      const semanticDatasets = current.semanticDatasets.map((dataset) => {
        if (dataset.id !== datasetId) {
          return dataset;
        }
        const patch = typeof patchOrUpdater === "function" ? patchOrUpdater(dataset) : patchOrUpdater;
        const nextDataset = { ...dataset, ...patch };
        previousKey = dataset.semanticKey;
        nextKey = nextDataset.semanticKey;
        return nextDataset;
      });
      const relationships =
        previousKey && nextKey && previousKey !== nextKey
          ? current.relationships.map((relationship) => ({
              ...relationship,
              sourceDataset: relationship.sourceDataset === previousKey ? nextKey : relationship.sourceDataset,
              targetDataset: relationship.targetDataset === previousKey ? nextKey : relationship.targetDataset,
            }))
          : current.relationships;
      return { ...current, semanticDatasets, relationships };
    });
  }

  function patchField(datasetId, fieldType, fieldId, patch) {
    patchDataset(datasetId, (dataset) => ({
      [fieldType]: dataset[fieldType].map((field) => (field.id === fieldId ? { ...field, ...patch } : field)),
    }));
  }

  function removeField(datasetId, fieldType, fieldId) {
    patchDataset(datasetId, (dataset) => ({
      [fieldType]: dataset[fieldType].filter((field) => field.id !== fieldId),
    }));
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const payload = buildSemanticModelSubmitPayload({
        mode: isCreateMode ? "create" : "edit",
        form,
      });
      const nextResource = isCreateMode ? await onCreate(payload) : await onUpdate(payload);
      setActionResult({
        label: isCreateMode ? "Semantic model created" : "Semantic model updated",
        payload: nextResource,
      });
      setActiveTab(isCreateMode ? "overview" : "definition");
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to save semantic model.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm(`Delete ${resource?.name || "this semantic model"}? This cannot be undone.`)) {
      return;
    }
    setError("");
    setSubmitting(true);
    try {
      await onDelete();
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to delete semantic model.");
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
      <article className="resource-workspace semantic-workspace">
        <header className="resource-workspace-head">
          <div>
            <p className="eyebrow">{isCreateMode ? "Create runtime semantic model" : "Opened semantic model"}</p>
            <h3>{title}</h3>
            <span>
              {isCreateMode
                ? "Build a runtime-managed business layer from governed datasets."
                : resource?.subtitle}
            </span>
          </div>
          {!isCreateMode && resource ? <ManagementPill mode={resource.management} /> : null}
        </header>

        <nav className="resource-workspace-tabs" aria-label="Semantic model workspace tabs">
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
        {datasetsError ? <div className="resource-error">{datasetsError}</div> : null}
        {detailLoading ? <div className="resource-loading">Loading runtime detail...</div> : null}

        {activeTab === "overview" && resource ? (
          <SemanticModelOverview resource={resource} form={form} />
        ) : null}

        {activeTab === "definition" ? (
          <section className="resource-editor-panel">
            {!canMutate && !isCreateMode ? <ReadOnlyNotice resource={resource} /> : null}
            {form.unsupported ? (
              <div className="resource-readonly-notice compact">
                <strong>Inspect-only model shape</strong>
                <span>{form.unsupportedReason || "This semantic model uses an advanced shape not supported by the guided editor."}</span>
              </div>
            ) : null}
            <form className="connector-form semantic-form" onSubmit={handleSubmit}>
              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Model identity</h4>
                    <p>Name the semantic model and describe the business area it supports.</p>
                  </div>
                </div>
                <div className="connector-field-grid">
                  <label className="connector-field">
                    <span>Name</span>
                    <input
                      type="text"
                      value={form.name}
                      disabled={!isCreateMode || guidedEditorDisabled}
                      onChange={(event) => patchForm({ name: event.target.value })}
                      placeholder="commerce_performance"
                    />
                    {!isCreateMode ? <small>Runtime-managed semantic model names are stable after creation.</small> : null}
                  </label>
                  <label className="connector-field connector-field--full">
                    <span>Description</span>
                    <input
                      type="text"
                      value={form.description}
                      disabled={guidedEditorDisabled}
                      onChange={(event) => patchForm({ description: event.target.value })}
                      placeholder="Governed commerce metrics and dimensions"
                    />
                  </label>
                </div>
                <details className="semantic-advanced-section">
                  <summary>SQL instructions for the analyst</summary>
                  <label className="connector-field connector-field--full">
                    <span>Analyst SQL guidance</span>
                    <textarea
                      value={form.sqlInstructions}
                      disabled={guidedEditorDisabled}
                      onChange={(event) => patchForm({ sqlInstructions: event.target.value })}
                      placeholder="Example: Always use net_revenue for revenue questions. Prefer order_date for time analysis. Do not use gross sales unless explicitly requested."
                    />
                    <small>These instructions are stored on the semantic model and included in the SQL analyst context.</small>
                  </label>
                </details>
              </section>

              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Source datasets</h4>
                    <p>Select runtime datasets to expose as semantic datasets. Columns are used to infer starter dimensions and measures.</p>
                  </div>
                  <span className="semantic-count-pill">{datasetsLoading ? "Loading" : `${datasets.length} available`}</span>
                </div>
                <label className="connector-field connector-field--full">
                  <span>Find datasets</span>
                  <input
                    type="search"
                    value={datasetSearch}
                    disabled={datasetsLoading}
                    onChange={(event) => setDatasetSearch(event.target.value)}
                    placeholder="Search by name, label, connector, or description"
                  />
                </label>
                <div className="semantic-dataset-list">
                  {filteredDatasets.length > 0 ? (
                    filteredDatasets.map((dataset) => {
                      const selected = selectedDatasetNames.has(dataset.name);
                      const loading = loadingDatasetNames.includes(dataset.name);
                      return (
                        <button
                          key={dataset.id || dataset.name}
                          type="button"
                          className={classNames("semantic-dataset-card", selected && "selected")}
                          disabled={guidedEditorDisabled || loading}
                          onClick={() => void handleToggleDataset(dataset)}
                        >
                          <strong>{dataset.label || dataset.name}</strong>
                          <span>{[dataset.connector, dataset.materialization_mode || dataset.materialization?.mode].filter(Boolean).join(" | ") || "Runtime dataset"}</span>
                          <small>{loading ? "Loading schema..." : selected ? "Included" : "Click to include"}</small>
                        </button>
                      );
                    })
                  ) : (
                    <div className="connector-empty-note">
                      {datasetsLoading ? "Loading datasets..." : "No datasets match this filter."}
                    </div>
                  )}
                </div>
              </section>

              <SemanticDatasetEditor
                disabled={guidedEditorDisabled}
                semanticDatasets={form.semanticDatasets}
                onPatchDataset={patchDataset}
                onPatchField={patchField}
                onRemoveField={removeField}
              />

              <MetricEditor
                disabled={guidedEditorDisabled}
                metrics={form.metrics}
                onAdd={() =>
                  setForm((current) => ({
                    ...current,
                    metrics: [...current.metrics, createEmptySemanticMetric()],
                  }))
                }
                onPatch={(metricId, patch) =>
                  setForm((current) => ({
                    ...current,
                    metrics: current.metrics.map((metric) =>
                      metric.id === metricId ? { ...metric, ...patch } : metric,
                    ),
                  }))
                }
                onRemove={(metricId) =>
                  setForm((current) => ({
                    ...current,
                    metrics: current.metrics.filter((metric) => metric.id !== metricId),
                  }))
                }
              />

              <RelationshipEditor
                disabled={guidedEditorDisabled}
                relationships={form.relationships}
                datasetOptions={relationshipDatasetOptions}
                semanticDatasets={form.semanticDatasets}
                onAdd={() =>
                  setForm((current) => ({
                    ...current,
                    relationships: [
                      ...current.relationships,
                      createEmptySemanticRelationship(current.semanticDatasets.map((dataset) => dataset.semanticKey)),
                    ],
                  }))
                }
                onPatch={(relationshipId, patch) =>
                  setForm((current) => ({
                    ...current,
                    relationships: current.relationships.map((relationship) =>
                      relationship.id === relationshipId ? { ...relationship, ...patch } : relationship,
                    ),
                  }))
                }
                onRemove={(relationshipId) =>
                  setForm((current) => ({
                    ...current,
                    relationships: current.relationships.filter((relationship) => relationship.id !== relationshipId),
                  }))
                }
              />

              <DefinitionSummary form={form} />

              <div className="semantic-builder-footer">
                <div className="inline-notes">
                  <span>{stats.datasets} semantic datasets</span>
                  <span>{stats.dimensions} dimensions</span>
                  <span>{stats.measures} measures</span>
                  <span>{stats.metrics} metrics</span>
                  <span>{stats.relationships} relationships</span>
                </div>
                <div className="resource-editor-actions">
                  <button type="submit" disabled={guidedEditorDisabled || submitting}>
                    {submitting ? "Saving..." : isCreateMode ? "Create semantic model" : "Save semantic model"}
                  </button>
                  {!isCreateMode && canDelete ? (
                    <button className="danger" type="button" disabled={submitting} onClick={handleDelete}>
                      Delete
                    </button>
                  ) : null}
                </div>
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

function SemanticDatasetEditor({ disabled, semanticDatasets, onPatchDataset, onPatchField, onRemoveField }) {
  return (
    <section className="connector-form-section">
      <div className="connector-form-section-head">
        <div>
          <h4>Semantic datasets</h4>
          <p>Edit semantic keys, relation names, dimensions, and measures for selected datasets.</p>
        </div>
      </div>
      {semanticDatasets.length > 0 ? (
        <div className="semantic-dataset-editor-list">
          {semanticDatasets.map((dataset) => (
            <section key={dataset.id} className="semantic-dataset-editor-card">
              <div className="connector-form-section-head">
                <div>
                  <h4>{dataset.semanticKey}</h4>
                  <p>{dataset.sourceDatasetLabel || dataset.sourceDatasetName}</p>
                </div>
                <button
                  type="button"
                  disabled={disabled}
                  onClick={() => onPatchDataset(dataset.id, { dimensions: [...dataset.dimensions, createEmptySemanticDimension()] })}
                >
                  Add dimension
                </button>
              </div>
              <div className="connector-field-grid">
                <label className="connector-field">
                  <span>Semantic key</span>
                  <input
                    type="text"
                    value={dataset.semanticKey}
                    disabled={disabled}
                    onChange={(event) =>
                      onPatchDataset(dataset.id, {
                        semanticKey: sanitizeSemanticKey(event.target.value) || dataset.semanticKey,
                      })
                    }
                  />
                </label>
                <label className="connector-field">
                  <span>Relation name</span>
                  <input
                    type="text"
                    value={dataset.relationName}
                    disabled={disabled}
                    onChange={(event) => onPatchDataset(dataset.id, { relationName: event.target.value })}
                  />
                </label>
                <label className="connector-field connector-field--full">
                  <span>Description</span>
                  <input
                    type="text"
                    value={dataset.description}
                    disabled={disabled}
                    onChange={(event) => onPatchDataset(dataset.id, { description: event.target.value })}
                  />
                </label>
              </div>

              <FieldCollection
                title="Dimensions"
                emptyLabel="No dimensions yet."
                fields={dataset.dimensions}
                typeOptions={SEMANTIC_DIMENSION_TYPES}
                disabled={disabled}
                showPrimaryKey
                onAdd={() => onPatchDataset(dataset.id, { dimensions: [...dataset.dimensions, createEmptySemanticDimension()] })}
                onPatch={(fieldId, patch) => onPatchField(dataset.id, "dimensions", fieldId, patch)}
                onRemove={(fieldId) => onRemoveField(dataset.id, "dimensions", fieldId)}
              />
              <FieldCollection
                title="Measures"
                emptyLabel="No measures yet."
                fields={dataset.measures}
                typeOptions={SEMANTIC_MEASURE_TYPES}
                disabled={disabled}
                showAggregation
                onAdd={() => onPatchDataset(dataset.id, { measures: [...dataset.measures, createEmptySemanticMeasure()] })}
                onPatch={(fieldId, patch) => onPatchField(dataset.id, "measures", fieldId, patch)}
                onRemove={(fieldId) => onRemoveField(dataset.id, "measures", fieldId)}
              />
            </section>
          ))}
        </div>
      ) : (
        <div className="connector-empty-note">Select at least one source dataset to start building the semantic model.</div>
      )}
    </section>
  );
}

function FieldCollection({ title, emptyLabel, fields, typeOptions, disabled, showPrimaryKey, showAggregation, onAdd, onPatch, onRemove }) {
  return (
    <section className="semantic-field-collection">
      <div className="connector-form-section-head">
        <div>
          <h4>{title}</h4>
          <p>{fields.length} configured</p>
        </div>
        <button type="button" disabled={disabled} onClick={onAdd}>Add {title.toLowerCase().slice(0, -1)}</button>
      </div>
      {fields.length > 0 ? (
        <div className="semantic-field-list">
          {fields.map((field) => (
            <SemanticFieldRow
              key={field.id}
              field={field}
              typeOptions={typeOptions}
              disabled={disabled}
              showPrimaryKey={showPrimaryKey}
              showAggregation={showAggregation}
              onPatch={(patch) => onPatch(field.id, patch)}
              onRemove={() => onRemove(field.id)}
            />
          ))}
        </div>
      ) : (
        <div className="connector-empty-note">{emptyLabel}</div>
      )}
    </section>
  );
}

function SemanticFieldRow({ field, typeOptions, disabled, showPrimaryKey, showAggregation, onPatch, onRemove }) {
  return (
    <div className="semantic-field-row">
      <label className="connector-field">
        <span>Name</span>
        <input
          type="text"
          value={field.name}
          disabled={disabled}
          onChange={(event) => onPatch({ name: event.target.value })}
          placeholder="field_name"
        />
      </label>
      <label className="connector-field">
        <span>Expression</span>
        <input
          type="text"
          value={field.expression}
          disabled={disabled}
          onChange={(event) => onPatch({ expression: event.target.value })}
          placeholder="source_column"
        />
      </label>
      <label className="connector-field">
        <span>Type</span>
        <select value={field.type} disabled={disabled} onChange={(event) => onPatch({ type: event.target.value })}>
          {typeOptions.map((option) => <option key={option} value={option}>{option}</option>)}
        </select>
      </label>
      {showAggregation ? (
        <label className="connector-field">
          <span>Aggregation</span>
          <select
            value={field.aggregation || "sum"}
            disabled={disabled}
            onChange={(event) => onPatch({ aggregation: event.target.value })}
          >
            {SEMANTIC_MEASURE_AGGREGATIONS.map((option) => <option key={option} value={option}>{option}</option>)}
          </select>
        </label>
      ) : null}
      {showPrimaryKey ? (
        <label className="dataset-checkbox-field semantic-checkbox-field">
          <input
            type="checkbox"
            checked={Boolean(field.primaryKey)}
            disabled={disabled}
            onChange={(event) => onPatch({ primaryKey: event.target.checked })}
          />
          <span>Primary key</span>
        </label>
      ) : null}
      <button className="semantic-remove-button" type="button" disabled={disabled} onClick={onRemove}>Remove</button>
    </div>
  );
}

function MetricEditor({ disabled, metrics, onAdd, onPatch, onRemove }) {
  return (
    <section className="connector-form-section">
      <div className="connector-form-section-head">
        <div>
          <h4>Metrics</h4>
          <p>Define reusable semantic metrics that the analyst can select directly in semantic SQL.</p>
        </div>
        <button type="button" disabled={disabled} onClick={onAdd}>Add metric</button>
      </div>
      {metrics.length > 0 ? (
        <div className="semantic-field-list">
          {metrics.map((metric) => (
            <MetricRow
              key={metric.id}
              metric={metric}
              disabled={disabled}
              onPatch={(patch) => onPatch(metric.id, patch)}
              onRemove={() => onRemove(metric.id)}
            />
          ))}
        </div>
      ) : (
        <div className="connector-empty-note">
          Add metrics for reusable business calculations, for example conversion_rate or average_order_value.
        </div>
      )}
    </section>
  );
}

function MetricRow({ metric, disabled, onPatch, onRemove }) {
  return (
    <div className="semantic-metric-row">
      <label className="connector-field">
        <span>Name</span>
        <input
          type="text"
          value={metric.name}
          disabled={disabled}
          onChange={(event) => onPatch({ name: event.target.value })}
          placeholder="average_order_value"
        />
      </label>
      <label className="connector-field connector-field--full">
        <span>Expression</span>
        <textarea
          value={metric.expression}
          disabled={disabled}
          onChange={(event) => onPatch({ expression: event.target.value })}
          spellCheck="false"
          placeholder="SUM(orders.net_revenue) / NULLIF(COUNT(orders.order_id), 0)"
        />
      </label>
      <label className="connector-field connector-field--full">
        <span>Description</span>
        <input
          type="text"
          value={metric.description}
          disabled={disabled}
          onChange={(event) => onPatch({ description: event.target.value })}
          placeholder="Average value per order"
        />
      </label>
      <button className="semantic-remove-button" type="button" disabled={disabled} onClick={onRemove}>Remove</button>
    </div>
  );
}

function RelationshipEditor({ disabled, relationships, datasetOptions, semanticDatasets, onAdd, onPatch, onRemove }) {
  return (
    <section className="connector-form-section">
      <div className="connector-form-section-head">
        <div>
          <h4>Relationships</h4>
          <p>Define joins for multi-dataset semantic questions.</p>
        </div>
        <button type="button" disabled={disabled || semanticDatasets.length < 2} onClick={onAdd}>Add relationship</button>
      </div>
      {relationships.length > 0 ? (
        <div className="semantic-field-list">
          {relationships.map((relationship) => (
            <SemanticRelationshipRow
              key={relationship.id}
              relationship={relationship}
              datasetOptions={datasetOptions}
              disabled={disabled}
              onPatch={(patch) => onPatch(relationship.id, patch)}
              onRemove={() => onRemove(relationship.id)}
            />
          ))}
        </div>
      ) : (
        <div className="connector-empty-note">Add relationships when the model needs guided joins across datasets.</div>
      )}
    </section>
  );
}

function SemanticRelationshipRow({ relationship, datasetOptions, disabled, onPatch, onRemove }) {
  const sourceDataset = datasetOptions.find((item) => item.value === relationship.sourceDataset);
  const targetDataset = datasetOptions.find((item) => item.value === relationship.targetDataset);
  return (
    <div className="semantic-relationship-row">
      <label className="connector-field">
        <span>Name</span>
        <input
          type="text"
          value={relationship.name}
          disabled={disabled}
          onChange={(event) => onPatch({ name: event.target.value })}
          placeholder="orders_to_customers"
        />
      </label>
      <label className="connector-field">
        <span>Type</span>
        <select value={relationship.type} disabled={disabled} onChange={(event) => onPatch({ type: event.target.value })}>
          {SEMANTIC_RELATIONSHIP_TYPES.map((option) => <option key={option} value={option}>{option}</option>)}
        </select>
      </label>
      <label className="connector-field">
        <span>Source dataset</span>
        <select
          value={relationship.sourceDataset}
          disabled={disabled}
          onChange={(event) => onPatch({ sourceDataset: event.target.value, sourceField: "" })}
        >
          <option value="">Select dataset</option>
          {datasetOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
        </select>
      </label>
      <label className="connector-field">
        <span>Source field</span>
        <select
          value={relationship.sourceField}
          disabled={disabled || !sourceDataset}
          onChange={(event) => onPatch({ sourceField: event.target.value })}
        >
          <option value="">Select field</option>
          {(sourceDataset?.fields || []).map((fieldName) => (
            <option key={`${relationship.id}-${fieldName}-source`} value={fieldName}>{fieldName}</option>
          ))}
        </select>
      </label>
      <label className="connector-field">
        <span>Target dataset</span>
        <select
          value={relationship.targetDataset}
          disabled={disabled}
          onChange={(event) => onPatch({ targetDataset: event.target.value, targetField: "" })}
        >
          <option value="">Select dataset</option>
          {datasetOptions.map((option) => <option key={`${option.value}-target`} value={option.value}>{option.label}</option>)}
        </select>
      </label>
      <label className="connector-field">
        <span>Target field</span>
        <select
          value={relationship.targetField}
          disabled={disabled || !targetDataset}
          onChange={(event) => onPatch({ targetField: event.target.value })}
        >
          <option value="">Select field</option>
          {(targetDataset?.fields || []).map((fieldName) => (
            <option key={`${relationship.id}-${fieldName}-target`} value={fieldName}>{fieldName}</option>
          ))}
        </select>
      </label>
      <button className="semantic-remove-button" type="button" disabled={disabled} onClick={onRemove}>Remove</button>
    </div>
  );
}

function DefinitionSummary({ form }) {
  let definition = null;
  try {
    definition = form.unsupported ? null : buildSemanticModelDefinition(form);
  } catch {
    definition = null;
  }
  const datasets = definition?.datasets || {};
  const metrics = definition?.metrics || {};
  return (
    <section className="connector-form-section">
      <div className="connector-form-section-head">
        <div>
          <h4>Generated definition summary</h4>
          <p>Readable summary of the model that will be sent to the runtime.</p>
        </div>
      </div>
      {Object.keys(datasets).length > 0 ? (
        <div className="semantic-summary-grid">
          {Object.entries(datasets).map(([key, dataset]) => (
            <article key={key} className="semantic-summary-card">
              <strong>{key}</strong>
              <span>{dataset.relation_name}</span>
              <div className="tag-list">
                <span>{(dataset.dimensions || []).length} dimensions</span>
                <span>{(dataset.measures || []).length} measures</span>
              </div>
            </article>
          ))}
        </div>
      ) : (
        <div className="connector-empty-note">Select datasets and complete model identity to see the generated summary.</div>
      )}
      {Object.keys(metrics).length > 0 ? (
        <div className="semantic-relationship-summary">
          {Object.entries(metrics).map(([metricName, metric]) => (
            <span key={metricName}>
              {metricName}: {metric.expression}
            </span>
          ))}
        </div>
      ) : null}
      {(definition?.relationships || []).length > 0 ? (
        <div className="semantic-relationship-summary">
          {definition.relationships.map((relationship) => (
            <span key={relationship.name}>
              {relationship.name}: {relationship.source_dataset}.{relationship.source_field} to {relationship.target_dataset}.{relationship.target_field}
            </span>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function SemanticModelOverview({ resource, form }) {
  const raw = resource.rawPayload || {};
  const stats = semanticModelStats(form);
  return (
    <div className="config-resource-detail">
      <div className="resource-meta-grid">
        <div><span>Status</span><strong>{resource.status}</strong></div>
        <div><span>Default</span><strong>{raw.default ? "Yes" : "No"}</strong></div>
        <div><span>Datasets</span><strong>{stats.datasets || raw.dataset_count || 0}</strong></div>
      </div>

      <div className="resource-state-grid">
        <ResourceSection title="Runtime state" rows={resource.runtimeState} />
        <ResourceSection title="Config definition" rows={resource.configDefinition} />
      </div>

      <div className="resource-detail-block">
        <h4>Semantic structure</h4>
        <div className="semantic-summary-grid">
          <article className="semantic-summary-card">
            <strong>{stats.dimensions || raw.dimension_count || 0}</strong>
            <span>Dimensions</span>
          </article>
          <article className="semantic-summary-card">
            <strong>{stats.measures || raw.measure_count || 0}</strong>
            <span>Measures</span>
          </article>
          <article className="semantic-summary-card">
            <strong>{stats.metrics}</strong>
            <span>Metrics</span>
          </article>
          <article className="semantic-summary-card">
            <strong>{stats.relationships}</strong>
            <span>Relationships</span>
          </article>
        </div>
      </div>

      {form.sqlInstructions ? (
        <div className="resource-detail-block">
          <h4>Analyst SQL guidance</h4>
          <div className="dataset-source-summary">
            <span>{form.sqlInstructions}</span>
          </div>
        </div>
      ) : null}

      <div className="resource-detail-block">
        <h4>Datasets</h4>
        {form.semanticDatasets.length > 0 ? (
          <div className="semantic-summary-grid">
            {form.semanticDatasets.map((dataset) => (
              <article key={dataset.id} className="semantic-summary-card">
                <strong>{dataset.semanticKey}</strong>
                <span>{dataset.sourceDatasetLabel || dataset.sourceDatasetName}</span>
                <div className="tag-list">
                  <span>{dataset.dimensions.length} dimensions</span>
                  <span>{dataset.measures.length} measures</span>
                </div>
              </article>
            ))}
          </div>
        ) : (
          <div className="connector-empty-note">No dataset structure returned.</div>
        )}
      </div>

      {form.unsupported ? (
        <div className="resource-detail-block">
          <h4>Advanced shape</h4>
          <div className="resource-action-payload">
            <DynamicFieldValue value={raw.content_json || raw.model || {}} />
          </div>
        </div>
      ) : null}
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
      <strong>Config-managed semantic model</strong>
      <span>This semantic model is loaded from runtime configuration and is inspectable here, but edits and deletes are disabled.</span>
    </div>
  );
}

function ActionResult({ result }) {
  if (!result) {
    return null;
  }
  return (
    <section className="resource-action-result">
      <h4>{result.label}</h4>
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
    name: raw.name,
    description: raw.description,
    content_json: raw.content_json,
    model: raw.model,
    dataset_names: raw.dataset_names,
  });
}
