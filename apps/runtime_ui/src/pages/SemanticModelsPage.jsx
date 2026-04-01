import { useDeferredValue, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import {
  DetailList,
  ManagementBadge,
  ManagementModeNotice,
  PageEmpty,
  Panel,
  SectionTabs,
} from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import {
  createSemanticModel,
  deleteSemanticModel,
  fetchDatasets,
  fetchSemanticModel,
  fetchSemanticModels,
  updateSemanticModel,
} from "../lib/runtimeApi";
import { formatList, formatValue, getErrorMessage } from "../lib/format";
import {
  buildSemanticModelDraft,
  describeManagementMode,
  parseJsonObjectInput,
} from "../lib/managedResources";
import {
  buildItemRef,
  extractSemanticDatasets,
  extractSemanticFields,
  renderJson,
  resolveItemByRef,
} from "../lib/runtimeUi";

function buildSemanticModelFormState() {
  return {
    name: "",
    description: "",
    datasets: [],
    modelText: buildSemanticModelDraft({ name: "", description: "", datasets: [] }),
    modelDirty: false,
  };
}

function buildSemanticModelEditFormState(detail) {
  return {
    description: detail?.description || "",
    datasets: Array.isArray(detail?.dataset_names) ? detail.dataset_names : [],
    modelText: JSON.stringify(detail?.content_json || {}, null, 2),
  };
}

export function SemanticModelsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [fieldSearch, setFieldSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const deferredSearch = useDeferredValue(search);
  const deferredFieldSearch = useDeferredValue(fieldSearch);
  const { data, loading, error, reload, setData } = useAsyncData(fetchSemanticModels);
  const { data: datasetPayload } = useAsyncData(fetchDatasets);
  const models = Array.isArray(data?.items) ? data.items : [];
  const datasets = Array.isArray(datasetPayload?.items) ? datasetPayload.items : [];
  const selected = resolveItemByRef(models, params.id);
  const filteredModels = models.filter((item) => {
    const haystack = [
      item.name,
      item.description,
      item.management_mode,
      ...(Array.isArray(item.dataset_names) ? item.dataset_names : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const semanticDatasets = extractSemanticDatasets(detail);
  const semanticFields = extractSemanticFields(detail);
  const filteredSemanticDatasets = semanticDatasets
    .map((dataset) => {
      const searchTerm = String(deferredFieldSearch || "").trim().toLowerCase();
      if (!searchTerm) {
        return dataset;
      }
      return {
        ...dataset,
        dimensions: dataset.dimensions.filter((item) =>
          String(item?.name || "").toLowerCase().includes(searchTerm),
        ),
        measures: dataset.measures.filter((item) =>
          String(item?.name || "").toLowerCase().includes(searchTerm),
        ),
      };
    })
      .filter(
        (dataset) =>
          !deferredFieldSearch ||
        String(dataset.name)
          .toLowerCase()
          .includes(String(deferredFieldSearch).toLowerCase()) ||
        dataset.dimensions.length > 0 ||
        dataset.measures.length > 0,
    );

  function syncSemanticModelDraft(nextValues) {
    setCreateForm((current) => {
      const nextState =
        typeof nextValues === "function" ? nextValues(current) : { ...current, ...nextValues };
      if (!nextState.modelDirty) {
        nextState.modelText = buildSemanticModelDraft({
          name: nextState.name,
          description: nextState.description,
          datasets: nextState.datasets,
        });
      }
      return nextState;
    });
  }

  function toggleCreateDataset(datasetName) {
    syncSemanticModelDraft((current) => ({
      ...current,
      datasets: current.datasets.includes(datasetName)
        ? current.datasets.filter((item) => item !== datasetName)
        : [...current.datasets, datasetName],
    }));
  }

  function resetCreateForm() {
    setCreateForm(buildSemanticModelFormState());
    setCreateError("");
  }

  async function handleCreateSemanticModel(event) {
    event.preventDefault();
    setCreateSubmitting(true);
    setCreateError("");
    setCreateSuccess("");

    try {
      const name = String(createForm.name || "").trim();
      if (!name) {
        throw new Error("Semantic model name is required.");
      }
      if (createForm.datasets.length === 0) {
        throw new Error("Select at least one dataset for the semantic model.");
      }

      const payload = {
        name,
        datasets: createForm.datasets,
        model: parseJsonObjectInput(createForm.modelText, "Semantic model JSON", {}),
      };
      const description = String(createForm.description || "").trim();
      if (description) {
        payload.description = description;
      }

      const created = await createSemanticModel(payload);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = [
          created,
          ...items.filter(
            (item) => String(item?.id || item?.name) !== String(created?.id || created?.name),
          ),
        ];
        return {
          items: nextItems,
          total: nextItems.length,
        };
      });
      setDetail(created);
      setCreateSuccess(`${created.name} is available as a runtime_managed semantic model.`);
      setShowCreate(false);
      resetCreateForm();
      navigate(`/semantic-models/${buildItemRef(created)}`);
      void reload();
    } catch (caughtError) {
      setCreateError(getErrorMessage(caughtError));
    } finally {
      setCreateSubmitting(false);
    }
  }

  const [showCreate, setShowCreate] = useState(false);
  const [createForm, setCreateForm] = useState(buildSemanticModelFormState);
  const [createSubmitting, setCreateSubmitting] = useState(false);
  const [createError, setCreateError] = useState("");
  const [createSuccess, setCreateSuccess] = useState("");
  const [showEdit, setShowEdit] = useState(false);
  const [editForm, setEditForm] = useState(buildSemanticModelEditFormState(null));
  const [editSubmitting, setEditSubmitting] = useState(false);
  const [editError, setEditError] = useState("");
  const [editSuccess, setEditSuccess] = useState("");
  const [deleteSubmitting, setDeleteSubmitting] = useState(false);
  const [deleteError, setDeleteError] = useState("");

  function resetEditForm(nextDetail = detail) {
    setEditForm(buildSemanticModelEditFormState(nextDetail));
    setEditError("");
  }

  function toggleEditDataset(datasetName) {
    setEditForm((current) => ({
      ...current,
      datasets: current.datasets.includes(datasetName)
        ? current.datasets.filter((item) => item !== datasetName)
        : [...current.datasets, datasetName],
    }));
  }

  function beginEditSemanticModel() {
    resetEditForm(detail);
    setShowEdit(true);
    setShowCreate(false);
    setEditSuccess("");
    setDeleteError("");
  }

  async function handleUpdateSemanticModel(event) {
    event.preventDefault();
    if (!detail) {
      return;
    }
    setEditSubmitting(true);
    setEditError("");
    setEditSuccess("");
    setDeleteError("");
    try {
      if (editForm.datasets.length === 0) {
        throw new Error("Select at least one dataset for the semantic model.");
      }
      const updated = await updateSemanticModel(String(detail.id || detail.name), {
        description: String(editForm.description || "").trim() || null,
        datasets: editForm.datasets,
        model: parseJsonObjectInput(editForm.modelText, "Semantic model JSON", {}),
      });
      setDetail(updated);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = items.map((item) =>
          String(item?.id || item?.name) === String(updated?.id || updated?.name)
            ? updated
            : item,
        );
        return { items: nextItems, total: nextItems.length };
      });
      setShowEdit(false);
      setEditSuccess(`${updated.name} was updated.`);
      void reload();
    } catch (caughtError) {
      setEditError(getErrorMessage(caughtError));
    } finally {
      setEditSubmitting(false);
    }
  }

  async function handleDeleteSemanticModel() {
    if (!detail?.id || deleteSubmitting) {
      return;
    }
    const confirmed = window.confirm(
      `Delete runtime-managed semantic model '${detail.name}'? This cannot be undone.`,
    );
    if (!confirmed) {
      return;
    }
    setDeleteSubmitting(true);
    setDeleteError("");
    setEditSuccess("");
    try {
      await deleteSemanticModel(String(detail.id));
      setDetail(null);
      setShowEdit(false);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = items.filter(
          (item) => String(item?.id || item?.name) !== String(detail?.id || detail?.name),
        );
        return { items: nextItems, total: nextItems.length };
      });
      navigate("/semantic-models");
      void reload();
    } catch (caughtError) {
      setDeleteError(getErrorMessage(caughtError));
    } finally {
      setDeleteSubmitting(false);
    }
  }

  useEffect(() => {
    let cancelled = false;

    async function loadDetail() {
      if (!selected) {
        setDetail(null);
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchSemanticModel(String(selected.id || selected.name));
        if (!cancelled) {
          setDetail(payload);
        }
      } catch (caughtError) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadDetail();

    return () => {
      cancelled = true;
    };
  }, [selected?.id, selected?.name]);

  return (
    <div className="page-stack">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Semantic Models</p>
            <h2>{selected?.name || "Semantic inventory"}</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(models.length)} models</span>
              <span className="chip">{formatValue(detail?.dataset_count || semanticDatasets.length)} datasets</span>
              <span className="chip">{formatValue(detail?.dimension_count || semanticFields.dimensions.length)} dimensions</span>
              <span className="chip">{formatValue(detail?.measure_count || semanticFields.measures.length)} measures</span>
            </div>
          </div>
          <div className="product-command-bar-actions">
            <button
              className="primary-button"
              type="button"
              onClick={() => {
                setShowCreate((current) => !current);
                setCreateError("");
                setCreateSuccess("");
              }}
            >
              {showCreate ? "Close create flow" : "Create runtime-managed semantic model"}
            </button>
          </div>
        </div>
      </section>

      <section className="product-search-bar">
        <input
          className="text-input search-input"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Filter semantic models by name or dataset"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh semantic models"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Semantic models" className="list-panel compact-panel">
          <ManagementModeNotice
            mode={selected?.management_mode || "config_managed"}
            resourceLabel="Semantic model ownership"
          />
          {filteredModels.length > 0 ? (
            <div className="stack-list">
              {filteredModels.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/semantic-models/${buildItemRef(item)}`}
                >
                  <div className="list-card-topline">
                    <strong>{item.name}</strong>
                    <ManagementBadge mode={item.management_mode} />
                  </div>
                  <span>
                    {[
                      `${item.dataset_count || 0} datasets`,
                      `${item.measure_count || 0} measures`,
                      item.default ? "default" : null,
                    ]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                  <small>{describeManagementMode(item.management_mode)}</small>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty
              title="No semantic models"
              message="This runtime does not expose semantic model metadata yet."
            />
          )}
        </Panel>

        <div className="detail-stack">
          {createSuccess ? (
            <div className="callout success">
              <strong>Semantic model created</strong>
              <span>{createSuccess}</span>
            </div>
          ) : null}
          {editSuccess ? (
            <div className="callout success">
              <strong>Semantic model updated</strong>
              <span>{editSuccess}</span>
            </div>
          ) : null}

          {showCreate ? (
            <Panel
              title="Create runtime-managed semantic model"
              eyebrow="Create"
              actions={
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => {
                    setShowCreate(false);
                    resetCreateForm();
                  }}
                  disabled={createSubmitting}
                >
                  Cancel
                </button>
              }
            >
              <ManagementModeNotice mode="runtime_managed" resourceLabel="New semantic models" />
              <form className="form-grid" onSubmit={handleCreateSemanticModel}>
                <label className="field">
                  <span>Name</span>
                  <input
                    className="text-input"
                    type="text"
                    value={createForm.name}
                    onChange={(event) =>
                      syncSemanticModelDraft({ name: event.target.value })
                    }
                    placeholder="runtime_orders_model"
                    disabled={createSubmitting}
                  />
                </label>

                <label className="field">
                  <span>Description</span>
                  <input
                    className="text-input"
                    type="text"
                    value={createForm.description}
                    onChange={(event) =>
                      syncSemanticModelDraft({ description: event.target.value })
                    }
                    placeholder="Short semantic model description"
                    disabled={createSubmitting}
                  />
                </label>

                <div className="field field-full">
                  <div className="callout">
                    <strong>Dataset bindings: {createForm.datasets.length}</strong>
                    <span>
                      Choose the runtime datasets this semantic model should bind to, then refine the generated JSON contract.
                    </span>
                  </div>
                </div>

                <div className="field field-full">
                  <span>Datasets</span>
                  {datasets.length > 0 ? (
                    <div className="stack-list">
                      {datasets.map((dataset) => {
                        const checked = createForm.datasets.includes(dataset.name);
                        return (
                          <div key={dataset.id || dataset.name} className="list-card static">
                            <div className="list-card-topline">
                              <strong>{dataset.label || dataset.name}</strong>
                              <ManagementBadge mode={dataset.management_mode} />
                            </div>
                            <span>
                              {[dataset.connector, dataset.materialization_mode]
                                .filter(Boolean)
                                .join(" | ")}
                            </span>
                            <small>{describeManagementMode(dataset.management_mode)}</small>
                            <label className="checkbox-field">
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={() => toggleCreateDataset(dataset.name)}
                                disabled={createSubmitting}
                              />
                              <span>{checked ? "Included" : "Include dataset"}</span>
                            </label>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <PageEmpty
                      title="No datasets available"
                      message="Create at least one dataset before creating a semantic model."
                    />
                  )}
                </div>

                <label className="field field-full">
                  <span>Semantic model JSON</span>
                  <textarea
                    className="textarea-input"
                    value={createForm.modelText}
                    onChange={(event) =>
                      setCreateForm((current) => ({
                        ...current,
                        modelText: event.target.value,
                        modelDirty: true,
                      }))
                    }
                    disabled={createSubmitting}
                  />
                  <small className="field-hint">
                    The draft is scaffolded from the selected datasets. Edit it directly to define dimensions, measures, and any additional semantic metadata.
                  </small>
                </label>

                <div className="page-actions field-full">
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() =>
                      setCreateForm((current) => ({
                        ...current,
                        modelText: buildSemanticModelDraft({
                          name: current.name,
                          description: current.description,
                          datasets: current.datasets,
                        }),
                        modelDirty: false,
                      }))
                    }
                    disabled={createSubmitting}
                  >
                    Reset draft from selected datasets
                  </button>
                </div>

                {createError ? <div className="error-banner field-full">{createError}</div> : null}
                <div className="page-actions field-full">
                  <button
                    className="primary-button"
                    type="submit"
                    disabled={createSubmitting || datasets.length === 0}
                  >
                    {createSubmitting ? "Creating semantic model..." : "Create semantic model"}
                  </button>
                </div>
              </form>
            </Panel>
          ) : null}

          {showEdit && detail ? (
            <Panel
              title={`Edit ${detail.name}`}
              eyebrow="Edit"
              actions={
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => {
                    setShowEdit(false);
                    resetEditForm(detail);
                  }}
                  disabled={editSubmitting}
                >
                  Cancel
                </button>
              }
            >
              <ManagementModeNotice mode="runtime_managed" resourceLabel="Editable semantic model" />
              <form className="form-grid" onSubmit={handleUpdateSemanticModel}>
                <label className="field">
                  <span>Name</span>
                  <input className="text-input" type="text" value={detail.name} disabled />
                </label>

                <label className="field">
                  <span>Description</span>
                  <input
                    className="text-input"
                    type="text"
                    value={editForm.description}
                    onChange={(event) =>
                      setEditForm((current) => ({ ...current, description: event.target.value }))
                    }
                    disabled={editSubmitting}
                  />
                </label>

                <div className="field field-full">
                  <span>Datasets</span>
                  <div className="stack-list">
                    {datasets.map((dataset) => {
                      const checked = editForm.datasets.includes(dataset.name);
                      return (
                        <div key={`edit-${dataset.id || dataset.name}`} className="list-card static">
                          <div className="list-card-topline">
                            <strong>{dataset.label || dataset.name}</strong>
                            <ManagementBadge mode={dataset.management_mode} />
                          </div>
                          <span>
                            {[dataset.connector, dataset.materialization_mode]
                              .filter(Boolean)
                              .join(" | ")}
                          </span>
                          <label className="checkbox-field">
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={() => toggleEditDataset(dataset.name)}
                              disabled={editSubmitting}
                            />
                            <span>{checked ? "Included" : "Include dataset"}</span>
                          </label>
                        </div>
                      );
                    })}
                  </div>
                </div>

                <label className="field field-full">
                  <span>Semantic model JSON</span>
                  <textarea
                    className="textarea-input"
                    value={editForm.modelText}
                    onChange={(event) =>
                      setEditForm((current) => ({ ...current, modelText: event.target.value }))
                    }
                    disabled={editSubmitting}
                  />
                </label>

                {editError ? <div className="error-banner field-full">{editError}</div> : null}
                <div className="settings-form-actions field-full">
                  <button className="primary-button" type="submit" disabled={editSubmitting}>
                    {editSubmitting ? "Saving..." : "Save semantic model"}
                  </button>
                  <button
                    className="ghost-button danger-button"
                    type="button"
                    onClick={() => void handleDeleteSemanticModel()}
                    disabled={editSubmitting || deleteSubmitting}
                  >
                    {deleteSubmitting ? "Deleting..." : "Delete semantic model"}
                  </button>
                </div>
              </form>
            </Panel>
          ) : null}

          {selected ? (
            <>
              <Panel
                title={selected.name}
                className="compact-panel"
                actions={
                  <div className="panel-actions-inline">
                    <ManagementBadge mode={detail?.management_mode || selected.management_mode} />
                    {(detail?.management_mode || selected.management_mode) === "runtime_managed" ? (
                      <button className="ghost-button" type="button" onClick={beginEditSemanticModel}>
                        Edit
                      </button>
                    ) : null}
                  </div>
                }
              >
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {deleteError ? <div className="error-banner">{deleteError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading semantic model detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.default ? "Default runtime model" : "Secondary model"}</span>
                      <span>
                        {detail.dataset_count || semanticDatasets.length} semantic datasets
                      </span>
                      <span>
                        {detail.measure_count || semanticFields.measures.length} measures
                      </span>
                      <span>{describeManagementMode(detail.management_mode)}</span>
                    </div>
                    <DetailList
                      items={[
                        { label: "Description", value: formatValue(detail.description) },
                        { label: "Default", value: formatValue(detail.default) },
                        { label: "Datasets", value: formatList(detail.dataset_names) },
                        {
                          label: "Dimension count",
                          value: formatValue(detail.dimension_count),
                        },
                        { label: "Measure count", value: formatValue(detail.measure_count) },
                        {
                          label: "Management mode",
                          value: formatValue(detail.management_mode),
                        },
                      ]}
                    />
                    <div className="panel-actions-inline">
                      <button className="ghost-button" type="button" onClick={() => navigate("/dashboards")}>
                        Open Dashboard studio
                      </button>
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => navigate("/chat")}
                      >
                        Open chat
                      </button>
                    </div>
                  </>
                ) : (
                  <PageEmpty
                    title="No detail"
                    message="The runtime did not return semantic model detail."
                  />
                )}
              </Panel>

              <ManagementModeNotice
                mode={detail?.management_mode || selected.management_mode}
                resourceLabel={selected.name}
              />

              <section className="summary-grid">
                <Panel title="Dataset explorer" eyebrow="Model structure">
                  {semanticDatasets.length > 0 ? (
                    <div className="detail-card-grid">
                      {semanticDatasets.map((item) => (
                        <article key={item.name} className="detail-card">
                          <strong>{item.name}</strong>
                          <span>{item.relationName || "No explicit relation name"}</span>
                          <div className="tag-list">
                            <span className="tag">{item.dimensions.length} dimensions</span>
                            <span className="tag">{item.measures.length} measures</span>
                          </div>
                          <small>
                            {[
                              item.dimensions
                                .slice(0, 3)
                                .map((field) => field.name)
                                .join(", "),
                              item.measures
                                .slice(0, 3)
                                .map((field) => field.name)
                                .join(", "),
                            ]
                              .filter(Boolean)
                              .join(" | ")}
                          </small>
                        </article>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty
                      title="No semantic datasets"
                      message="This model did not expose semantic dataset groups."
                    />
                  )}
                </Panel>

                <Panel title="Field inventory" eyebrow="Dimensions and measures">
                  {semanticFields.dimensions.length > 0 ||
                  semanticFields.measures.length > 0 ? (
                    <div className="field-section-list">
                      <div className="field-group">
                        <div className="field-group-header">
                          <strong>Dimensions</strong>
                          <span>{semanticFields.dimensions.length}</span>
                        </div>
                        <div className="field-pill-list">
                          {semanticFields.dimensions.map((item) => (
                            <span key={item.value} className="field-pill static">
                              {item.label}
                            </span>
                          ))}
                        </div>
                      </div>
                      <div className="field-group">
                        <div className="field-group-header">
                          <strong>Measures</strong>
                          <span>{semanticFields.measures.length}</span>
                        </div>
                        <div className="field-pill-list">
                          {semanticFields.measures.map((item) => (
                            <span key={item.value} className="field-pill static">
                              {item.label}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <PageEmpty
                      title="No fields exposed"
                      message="This model did not expose dimensions or measures."
                    />
                  )}
                </Panel>
              </section>

              <Panel title="Semantic workspace" eyebrow="Inspect">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "datasets", label: "Datasets" },
                    { value: "fields", label: "Fields" },
                    { value: "yaml", label: "YAML" },
                    { value: "json", label: "JSON" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="detail-card-grid">
                    {semanticDatasets.map((item) => (
                      <article key={item.name} className="detail-card">
                        <strong>{item.name}</strong>
                        <span>{item.relationName || "No relation name provided"}</span>
                        <div className="tag-list">
                          <span className="tag">{item.dimensions.length} dimensions</span>
                          <span className="tag">{item.measures.length} measures</span>
                        </div>
                        <small>
                          {[
                            item.dimensions
                              .slice(0, 3)
                              .map((field) => field.name)
                              .join(", "),
                            item.measures
                              .slice(0, 3)
                              .map((field) => field.name)
                              .join(", "),
                          ]
                            .filter(Boolean)
                            .join(" | ")}
                        </small>
                      </article>
                    ))}
                  </div>
                ) : null}

                {activeTab === "datasets" ? (
                  filteredSemanticDatasets.length > 0 ? (
                    <div className="field-section-list">
                      {filteredSemanticDatasets.map((dataset) => (
                        <div key={dataset.name} className="field-group">
                          <div className="field-group-header">
                            <strong>{dataset.name}</strong>
                            <span>{dataset.relationName || "semantic dataset"}</span>
                          </div>
                          <div className="tag-list">
                            <span className="tag">{dataset.dimensions.length} dimensions</span>
                            <span className="tag">{dataset.measures.length} measures</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty
                      title="No semantic datasets"
                      message="This model did not expose semantic dataset groups."
                    />
                  )
                ) : null}

                {activeTab === "fields" ? (
                  <div className="page-stack">
                    <label className="field">
                      <span>Find fields</span>
                      <input
                        className="text-input"
                        type="search"
                        value={fieldSearch}
                        onChange={(event) => setFieldSearch(event.target.value)}
                        placeholder="Filter datasets, dimensions, or measures"
                      />
                    </label>
                    {filteredSemanticDatasets.length > 0 ? (
                      <div className="field-section-list">
                        {filteredSemanticDatasets.map((dataset) => (
                          <div key={`${dataset.name}-fields`} className="field-group">
                            <div className="field-group-header">
                              <strong>{dataset.name}</strong>
                              <span>{dataset.relationName || "semantic dataset"}</span>
                            </div>
                            <div className="field-pill-list">
                              {dataset.dimensions.map((item) => (
                                <span
                                  key={`${dataset.name}-${item.name}-dimension`}
                                  className="field-pill static"
                                >
                                  {item.name}
                                </span>
                              ))}
                              {dataset.measures.map((item) => (
                                <span
                                  key={`${dataset.name}-${item.name}-measure`}
                                  className="field-pill static"
                                >
                                  {item.name}
                                </span>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <PageEmpty title="No fields found" message="Adjust the filter or switch models." />
                    )}
                  </div>
                ) : null}

                {activeTab === "yaml" ? (
                  detail?.content_yaml ? (
                    <pre className="code-block">{detail.content_yaml}</pre>
                  ) : (
                    <PageEmpty
                      title="No YAML available"
                      message="This semantic model did not expose YAML content."
                    />
                  )
                ) : null}

                {activeTab === "json" ? (
                  detail?.content_json ? (
                    <pre className="code-block">{renderJson(detail.content_json)}</pre>
                  ) : (
                    <PageEmpty
                      title="No JSON payload"
                      message="This semantic model did not expose a JSON representation."
                    />
                  )
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Semantic model detail" eyebrow="Runtime">
              <PageEmpty
                title="No model selected"
                message="Pick a semantic model to inspect its runtime definition."
              />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}
