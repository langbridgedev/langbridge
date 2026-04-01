import { useDeferredValue, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import { ResultTable } from "../components/ResultTable";
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
  createDataset,
  deleteDataset,
  fetchConnectorResources,
  fetchConnectors,
  fetchDataset,
  fetchDatasets,
  previewDataset,
  updateDataset,
} from "../lib/runtimeApi";
import {
  formatDateTime,
  formatList,
  formatValue,
  getErrorMessage,
  splitCsv,
  toSqlAlias,
} from "../lib/format";
import {
  describeManagementMode,
  formatConnectorFamilyLabel,
  normalizeConnectorFamily,
} from "../lib/managedResources";
import {
  buildItemRef,
  countUniqueValues,
  downloadTextFile,
  normalizeTabularResult,
  resolveItemByRef,
  toCsvText,
} from "../lib/runtimeUi";

function buildDatasetFormState(defaultConnector = "") {
  return {
    name: "",
    description: "",
    connectorFamily: "",
    connector: defaultConnector,
    materializationMode: "live",
    sourceMode: "table",
    table: "",
    resource: "",
    sql: "",
    path: "",
    format: "csv",
    header: true,
    delimiter: ",",
    quote: '"',
    tags: "",
  };
}

function buildDatasetEditFormState(detail) {
  const relationIdentity =
    detail?.relation_identity && typeof detail.relation_identity === "object"
      ? detail.relation_identity
      : {};
  const fileConfig =
    detail?.file_config && typeof detail.file_config === "object" ? detail.file_config : {};
  const sourceMode = detail?.sync_resource
    ? "resource"
    : detail?.sql_text
      ? "sql"
      : detail?.storage_uri
        ? "file"
        : "table";
  const tableName =
    relationIdentity.qualified_name ||
    [relationIdentity.catalog_name, relationIdentity.schema_name, relationIdentity.table_name]
      .filter(Boolean)
      .join(".") ||
    detail?.table_name ||
    "";
  return {
    description: detail?.description || "",
    materializationMode: detail?.materialization_mode || "live",
    sourceMode,
    table: sourceMode === "table" ? tableName : "",
    resource: detail?.sync_resource || "",
    sql: detail?.sql_text || "",
    path: detail?.storage_uri || "",
    format: fileConfig.format || fileConfig.file_format || "csv",
    header: Boolean(fileConfig.header),
    delimiter: fileConfig.delimiter || ",",
    quote: fileConfig.quote || '"',
    tags: Array.isArray(detail?.tags) ? detail.tags.join(", ") : "",
  };
}

function toDatasetListItem(dataset) {
  return {
    id: dataset.id,
    name: dataset.name,
    label: dataset.label,
    description: dataset.description,
    connector: dataset.connector,
    semantic_model: dataset.semantic_model || null,
    materialization_mode: dataset.materialization_mode,
    status: dataset.status,
    sync_resource: dataset.sync_resource,
    last_sync_at: dataset.sync_state?.last_sync_at || null,
    management_mode: dataset.management_mode,
    managed: dataset.managed,
  };
}

const DATASET_CREATE_STEPS = [
  { value: "identity", label: "Identity" },
  { value: "binding", label: "Binding" },
  { value: "source", label: "Source" },
  { value: "review", label: "Review" },
];

const DATASET_SOURCE_MODE_OPTIONS = [
  {
    value: "table",
    label: "Live table",
    description: "Point at a connector-visible relation and query it directly.",
    requirement: "Connector required",
  },
  {
    value: "sql",
    label: "Live SQL",
    description: "Define the dataset as a runtime-managed SQL projection.",
    requirement: "Connector required",
  },
  {
    value: "file",
    label: "File upload",
    description: "Register a local file or uploaded asset with optional connector binding.",
    requirement: "Connector optional",
  },
  {
    value: "resource",
    label: "Synced resource",
    description: "Materialize a connector resource into managed runtime storage.",
    requirement: "Connector required",
  },
];

function isConnectorRequiredForCreate(form, resolvedSourceMode) {
  return form.materializationMode === "synced" || resolvedSourceMode === "table" || resolvedSourceMode === "sql";
}

function getCreateSourceValue(form, resolvedSourceMode) {
  if (resolvedSourceMode === "table") {
    return String(form.table || "").trim();
  }
  if (resolvedSourceMode === "sql") {
    return String(form.sql || "").trim();
  }
  if (resolvedSourceMode === "file") {
    return String(form.path || "").trim();
  }
  return String(form.resource || "").trim();
}

function describeCreateSource(form, resolvedSourceMode) {
  const sourceValue = getCreateSourceValue(form, resolvedSourceMode);
  if (!sourceValue) {
    return "Not configured yet";
  }
  if (resolvedSourceMode === "table") {
    return `Table: ${sourceValue}`;
  }
  if (resolvedSourceMode === "sql") {
    return `SQL query (${sourceValue.length} chars)`;
  }
  if (resolvedSourceMode === "file") {
    return `File: ${sourceValue}`;
  }
  return `Resource: ${sourceValue}`;
}

function getCreateStepState({
  createForm,
  createSourceMode,
  connectorRequired,
}) {
  const identityReady = Boolean(String(createForm.name || "").trim());
  const bindingReady = Boolean(createSourceMode) && (!connectorRequired || Boolean(createForm.connector));
  const sourceReady = Boolean(getCreateSourceValue(createForm, createSourceMode));
  const reviewReady = identityReady && bindingReady && sourceReady;

  return {
    identityReady,
    bindingReady,
    sourceReady,
    reviewReady,
  };
}

export function DatasetsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload, setData } = useAsyncData(fetchDatasets);
  const { data: connectorPayload } = useAsyncData(fetchConnectors);
  const datasets = Array.isArray(data?.items) ? data.items : [];
  const connectors = Array.isArray(connectorPayload?.items) ? connectorPayload.items : [];
  const selected = resolveItemByRef(datasets, params.id);
  const filteredDatasets = datasets.filter((item) => {
    const haystack = [
      item.name,
      item.label,
      item.description,
      item.connector,
      item.semantic_model,
      item.management_mode,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [showCreate, setShowCreate] = useState(false);
  const [createForm, setCreateForm] = useState(() => buildDatasetFormState(""));
  const [createStep, setCreateStep] = useState("identity");
  const [createSubmitting, setCreateSubmitting] = useState(false);
  const [createError, setCreateError] = useState("");
  const [createSuccess, setCreateSuccess] = useState("");
  const [showEdit, setShowEdit] = useState(false);
  const [editForm, setEditForm] = useState(() => buildDatasetEditFormState(null));
  const [editSubmitting, setEditSubmitting] = useState(false);
  const [editError, setEditError] = useState("");
  const [editSuccess, setEditSuccess] = useState("");
  const [deleteSubmitting, setDeleteSubmitting] = useState(false);
  const [deleteError, setDeleteError] = useState("");
  const [syncResources, setSyncResources] = useState({ items: [], loading: false, error: "" });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [preview, setPreview] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const [previewLimit, setPreviewLimit] = useState("25");
  const boundConnectorCount = countUniqueValues(datasets, (item) => item.connector);
  const boundSemanticModelCount = countUniqueValues(datasets, (item) => item.semantic_model);
  const schemaColumns = Array.isArray(detail?.columns) ? detail.columns : [];
  const nullableColumns = schemaColumns.filter((column) => column.nullable).length;
  const computedColumns = schemaColumns.filter((column) => column.is_computed).length;
  const policy = detail?.policy && typeof detail.policy === "object" ? detail.policy : null;
  const previewResult = preview ? normalizeTabularResult(preview) : null;
  const connectorFamilyOptions = [];
  const seenConnectorFamilies = new Set();
  for (const connector of connectors) {
    const family = normalizeConnectorFamily(connector?.connector_family);
    if (!family || seenConnectorFamilies.has(family)) {
      continue;
    }
    seenConnectorFamilies.add(family);
    connectorFamilyOptions.push({
      value: family,
      label: formatConnectorFamilyLabel(family),
    });
  }
  const filteredCreateConnectors = connectors.filter((item) => {
    if (!createForm.connectorFamily) {
      return true;
    }
    return normalizeConnectorFamily(item?.connector_family) === createForm.connectorFamily;
  });
  const selectedConnector = connectors.find((item) => item.name === createForm.connector) || null;
  const selectedConnectorCapabilities =
    selectedConnector?.capabilities && typeof selectedConnector.capabilities === "object"
      ? selectedConnector.capabilities
      : {};
  const createSourceMode =
    createForm.materializationMode === "synced" ? "resource" : createForm.sourceMode;
  const createConnectorRequired = isConnectorRequiredForCreate(createForm, createSourceMode);
  const liveDatasetsSupported = Boolean(selectedConnectorCapabilities.supports_live_datasets);
  const syncedDatasetsSupported = Boolean(selectedConnectorCapabilities.supports_synced_datasets);
  const queryPushdownSupported = Boolean(selectedConnectorCapabilities.supports_query_pushdown);
  const createStepState = getCreateStepState({
    createForm,
    createSourceMode,
    connectorRequired: createConnectorRequired,
  });
  const createStepCompletion = {
    identity: createStepState.identityReady,
    binding: createStepState.bindingReady,
    source: createStepState.sourceReady,
    review: createStepState.reviewReady,
  };
  const activeCreateStepIndex = DATASET_CREATE_STEPS.findIndex((step) => step.value === createStep);
  const createStepCopy = {
    identity: {
      title: "Name and describe the dataset",
      description: "Start with the runtime identity users will search for and understand.",
    },
    binding: {
      title: "Choose how the runtime should bind it",
      description: "Pick the materialization mode, source pattern, and connector strategy.",
    },
    source: {
      title: "Configure the underlying source",
      description: "Only the fields needed for the selected source mode are shown here.",
    },
    review: {
      title: "Review the dataset definition",
      description: "Confirm the runtime alias, binding, and source before creating it.",
    },
  }[createStep];
  const createReviewAlias = toSqlAlias(createForm.name || "dataset") || "dataset";
  const createSourceSummary = describeCreateSource(createForm, createSourceMode);
  const createReviewTags = splitCsv(createForm.tags);

  async function loadDatasetDetail(target = selected) {
    if (!target) {
      setDetail(null);
      setPreview(null);
      return;
    }
    setDetailLoading(true);
    setDetailError("");
    setPreviewLoading(true);
    setPreviewError("");
    try {
      const [detailPayload, previewPayload] = await Promise.all([
        fetchDataset(String(target.id || target.name)),
        previewDataset(String(target.id || target.name), {
          limit: Number(previewLimit) > 0 ? Number(previewLimit) : 25,
        }),
      ]);
      setDetail(detailPayload);
      setPreview(previewPayload);
    } catch (caughtError) {
      const message = getErrorMessage(caughtError);
      setDetail(null);
      setDetailError(message);
      setPreview(null);
      setPreviewError(message);
    } finally {
      setDetailLoading(false);
      setPreviewLoading(false);
    }
  }

  useEffect(() => {
    setCreateForm((current) => {
      const nextFamily =
        !current.connectorFamily ||
        connectors.some(
          (item) =>
            normalizeConnectorFamily(item?.connector_family) === current.connectorFamily,
        )
          ? current.connectorFamily
          : "";
      const matchingConnectors = connectors.filter((item) => {
        if (!nextFamily) {
          return true;
        }
        return normalizeConnectorFamily(item?.connector_family) === nextFamily;
      });
      let nextConnector = current.connector;
      if (
        nextConnector &&
        !matchingConnectors.some((item) => item.name === nextConnector)
      ) {
        nextConnector = "";
      }
      if (createConnectorRequired && !nextConnector && matchingConnectors.length > 0) {
        nextConnector = matchingConnectors[0].name;
      }
      if (nextFamily === current.connectorFamily && nextConnector === current.connector) {
        return current;
      }
      return {
        ...current,
        connectorFamily: nextFamily,
        connector: nextConnector,
      };
    });
  }, [connectors, createConnectorRequired, createForm.connector, createForm.connectorFamily]);

  useEffect(() => {
    if (createForm.materializationMode === "synced" && createForm.sourceMode !== "resource") {
      setCreateForm((current) => ({ ...current, sourceMode: "resource" }));
    }
    if (createForm.materializationMode === "live" && createForm.sourceMode === "resource") {
      setCreateForm((current) => ({ ...current, sourceMode: "table" }));
    }
  }, [createForm.materializationMode, createForm.sourceMode]);

  useEffect(() => {
    if (!showCreate) {
      setCreateStep("identity");
      return;
    }
    if (!createStepCompletion.identity) {
      setCreateStep("identity");
      return;
    }
    if (!createStepCompletion.binding && createStep === "review") {
      setCreateStep("binding");
      return;
    }
    if (!createStepCompletion.source && createStep === "review") {
      setCreateStep("source");
    }
  }, [
    createStep,
    createStepCompletion.binding,
    createStepCompletion.identity,
    createStepCompletion.source,
    showCreate,
  ]);

  useEffect(() => {
    let cancelled = false;

    async function loadSyncResources() {
      if (
        !showCreate ||
        createForm.materializationMode !== "synced" ||
        !selectedConnector?.supports_sync
      ) {
        setSyncResources({ items: [], loading: false, error: "" });
        return;
      }

      setSyncResources({ items: [], loading: true, error: "" });
      try {
        const payload = await fetchConnectorResources(selectedConnector.name);
        if (cancelled) {
          return;
        }
        setSyncResources({
          items: Array.isArray(payload?.items) ? payload.items : [],
          loading: false,
          error: "",
        });
      } catch (caughtError) {
        if (cancelled) {
          return;
        }
        setSyncResources({
          items: [],
          loading: false,
          error: getErrorMessage(caughtError),
        });
      }
    }

    void loadSyncResources();

    return () => {
      cancelled = true;
    };
  }, [
    createForm.materializationMode,
    selectedConnector?.name,
    selectedConnector?.supports_sync,
    showCreate,
  ]);

  function resetCreateForm() {
    setCreateForm(buildDatasetFormState(""));
    setCreateError("");
    setCreateStep("identity");
  }

  function resetEditForm(nextDetail = detail) {
    setEditForm(buildDatasetEditFormState(nextDetail));
    setEditError("");
  }

  function moveCreateStep(direction) {
    const nextIndex = activeCreateStepIndex + direction;
    if (nextIndex < 0 || nextIndex >= DATASET_CREATE_STEPS.length) {
      return;
    }
    setCreateStep(DATASET_CREATE_STEPS[nextIndex].value);
  }

  async function handleCreateDataset(event) {
    event.preventDefault();
    setCreateSubmitting(true);
    setCreateError("");
    setCreateSuccess("");

    try {
      const name = String(createForm.name || "").trim();
      if (!name) {
        throw new Error("Dataset name is required.");
      }
      if (createConnectorRequired && !createForm.connector) {
        throw new Error("Select a connector for this dataset source.");
      }
      if (
        createForm.materializationMode === "live" &&
        createConnectorRequired &&
        !liveDatasetsSupported
      ) {
        throw new Error(
          `Connector '${createForm.connector}' does not advertise live dataset support.`,
        );
      }
      if (createForm.materializationMode === "synced" && !syncedDatasetsSupported) {
        throw new Error(
          `Connector '${createForm.connector}' does not advertise synced dataset support.`,
        );
      }
      if (
        createForm.materializationMode === "live" &&
        (createSourceMode === "table" || createSourceMode === "sql") &&
        !queryPushdownSupported
      ) {
        throw new Error(
          `Connector '${createForm.connector}' does not advertise query pushdown for table/sql live datasets.`,
        );
      }

      const payload = {
        name,
        connector: createForm.connector || null,
        materialization_mode: createForm.materializationMode,
        source: {},
      };
      const description = String(createForm.description || "").trim();
      if (description) {
        payload.description = description;
      }

      if (createSourceMode === "table") {
        const table = String(createForm.table || "").trim();
        if (!table) {
          throw new Error("Dataset source.table is required.");
        }
        payload.source.table = table;
      } else if (createSourceMode === "sql") {
        const sql = String(createForm.sql || "").trim();
        if (!sql) {
          throw new Error("Dataset source.sql is required.");
        }
        payload.source.sql = sql;
      } else if (createSourceMode === "file") {
        const path = String(createForm.path || "").trim();
        if (!path) {
          throw new Error("Dataset source.path is required.");
        }
        payload.source.path = path;
        payload.source.format = createForm.format;
        payload.source.header = Boolean(createForm.header);
        if (String(createForm.delimiter || "").trim()) {
          payload.source.delimiter = createForm.delimiter;
        }
        if (String(createForm.quote || "").trim()) {
          payload.source.quote = createForm.quote;
        }
      } else {
        const resource = String(createForm.resource || "").trim();
        if (!resource) {
          throw new Error("Dataset source.resource is required for synced datasets.");
        }
        payload.source.resource = resource;
      }

      const tags = splitCsv(createForm.tags);
      if (tags.length > 0) {
        payload.tags = tags;
      }

      const created = await createDataset(payload);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = [
          toDatasetListItem(created),
          ...items.filter(
            (item) => String(item?.id || item?.name) !== String(created?.id || created?.name),
          ),
        ];
        return {
          items: nextItems,
          total: nextItems.length,
        };
      });
      setCreateSuccess(`${created.name} is available as a runtime_managed dataset.`);
      setDetail(created);
      setPreview(null);
      setPreviewError("");
      setShowCreate(false);
      resetCreateForm();
      navigate(`/datasets/${buildItemRef(created)}`);
      void loadDatasetDetail(created);
      void reload();
    } catch (caughtError) {
      setCreateError(getErrorMessage(caughtError));
    } finally {
      setCreateSubmitting(false);
    }
  }

  function beginEditDataset() {
    resetEditForm(detail);
    setShowEdit(true);
    setShowCreate(false);
    setEditSuccess("");
    setDeleteError("");
  }

  async function handleUpdateDataset(event) {
    event.preventDefault();
    if (!detail) {
      return;
    }
    setEditSubmitting(true);
    setEditError("");
    setEditSuccess("");
    setDeleteError("");

    try {
      const payload = {
        description: String(editForm.description || "").trim() || null,
        materialization_mode: editForm.materializationMode,
        tags: splitCsv(editForm.tags),
        source: {},
      };
      const editSourceMode =
        editForm.materializationMode === "synced" ? "resource" : editForm.sourceMode;
      if (editSourceMode === "table") {
        const table = String(editForm.table || "").trim();
        if (!table) {
          throw new Error("Dataset source.table is required.");
        }
        payload.source.table = table;
      } else if (editSourceMode === "sql") {
        const sql = String(editForm.sql || "").trim();
        if (!sql) {
          throw new Error("Dataset source.sql is required.");
        }
        payload.source.sql = sql;
      } else if (editSourceMode === "file") {
        const path = String(editForm.path || "").trim();
        if (!path) {
          throw new Error("Dataset source.path or source.storage_uri is required.");
        }
        payload.source.storage_uri = path;
        payload.source.format = editForm.format;
        payload.source.header = Boolean(editForm.header);
        if (String(editForm.delimiter || "").trim()) {
          payload.source.delimiter = editForm.delimiter;
        }
        if (String(editForm.quote || "").trim()) {
          payload.source.quote = editForm.quote;
        }
      } else {
        const resource = String(editForm.resource || "").trim();
        if (!resource) {
          throw new Error("Dataset source.resource is required for synced datasets.");
        }
        payload.source.resource = resource;
      }

      const updated = await updateDataset(String(detail.id || detail.name), payload);
      setDetail(updated);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = items.map((item) =>
          String(item?.id || item?.name) === String(updated?.id || updated?.name)
            ? toDatasetListItem(updated)
            : item,
        );
        return { items: nextItems, total: nextItems.length };
      });
      setShowEdit(false);
      setEditSuccess(`${updated.name} was updated.`);
      void loadDatasetDetail(updated);
      void reload();
    } catch (caughtError) {
      setEditError(getErrorMessage(caughtError));
    } finally {
      setEditSubmitting(false);
    }
  }

  async function handleDeleteDataset() {
    if (!detail?.id || deleteSubmitting) {
      return;
    }
    const confirmed = window.confirm(
      `Delete runtime-managed dataset '${detail.name}'? This cannot be undone.`,
    );
    if (!confirmed) {
      return;
    }
    setDeleteSubmitting(true);
    setDeleteError("");
    setEditSuccess("");
    try {
      await deleteDataset(String(detail.id));
      setDetail(null);
      setPreview(null);
      setShowEdit(false);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = items.filter(
          (item) => String(item?.id || item?.name) !== String(detail?.id || detail?.name),
        );
        return { items: nextItems, total: nextItems.length };
      });
      navigate("/datasets");
      void reload();
    } catch (caughtError) {
      setDeleteError(getErrorMessage(caughtError));
    } finally {
      setDeleteSubmitting(false);
    }
  }

  useEffect(() => {
    void loadDatasetDetail(selected);
  }, [selected?.id, selected?.name]);

  return (
    <div className="page-stack">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Datasets</p>
            <h2>{detail?.label || selected?.label || selected?.name || "Dataset inventory"}</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(datasets.length)} datasets</span>
              <span className="chip">{formatValue(boundConnectorCount)} connectors</span>
              <span className="chip">{formatValue(boundSemanticModelCount)} semantic links</span>
              <span className="chip">{formatValue(schemaColumns.length)} columns</span>
            </div>
          </div>
          <div className="product-command-bar-actions">
            <button
              className="primary-button"
              type="button"
              onClick={() => {
                setShowCreate((current) => !current);
                setShowEdit(false);
                setCreateError("");
                setCreateSuccess("");
                setCreateStep("identity");
              }}
            >
              {showCreate ? "Close create flow" : "Create runtime-managed dataset"}
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
          placeholder="Filter datasets by name, connector, or semantic model"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh datasets"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Dataset inventory" className="compact-panel">
          <ManagementModeNotice
            mode={selected?.management_mode || "config_managed"}
            resourceLabel="Dataset ownership"
          />
          {filteredDatasets.length > 0 ? (
            <div className="stack-list">
              {filteredDatasets.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/datasets/${buildItemRef(item)}`}
                >
                  <div className="list-card-topline">
                    <strong>{item.label || item.name}</strong>
                    <ManagementBadge mode={item.management_mode} />
                  </div>
                  <span>
                    {[item.connector, item.semantic_model, item.materialization_mode]
                      .filter(Boolean)
                      .join(" | ") || "No bindings"}
                  </span>
                  <small>{describeManagementMode(item.management_mode)}</small>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty
              title="No datasets found"
              message="Adjust the filter or create a runtime-managed dataset."
            />
          )}
        </Panel>

        <div className="detail-stack">
          {createSuccess ? (
            <div className="callout success">
              <strong>Dataset created</strong>
              <span>{createSuccess}</span>
            </div>
          ) : null}
          {editSuccess ? (
            <div className="callout success">
              <strong>Dataset updated</strong>
              <span>{editSuccess}</span>
            </div>
          ) : null}

          {showCreate ? (
            <Panel
              title="Create runtime-managed dataset"
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
              <ManagementModeNotice mode="runtime_managed" resourceLabel="New datasets" />
              <div className="dataset-flow-shell">
                <div className="dataset-flow-nav">
                  {DATASET_CREATE_STEPS.map((step, index) => {
                    const isActive = step.value === createStep;
                    const isComplete = createStepCompletion[step.value];
                    const isAvailable =
                      index === 0 ||
                      DATASET_CREATE_STEPS.slice(0, index).every(
                        (candidate) => createStepCompletion[candidate.value],
                      );

                    return (
                      <button
                        key={step.value}
                        className={`dataset-flow-step ${isActive ? "active" : ""} ${isComplete ? "complete" : ""}`.trim()}
                        type="button"
                        onClick={() => setCreateStep(step.value)}
                        disabled={!isAvailable || createSubmitting}
                      >
                        <span className="dataset-flow-step-index">{index + 1}</span>
                        <span className="dataset-flow-step-copy">
                          <strong>{step.label}</strong>
                          <small>
                            {step.value === "identity"
                              ? "Name and metadata."
                              : step.value === "binding"
                                ? "Connector and mode."
                                : step.value === "source"
                                  ? "Source-specific fields."
                                  : "Final check before create."}
                          </small>
                        </span>
                      </button>
                    );
                  })}
                  <div className="dataset-flow-review-card">
                    <span className="eyebrow">Runtime alias</span>
                    <strong>{createReviewAlias}</strong>
                    <p>{createSourceSummary}</p>
                    <div className="dataset-flow-review-meta">
                      <span className="chip">{createForm.materializationMode}</span>
                      <span className="chip">{createSourceMode}</span>
                      <span className="chip">{createForm.connector || "No connector"}</span>
                    </div>
                  </div>
                </div>

                <form className="dataset-flow-stage" onSubmit={handleCreateDataset}>
                  <div className="dataset-flow-stage-header">
                    <div>
                      <p className="eyebrow">
                        Step {Math.max(activeCreateStepIndex, 0) + 1} of {DATASET_CREATE_STEPS.length}
                      </p>
                      <h3>{createStepCopy.title}</h3>
                    </div>
                    <p>{createStepCopy.description}</p>
                  </div>
                  {createStep === "identity" ? (
                    <div className="dataset-flow-section form-grid">
                      <label className="field">
                        <span>Name</span>
                        <input
                          className="text-input"
                          type="text"
                          value={createForm.name}
                          onChange={(event) =>
                            setCreateForm((current) => ({ ...current, name: event.target.value }))
                          }
                          placeholder="runtime_orders"
                          disabled={createSubmitting}
                        />
                        <small className="field-hint">
                          This becomes the canonical runtime dataset name and default alias.
                        </small>
                      </label>

                      <label className="field field-full">
                        <span>Description</span>
                        <input
                          className="text-input"
                          type="text"
                          value={createForm.description}
                          onChange={(event) =>
                            setCreateForm((current) => ({
                              ...current,
                              description: event.target.value,
                            }))
                          }
                          placeholder="Short runtime dataset description"
                          disabled={createSubmitting}
                        />
                      </label>
                    </div>
                  ) : null}

                  {createStep === "binding" ? (
                    <div className="dataset-flow-section">
                      <div className="field">
                        <span>Materialization mode</span>
                        <div className="dataset-mode-grid">
                          {[
                            {
                              value: "live",
                              label: "Live",
                              description: "Query the source directly at runtime.",
                            },
                            {
                              value: "synced",
                              label: "Synced",
                              description: "Materialize connector resources into managed storage.",
                            },
                          ].map((option) => (
                            <button
                              key={option.value}
                              className={`dataset-mode-card ${createForm.materializationMode === option.value ? "active" : ""}`.trim()}
                              type="button"
                              onClick={() =>
                                setCreateForm((current) => ({
                                  ...current,
                                  materializationMode: option.value,
                                }))
                              }
                              disabled={
                                createSubmitting ||
                                (option.value === "synced" && connectors.length === 0)
                              }
                            >
                              <strong>{option.label}</strong>
                              <small>{option.description}</small>
                            </button>
                          ))}
                        </div>
                      </div>

                      <div className="field">
                        <span>Source mode</span>
                        <div className="dataset-mode-grid source-grid">
                          {DATASET_SOURCE_MODE_OPTIONS.filter((option) =>
                            createForm.materializationMode === "synced"
                              ? option.value === "resource"
                              : option.value !== "resource",
                          ).map((option) => (
                            <button
                              key={option.value}
                              className={`dataset-mode-card ${createSourceMode === option.value ? "active" : ""}`.trim()}
                              type="button"
                              onClick={() =>
                                setCreateForm((current) => ({
                                  ...current,
                                  sourceMode: option.value,
                                }))
                              }
                              disabled={createSubmitting}
                            >
                              <strong>{option.label}</strong>
                              <small>{option.description}</small>
                              <small>{option.requirement}</small>
                            </button>
                          ))}
                        </div>
                      </div>

                      <label className="field">
                        <span>Connector family</span>
                        <select
                          className="select-input"
                          value={createForm.connectorFamily}
                          onChange={(event) =>
                            setCreateForm((current) => ({
                              ...current,
                              connectorFamily: event.target.value,
                            }))
                          }
                          disabled={createSubmitting || connectors.length === 0}
                        >
                          <option value="">
                            {connectors.length === 0
                              ? "No connector families available"
                              : "All connector families"}
                          </option>
                          {connectorFamilyOptions.map((family) => (
                            <option key={family.value} value={family.value}>
                              {family.label}
                            </option>
                          ))}
                        </select>
                        <small className="field-hint">
                          Filter the runtime connector inventory before choosing a concrete binding.
                        </small>
                      </label>

                      <label className="field">
                        <span>Connector</span>
                        <select
                          className="select-input"
                          value={createForm.connector}
                          onChange={(event) =>
                            setCreateForm((current) => ({
                              ...current,
                              connector: event.target.value,
                            }))
                          }
                          disabled={
                            createSubmitting ||
                            (createConnectorRequired && filteredCreateConnectors.length === 0)
                          }
                        >
                          <option value="">
                            {filteredCreateConnectors.length === 0
                              ? createForm.connectorFamily
                                ? `No ${formatConnectorFamilyLabel(createForm.connectorFamily)} connectors`
                                : "No connectors available"
                              : createConnectorRequired
                                ? "Select a connector"
                                : "No connector"}
                          </option>
                          {filteredCreateConnectors.map((connector) => (
                            <option key={connector.id || connector.name} value={connector.name}>
                              {createForm.connectorFamily
                                ? connector.name
                                : `${connector.name} (${formatConnectorFamilyLabel(
                                    connector.connector_family,
                                  )})`}
                            </option>
                          ))}
                        </select>
                        <small className="field-hint">
                          {createConnectorRequired
                            ? "This source must stay bound to a connector."
                            : "File-backed datasets can be created without a connector binding."}
                        </small>
                      </label>

                      <div className="callout">
                        <strong>
                          {selectedConnector?.name ||
                            (createConnectorRequired
                              ? "Connector required"
                              : "Connector optional for this source")}
                        </strong>
                        <span>
                          {createForm.connector
                            ? [
                                formatConnectorFamilyLabel(selectedConnector?.connector_family),
                                liveDatasetsSupported ? "live datasets" : "no live datasets",
                                syncedDatasetsSupported ? "synced datasets" : "no synced datasets",
                                queryPushdownSupported ? "query pushdown" : "no query pushdown",
                              ].join(" | ")
                            : createForm.connectorFamily
                              ? `No ${formatConnectorFamilyLabel(createForm.connectorFamily)} connectors matched the current filter.`
                            : "Select a connector to inspect runtime dataset capabilities."}
                        </span>
                      </div>
                    </div>
                  ) : null}

                  {createStep === "source" ? (
                    <div className="dataset-flow-section form-grid">
                      {createSourceMode === "table" ? (
                        <label className="field field-full">
                          <span>Source table</span>
                          <input
                            className="text-input"
                            type="text"
                            value={createForm.table}
                            onChange={(event) =>
                              setCreateForm((current) => ({ ...current, table: event.target.value }))
                            }
                            placeholder="orders_enriched"
                            disabled={createSubmitting}
                          />
                          <small className="field-hint">
                            Use a connector-visible relation name. Live table datasets require query pushdown.
                          </small>
                        </label>
                      ) : null}

                      {createSourceMode === "sql" ? (
                        <label className="field field-full">
                          <span>Source SQL</span>
                          <textarea
                            className="textarea-input"
                            value={createForm.sql}
                            onChange={(event) =>
                              setCreateForm((current) => ({ ...current, sql: event.target.value }))
                            }
                            placeholder="SELECT * FROM orders_enriched"
                            disabled={createSubmitting}
                          />
                          <small className="field-hint">
                            Define the live dataset as a SQL statement executed through the connector.
                          </small>
                        </label>
                      ) : null}

                      {createSourceMode === "file" ? (
                        <>
                          <label className="field field-full">
                            <span>Source path</span>
                            <input
                              className="text-input"
                              type="text"
                              value={createForm.path}
                              onChange={(event) =>
                                setCreateForm((current) => ({ ...current, path: event.target.value }))
                              }
                              placeholder="/var/lib/langbridge/orders.csv"
                              disabled={createSubmitting}
                            />
                            <small className="field-hint">
                              Point at a local file or uploaded asset. Connector binding is optional here.
                            </small>
                          </label>
                          <label className="field">
                            <span>File format</span>
                            <select
                              className="select-input"
                              value={createForm.format}
                              onChange={(event) =>
                                setCreateForm((current) => ({ ...current, format: event.target.value }))
                              }
                              disabled={createSubmitting}
                            >
                              <option value="csv">csv</option>
                              <option value="parquet">parquet</option>
                            </select>
                          </label>
                          <label className="checkbox-field">
                            <input
                              type="checkbox"
                              checked={createForm.header}
                              onChange={(event) =>
                                setCreateForm((current) => ({
                                  ...current,
                                  header: event.target.checked,
                                }))
                              }
                              disabled={createSubmitting}
                            />
                            <span>Header row</span>
                          </label>
                          <label className="field">
                            <span>Delimiter</span>
                            <input
                              className="text-input"
                              type="text"
                              value={createForm.delimiter}
                              onChange={(event) =>
                                setCreateForm((current) => ({
                                  ...current,
                                  delimiter: event.target.value,
                                }))
                              }
                              disabled={createSubmitting}
                            />
                          </label>
                          <label className="field">
                            <span>Quote</span>
                            <input
                              className="text-input"
                              type="text"
                              value={createForm.quote}
                              onChange={(event) =>
                                setCreateForm((current) => ({ ...current, quote: event.target.value }))
                              }
                              disabled={createSubmitting}
                            />
                          </label>
                        </>
                      ) : null}

                      {createSourceMode === "resource" ? (
                        <label className="field field-full">
                          <span>Connector resource</span>
                          {syncResources.items.length > 0 ? (
                            <select
                              className="select-input"
                              value={createForm.resource}
                              onChange={(event) =>
                                setCreateForm((current) => ({
                                  ...current,
                                  resource: event.target.value,
                                }))
                              }
                              disabled={createSubmitting || syncResources.loading}
                            >
                              <option value="">Select a resource</option>
                              {syncResources.items.map((resource) => (
                                <option key={resource.name} value={resource.name}>
                                  {resource.label || resource.name}
                                </option>
                              ))}
                            </select>
                          ) : (
                            <input
                              className="text-input"
                              type="text"
                              value={createForm.resource}
                              onChange={(event) =>
                                setCreateForm((current) => ({
                                  ...current,
                                  resource: event.target.value,
                                }))
                              }
                              placeholder="orders"
                              disabled={createSubmitting}
                            />
                          )}
                          <small className="field-hint">
                            Synced datasets materialize a connector resource into runtime-managed storage.
                          </small>
                          {syncResources.error ? <div className="error-banner">{syncResources.error}</div> : null}
                        </label>
                      ) : null}
                    </div>
                  ) : null}

                  {createStep === "identity" ? (
                    <div className="dataset-flow-section form-grid">
                      <label className="field field-full">
                        <span>Tags</span>
                        <input
                          className="text-input"
                          type="text"
                          value={createForm.tags}
                          onChange={(event) =>
                            setCreateForm((current) => ({ ...current, tags: event.target.value }))
                          }
                          placeholder="finance, runtime, orders"
                          disabled={createSubmitting}
                        />
                        <small className="field-hint">Comma-separated optional tags.</small>
                      </label>
                    </div>
                  ) : null}

                  {createStep === "review" ? (
                    <div className="dataset-flow-section summary-grid">
                      <Panel title="Definition" className="panel--flat">
                        <DetailList
                          items={[
                            { label: "Name", value: createForm.name || "Not set" },
                            { label: "Alias", value: createReviewAlias },
                            {
                              label: "Description",
                              value: createForm.description || "No description provided",
                            },
                            {
                              label: "Tags",
                              value:
                                createReviewTags.length > 0
                                  ? formatList(createReviewTags)
                                  : "No tags",
                            },
                          ]}
                        />
                      </Panel>
                      <Panel title="Binding" className="panel--flat">
                        <DetailList
                          items={[
                            {
                              label: "Materialization",
                              value: createForm.materializationMode,
                            },
                            { label: "Source mode", value: createSourceMode },
                            {
                              label: "Connector",
                              value:
                                createForm.connector ||
                                (createConnectorRequired ? "Required but missing" : "None"),
                            },
                            { label: "Source", value: createSourceSummary },
                          ]}
                        />
                      </Panel>
                    </div>
                  ) : null}

                  {createError ? <div className="error-banner">{createError}</div> : null}
                  <div className="dataset-flow-actions">
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => moveCreateStep(-1)}
                      disabled={createSubmitting || activeCreateStepIndex <= 0}
                    >
                      Back
                    </button>
                    {createStep !== "review" ? (
                      <button
                        className="primary-button"
                        type="button"
                        onClick={() => moveCreateStep(1)}
                        disabled={
                          createSubmitting ||
                          (createStep === "identity" && !createStepState.identityReady) ||
                          (createStep === "binding" && !createStepState.bindingReady) ||
                          (createStep === "source" && !createStepState.sourceReady)
                        }
                      >
                        Continue
                      </button>
                    ) : (
                      <button
                        className="primary-button"
                        type="submit"
                        disabled={
                          createSubmitting ||
                          !createStepState.reviewReady ||
                          (createConnectorRequired && connectors.length === 0)
                        }
                      >
                        {createSubmitting ? "Creating dataset..." : "Create dataset"}
                      </button>
                    )}
                  </div>
                </form>
              </div>
            </Panel>
          ) : null}

          {showEdit && detail ? (
            <Panel
              title={`Edit ${detail.label || detail.name}`}
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
              <ManagementModeNotice mode="runtime_managed" resourceLabel="Editable dataset" />
              <form className="form-grid" onSubmit={handleUpdateDataset}>
                <label className="field">
                  <span>Name</span>
                  <input className="text-input" type="text" value={detail.name} disabled />
                </label>

                <label className="field">
                  <span>Connector</span>
                  <input className="text-input" type="text" value={detail.connector || ""} disabled />
                </label>

                <label className="field field-full">
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

                <label className="field">
                  <span>Materialization mode</span>
                  <select
                    className="select-input"
                    value={editForm.materializationMode}
                    onChange={(event) =>
                      setEditForm((current) => ({
                        ...current,
                        materializationMode: event.target.value,
                        sourceMode:
                          event.target.value === "synced" ? "resource" : current.sourceMode === "resource" ? "table" : current.sourceMode,
                      }))
                    }
                    disabled={editSubmitting}
                  >
                    <option value="live">live</option>
                    <option value="synced">synced</option>
                  </select>
                </label>

                <label className="field">
                  <span>Source mode</span>
                  <select
                    className="select-input"
                    value={editForm.materializationMode === "synced" ? "resource" : editForm.sourceMode}
                    onChange={(event) =>
                      setEditForm((current) => ({ ...current, sourceMode: event.target.value }))
                    }
                    disabled={editSubmitting || editForm.materializationMode === "synced"}
                  >
                    {editForm.materializationMode === "synced" ? (
                      <option value="resource">resource</option>
                    ) : (
                      <>
                        <option value="table">table</option>
                        <option value="sql">sql</option>
                        <option value="file">file</option>
                      </>
                    )}
                  </select>
                </label>

                {(editForm.materializationMode === "synced" ? "resource" : editForm.sourceMode) === "table" ? (
                  <label className="field field-full">
                    <span>Source table</span>
                    <input
                      className="text-input"
                      type="text"
                      value={editForm.table}
                      onChange={(event) =>
                        setEditForm((current) => ({ ...current, table: event.target.value }))
                      }
                      disabled={editSubmitting}
                    />
                  </label>
                ) : null}

                {(editForm.materializationMode === "synced" ? "resource" : editForm.sourceMode) === "sql" ? (
                  <label className="field field-full">
                    <span>Source SQL</span>
                    <textarea
                      className="textarea-input"
                      value={editForm.sql}
                      onChange={(event) =>
                        setEditForm((current) => ({ ...current, sql: event.target.value }))
                      }
                      disabled={editSubmitting}
                    />
                  </label>
                ) : null}

                {(editForm.materializationMode === "synced" ? "resource" : editForm.sourceMode) === "file" ? (
                  <>
                    <label className="field field-full">
                      <span>Storage URI</span>
                      <input
                        className="text-input"
                        type="text"
                        value={editForm.path}
                        onChange={(event) =>
                          setEditForm((current) => ({ ...current, path: event.target.value }))
                        }
                        disabled={editSubmitting}
                      />
                    </label>
                    <label className="field">
                      <span>File format</span>
                      <select
                        className="select-input"
                        value={editForm.format}
                        onChange={(event) =>
                          setEditForm((current) => ({ ...current, format: event.target.value }))
                        }
                        disabled={editSubmitting}
                      >
                        <option value="csv">csv</option>
                        <option value="parquet">parquet</option>
                      </select>
                    </label>
                    <label className="checkbox-field">
                      <input
                        type="checkbox"
                        checked={editForm.header}
                        onChange={(event) =>
                          setEditForm((current) => ({ ...current, header: event.target.checked }))
                        }
                        disabled={editSubmitting}
                      />
                      <span>Header row</span>
                    </label>
                    <label className="field">
                      <span>Delimiter</span>
                      <input
                        className="text-input"
                        type="text"
                        value={editForm.delimiter}
                        onChange={(event) =>
                          setEditForm((current) => ({ ...current, delimiter: event.target.value }))
                        }
                        disabled={editSubmitting}
                      />
                    </label>
                    <label className="field">
                      <span>Quote</span>
                      <input
                        className="text-input"
                        type="text"
                        value={editForm.quote}
                        onChange={(event) =>
                          setEditForm((current) => ({ ...current, quote: event.target.value }))
                        }
                        disabled={editSubmitting}
                      />
                    </label>
                  </>
                ) : null}

                {(editForm.materializationMode === "synced" ? "resource" : editForm.sourceMode) === "resource" ? (
                  <label className="field field-full">
                    <span>Connector resource</span>
                    <input
                      className="text-input"
                      type="text"
                      value={editForm.resource}
                      onChange={(event) =>
                        setEditForm((current) => ({ ...current, resource: event.target.value }))
                      }
                      disabled={editSubmitting}
                    />
                  </label>
                ) : null}

                <label className="field field-full">
                  <span>Tags</span>
                  <input
                    className="text-input"
                    type="text"
                    value={editForm.tags}
                    onChange={(event) =>
                      setEditForm((current) => ({ ...current, tags: event.target.value }))
                    }
                    disabled={editSubmitting}
                  />
                </label>

                {editError ? <div className="error-banner field-full">{editError}</div> : null}
                <div className="settings-form-actions field-full">
                  <button className="primary-button" type="submit" disabled={editSubmitting}>
                    {editSubmitting ? "Saving..." : "Save dataset"}
                  </button>
                  <button
                    className="ghost-button danger-button"
                    type="button"
                    onClick={() => void handleDeleteDataset()}
                    disabled={editSubmitting || deleteSubmitting}
                  >
                    {deleteSubmitting ? "Deleting..." : "Delete dataset"}
                  </button>
                </div>
              </form>
            </Panel>
          ) : null}

          {selected ? (
            <>
              <Panel
                title={detail?.label || selected.label || selected.name}
                className="compact-panel"
                actions={
                  <div className="panel-actions-inline">
                    <ManagementBadge mode={detail?.management_mode || selected.management_mode} />
                    {(detail?.management_mode || selected.management_mode) === "runtime_managed" ? (
                      <button className="ghost-button" type="button" onClick={beginEditDataset}>
                        Edit
                      </button>
                    ) : null}
                    <button className="ghost-button" type="button" onClick={() => navigate("/sql")}>
                      Open SQL
                    </button>
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => void loadDatasetDetail()}
                      disabled={detailLoading || previewLoading}
                    >
                      {detailLoading || previewLoading ? "Refreshing..." : "Refresh detail"}
                    </button>
                  </div>
                }
              >
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {deleteError ? <div className="error-banner">{deleteError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading dataset detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.connector || "No connector binding"}</span>
                      <span>{detail.semantic_model || "No semantic model binding"}</span>
                      <span>{detail.dataset_type || "runtime dataset"}</span>
                      <span>{describeManagementMode(detail.management_mode)}</span>
                    </div>
                    {Array.isArray(detail.tags) && detail.tags.length > 0 ? (
                      <div className="tag-list">
                        {detail.tags.map((tag) => (
                          <span key={tag} className="tag">
                            #{tag}
                          </span>
                        ))}
                      </div>
                    ) : null}
                    <DetailList
                      items={[
                        { label: "Name", value: formatValue(detail.name) },
                        { label: "SQL alias", value: formatValue(detail.sql_alias) },
                        { label: "Connector", value: formatValue(detail.connector) },
                        { label: "Semantic model", value: formatValue(detail.semantic_model) },
                        { label: "Type", value: formatValue(detail.dataset_type) },
                        {
                          label: "Management mode",
                          value: formatValue(detail.management_mode),
                        },
                        { label: "Tags", value: formatList(detail.tags) },
                      ]}
                    />
                  </>
                ) : (
                  <PageEmpty
                    title="No detail"
                    message="The runtime did not return dataset detail for this item."
                  />
                )}
              </Panel>

              <ManagementModeNotice
                mode={detail?.management_mode || selected.management_mode}
                resourceLabel={detail?.label || selected.label || selected.name}
              />

              <section className="summary-grid">
                <Panel title="Bindings and execution" eyebrow="Operational">
                  {detail ? (
                    <DetailList
                      items={[
                        { label: "Source kind", value: formatValue(detail.source_kind) },
                        { label: "Storage kind", value: formatValue(detail.storage_kind) },
                        { label: "Storage URI", value: formatValue(detail.storage_uri) },
                        { label: "Table name", value: formatValue(detail.table_name) },
                        { label: "Dialect", value: formatValue(detail.dialect) },
                        {
                          label: "Preview row count",
                          value: formatValue(preview?.rowCount || preview?.row_count_preview),
                        },
                      ]}
                    />
                  ) : (
                    <PageEmpty
                      title="No runtime binding"
                      message="Select a dataset to inspect execution metadata."
                    />
                  )}
                </Panel>

                <Panel title="Schema signals" eyebrow="Columns">
                  {detail ? (
                    <DetailList
                      items={[
                        { label: "Columns", value: formatValue(schemaColumns.length) },
                        { label: "Nullable columns", value: formatValue(nullableColumns) },
                        { label: "Computed columns", value: formatValue(computedColumns) },
                        { label: "Preview limit", value: formatValue(previewLimit) },
                      ]}
                    />
                  ) : (
                    <PageEmpty
                      title="No schema signals"
                      message="Select a dataset to inspect schema detail."
                    />
                  )}
                </Panel>
              </section>

              <Panel title="Dataset workspace" eyebrow="Inspect">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "schema", label: "Schema" },
                    { value: "preview", label: "Preview" },
                    { value: "runtime", label: "Runtime meta" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="detail-card-grid">
                    <article className="detail-card">
                      <strong>Connector binding</strong>
                      <span>{detail?.connector || "None"}</span>
                      {detail?.connector_id ? (
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() =>
                            navigate(`/connectors/${encodeURIComponent(String(detail.connector_id))}`)
                          }
                        >
                          Open connector
                        </button>
                      ) : null}
                    </article>
                    <article className="detail-card">
                      <strong>Semantic binding</strong>
                      <span>{detail?.semantic_model || "Not attached"}</span>
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => navigate("/semantic-models")}
                      >
                        Open semantic models
                      </button>
                    </article>
                    <article className="detail-card">
                      <strong>SQL alias</strong>
                      <span>{detail?.sql_alias || toSqlAlias(detail?.name || selected.name)}</span>
                      <small>Use this alias from the runtime SQL workspace.</small>
                    </article>
                    <article className="detail-card">
                      <strong>Policy posture</strong>
                      <span>
                        {policy ? `${policy.max_rows_preview || "n/a"} preview rows` : "No policy metadata"}
                      </span>
                      <small>Runtime UI intentionally excludes cloud revisioning and governance workflows.</small>
                    </article>
                  </div>
                ) : null}

                {activeTab === "schema" ? (
                  schemaColumns.length > 0 ? (
                    <div className="table-wrap">
                      <table className="result-table">
                        <thead>
                          <tr>
                            <th>Name</th>
                            <th>Type</th>
                            <th>Nullable</th>
                            <th>Computed</th>
                            <th>Description</th>
                          </tr>
                        </thead>
                        <tbody>
                          {schemaColumns.map((column) => (
                            <tr key={column.id || column.name}>
                              <td>{column.name}</td>
                              <td>{formatValue(column.data_type)}</td>
                              <td>{formatValue(column.nullable)}</td>
                              <td>{formatValue(column.is_computed)}</td>
                              <td>{formatValue(column.description)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <PageEmpty
                      title="No column metadata"
                      message="This dataset did not expose column metadata."
                    />
                  )
                ) : null}

                {activeTab === "preview" ? (
                  <>
                    <div className="panel-actions-inline">
                      <input
                        className="text-input narrow-input"
                        type="number"
                        min="1"
                        value={previewLimit}
                        onChange={(event) => setPreviewLimit(event.target.value)}
                      />
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => void loadDatasetDetail()}
                        disabled={previewLoading}
                      >
                        {previewLoading ? "Loading..." : "Run preview"}
                      </button>
                      {previewResult ? (
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() =>
                            downloadTextFile(
                              `${toSqlAlias(detail?.name || selected.name)}-preview.csv`,
                              toCsvText(previewResult),
                              "text/csv;charset=utf-8",
                            )
                          }
                        >
                          Download CSV
                        </button>
                      ) : null}
                    </div>
                    {previewError ? <div className="error-banner">{previewError}</div> : null}
                    {previewLoading ? (
                      <div className="empty-box">Running dataset preview...</div>
                    ) : previewResult ? (
                      <>
                        <div className="inline-notes">
                          <span>Rows: {formatValue(preview.rowCount || preview.row_count_preview)}</span>
                          <span>Limit: {formatValue(preview.effective_limit || previewLimit)}</span>
                          <span>Redaction: {formatValue(preview.redaction_applied)}</span>
                        </div>
                        <ResultTable result={previewResult} maxPreviewRows={12} />
                        {preview.generated_sql ? <pre className="code-block">{preview.generated_sql}</pre> : null}
                      </>
                    ) : (
                      <PageEmpty
                        title="No preview"
                        message="Run a preview to inspect dataset rows from the runtime."
                      />
                    )}
                  </>
                ) : null}

                {activeTab === "runtime" ? (
                  <div className="summary-grid">
                    <Panel title="Policy" eyebrow="Runtime guardrails" className="panel--flat">
                      {policy ? (
                        <DetailList
                          items={[
                            { label: "Max preview rows", value: formatValue(policy.max_rows_preview) },
                            { label: "Max export rows", value: formatValue(policy.max_export_rows) },
                            { label: "Allow DML", value: formatValue(policy.allow_dml) },
                            {
                              label: "Redaction rules",
                              value: formatValue(Object.keys(policy.redaction_rules || {}).length),
                            },
                            {
                              label: "Row filters",
                              value: formatValue((policy.row_filters || []).length),
                            },
                          ]}
                        />
                      ) : (
                        <PageEmpty
                          title="No policy metadata"
                          message="This dataset did not expose runtime policy data."
                        />
                      )}
                    </Panel>
                    <Panel title="Execution" eyebrow="Runtime contracts" className="panel--flat">
                      {detail ? (
                        <>
                          <div className="detail-card">
                            <strong>Relation identity</strong>
                            <pre className="code-block compact">
                              {JSON.stringify(detail.relation_identity || {}, null, 2)}
                            </pre>
                          </div>
                          <div className="detail-card">
                            <strong>Execution capabilities</strong>
                            <pre className="code-block compact">
                              {JSON.stringify(detail.execution_capabilities || {}, null, 2)}
                            </pre>
                          </div>
                        </>
                      ) : (
                        <PageEmpty
                          title="No runtime metadata"
                          message="Select a dataset to inspect runtime execution metadata."
                        />
                      )}
                    </Panel>
                  </div>
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Dataset detail" eyebrow="Runtime">
              <PageEmpty
                title="No dataset selected"
                message="Pick a dataset to inspect its metadata and preview rows."
              />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}
