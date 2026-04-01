import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import {
  DetailList,
  ManagementBadge,
  ManagementModeNotice,
  PageEmpty,
  Panel,
} from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { formatDateTime, formatList, formatValue, getErrorMessage } from "../lib/format";
import {
  describeManagementMode,
  formatConnectorFamilyLabel,
  normalizeConnectorFamily,
  parseJsonObjectInput,
  stringifyJsonInput,
} from "../lib/managedResources";
import {
  createConnector,
  deleteConnector,
  fetchConnector,
  fetchConnectorResources,
  fetchConnectorStates,
  fetchConnectorTypeConfig,
  fetchConnectors,
  fetchConnectorTypes,
  runConnectorSync,
  updateConnector,
} from "../lib/runtimeApi";
import { buildItemRef, resolveItemByRef } from "../lib/runtimeUi";

function buildConnectorFormState() {
  return {
    name: "",
    family: "",
    type: "",
    description: "",
    configValues: {},
    metadataText: stringifyJsonInput({}),
    secretsText: stringifyJsonInput({}),
  };
}

function buildConnectorConfigValues(schema, connection = {}) {
  const normalizedConnection =
    connection && typeof connection === "object" && !Array.isArray(connection) ? connection : {};

  if (!schema || !Array.isArray(schema.config)) {
    return Object.fromEntries(
      Object.entries(normalizedConnection)
        .filter(([, value]) => value !== null && value !== undefined)
        .map(([key, value]) => [key, String(value)]),
    );
  }

  return Object.fromEntries(
    schema.config.map((entry) => {
      const fallback =
        normalizedConnection[entry.field] ?? entry.default ?? entry.value ?? "";
      return [
        entry.field,
        fallback === null || fallback === undefined ? "" : String(fallback),
      ];
    }),
  );
}

function buildConnectorConnectionPayload(schema, configValues) {
  if (!schema || !Array.isArray(schema.config)) {
    throw new Error("Connector schema is unavailable for the selected connector type.");
  }

  const payload = {};
  const missingFields = [];
  for (const entry of schema.config) {
    const rawValue = configValues?.[entry.field] ?? "";
    const trimmedValue =
      typeof rawValue === "string" ? rawValue.trim() : String(rawValue).trim();
    if (!trimmedValue) {
      if (entry.required) {
        missingFields.push(entry.label || entry.field);
      }
      continue;
    }

    if (entry.type === "number") {
      const parsed = Number(trimmedValue);
      if (Number.isNaN(parsed)) {
        throw new Error(`${entry.label || entry.field} must be a number.`);
      }
      payload[entry.field] = parsed;
      continue;
    }

    if (entry.type === "boolean") {
      payload[entry.field] = trimmedValue.toLowerCase() === "true";
      continue;
    }

    payload[entry.field] = rawValue;
  }

  if (missingFields.length > 0) {
    throw new Error(
      `Complete the required field${missingFields.length === 1 ? "" : "s"}: ${missingFields.join(", ")}.`,
    );
  }

  return payload;
}

function buildConnectorEditFormState(detail, schema) {
  const connection = detail?.connection && typeof detail.connection === "object" ? detail.connection : {};
  return {
    description: detail?.description || "",
    configValues: buildConnectorConfigValues(schema, connection),
    metadataText: stringifyJsonInput(detail?.metadata || {}),
    secretsText: stringifyJsonInput(detail?.secrets || {}),
  };
}

function normalizeConnectorTypeName(value) {
  return String(value || "").trim().toUpperCase();
}

export function ConnectorsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload, setData } = useAsyncData(fetchConnectors);
  const {
    data: connectorTypePayload,
    loading: connectorTypesLoading,
    error: connectorTypesError,
  } = useAsyncData(fetchConnectorTypes);
  const connectors = Array.isArray(data?.items) ? data.items : [];
  const connectorTypes = Array.isArray(connectorTypePayload?.items)
    ? connectorTypePayload.items
    : [];
  const selected = resolveItemByRef(connectors, params.id);
  const availableConnectorTypes = useMemo(() => connectorTypes, [connectorTypes]);
  const [connectorSchemasByType, setConnectorSchemasByType] = useState({});
  const [connectorSchemaErrors, setConnectorSchemaErrors] = useState({});
  const [connectorSchemaLoading, setConnectorSchemaLoading] = useState({});
  const pendingConnectorSchemaRequests = useRef(new Map());
  const lastInitializedCreateType = useRef("");
  const lastInitializedEditState = useRef("");
  const connectorFamilyOptions = useMemo(() => {
    const seen = new Set();
    const families = [];
    for (const item of availableConnectorTypes) {
      const family = normalizeConnectorFamily(item?.family);
      if (!family || seen.has(family)) {
        continue;
      }
      seen.add(family);
      families.push({
        value: family,
        label: formatConnectorFamilyLabel(family),
      });
    }
    return families;
  }, [availableConnectorTypes]);
  const filteredConnectors = connectors.filter((item) => {
    const haystack = [
      item.name,
      item.description,
      item.connector_type,
      item.connector_family,
      item.management_mode,
      ...(Array.isArray(item.supported_resources) ? item.supported_resources : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [showCreate, setShowCreate] = useState(false);
  const [createForm, setCreateForm] = useState(buildConnectorFormState);
  const [createSubmitting, setCreateSubmitting] = useState(false);
  const [createError, setCreateError] = useState("");
  const [createSuccess, setCreateSuccess] = useState("");
  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [showEdit, setShowEdit] = useState(false);
  const [editForm, setEditForm] = useState(buildConnectorEditFormState(null));
  const [editSubmitting, setEditSubmitting] = useState(false);
  const [editError, setEditError] = useState("");
  const [editSuccess, setEditSuccess] = useState("");
  const [deleteSubmitting, setDeleteSubmitting] = useState(false);
  const [deleteError, setDeleteError] = useState("");

  const [resources, setResources] = useState({ items: [], loading: false, error: "" });
  const [states, setStates] = useState({ items: [], loading: false, error: "" });
  const [selectedResources, setSelectedResources] = useState([]);
  const [syncMode, setSyncMode] = useState("INCREMENTAL");
  const [forceFullRefresh, setForceFullRefresh] = useState(false);
  const [syncResult, setSyncResult] = useState(null);
  const [syncing, setSyncing] = useState(false);
  const [syncError, setSyncError] = useState("");
  const loadConnectorSchema = useCallback(
    async (connectorType) => {
      const normalizedType = normalizeConnectorTypeName(connectorType);
      if (!normalizedType) {
        return null;
      }
      if (connectorSchemasByType[normalizedType]) {
        return connectorSchemasByType[normalizedType];
      }
      const pending = pendingConnectorSchemaRequests.current.get(normalizedType);
      if (pending) {
        return pending;
      }

      setConnectorSchemaLoading((current) => ({ ...current, [normalizedType]: true }));
      setConnectorSchemaErrors((current) => ({ ...current, [normalizedType]: "" }));

      const request = fetchConnectorTypeConfig(normalizedType)
        .then((schema) => {
          setConnectorSchemasByType((current) => {
            if (current[normalizedType]) {
              return current;
            }
            return { ...current, [normalizedType]: schema };
          });
          return schema;
        })
        .catch((caughtError) => {
          const message = getErrorMessage(caughtError);
          setConnectorSchemaErrors((current) => ({ ...current, [normalizedType]: message }));
          throw caughtError;
        })
        .finally(() => {
          pendingConnectorSchemaRequests.current.delete(normalizedType);
          setConnectorSchemaLoading((current) => ({ ...current, [normalizedType]: false }));
        });

      pendingConnectorSchemaRequests.current.set(normalizedType, request);
      return request;
    },
    [connectorSchemasByType],
  );
  const filteredConnectorTypes = useMemo(
    () =>
      availableConnectorTypes.filter((item) => {
        if (!createForm.family) {
          return true;
        }
        return normalizeConnectorFamily(item?.family) === createForm.family;
      }),
    [availableConnectorTypes, createForm.family],
  );
  const selectedCreateConnectorType = useMemo(
    () =>
      availableConnectorTypes.find(
        (item) =>
          normalizeConnectorTypeName(item?.name) === normalizeConnectorTypeName(createForm.type),
      ) || null,
    [availableConnectorTypes, createForm.type],
  );
  const selectedCreateConnectorSchema =
    connectorSchemasByType[normalizeConnectorTypeName(createForm.type)] || null;
  const selectedCreateConnectorSchemaError =
    connectorSchemaErrors[normalizeConnectorTypeName(createForm.type)] || "";
  const selectedCreateConnectorSchemaLoading = Boolean(
    connectorSchemaLoading[normalizeConnectorTypeName(createForm.type)],
  );
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
  const selectedDetail = detail || selected;
  const selectedEditConnectorSchema =
    connectorSchemasByType[normalizeConnectorTypeName(detail?.connector_type)] || null;
  const selectedEditConnectorSchemaError =
    connectorSchemaErrors[normalizeConnectorTypeName(detail?.connector_type)] || "";
  const selectedEditConnectorSchemaLoading = Boolean(
    connectorSchemaLoading[normalizeConnectorTypeName(detail?.connector_type)],
  );

  useEffect(() => {
    if (!showCreate) {
      lastInitializedCreateType.current = "";
      return;
    }
    setCreateForm((current) => {
      if (availableConnectorTypes.length === 0) {
        return current;
      }
      let nextFamily = current.family;
      if (connectorFamilyOptions.length > 0) {
        const familyExists = connectorFamilyOptions.some((option) => option.value === current.family);
        if (!familyExists) {
          nextFamily = connectorFamilyOptions[0].value;
        }
      } else if (nextFamily) {
        nextFamily = "";
      }
      const nextTypes = availableConnectorTypes.filter((item) => {
        if (!nextFamily) {
          return true;
        }
        return normalizeConnectorFamily(item?.family) === nextFamily;
      });
      const nextType =
        nextTypes.find(
          (item) =>
            normalizeConnectorTypeName(item?.name) === normalizeConnectorTypeName(current.type),
        )?.name || nextTypes[0]?.name || "";
      const normalizedNextType = normalizeConnectorTypeName(nextType);
      if (nextFamily === current.family && normalizedNextType === current.type) {
        return current;
      }
      return {
        ...current,
        family: nextFamily,
        type: normalizedNextType,
        configValues: normalizedNextType !== current.type ? {} : current.configValues,
      };
    });
  }, [availableConnectorTypes, connectorFamilyOptions, showCreate]);

  useEffect(() => {
    if (!showCreate || !createForm.type) {
      return;
    }
    void loadConnectorSchema(createForm.type).catch(() => undefined);
  }, [createForm.type, loadConnectorSchema, showCreate]);

  useEffect(() => {
    if (!showCreate) {
      return;
    }
    const normalizedType = normalizeConnectorTypeName(createForm.type);
    if (!normalizedType || !selectedCreateConnectorSchema) {
      return;
    }
    if (lastInitializedCreateType.current === normalizedType) {
      return;
    }
    setCreateForm((current) => ({
      ...current,
      configValues: buildConnectorConfigValues(selectedCreateConnectorSchema),
    }));
    lastInitializedCreateType.current = normalizedType;
  }, [createForm.type, selectedCreateConnectorSchema, showCreate]);

  useEffect(() => {
    let cancelled = false;

    async function loadConnectorDetail() {
      if (!selected?.name) {
        setDetail(null);
        setDetailError("");
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchConnector(selected.name);
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

    void loadConnectorDetail();

    return () => {
      cancelled = true;
    };
  }, [selected?.name]);

  useEffect(() => {
    if (!detail?.connector_type) {
      return;
    }
    void loadConnectorSchema(detail.connector_type).catch(() => undefined);
  }, [detail?.connector_type, loadConnectorSchema]);

  useEffect(() => {
    if (!showEdit || !detail) {
      lastInitializedEditState.current = "";
      return;
    }
    const normalizedType = normalizeConnectorTypeName(detail.connector_type);
    if (!normalizedType || !selectedEditConnectorSchema) {
      return;
    }
    const detailKey = `${detail.name}:${normalizedType}`;
    if (lastInitializedEditState.current === detailKey) {
      return;
    }
    setEditForm(buildConnectorEditFormState(detail, selectedEditConnectorSchema));
    lastInitializedEditState.current = detailKey;
  }, [detail, selectedEditConnectorSchema, showEdit]);

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

  function resetCreateForm() {
    lastInitializedCreateType.current = "";
    setCreateForm(buildConnectorFormState());
    setCreateError("");
  }

  function resetEditForm(nextDetail = detail, schema = selectedEditConnectorSchema) {
    lastInitializedEditState.current =
      nextDetail?.name && schema
        ? `${nextDetail.name}:${normalizeConnectorTypeName(nextDetail.connector_type)}`
        : "";
    setEditForm(buildConnectorEditFormState(nextDetail, schema));
    setEditError("");
  }

  function handleConnectorTypeChange(nextType) {
    const normalizedType = normalizeConnectorTypeName(nextType);
    const selectedType = availableConnectorTypes.find(
      (item) => normalizeConnectorTypeName(item?.name) === normalizedType,
    );
    setCreateForm((current) => ({
      ...current,
      family: selectedType ? normalizeConnectorFamily(selectedType.family) : current.family,
      type: normalizedType,
      configValues: {},
    }));
  }

  function handleConnectorFamilyChange(nextFamily) {
    const nextTypes = availableConnectorTypes.filter((item) => {
      if (!nextFamily) {
        return true;
      }
      return normalizeConnectorFamily(item?.family) === nextFamily;
    });
    const nextType = normalizeConnectorTypeName(nextTypes[0]?.name);
    setCreateForm((current) => ({
      ...current,
      family: nextFamily,
      type: nextType,
      configValues: nextType && nextType !== current.type ? {} : current.configValues,
    }));
  }

  async function handleCreateConnector(event) {
    event.preventDefault();
    setCreateSubmitting(true);
    setCreateError("");
    setCreateSuccess("");

    try {
      const normalizedName = String(createForm.name || "").trim();
      if (!normalizedName) {
        throw new Error("Connector name is required.");
      }
      const normalizedType = normalizeConnectorTypeName(createForm.type);
      if (!normalizedType) {
        throw new Error("Connector type is required.");
      }
      if (!selectedCreateConnectorSchema) {
        throw new Error("Connector schema is unavailable for the selected connector type.");
      }

      const payload = {
        name: normalizedName,
        type: normalizedType,
      };

      const description = String(createForm.description || "").trim();
      if (description) {
        payload.description = description;
      }

      payload.connection = buildConnectorConnectionPayload(
        selectedCreateConnectorSchema,
        createForm.configValues,
      );

      const metadata = parseJsonObjectInput(createForm.metadataText, "Metadata JSON", {});
      const secrets = parseJsonObjectInput(createForm.secretsText, "Secrets JSON", {});
      if (Object.keys(metadata).length > 0) {
        payload.metadata = metadata;
      }
      if (Object.keys(secrets).length > 0) {
        payload.secrets = secrets;
      }

      const created = await createConnector(payload);
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
      setCreateSuccess(`${created.name} is available as a runtime_managed connector.`);
      setShowCreate(false);
      resetCreateForm();
      navigate(`/connectors/${buildItemRef(created)}`);
      void reload();
    } catch (caughtError) {
      setCreateError(getErrorMessage(caughtError));
    } finally {
      setCreateSubmitting(false);
    }
  }

  function beginEditConnector() {
    lastInitializedEditState.current = "";
    resetEditForm(detail, selectedEditConnectorSchema);
    setShowEdit(true);
    setShowCreate(false);
    setEditSuccess("");
    setDeleteError("");
  }

  async function handleUpdateConnector(event) {
    event.preventDefault();
    if (!detail?.name) {
      return;
    }
    setEditSubmitting(true);
    setEditError("");
    setEditSuccess("");
    setDeleteError("");

    try {
      const payload = {};
      payload.description = String(editForm.description || "").trim() || null;
      if (!selectedEditConnectorSchema) {
        throw new Error("Connector schema is unavailable for this connector type.");
      }
      payload.connection = buildConnectorConnectionPayload(
        selectedEditConnectorSchema,
        editForm.configValues,
      );
      payload.metadata = parseJsonObjectInput(editForm.metadataText, "Metadata JSON", {});
      payload.secrets = parseJsonObjectInput(editForm.secretsText, "Secrets JSON", {});

      const updated = await updateConnector(detail.name, payload);
      setDetail(updated);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = items.map((item) =>
          String(item?.id || item?.name) === String(updated?.id || updated?.name)
            ? { ...item, ...updated }
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

  async function handleDeleteConnector() {
    if (!detail?.name || deleteSubmitting) {
      return;
    }
    const confirmed = window.confirm(
      `Delete runtime-managed connector '${detail.name}'? This cannot be undone.`,
    );
    if (!confirmed) {
      return;
    }
    setDeleteSubmitting(true);
    setDeleteError("");
    setEditSuccess("");
    try {
      await deleteConnector(detail.name);
      setDetail(null);
      setShowEdit(false);
      setData((current) => {
        const items = Array.isArray(current?.items) ? current.items : [];
        const nextItems = items.filter(
          (item) => String(item?.id || item?.name) !== String(detail?.id || detail?.name),
        );
        return { items: nextItems, total: nextItems.length };
      });
      navigate("/connectors");
      void reload();
    } catch (caughtError) {
      setDeleteError(getErrorMessage(caughtError));
    } finally {
      setDeleteSubmitting(false);
    }
  }

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
              {showCreate ? "Close create flow" : "Create runtime-managed connector"}
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
          placeholder="Filter connectors by name, type, or resource"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh connectors"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Connector inventory" className="compact-panel">
          <ManagementModeNotice
            mode={selected?.management_mode || "config_managed"}
            resourceLabel="Connector ownership"
          />
          {filteredConnectors.length > 0 ? (
            <div className="stack-list">
              {filteredConnectors.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/connectors/${buildItemRef(item)}`}
                >
                  <div className="list-card-topline">
                    <strong>{item.name}</strong>
                    <ManagementBadge mode={item.management_mode} />
                  </div>
                  <span>
                    {[
                      item.connector_family ? formatConnectorFamilyLabel(item.connector_family) : null,
                      item.connector_type,
                      item.supports_sync ? "sync enabled" : "query only",
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
              title="No connectors found"
              message="Adjust the filter or create a runtime-managed connector."
            />
          )}
        </Panel>

        <div className="detail-stack">
          {createSuccess ? (
            <div className="callout success">
              <strong>Connector created</strong>
              <span>{createSuccess}</span>
            </div>
          ) : null}
          {editSuccess ? (
            <div className="callout success">
              <strong>Connector updated</strong>
              <span>{editSuccess}</span>
            </div>
          ) : null}

          {showCreate ? (
            <Panel
              title="Create runtime-managed connector"
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
              <ManagementModeNotice mode="runtime_managed" resourceLabel="New connectors" />
              <form className="form-grid" onSubmit={handleCreateConnector}>
                <label className="field">
                  <span>Name</span>
                  <input
                    className="text-input"
                    type="text"
                    value={createForm.name}
                    onChange={(event) =>
                      setCreateForm((current) => ({ ...current, name: event.target.value }))
                    }
                    placeholder="runtime_demo"
                    disabled={createSubmitting}
                  />
                </label>

                <label className="field">
                  <span>Connector family</span>
                  <select
                    className="select-input"
                    value={createForm.family}
                    onChange={(event) => handleConnectorFamilyChange(event.target.value)}
                    disabled={createSubmitting || connectorFamilyOptions.length === 0}
                  >
                    {connectorFamilyOptions.length === 0 ? (
                      <option value="">
                        {connectorTypesLoading ? "Loading families..." : "No families available"}
                      </option>
                    ) : (
                      connectorFamilyOptions.map((family) => (
                        <option key={family.value} value={family.value}>
                          {family.label}
                        </option>
                      ))
                    )}
                  </select>
                  <small className="field-hint">
                    Pick a family first to narrow the connector types exposed by the runtime catalog.
                  </small>
                </label>

                <label className="field">
                  <span>Connector type</span>
                  <select
                    className="select-input"
                    value={createForm.type}
                    onChange={(event) => handleConnectorTypeChange(event.target.value)}
                    disabled={createSubmitting || filteredConnectorTypes.length === 0}
                  >
                    {filteredConnectorTypes.length === 0 ? (
                      <option value="">No connector types available</option>
                    ) : (
                      filteredConnectorTypes.map((type) => {
                        const name = normalizeConnectorTypeName(type?.name);
                        return (
                          <option key={name} value={name}>
                            {type?.label || name}
                          </option>
                        );
                      })
                    )}
                  </select>
                  <small className="field-hint">
                    {selectedCreateConnectorType
                      ? [
                          formatConnectorFamilyLabel(selectedCreateConnectorType.family),
                          selectedCreateConnectorType.supports_sync ? "sync enabled" : "query only",
                        ].join(" | ")
                      : "Select a connector type."}
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
                    placeholder="Short operator-friendly description"
                    disabled={createSubmitting}
                  />
                  <small className="field-hint">
                    Runtime-managed connectors are created locally for this runtime and stay distinct from config-managed entries.
                  </small>
                </label>

                {connectorTypesError ? (
                  <div className="callout warning field-full">
                    <strong>Connector type catalog unavailable</strong>
                    <span>{connectorTypesError}. The runtime cannot build connector forms until the catalog loads.</span>
                  </div>
                ) : null}

                <div className="field field-full">
                  <span>Connection settings</span>
                  <small className="field-hint">
                    {selectedCreateConnectorSchema?.description
                      || "The runtime builds these fields from the connector config schema."}
                  </small>
                </div>

                {selectedCreateConnectorSchemaLoading ? (
                  <div className="empty-box field-full">Loading connector schema...</div>
                ) : selectedCreateConnectorSchemaError ? (
                  <div className="callout warning field-full">
                    <strong>Connector schema unavailable</strong>
                    <span>{selectedCreateConnectorSchemaError}</span>
                  </div>
                ) : selectedCreateConnectorSchema ? (
                  renderConnectorConfigFields({
                    entries: selectedCreateConnectorSchema.config,
                    values: createForm.configValues,
                    disabled: createSubmitting,
                    onChange: (field, value) =>
                      setCreateForm((current) => ({
                        ...current,
                        configValues: {
                          ...current.configValues,
                          [field]: value,
                        },
                      })),
                  })
                ) : (
                  <div className="callout warning field-full">
                    <strong>No schema loaded</strong>
                    <span>Select a connector type to load its runtime config definition.</span>
                  </div>
                )}

                <label className="field field-full">
                  <span>Metadata JSON</span>
                  <textarea
                    className="textarea-input"
                    value={createForm.metadataText}
                    onChange={(event) =>
                      setCreateForm((current) => ({
                        ...current,
                        metadataText: event.target.value,
                      }))
                    }
                    disabled={createSubmitting}
                  />
                  <small className="field-hint">
                    Optional metadata merged into connector validation for connectors that need extra non-secret fields.
                  </small>
                </label>

                <label className="field field-full">
                  <span>Secrets JSON</span>
                  <textarea
                    className="textarea-input"
                    value={createForm.secretsText}
                    onChange={(event) =>
                      setCreateForm((current) => ({
                        ...current,
                        secretsText: event.target.value,
                      }))
                    }
                    disabled={createSubmitting}
                  />
                  <small className="field-hint">
                    Optional secret reference map when the runtime should resolve credentials from its configured secret providers.
                  </small>
                </label>

                {createError ? <div className="error-banner field-full">{createError}</div> : null}
                <div className="page-actions field-full">
                  <button className="primary-button" type="submit" disabled={createSubmitting}>
                    {createSubmitting ? "Creating connector..." : "Create connector"}
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
              <ManagementModeNotice mode="runtime_managed" resourceLabel="Editable connector" />
              <form className="form-grid" onSubmit={handleUpdateConnector}>
                <label className="field">
                  <span>Name</span>
                  <input className="text-input" type="text" value={detail.name} disabled />
                </label>

                <label className="field">
                  <span>Connector type</span>
                  <input className="text-input" type="text" value={detail.connector_type || ""} disabled />
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

                <div className="field field-full">
                  <span>Connection settings</span>
                  <small className="field-hint">
                    {selectedEditConnectorSchema?.description
                      || "The runtime builds these fields from the connector config schema."}
                  </small>
                </div>

                {selectedEditConnectorSchemaLoading ? (
                  <div className="empty-box field-full">Loading connector schema...</div>
                ) : selectedEditConnectorSchemaError ? (
                  <div className="callout warning field-full">
                    <strong>Connector schema unavailable</strong>
                    <span>{selectedEditConnectorSchemaError}</span>
                  </div>
                ) : selectedEditConnectorSchema ? (
                  renderConnectorConfigFields({
                    entries: selectedEditConnectorSchema.config,
                    values: editForm.configValues,
                    disabled: editSubmitting,
                    onChange: (field, value) =>
                      setEditForm((current) => ({
                        ...current,
                        configValues: {
                          ...current.configValues,
                          [field]: value,
                        },
                      })),
                  })
                ) : (
                  <div className="callout warning field-full">
                    <strong>No schema loaded</strong>
                    <span>The runtime could not load the connector config definition for editing.</span>
                  </div>
                )}

                <label className="field field-full">
                  <span>Metadata JSON</span>
                  <textarea
                    className="textarea-input"
                    value={editForm.metadataText}
                    onChange={(event) =>
                      setEditForm((current) => ({ ...current, metadataText: event.target.value }))
                    }
                    disabled={editSubmitting}
                  />
                </label>

                <label className="field field-full">
                  <span>Secrets JSON</span>
                  <textarea
                    className="textarea-input"
                    value={editForm.secretsText}
                    onChange={(event) =>
                      setEditForm((current) => ({ ...current, secretsText: event.target.value }))
                    }
                    disabled={editSubmitting}
                  />
                </label>

                {editError ? <div className="error-banner field-full">{editError}</div> : null}
                <div className="settings-form-actions field-full">
                  <button className="primary-button" type="submit" disabled={editSubmitting}>
                    {editSubmitting ? "Saving..." : "Save connector"}
                  </button>
                  <button
                    className="ghost-button danger-button"
                    type="button"
                    onClick={() => void handleDeleteConnector()}
                    disabled={deleteSubmitting || editSubmitting}
                  >
                    {deleteSubmitting ? "Deleting..." : "Delete connector"}
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
                    <ManagementBadge mode={selected.management_mode} />
                    <span className="chip">{selected.connector_type || "runtime connector"}</span>
                    {selectedDetail?.management_mode === "runtime_managed" ? (
                      <button className="ghost-button" type="button" onClick={beginEditConnector}>
                        Edit
                      </button>
                    ) : null}
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
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {deleteError ? <div className="error-banner">{deleteError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading connector detail...</div>
                ) : null}
                <div className="inline-notes">
                  <span>{formatConnectorFamilyLabel(selectedDetail?.connector_family)}</span>
                  <span>{selectedDetail?.supports_sync ? "Sync workflow available" : "Query-only connector"}</span>
                  <span>{selectedDetail?.sync_strategy || "No sync strategy advertised"}</span>
                  <span>{describeManagementMode(selectedDetail?.management_mode)}</span>
                </div>
                <DetailList
                  items={[
                    { label: "Description", value: formatValue(selectedDetail?.description) },
                    {
                      label: "Family",
                      value: formatValue(
                        selectedDetail?.connector_family
                          ? formatConnectorFamilyLabel(selectedDetail.connector_family)
                          : null,
                      ),
                    },
                    { label: "Supports sync", value: formatValue(selectedDetail?.supports_sync) },
                    { label: "Sync strategy", value: formatValue(selectedDetail?.sync_strategy) },
                    {
                      label: "Management mode",
                      value: formatValue(selectedDetail?.management_mode),
                    },
                    { label: "Supported resources", value: formatList(selectedDetail?.supported_resources) },
                  ]}
                />
              </Panel>

              <ManagementModeNotice mode={selectedDetail?.management_mode} resourceLabel={selected.name} />

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
                              Runtime sync stays local and single-workspace. Dataset materialization is still runtime-owned, not a cloud orchestration flow.
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
                    message="This connector does not expose runtime sync resources, so the runtime UI keeps creation and inspection focused on runtime ownership rather than fake edit controls."
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

function renderConnectorConfigFields({ entries, values, onChange, disabled }) {
  return entries.map((entry) => {
    const value = values?.[entry.field] ?? "";
    const label = entry.label || entry.field;
    const inputId = `connector-config-${entry.field}`;

    if (Array.isArray(entry.value_list) && entry.value_list.length > 0) {
      return (
        <label key={entry.field} className="field">
          <span>{label}</span>
          <select
            id={inputId}
            className="select-input"
            value={value}
            onChange={(event) => onChange(entry.field, event.target.value)}
            disabled={disabled}
          >
            <option value="">Select {label}</option>
            {entry.value_list.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
          <small className="field-hint">
            {entry.description}
            {entry.required ? " Required." : ""}
          </small>
        </label>
      );
    }

    if (entry.type === "boolean") {
      return (
        <label key={entry.field} className="field">
          <span>{label}</span>
          <select
            id={inputId}
            className="select-input"
            value={value}
            onChange={(event) => onChange(entry.field, event.target.value)}
            disabled={disabled}
          >
            <option value="">Select {label}</option>
            <option value="true">True</option>
            <option value="false">False</option>
          </select>
          <small className="field-hint">
            {entry.description}
            {entry.required ? " Required." : ""}
          </small>
        </label>
      );
    }

    if (entry.type === "textarea") {
      return (
        <label key={entry.field} className="field field-full">
          <span>{label}</span>
          <textarea
            id={inputId}
            className="textarea-input"
            value={value}
            onChange={(event) => onChange(entry.field, event.target.value)}
            disabled={disabled}
          />
          <small className="field-hint">
            {entry.description}
            {entry.required ? " Required." : ""}
          </small>
        </label>
      );
    }

    return (
      <label
        key={entry.field}
        className={`field ${entry.type === "password" ? "field-full" : ""}`.trim()}
      >
        <span>{label}</span>
        <input
          id={inputId}
          className="text-input"
          type={entry.type === "password" ? "password" : entry.type === "number" ? "number" : "text"}
          value={value}
          onChange={(event) => onChange(entry.field, event.target.value)}
          placeholder={entry.default || ""}
          disabled={disabled}
        />
        <small className="field-hint">
          {entry.description}
          {entry.required ? " Required." : ""}
        </small>
      </label>
    );
  });
}
