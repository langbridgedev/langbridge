import { useEffect, useMemo, useState } from "react";

import {
  LLM_PROVIDER_OPTIONS,
  STRUCTURED_OUTPUT_OPTIONS,
  buildLLMConnectionFormState,
  buildLLMConnectionPayload,
  isRuntimeManagedLLMConnection,
} from "../../features/configuration/llmConnectionModel.js";
import { classNames } from "../../utils/classNames.js";
import { DynamicFieldValue } from "../resources/DynamicFieldViewer.jsx";
import { ManagementPill } from "../resources/ManagementPill.jsx";
import { Modal } from "../ui/Modal.jsx";

const tabLabels = {
  overview: "Overview",
  connection: "Connection",
  actions: "Actions",
};

export function LLMConnectionWorkspaceModal({
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
  const canMutate = isCreateMode
    ? Boolean(capabilities?.canCreate)
    : Boolean(capabilities?.canUpdate && isRuntimeManagedLLMConnection(resource));
  const canDelete = Boolean(capabilities?.canDelete && isRuntimeManagedLLMConnection(resource));
  const [activeTab, setActiveTab] = useState(isCreateMode ? "connection" : "overview");
  const [formState, setFormState] = useState(() => buildLLMConnectionFormState(resource));
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [actionResult, setActionResult] = useState(null);
  const title = isCreateMode ? "Add LLM connection" : resource?.name || "LLM connection";
  const visibleTabs = useMemo(
    () => (isCreateMode ? ["connection"] : ["overview", "connection", "actions"]),
    [isCreateMode],
  );

  useEffect(() => {
    setActiveTab(isCreateMode ? "connection" : "overview");
    setFormState(buildLLMConnectionFormState(resource));
    setError("");
    setActionResult(null);
  }, [isCreateMode, resource?.id]);

  function updateField(field, value) {
    setFormState((current) => ({ ...current, [field]: value }));
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const payload = buildLLMConnectionPayload(formState, { mode: isCreateMode ? "create" : "update" });
      const nextResource = isCreateMode ? await onCreate(payload) : await onUpdate(payload);
      setActionResult({
        label: isCreateMode ? "Connection created" : "Connection updated",
        payload: nextResource,
      });
      if (isCreateMode) {
        setActiveTab("overview");
      }
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to save LLM connection.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm(`Delete ${resource?.name || "this LLM connection"}? This cannot be undone.`)) {
      return;
    }
    setError("");
    setSubmitting(true);
    try {
      await onDelete();
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to delete LLM connection.");
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
            <p className="eyebrow">{isCreateMode ? "Create runtime LLM connection" : "Opened LLM connection"}</p>
            <h3>{title}</h3>
            <span>
              {isCreateMode
                ? "Add a runtime-managed model provider. The API key is write-only and will not be returned by the API."
                : resource?.subtitle}
            </span>
          </div>
          {!isCreateMode && resource ? <ManagementPill mode={resource.management} /> : null}
        </header>

        <nav className="resource-workspace-tabs" aria-label="LLM connection tabs">
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
        {detailLoading ? <div className="resource-loading">Loading LLM connection detail...</div> : null}

        {activeTab === "overview" && resource ? <LLMConnectionOverview resource={resource} /> : null}

        {activeTab === "connection" ? (
          <section className="resource-editor-panel">
            {!canMutate && !isCreateMode ? <ReadOnlyNotice resource={resource} /> : null}
            <form className="connector-form" onSubmit={handleSubmit}>
              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Provider</h4>
                    <p>Name and provider are immutable after creation. Replace the API key only when rotating credentials.</p>
                  </div>
                </div>
                <div className="connector-field-grid">
                  <label className="connector-field">
                    <span>Name</span>
                    <input
                      value={formState.name}
                      disabled={!isCreateMode || !canMutate}
                      onChange={(event) => updateField("name", event.target.value)}
                      placeholder="local_openai"
                    />
                  </label>
                  <label className="connector-field">
                    <span>Provider</span>
                    <select
                      value={formState.provider}
                      disabled={!isCreateMode || !canMutate}
                      onChange={(event) => updateField("provider", event.target.value)}
                    >
                      {LLM_PROVIDER_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>{option.label}</option>
                      ))}
                    </select>
                  </label>
                  <label className="connector-field">
                    <span>Model</span>
                    <input
                      value={formState.model}
                      disabled={!canMutate}
                      onChange={(event) => updateField("model", event.target.value)}
                      placeholder="gpt-4.1-mini"
                    />
                  </label>
                  <label className="connector-field">
                    <span>API key <i>write-only</i></span>
                    <input
                      type="password"
                      value={formState.apiKey}
                      disabled={!canMutate}
                      onChange={(event) => updateField("apiKey", event.target.value)}
                      placeholder={isCreateMode ? "Required except for Ollama" : "Leave blank to keep existing key"}
                    />
                    <small>The runtime never returns API keys. Enter a value only to set or rotate it.</small>
                  </label>
                  <label className="connector-field connector-field--full">
                    <span>Description</span>
                    <input
                      value={formState.description}
                      disabled={!canMutate}
                      onChange={(event) => updateField("description", event.target.value)}
                      placeholder="Runtime-managed model provider"
                    />
                  </label>
                </div>
              </section>

              <section className="connector-form-section">
                <div className="connector-form-section-head">
                  <div>
                    <h4>Structured output</h4>
                    <p>Langbridge sends structured-first requests. Providers use native structured output when supported, then fall back to JSON extraction.</p>
                  </div>
                </div>
                <div className="connector-field-grid">
                  <label className="connector-field">
                    <span>Mode</span>
                    <select
                      value={formState.structuredOutputs}
                      disabled={!canMutate}
                      onChange={(event) => updateField("structuredOutputs", event.target.value)}
                    >
                      {STRUCTURED_OUTPUT_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>{option.label}</option>
                      ))}
                    </select>
                  </label>
                  <label className="connector-field">
                    <span>Base URL</span>
                    <input
                      value={formState.baseUrl}
                      disabled={!canMutate}
                      onChange={(event) => updateField("baseUrl", event.target.value)}
                      placeholder="https://api.openai.com/v1"
                    />
                  </label>
                  <label className="connector-field">
                    <span>Active</span>
                    <select
                      value={String(formState.isActive)}
                      disabled={!canMutate}
                      onChange={(event) => updateField("isActive", event.target.value === "true")}
                    >
                      <option value="true">Active</option>
                      <option value="false">Disabled</option>
                    </select>
                  </label>
                  <label className="connector-field">
                    <span>Default</span>
                    <select
                      value={String(formState.default)}
                      disabled={!canMutate}
                      onChange={(event) => updateField("default", event.target.value === "true")}
                    >
                      <option value="false">No</option>
                      <option value="true">Yes</option>
                    </select>
                    <small>Setting this clears the default flag from other LLM connections in this workspace.</small>
                  </label>
                  <label className="connector-field connector-field--full">
                    <span>Advanced configuration JSON</span>
                    <textarea
                      value={formState.configurationText}
                      disabled={!canMutate}
                      spellCheck="false"
                      onChange={(event) => updateField("configurationText", event.target.value)}
                    />
                    <small>Do not put secrets here. Use the write-only API key field for credentials.</small>
                  </label>
                </div>
              </section>

              <div className="resource-editor-actions">
                <button type="submit" disabled={!canMutate || submitting}>
                  {submitting ? "Saving..." : isCreateMode ? "Create connection" : "Save changes"}
                </button>
                {!isCreateMode && canDelete ? (
                  <button className="danger" type="button" disabled={submitting} onClick={handleDelete}>
                    Delete
                  </button>
                ) : null}
              </div>
            </form>
            <ActionResult result={actionResult} />
          </section>
        ) : null}

        {activeTab === "actions" && resource ? (
          <section className="resource-actions-panel">
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

function LLMConnectionOverview({ resource }) {
  return (
    <div className="config-resource-detail">
      <div className="resource-meta-grid">
        <div><span>Status</span><strong>{resource.status}</strong></div>
        <div><span>Provider</span><strong>{resource.rawPayload?.provider || "n/a"}</strong></div>
        <div><span>Updated</span><strong>{resource.lastUpdated}</strong></div>
      </div>

      <div className="resource-state-grid">
        <ResourceSection title="Runtime state" rows={resource.runtimeState} />
        <ResourceSection title="Configuration" rows={resource.configDefinition} />
      </div>

      <div className="resource-detail-block">
        <h4>Agent usage</h4>
        <div className="resource-chip-row">
          {(resource.relationships || []).map((item, index) => (
            <span key={`${String(item)}-${index}`}>{item}</span>
          ))}
        </div>
      </div>

      <div className="resource-detail-block">
        <h4>Detail</h4>
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

function ReadOnlyNotice({ resource }) {
  if (!resource || isRuntimeManagedLLMConnection(resource)) {
    return null;
  }
  return (
    <div className="resource-readonly-notice">
      <strong>Config-managed LLM connection</strong>
      <span>This connection is loaded from runtime configuration. You can inspect and test it here, but edits and deletes are disabled.</span>
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
