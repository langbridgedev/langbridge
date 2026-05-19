import { useEffect, useMemo, useState } from "react";

import { ResourceList } from "../../components/resources/ResourceList.jsx";
import { ResourceWorkspaceModal } from "../../components/resources/ResourceWorkspaceModal.jsx";
import { AgentWorkspaceModal } from "../../components/agents/AgentWorkspaceModal.jsx";
import { ConnectorConfigurationList } from "../../components/connectors/ConnectorConfigurationList.jsx";
import { ConnectorWorkspaceModal } from "../../components/connectors/ConnectorWorkspaceModal.jsx";
import { DatasetWorkspaceModal } from "../../components/datasets/DatasetWorkspaceModal.jsx";
import { LLMConnectionWorkspaceModal } from "../../components/llm-connections/LLMConnectionWorkspaceModal.jsx";
import { SemanticModelWorkspaceModal } from "../../components/semantic-models/SemanticModelWorkspaceModal.jsx";
import { SecurityManagementPage } from "./SecurityManagementPage.jsx";
import {
  createConfigurationResource,
  deleteConfigurationResource,
  // getConfigurationCopy,
  getConfigurationResource,
  getCreateTemplate,
  getResourceActions,
  getSectionCapabilities,
  getUpdateTemplate,
  listConfigurationResources,
  listConfigurationSections,
  runConfigurationResourceAction,
  updateConfigurationResource,
} from "../../services/configurationService.js";
import { classNames } from "../../utils/classNames.js";

const defaultSection = "connectors";

export function ConfigurationPage({ section = defaultSection, navigate, authStatus, session }) {
  const [sections, setSections] = useState([]);
  const [resourceCache, setResourceCache] = useState({});
  const [activeSection, setActiveSection] = useState(null);
  const [loadingResources, setLoadingResources] = useState(false);
  const [activeResourceId, setActiveResourceId] = useState(null);
  const [activeResource, setActiveResource] = useState(null);
  const [resourceLoading, setResourceLoading] = useState(false);
  const [workspaceMode, setWorkspaceMode] = useState(null);
  const [resourceListError, setResourceListError] = useState("");
  const [resourceDetailError, setResourceDetailError] = useState("");
  const activeSectionId = activeSection?.id || "";
  const resources = useMemo(
    () => (activeSectionId && activeSectionId !== "security" ? resourceCache[activeSectionId] || [] : []),
    [activeSectionId, resourceCache],
  );
  const capabilities = useMemo(() => getSectionCapabilities(activeSectionId), [activeSectionId]);
  const createTemplate = useMemo(() => getCreateTemplate(activeSectionId), [activeSectionId]);
  const updateTemplate = useMemo(
    () => (activeResource ? getUpdateTemplate(activeSectionId, activeResource) : {}),
    [activeResource, activeSectionId],
  );
  const resourceActions = useMemo(
    () => (activeResource ? getResourceActions(activeSectionId, activeResource) : []),
    [activeResource, activeSectionId],
  );

  useEffect(() => {
    setActiveSection(sections.find((item) => item.id === section) || sections.find((item) => item.id === defaultSection) || null);
  }, [section, sections]);

  useEffect(() => {
    void listConfigurationSections().then(setSections);
  }, []);

  useEffect(() => {
    if (!activeSection) {
      return;
    }

    setActiveResourceId(null);
    setActiveResource(null);
    setResourceListError("");
    setResourceDetailError("");
    setWorkspaceMode(null);
    if (activeSection.id === "security") {
      setLoadingResources(false);
      return;
    }
    if (Object.hasOwn(resourceCache, activeSection.id)) {
      setLoadingResources(false);
      return;
    }
    void loadResources(activeSection.id);
  }, [activeSection]);

  async function loadResources(targetSection = activeSectionId, options = {}) {
    if (!options.force && Object.hasOwn(resourceCache, targetSection)) {
      return resourceCache[targetSection];
    }
    setLoadingResources(true);
    setResourceListError("");
    try {
      const loadedResources = await listConfigurationResources(targetSection);
      setResourceCache((current) => ({
        ...current,
        [targetSection]: loadedResources,
      }));
      return loadedResources;
    } catch (caughtError) {
      setResourceListError(caughtError?.message || "Unable to load runtime resources.");
      return [];
    } finally {
      setLoadingResources(false);
    }
  }

  async function openResource(resourceId) {
    const listResource = resources.find((resource) => resource.id === resourceId);
    setActiveResourceId(resourceId);
    setActiveResource(listResource || null);
    setResourceDetailError("");
    setWorkspaceMode("detail");
    if (!listResource) {
      return;
    }

    setResourceLoading(true);
    try {
      const detailed = await getConfigurationResource(activeSectionId, listResource);
      setActiveResource(detailed);
      mergeResource(detailed);
    } catch (caughtError) {
      setResourceDetailError(caughtError?.message || "Unable to load runtime detail.");
    } finally {
      setResourceLoading(false);
    }
  }

  function openCreateWorkspace() {
    setActiveResourceId(null);
    setActiveResource(null);
    setResourceDetailError("");
    setWorkspaceMode("create");
  }

  function closeWorkspace() {
    setWorkspaceMode(null);
    setActiveResourceId(null);
    setActiveResource(null);
    setResourceDetailError("");
  }

  async function handleCreate(payload) {
    const created = await createConfigurationResource(activeSectionId, payload);
    setSectionResources(activeSectionId, (current) => [
      created,
      ...current.filter((resource) => resource.id !== created.id),
    ]);
    setActiveResource(created);
    setActiveResourceId(created.id);
    setWorkspaceMode("detail");
    return created;
  }

  async function handleUpdate(payload) {
    const updated = await updateConfigurationResource(activeSectionId, activeResource, payload);
    setActiveResource(updated);
    mergeResource(updated);
    return updated;
  }

  async function handleDelete() {
    await deleteConfigurationResource(activeSectionId, activeResource);
    setSectionResources(activeSectionId, (current) =>
      current.filter((resource) => resource.id !== activeResource.id),
    );
    closeWorkspace();
  }

  async function handleAction(actionId) {
    const result = await runConfigurationResourceAction(activeSectionId, activeResource, actionId);
    if (actionId === "refresh_detail" && result?.id) {
      setActiveResource(result);
      mergeResource(result);
    }
    return result;
  }

  function mergeResource(nextResource) {
    setSectionResources(activeSectionId, (current) =>
      current.map((resource) =>
        resource.id === nextResource.id || resource.ref === nextResource.ref ? nextResource : resource,
      ),
    );
  }

  function setSectionResources(targetSection, updater) {
    if (!targetSection) {
      return;
    }
    setResourceCache((current) => {
      const currentResources = current[targetSection] || [];
      const nextResources = typeof updater === "function" ? updater(currentResources) : updater;
      return {
        ...current,
        [targetSection]: nextResources,
      };
    });
  }

  async function refreshActiveSection() {
    if (!activeSectionId || activeSectionId === "security") {
      return;
    }
    await loadResources(activeSectionId, { force: true });
    closeWorkspace();
  }

  function openSection(sectionId) {
    if (sectionId === activeSectionId) {
      return;
    }
    navigate(`/configure/${sectionId}`);
  }

  function sectionHasLoaded(sectionId) {
    return Object.hasOwn(resourceCache, sectionId);
  }

  function sectionResourceCount(sectionId) {
    const cachedResources = resourceCache[sectionId];
    return Array.isArray(cachedResources) ? cachedResources.length : null;
  }

  function tabDescription(item) {
    return `${item.description}`;
  }

  function renderRefreshButton() {
    if (activeSection.id === "security") {
      return null;
    }
    return (
      <button
        className="config-refresh-button"
        type="button"
        disabled={loadingResources}
        onClick={() => void refreshActiveSection()}
      >
        {loadingResources ? "Refreshing..." : "Refresh"}
      </button>
    );
  }

  if (!activeSection) {
    return <section className="empty-state"><p>Loading...</p></section>;
  }

  return (
    <section className="configure-page">
      <aside className="config-nav">
        <p className="eyebrow">Configure</p>
        {sections.map((item) => (
          <button
            key={item.id}
            className={classNames("config-nav-item", activeSectionId === item.id && "active")}
            type="button"
            onClick={() => openSection(item.id)}
          >
            <strong>{item.label}</strong>
            <span>{tabDescription(item)}</span>
          </button>
        ))}
      </aside>

      <div className="config-detail">
        <section className="config-management-header">
          <div>
            <p className="eyebrow">{activeSection?.label || "Configure"}</p>
            <h2>{activeSection?.label || "Resources"}</h2>
          </div>
          {activeSection.id === "security" ? null : (
            <div className="config-management-actions">
              <div className="management-legend">
                <span><i /> Runtime managed</span>
                <span><i /> Config managed</span>
              </div>
              {renderRefreshButton()}
            </div>
          )}
        </section>

        {resourceListError ? <div className="resource-error">{resourceListError}</div> : null}

        {activeSection.id === "security" ? (
          <SecurityManagementPage authStatus={authStatus} session={session} />
        ) : (
          <section className="config-resource-layout config-resource-layout--list">
          {activeSection.id === "connectors" ? (
            <ConnectorConfigurationList
              resources={resources}
              resourceLabel={activeSection?.label || "resources"}
              activeResourceId={activeResourceId}
              onOpenResource={(resourceId) => void openResource(resourceId)}
              onCreateResource={openCreateWorkspace}
              loading={loadingResources}
            />
          ) : (
            <ResourceList
              resources={resources}
              resourceLabel={activeSection?.label || "resources"}
              activeResourceId={activeResourceId}
              onOpenResource={(resourceId) => void openResource(resourceId)}
              onCreateResource={openCreateWorkspace}
              canCreate={capabilities.canCreate}
              createLabel={capabilities.createLabel}
              loading={loadingResources}
            />
          )}
          </section>
        )}

        {workspaceMode && activeSectionId === "connectors" ? (
          <ConnectorWorkspaceModal
            mode={workspaceMode}
            resource={activeResource}
            detailLoading={resourceLoading}
            capabilities={capabilities}
            actions={resourceActions}
            detailError={resourceDetailError}
            onClose={closeWorkspace}
            onCreate={handleCreate}
            onUpdate={handleUpdate}
            onDelete={handleDelete}
            onAction={handleAction}
          />
        ) : null}

        {workspaceMode && activeSectionId === "datasets" ? (
          <DatasetWorkspaceModal
            mode={workspaceMode}
            resource={activeResource}
            detailLoading={resourceLoading}
            capabilities={capabilities}
            actions={resourceActions}
            detailError={resourceDetailError}
            onClose={closeWorkspace}
            onCreate={handleCreate}
            onUpdate={handleUpdate}
            onDelete={handleDelete}
            onAction={handleAction}
          />
        ) : null}

        {workspaceMode && activeSectionId === "semantic-models" ? (
          <SemanticModelWorkspaceModal
            mode={workspaceMode}
            resource={activeResource}
            detailLoading={resourceLoading}
            capabilities={capabilities}
            actions={resourceActions}
            detailError={resourceDetailError}
            onClose={closeWorkspace}
            onCreate={handleCreate}
            onUpdate={handleUpdate}
            onDelete={handleDelete}
            onAction={handleAction}
          />
        ) : null}

        {workspaceMode && activeSectionId === "llm-connections" ? (
          <LLMConnectionWorkspaceModal
            mode={workspaceMode}
            resource={activeResource}
            detailLoading={resourceLoading}
            capabilities={capabilities}
            actions={resourceActions}
            detailError={resourceDetailError}
            onClose={closeWorkspace}
            onCreate={handleCreate}
            onUpdate={handleUpdate}
            onDelete={handleDelete}
            onAction={handleAction}
          />
        ) : null}

        {workspaceMode && activeSectionId === "agents" ? (
          <AgentWorkspaceModal
            resource={activeResource}
            detailLoading={resourceLoading}
            actions={resourceActions}
            detailError={resourceDetailError}
            onClose={closeWorkspace}
            onAction={handleAction}
          />
        ) : null}

        {workspaceMode && !["connectors", "datasets", "semantic-models", "llm-connections", "agents", "security"].includes(activeSectionId) ? (
          <ResourceWorkspaceModal
            section={activeSection}
            mode={workspaceMode}
            resource={activeResource}
            detailLoading={resourceLoading}
            capabilities={capabilities}
            createTemplate={createTemplate}
            updateTemplate={updateTemplate}
            actions={resourceActions}
            detailError={resourceDetailError}
            onClose={closeWorkspace}
            onCreate={handleCreate}
            onUpdate={handleUpdate}
            onDelete={handleDelete}
            onAction={handleAction}
          />
        ) : null}
      </div>
    </section>
  );
}
