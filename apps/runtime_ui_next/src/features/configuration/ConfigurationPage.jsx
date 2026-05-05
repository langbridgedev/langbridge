import { useEffect, useMemo, useState } from "react";

import { ResourceList } from "../../components/resources/ResourceList.jsx";
import { ResourceWorkspaceModal } from "../../components/resources/ResourceWorkspaceModal.jsx";
import { AgentWorkspaceModal } from "../../components/agents/AgentWorkspaceModal.jsx";
import { ConnectorConfigurationList } from "../../components/connectors/ConnectorConfigurationList.jsx";
import { ConnectorWorkspaceModal } from "../../components/connectors/ConnectorWorkspaceModal.jsx";
import { DatasetWorkspaceModal } from "../../components/datasets/DatasetWorkspaceModal.jsx";
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
  const [resources, setResources] = useState([]);
  const [activeSection, setActiveSection] = useState(null);
  const [loadingResources, setLoadingResources] = useState(false);
  const [activeResourceId, setActiveResourceId] = useState(null);
  const [activeResource, setActiveResource] = useState(null);
  const [resourceLoading, setResourceLoading] = useState(false);
  const [workspaceMode, setWorkspaceMode] = useState(null);
  const [resourceListError, setResourceListError] = useState("");
  const [resourceDetailError, setResourceDetailError] = useState("");
  const activeSectionId = activeSection?.id || "";
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
      setResources([]);
      setLoadingResources(false);
      return;
    }
    void loadResources(activeSection.id);
  }, [activeSection]);

  async function loadResources(targetSection = activeSectionId) {
    setLoadingResources(true);
    setResourceListError("");
    try {
      setResources(await listConfigurationResources(targetSection));
    } catch (caughtError) {
      setResources([]);
      setResourceListError(caughtError?.message || "Unable to load runtime resources.");
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
    setResources((current) => [created, ...current.filter((resource) => resource.id !== created.id)]);
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
    setResources((current) => current.filter((resource) => resource.id !== activeResource.id));
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
    setResources((current) =>
      current.map((resource) =>
        resource.id === nextResource.id || resource.ref === nextResource.ref ? nextResource : resource,
      ),
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
            onClick={() => navigate(`/configure/${item.id}`)}
          >
            <strong>{item.label}</strong>
            <span>{item.description}</span>
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
            <div className="management-legend">
              <span><i /> Runtime managed</span>
              <span><i /> Config managed</span>
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

        {workspaceMode && !["connectors", "datasets", "semantic-models", "agents", "security"].includes(activeSectionId) ? (
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
