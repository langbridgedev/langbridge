import { useEffect, useMemo, useState } from "react";

import { ResourceList } from "../../components/resources/ResourceList.jsx";
import { ResourceWorkspaceModal } from "../../components/resources/ResourceWorkspaceModal.jsx";
import { ConnectorConfigurationList } from "../../components/connectors/ConnectorConfigurationList.jsx";
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

export function ConfigurationPage({ section = defaultSection, navigate }) {
  const [sections, setSections] = useState([]);
  const [resources, setResources] = useState([]);
  const [activeSection, setActiveSection] = useState(null);
  const [loadingResources, setLoadingResources] = useState(false);
  const [activeResourceId, setActiveResourceId] = useState(null);
  const [activeResource, setActiveResource] = useState(null);
  const [resourceLoading, setResourceLoading] = useState(false);
  const [workspaceMode, setWorkspaceMode] = useState(null);
  const capabilities = useMemo(() => getSectionCapabilities(activeSection), [activeSection]);
  const createTemplate = useMemo(() => getCreateTemplate(activeSection), [activeSection]);
  const updateTemplate = useMemo(
    () => (activeResource ? getUpdateTemplate(activeSection, activeResource) : {}),
    [activeResource, activeSection],
  );
  const resourceActions = useMemo(
    () => (activeResource ? getResourceActions(activeSection, activeResource) : []),
    [activeResource, activeSection],
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
    setWorkspaceMode(null);
    void loadResources(activeSection.id);
  }, [activeSection]);

  async function loadResources(targetSection = activeSection) {
    setLoadingResources(true);
    try {
      setResources(await listConfigurationResources(targetSection));
    } finally {
      setLoadingResources(false);
    }
  }

  async function openResource(resourceId) {
    const listResource = resources.find((resource) => resource.id === resourceId);
    setActiveResourceId(resourceId);
    setActiveResource(listResource || null);
    setWorkspaceMode("detail");
    if (!listResource) {
      return;
    }

    setResourceLoading(true);
    try {
      const detailed = await getConfigurationResource(activeSection, listResource);
      setActiveResource(detailed);
      mergeResource(detailed);
    } finally {
      setResourceLoading(false);
    }
  }

  function openCreateWorkspace() {
    setActiveResourceId(null);
    setActiveResource(null);
    setWorkspaceMode("create");
  }

  function closeWorkspace() {
    setWorkspaceMode(null);
    setActiveResourceId(null);
    setActiveResource(null);
  }

  async function handleCreate(payload) {
    const created = await createConfigurationResource(activeSection, payload);
    setResources((current) => [created, ...current.filter((resource) => resource.id !== created.id)]);
    setActiveResource(created);
    setActiveResourceId(created.id);
    setWorkspaceMode("detail");
    return created;
  }

  async function handleUpdate(payload) {
    const updated = await updateConfigurationResource(activeSection, activeResource, payload);
    setActiveResource(updated);
    mergeResource(updated);
    return updated;
  }

  async function handleDelete() {
    await deleteConfigurationResource(activeSection, activeResource);
    setResources((current) => current.filter((resource) => resource.id !== activeResource.id));
    closeWorkspace();
  }

  async function handleAction(actionId) {
    const result = await runConfigurationResourceAction(activeSection, activeResource, actionId);
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
            className={classNames("config-nav-item", activeSection === item.id && "active")}
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
          <div className="management-legend">
            <span><i /> Runtime managed</span>
            <span><i /> Config managed</span>
          </div>
        </section>

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

        {workspaceMode ? (
          <ResourceWorkspaceModal
            section={activeSection}
            mode={workspaceMode}
            resource={activeResource}
            detailLoading={resourceLoading}
            capabilities={capabilities}
            createTemplate={createTemplate}
            updateTemplate={updateTemplate}
            actions={resourceActions}
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
