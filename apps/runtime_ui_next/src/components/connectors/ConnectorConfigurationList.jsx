import { useMemo, useState } from "react";

import { classNames } from "../../utils/classNames.js";
import { ManagementPill } from "../resources/ManagementPill.jsx";

export function ConnectorConfigurationList({
  resources,
  resourceLabel,
  activeResourceId,
  onOpenResource,
  onCreateResource,
  loading = false,
}) {
  const [query, setQuery] = useState("");
  const filteredResources = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    if (!normalizedQuery) {
      return resources;
    }
    return resources.filter((resource) => {
      const haystack = [resource.name, resource.subtitle, resource.status, resource.management]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return haystack.includes(normalizedQuery);
    });
  }, [query, resources]);

  return (
    <div className="config-resource-list">
      <div className="config-resource-list-head">
        <div className="config-resource-list-actions">
          <input
            type="search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search resources"
          />
          <button className="config-resource-list-create" type="button" onClick={onCreateResource}>
            Create Connector
          </button>
        </div>
      </div>
      {filteredResources.map((resource) => (
        <button
          key={resource.id}
          className={classNames("config-resource-card", activeResourceId === resource.id && "active")}
          type="button"
          onClick={() => onOpenResource(resource.id)}
        >
          <div>
            <strong>{resource.name}</strong>
            <span>{resource.subtitle}</span>
          </div>
          <div className="config-resource-card-meta">
            <ManagementPill mode={resource.management} />
            <span>{resource.status}</span>
          </div>
        </button>
      ))}
      {!loading && filteredResources.length === 0 ? (
        <div className="config-resource-empty">
          <strong>No resources found</strong>
          <span>Try a different search, or add a runtime-managed resource if this section supports it.</span>
        </div>
      ) : null}
    </div>
  );
}
