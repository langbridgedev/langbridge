import { Modal } from "../ui/Modal.jsx";
import { ManagementPill } from "./ManagementPill.jsx";

export function ResourceDetailModal({ resource, onClose }) {
  if (!resource) {
    return null;
  }

  return (
    <Modal title="Resource details" onClose={onClose}>
      <article className="config-resource-detail">
        <div className="resource-detail-head">
          <div>
            <p className="eyebrow">Opened resource</p>
            <h3>{resource.name}</h3>
            <span>{resource.subtitle}</span>
          </div>
          <ManagementPill mode={resource.management} />
        </div>

        <div className="resource-meta-grid">
          <div><span>Status</span><strong>{resource.status}</strong></div>
          <div><span>Owner</span><strong>{resource.owner}</strong></div>
          <div><span>Updated</span><strong>{resource.lastUpdated}</strong></div>
        </div>

        <div className="resource-state-grid">
          <ResourceSection title="Runtime state" rows={resource.runtimeState} />
          <ResourceSection title="Config definition" rows={resource.configDefinition} />
        </div>

        <div className="resource-detail-block">
          <h4>Relationships</h4>
          <div className="resource-chip-row">
            {resource.relationships.map((item) => <span key={item}>{item}</span>)}
          </div>
        </div>

        <div className="resource-detail-block">
          <h4>Resource detail</h4>
          <dl className="resource-definition-list">
            {Object.entries(resource.details).map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
        </div>
      </article>
    </Modal>
  );
}

function ResourceSection({ title, rows }) {
  return (
    <section className="resource-detail-block">
      <h4>{title}</h4>
      <dl className="resource-definition-list">
        {rows.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
