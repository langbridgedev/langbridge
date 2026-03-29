import { ArrowRight } from "lucide-react";
import { Link } from "react-router-dom";

import { formatRelativeTime } from "../../lib/runtimeUi";
import { PageEmpty } from "../PagePrimitives";

export function QuickActionPanel({ actions }) {
  return (
    <div className="command-action-stack">
      <div className="command-panel-heading">
        <div>
          <p className="command-panel-eyebrow">Quick actions</p>
          <p className="command-panel-copy">
            Move directly from setup to analysis without leaving the runtime shell.
          </p>
        </div>
      </div>
      <div className="command-action-grid">
        {actions.map((action) => {
          const Icon = action.icon;
          return (
            <Link
              key={action.to}
              to={action.to}
              className={`command-action-card ${action.emphasis || ""}`.trim()}
            >
              <span className="command-action-icon">
                <Icon className="metric-card-icon-svg" aria-hidden="true" />
              </span>
              <strong>{action.label}</strong>
              <span>{action.description}</span>
            </Link>
          );
        })}
      </div>
    </div>
  );
}

export function ActivityPanel({ title, eyebrow, items, emptyTitle, emptyMessage }) {
  return (
    <section className="command-activity-panel surface-panel">
      <div className="command-panel-heading">
        <div>
          <p className="command-panel-eyebrow">{eyebrow}</p>
          <h3>{title}</h3>
        </div>
      </div>
      {items.length > 0 ? (
        <div className="command-activity-list">
          {items.map((item) => (
            <Link key={item.id} to={item.href} className="command-activity-card">
              <div>
                <div className="command-activity-topline">
                  <strong>{item.title}</strong>
                  <span className="tag">{item.kind}</span>
                </div>
                <p>{item.description}</p>
              </div>
              <div className="command-activity-foot">
                <span>{formatRelativeTime(item.timestamp)}</span>
                <ArrowRight className="feature-card-arrow" aria-hidden="true" />
              </div>
            </Link>
          ))}
        </div>
      ) : (
        <PageEmpty title={emptyTitle} message={emptyMessage} />
      )}
    </section>
  );
}

export function RuntimeMemoryPanel({ items }) {
  return (
    <section className="command-activity-panel surface-panel">
      <div className="command-panel-heading">
        <div>
          <p className="command-panel-eyebrow">Local workspace memory</p>
          <h3>Runtime-local artifacts</h3>
        </div>
      </div>
      <div className="command-memory-list">
        {items.map((item) => (
          <article key={item.label} className="command-memory-card">
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <p>{item.detail}</p>
          </article>
        ))}
      </div>
    </section>
  );
}
