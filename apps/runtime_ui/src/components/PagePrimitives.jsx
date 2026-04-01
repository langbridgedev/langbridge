import { ArrowRight } from "lucide-react";
import { Link } from "react-router-dom";

import {
  describeManagementMode,
  formatManagementModeLabel,
  normalizeManagementMode,
} from "../lib/managedResources";

export function PageEmpty({ title, message, action }) {
  return (
    <div className="empty-box page-empty">
      <strong>{title}</strong>
      <span>{message}</span>
      {action}
    </div>
  );
}

export function Panel({ title, eyebrow, actions, children, className = "" }) {
  return (
    <section className={`panel ${className}`.trim()}>
      {title || eyebrow || actions ? (
        <header className="panel-header">
          <div>
            {eyebrow ? <p className="eyebrow">{eyebrow}</p> : null}
            {title ? <h2>{title}</h2> : null}
          </div>
          {actions ? <div className="panel-actions">{actions}</div> : null}
        </header>
      ) : null}
      {children}
    </section>
  );
}

export function DetailList({ items }) {
  return (
    <dl className="detail-list">
      {items.map((item) => (
        <div key={item.label}>
          <dt>{item.label}</dt>
          <dd>{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

export function ManagementBadge({ mode }) {
  const normalized = normalizeManagementMode(mode);
  return (
    <span className={`management-badge ${normalized === "runtime_managed" ? "runtime" : "config"}`}>
      {formatManagementModeLabel(normalized)}
    </span>
  );
}

export function ManagementModeNotice({ mode, resourceLabel = "resource" }) {
  const normalized = normalizeManagementMode(mode);
  return (
    <div className={`callout ${normalized === "runtime_managed" ? "success" : "warning"}`} style={{marginBottom: "1.5rem"}}>
      <strong>
        <ManagementBadge mode={normalized} /> {resourceLabel}
      </strong>
      <span>{describeManagementMode(normalized)}</span>
    </div>
  );
}

export function SectionTabs({ tabs, value, onChange }) {
  return (
    <div className="section-tabs" role="tablist" aria-label="Section tabs">
      {tabs.map((tab) => (
        <button
          key={tab.value}
          className={`section-tab ${value === tab.value ? "active" : ""}`}
          type="button"
          role="tab"
          aria-selected={value === tab.value}
          onClick={() => onChange(tab.value)}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}

export function MetricCard({ icon: Icon, label, value, detail, tone = "" }) {
  return (
    <article className={`metric-card ${tone}`.trim()}>
      <div className="metric-card-top">
        <span className="metric-card-icon">
          <Icon className="metric-card-icon-svg" aria-hidden="true" />
        </span>
        <p>{label}</p>
      </div>
      <strong>{value}</strong>
      {detail ? <span>{detail}</span> : null}
    </article>
  );
}

export function FeatureCard({ to, icon: Icon, metric, title, description, cta }) {
  return (
    <Link className="feature-card" to={to}>
      <div className="feature-card-top">
        <span className="feature-card-icon">
          <Icon className="feature-card-icon-svg" aria-hidden="true" />
        </span>
        <p>{metric}</p>
      </div>
      <div>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
      <span className="feature-card-cta">
        {cta}
        <ArrowRight className="feature-card-arrow" aria-hidden="true" />
      </span>
    </Link>
  );
}
