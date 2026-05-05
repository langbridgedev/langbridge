import { CheckCircle2, Copy, Database, GitBranch, Maximize2, ShieldCheck, XCircle } from "lucide-react";
import { useEffect, useId, useMemo, useState } from "react";
import { createPortal } from "react-dom";

import { renderJson } from "../../lib/runtimeUi";
import { buildRunInspectorModel, formatInspectorLabel } from "./runInspectorModel.js";

const TABS = [
  { id: "flow", label: "Flow" },
  { id: "queries", label: "Queries" },
  { id: "checks", label: "Checks" },
];

function toneClass(tone) {
  return `run-inspector-tone--${tone || "neutral"}`;
}

function formatCount(value, singular, plural = `${singular}s`) {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? `${numeric.toLocaleString()} ${numeric === 1 ? singular : plural}`
    : "";
}

function summaryParts(summary) {
  return [
    summary.executionMode ? formatInspectorLabel(summary.executionMode) : "",
    summary.selectedAgent,
    formatCount(summary.sqlCount, "SQL", "SQL"),
    formatCount(summary.rowCount, "row"),
    summary.checksStatus,
  ].filter(Boolean);
}

function RunIcon({ type, tone }) {
  if (type === "execute" || type === "query") {
    return <Database className="run-inspector-icon" aria-hidden="true" />;
  }
  if (type === "review" || type === "check") {
    return tone === "danger" ? (
      <XCircle className="run-inspector-icon" aria-hidden="true" />
    ) : (
      <ShieldCheck className="run-inspector-icon" aria-hidden="true" />
    );
  }
  if (tone === "success") {
    return <CheckCircle2 className="run-inspector-icon" aria-hidden="true" />;
  }
  return <GitBranch className="run-inspector-icon" aria-hidden="true" />;
}

function FlowPanel({ items }) {
  if (!items.length) {
    return <p className="run-inspector-empty">No agent flow details were returned.</p>;
  }
  return (
    <div className="run-inspector-flow">
      {items.map((item) => (
        <article key={item.id} className={`run-inspector-flow-item ${toneClass(item.tone)}`}>
          <div className="run-inspector-flow-marker">
            <RunIcon type={item.type} tone={item.tone} />
          </div>
          <div className="run-inspector-flow-copy">
            <div className="run-inspector-flow-title">
              <strong>{item.title}</strong>
              {item.meta ? <span>{item.meta}</span> : null}
            </div>
            {item.description ? <p>{item.description}</p> : null}
          </div>
        </article>
      ))}
    </div>
  );
}

function SqlBlock({ label, sql, onCopy, copied }) {
  if (!sql) {
    return null;
  }
  return (
    <div className="run-inspector-sql-block">
      <div className="run-inspector-sql-head">
        <span>{label}</span>
        <button type="button" onClick={() => onCopy(sql)}>
          <Copy className="button-icon" aria-hidden="true" />
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre className="code-block compact">{sql}</pre>
    </div>
  );
}

function QueryPanel({ items }) {
  const [copiedKey, setCopiedKey] = useState("");

  async function copySql(key, sql) {
    if (
      typeof navigator === "undefined" ||
      !navigator.clipboard ||
      typeof navigator.clipboard.writeText !== "function"
    ) {
      return;
    }
    await navigator.clipboard.writeText(sql);
    setCopiedKey(key);
    window.setTimeout(() => setCopiedKey(""), 1200);
  }

  if (!items.length) {
    return <p className="run-inspector-empty">No SQL query was generated for this run.</p>;
  }

  return (
    <div className="run-inspector-query-list">
      {items.map((item, index) => {
        const detailPills = [
          item.agentName,
          item.toolName,
          item.selectedDatasets.length > 0 ? `${item.selectedDatasets.length} dataset(s)` : "",
          item.selectedSemanticModels.length > 0 ? `${item.selectedSemanticModels.length} semantic model(s)` : "",
          item.usedFallback ? "Fallback used" : "",
        ].filter(Boolean);
        return (
          <article key={`${item.id}-${index}`} className={`run-inspector-query-card ${toneClass(item.tone)}`}>
            <header className="run-inspector-query-header">
              <div>
                <strong>{item.title}</strong>
                <span>
                  {[item.scope, item.status, item.stage, formatCount(item.rowCount, "row")]
                    .filter(Boolean)
                    .join(" | ")}
                </span>
              </div>
            </header>
            {detailPills.length > 0 ? (
              <div className="run-inspector-query-pills">
                {detailPills.map((pill) => (
                  <span key={pill} className="run-inspector-pill">
                    {pill}
                  </span>
                ))}
              </div>
            ) : null}
            {item.roundQuestion ? <p className="run-inspector-query-question">{item.roundQuestion}</p> : null}
            {item.message ? <p className="run-inspector-query-message">{item.message}</p> : null}
            <SqlBlock
              label="Executable SQL"
              sql={item.executableSql}
              copied={copiedKey === `${item.id}-executable`}
              onCopy={(sql) => void copySql(`${item.id}-executable`, sql)}
            />
            <SqlBlock
              label="Generated SQL"
              sql={item.canonicalSql && item.canonicalSql !== item.executableSql ? item.canonicalSql : ""}
              copied={copiedKey === `${item.id}-canonical`}
              onCopy={(sql) => void copySql(`${item.id}-canonical`, sql)}
            />
          </article>
        );
      })}
    </div>
  );
}

function ChecksPanel({ items }) {
  if (!items.length) {
    return <p className="run-inspector-empty">No review details were returned.</p>;
  }
  return (
    <div className="run-inspector-check-list">
      {items.map((item) => (
        <article key={item.id} className={`run-inspector-check-card ${toneClass(item.tone)}`}>
          <div className="run-inspector-check-icon">
            <RunIcon type="check" tone={item.tone} />
          </div>
          <div>
            <span>{item.group}</span>
            <strong>{item.title}</strong>
            {item.description ? <p>{item.description}</p> : null}
            {item.meta ? <small>{item.meta}</small> : null}
          </div>
        </article>
      ))}
    </div>
  );
}

function RawDiagnosticsDialog({ titleId, raw, onClose, onCopy }) {
  useEffect(() => {
    const originalOverflow = document.body.style.overflow;
    const handleKeyDown = (event) => {
      if (event.key === "Escape") {
        onClose();
      }
    };
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = originalOverflow;
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [onClose]);

  return createPortal(
    <div className="diagnostics-fullscreen-overlay" role="presentation" onClick={onClose}>
      <div
        className="diagnostics-fullscreen-dialog run-inspector-raw-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="diagnostics-fullscreen-header">
          <div className="diagnostics-fullscreen-copy">
            <p className="eyebrow">Execution</p>
            <h3 id={titleId}>Raw diagnostics</h3>
            <p>Original runtime diagnostics payload for this run.</p>
          </div>
          <div className="run-inspector-raw-actions">
            <button type="button" className="diagnostics-inline-open" onClick={onCopy}>
              <Copy className="button-icon" aria-hidden="true" />
              Copy JSON
            </button>
            <button
              type="button"
              className="diagnostics-fullscreen-button diagnostics-fullscreen-close"
              aria-label="Close raw diagnostics"
              title="Close raw diagnostics"
              onClick={onClose}
            >
              <XCircle className="button-icon" aria-hidden="true" />
            </button>
          </div>
        </div>
        <div className="diagnostics-fullscreen-body">
          <pre className="code-block compact">{renderJson(raw)}</pre>
        </div>
      </div>
    </div>,
    document.body,
  );
}

export function RunInspector({ diagnostics }) {
  const model = useMemo(() => buildRunInspectorModel(diagnostics), [diagnostics]);
  const [open, setOpen] = useState(false);
  const [activeTab, setActiveTab] = useState("flow");
  const [rawOpen, setRawOpen] = useState(false);
  const [copiedRaw, setCopiedRaw] = useState(false);
  const rawTitleId = useId();

  if (!model) {
    return null;
  }

  const { summary } = model;
  const parts = summaryParts(summary);
  const title = summary.route ? `Execution: ${summary.route}` : "Execution";
  const subtitle = parts.join(" | ") || "Runtime diagnostics available";

  async function copyRaw() {
    if (
      typeof navigator === "undefined" ||
      !navigator.clipboard ||
      typeof navigator.clipboard.writeText !== "function"
    ) {
      return;
    }
    await navigator.clipboard.writeText(renderJson(model.raw));
    setCopiedRaw(true);
    window.setTimeout(() => setCopiedRaw(false), 1200);
  }

  return (
    <section className={`run-inspector ${open ? "open" : ""}`}>
      <button
        className="run-inspector-summary"
        type="button"
        onClick={() => setOpen((current) => !current)}
        aria-expanded={open}
      >
        <span className={`run-inspector-status-dot ${toneClass(summary.tone)}`} aria-hidden="true" />
        <span className="run-inspector-summary-copy">
          <strong>{title}</strong>
          <span>{subtitle}</span>
        </span>
        <span className="run-inspector-summary-action">{open ? "Hide" : "Inspect"}</span>
      </button>

      {open ? (
        <div className="run-inspector-body">
          <div className="run-inspector-tabs" role="tablist" aria-label="Run inspector">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={activeTab === tab.id}
                className={activeTab === tab.id ? "active" : ""}
                onClick={() => setActiveTab(tab.id)}
              >
                {tab.label}
              </button>
            ))}
          </div>

          <div className="run-inspector-panel">
            {activeTab === "flow" ? <FlowPanel items={model.flowItems} /> : null}
            {activeTab === "queries" ? <QueryPanel items={model.queryItems} /> : null}
            {activeTab === "checks" ? <ChecksPanel items={model.checkItems} /> : null}
          </div>

          <div className="run-inspector-footer">
            <button type="button" onClick={() => setRawOpen(true)}>
              <Maximize2 className="button-icon" aria-hidden="true" />
              View raw JSON
            </button>
            <button type="button" onClick={() => void copyRaw()}>
              <Copy className="button-icon" aria-hidden="true" />
              {copiedRaw ? "Copied" : "Copy diagnostics"}
            </button>
          </div>
        </div>
      ) : null}

      {rawOpen && typeof document !== "undefined" ? (
        <RawDiagnosticsDialog
          titleId={rawTitleId}
          raw={model.raw}
          onClose={() => setRawOpen(false)}
          onCopy={() => void copyRaw()}
        />
      ) : null}
    </section>
  );
}
