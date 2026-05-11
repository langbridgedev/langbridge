import { Copy, Download, XCircle } from "lucide-react";
import { useEffect } from "react";
import { createPortal } from "react-dom";

import { hasRenderableVisualization, renderJson } from "../../lib/runtimeUi.js";
import {
  copyText,
  csvForResult,
  downloadText,
  normalizeArtifactView,
  resolveArtifactDiagnostics,
  resolveArtifactSqlPair,
  resolveArtifactTableResult,
  resolveArtifactVisualization,
  safeFileName,
} from "./analyticsArtifacts.js";
import { AnalyticsChartPreview } from "./AnalyticsChartPreview.jsx";
import { AnalyticsResultTable } from "./AnalyticsResultTable.jsx";

export function ActionButton({ children, icon: Icon, onClick, title }) {
  return (
    <button type="button" title={title} onClick={onClick}>
      {Icon ? <Icon className="button-icon" aria-hidden="true" /> : null}
      {children}
    </button>
  );
}

export function ArtifactActions({ children }) {
  return <div className="artifact-markdown-actions">{children}</div>;
}

function currentRuntimeTheme() {
  if (typeof document === "undefined") {
    return "light";
  }
  return document.querySelector(".app-shell")?.dataset?.theme || "light";
}

export async function copyArtifactText({ key, text, onCopied }) {
  const copied = await copyText(text);
  if (!copied) {
    return;
  }
  onCopied?.(key);
}

export function AnalyticsArtifactModal({
  artifact,
  result,
  visualization,
  diagnostics,
  titleId,
  onClose,
  onCopyArtifact,
  copiedKey,
}) {
  useEffect(() => {
    if (typeof document === "undefined") {
      return undefined;
    }

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

  const view = normalizeArtifactView(artifact);
  if (!view || typeof document === "undefined") {
    return null;
  }

  const tableResult =
    view.type === "table" || view.type === "chart"
      ? resolveArtifactTableResult(view.raw, result)
      : null;
  const chartVisualization =
    view.type === "chart" ? resolveArtifactVisualization(view.raw, visualization) : null;
  const sqlPair = view.type === "sql" ? resolveArtifactSqlPair(view.raw) : null;
  const artifactDiagnostics =
    view.type === "diagnostics" ? resolveArtifactDiagnostics(view.raw, diagnostics) : null;
  const rawJson = renderJson(view.raw);
  const tableCsv = tableResult ? csvForResult(tableResult) : "";
  const chartJson = chartVisualization
    ? renderJson({
        title: chartVisualization.title || view.title,
        visualization: chartVisualization,
        provenance: view.provenance,
        data_ref: view.dataRef,
      })
    : "";
  const diagnosticsJson = artifactDiagnostics ? renderJson(artifactDiagnostics) : "";
  const theme = currentRuntimeTheme();

  return createPortal(
    <div
      className="artifact-modal-overlay"
      data-theme={theme}
      role="presentation"
      onClick={onClose}
    >
      <div
        className="artifact-modal-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="artifact-modal-header">
          <div>
            <p className="eyebrow">{view.type}</p>
            <h3 id={titleId}>{view.title}</h3>
          </div>
          <div className="artifact-modal-actions">
            {tableCsv ? (
              <>
                <ActionButton
                  icon={Copy}
                  title="Copy CSV"
                  onClick={() => onCopyArtifact(`${view.id}-modal-csv`, tableCsv)}
                >
                  {copiedKey === `${view.id}-modal-csv` ? "Copied" : "Copy CSV"}
                </ActionButton>
                <ActionButton
                  icon={Download}
                  title="Download CSV"
                  onClick={() =>
                    downloadText({
                      fileName: safeFileName(view.title || view.id, "csv"),
                      mimeType: "text/csv;charset=utf-8",
                      text: tableCsv,
                    })
                  }
                >
                  Export CSV
                </ActionButton>
              </>
            ) : null}
            {chartJson ? (
              <ActionButton
                icon={Copy}
                title="Copy chart JSON"
                onClick={() => onCopyArtifact(`${view.id}-modal-chart-json`, chartJson)}
              >
                {copiedKey === `${view.id}-modal-chart-json` ? "Copied" : "Copy chart"}
              </ActionButton>
            ) : null}
            {sqlPair?.executable ? (
              <ActionButton
                icon={Copy}
                title="Copy executable SQL"
                onClick={() => onCopyArtifact(`${view.id}-modal-executable`, sqlPair.executable)}
              >
                {copiedKey === `${view.id}-modal-executable` ? "Copied" : "Copy executable"}
              </ActionButton>
            ) : null}
            {sqlPair?.canonical && sqlPair.canonical !== sqlPair.executable ? (
              <ActionButton
                icon={Copy}
                title="Copy generated SQL"
                onClick={() => onCopyArtifact(`${view.id}-modal-canonical`, sqlPair.canonical)}
              >
                {copiedKey === `${view.id}-modal-canonical` ? "Copied" : "Copy generated"}
              </ActionButton>
            ) : null}
            {diagnosticsJson ? (
              <ActionButton
                icon={Copy}
                title="Copy diagnostics JSON"
                onClick={() => onCopyArtifact(`${view.id}-modal-diagnostics`, diagnosticsJson)}
              >
                {copiedKey === `${view.id}-modal-diagnostics` ? "Copied" : "Copy JSON"}
              </ActionButton>
            ) : null}
            <ActionButton
              icon={Copy}
              title="Copy raw artifact JSON"
              onClick={() => onCopyArtifact(`${view.id}-modal-raw`, rawJson)}
            >
              {copiedKey === `${view.id}-modal-raw` ? "Copied" : "Copy raw"}
            </ActionButton>
            <button
              type="button"
              className="artifact-modal-close"
              aria-label="Close artifact"
              onClick={onClose}
            >
              <XCircle className="button-icon" aria-hidden="true" />
            </button>
          </div>
        </header>

        <div className="artifact-modal-body">
          {view.type === "chart" &&
          chartVisualization &&
          tableResult &&
          hasRenderableVisualization(chartVisualization) ? (
            <AnalyticsChartPreview
              title={chartVisualization.title || view.title}
              result={tableResult}
              metadata={Array.isArray(tableResult?.metadata) ? tableResult.metadata : []}
              visualization={chartVisualization}
              preferredDimension={chartVisualization?.x}
              preferredMeasure={chartVisualization?.y?.[0]}
            />
          ) : null}

          {tableResult ? (
            <AnalyticsResultTable
              result={tableResult}
              maxPreviewRows={Array.isArray(tableResult.rows) ? tableResult.rows.length : 1000}
            />
          ) : null}

          {sqlPair?.executable ? (
            <section className="artifact-modal-code-section">
              <h4>Executable SQL</h4>
              <pre className="code-block compact">{sqlPair.executable}</pre>
            </section>
          ) : null}

          {sqlPair?.canonical && sqlPair.canonical !== sqlPair.executable ? (
            <section className="artifact-modal-code-section">
              <h4>Generated SQL</h4>
              <pre className="code-block compact">{sqlPair.canonical}</pre>
            </section>
          ) : null}

          {artifactDiagnostics ? (
            <section className="artifact-modal-code-section">
              <h4>Diagnostics</h4>
              <pre className="code-block compact">{diagnosticsJson}</pre>
            </section>
          ) : null}
        </div>
      </div>
    </div>,
    document.body,
  );
}
