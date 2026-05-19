import { Copy, Maximize2 } from "lucide-react";
import { useId, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { hasRenderableVisualization, renderJson } from "../../lib/runtimeUi.js";
import {
  buildArtifactRenderPlan,
  buildUnreferencedPrimaryArtifactIds,
  copyText,
  csvForResult,
  findArtifact,
  normalizeArtifactView,
  resolveArtifactDiagnostics,
  resolveArtifactSql,
  resolveArtifactTableResult,
  resolveArtifactVisualization,
  splitMarkdownArtifacts,
} from "./analyticsArtifacts.js";
import {
  ActionButton,
  AnalyticsArtifactModal,
  ArtifactActions,
} from "./AnalyticsArtifactModal.jsx";
import { AnalyticsChartPreview } from "./AnalyticsChartPreview.jsx";
import { AnalyticsResultTable } from "./AnalyticsResultTable.jsx";

const MARKDOWN_COMPONENTS = {
  a({ href, children, ...props }) {
    return (
      <a href={href} target="_blank" rel="noreferrer" {...props}>
        {children}
      </a>
    );
  },
  table({ children }) {
    return (
      <div className="table-wrap">
        <table className="result-table">{children}</table>
      </div>
    );
  },
  code({ inline, className, children, ...props }) {
    if (inline) {
      return (
        <code className="summary-inline-code" {...props}>
          {children}
        </code>
      );
    }
    return (
      <code className={className} {...props}>
        {children}
      </code>
    );
  },
  pre({ children }) {
    return <pre className="code-block">{children}</pre>;
  },
};

function MarkdownSegment({ children }) {
  if (!String(children || "").trim()) {
    return null;
  }

  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={MARKDOWN_COMPONENTS}>
      {children}
    </ReactMarkdown>
  );
}

function warnMissingArtifact(id) {
  if (import.meta.env?.DEV && id) {
    console.warn(`Langbridge artifact placeholder omitted because artifact was not returned: ${id}`);
  }
  return null;
}

function SupportingArtifact({ title, children, actions }) {
  return (
    <details className="artifact-markdown-supporting">
      <summary>
        <span>{title}</span>
        {actions ? <span className="artifact-markdown-supporting-actions">{actions}</span> : null}
      </summary>
      {children}
    </details>
  );
}

function ArtifactCardHeader({ eyebrow, title, actions }) {
  return (
    <div className="artifact-markdown-card-head">
      <div>
        <span>{eyebrow}</span>
        <strong>{title}</strong>
      </div>
      {actions}
    </div>
  );
}

function ArtifactBlock({
  artifact,
  id,
  result,
  visualization,
  diagnostics,
  maxPreviewRows,
  onOpenArtifact,
  onCopyArtifact,
  copiedKey,
}) {
  if (!artifact) {
    return warnMissingArtifact(id);
  }

  const view = normalizeArtifactView(artifact, id);
  if (!view) {
    return warnMissingArtifact(id);
  }

  if (view.type === "chart") {
    const chartResult = resolveArtifactTableResult(view.raw, result);
    const chartVisualization = resolveArtifactVisualization(view.raw, visualization);

    if (chartVisualization?.chartType === "table") {
      if (!chartResult) {
        return warnMissingArtifact(id);
      }

      const csv = csvForResult(chartResult);
      return (
        <div className="artifact-markdown-card artifact-markdown-card--table">
          <ArtifactCardHeader
            eyebrow="Table"
            title={chartVisualization.title || view.title}
            actions={
              <ArtifactActions>
                <ActionButton
                  icon={Copy}
                  title="Copy table CSV"
                  onClick={() => onCopyArtifact(`${view.id}-csv`, csv)}
                >
                  {copiedKey === `${view.id}-csv` ? "Copied" : "Copy CSV"}
                </ActionButton>
                <ActionButton
                  icon={Maximize2}
                  title="Open full table"
                  onClick={() => onOpenArtifact(view.id)}
                >
                  Expand
                </ActionButton>
              </ArtifactActions>
            }
          />
          <AnalyticsResultTable result={chartResult} maxPreviewRows={maxPreviewRows} />
        </div>
      );
    }

    if (!chartResult || !chartVisualization || !hasRenderableVisualization(chartVisualization)) {
      return warnMissingArtifact(id);
    }

    const chartJson = renderJson({
      title: chartVisualization.title || view.title,
      visualization: chartVisualization,
      provenance: view.provenance,
      data_ref: view.dataRef,
    });

    return (
      <div className="artifact-markdown-card artifact-markdown-card--chart">
        <ArtifactCardHeader
          eyebrow="Chart"
          title={chartVisualization.title || view.title}
          actions={
            <ArtifactActions>
              <ActionButton
                icon={Copy}
                title="Copy chart metadata"
                onClick={() => onCopyArtifact(`${view.id}-json`, chartJson)}
              >
                {copiedKey === `${view.id}-json` ? "Copied" : "Copy"}
              </ActionButton>
              <ActionButton
                icon={Maximize2}
                title="Open full chart"
                onClick={() => onOpenArtifact(view.id)}
              >
                Expand
              </ActionButton>
            </ArtifactActions>
          }
        />
        <AnalyticsChartPreview
          title={chartVisualization.title || view.title}
          result={chartResult}
          metadata={Array.isArray(chartResult?.metadata) ? chartResult.metadata : []}
          visualization={chartVisualization}
          preferredDimension={chartVisualization?.x}
          preferredMeasure={chartVisualization?.y?.[0]}
        />
      </div>
    );
  }

  if (view.type === "table") {
    const tableResult = resolveArtifactTableResult(view.raw, result);
    if (!tableResult) {
      return warnMissingArtifact(id);
    }

    const csv = csvForResult(tableResult);
    return (
      <div className="artifact-markdown-card artifact-markdown-card--table">
        <ArtifactCardHeader
          eyebrow="Table"
          title={view.title}
          actions={
            <ArtifactActions>
              <ActionButton
                icon={Copy}
                title="Copy table CSV"
                onClick={() => onCopyArtifact(`${view.id}-csv`, csv)}
              >
                {copiedKey === `${view.id}-csv` ? "Copied" : "Copy CSV"}
              </ActionButton>
              <ActionButton
                icon={Maximize2}
                title="Open full table"
                onClick={() => onOpenArtifact(view.id)}
              >
                Expand
              </ActionButton>
            </ArtifactActions>
          }
        />
        <AnalyticsResultTable result={tableResult} maxPreviewRows={maxPreviewRows} />
      </div>
    );
  }

  if (view.type === "sql") {
    const sql = resolveArtifactSql(view.raw);
    if (!sql) {
      return warnMissingArtifact(id);
    }

    return (
      <SupportingArtifact
        title={view.title || "Generated SQL"}
        actions={
          <ActionButton
            icon={Copy}
            title="Copy SQL"
            onClick={(event) => {
              event.preventDefault();
              onCopyArtifact(`${view.id}-sql`, sql);
            }}
          >
            {copiedKey === `${view.id}-sql` ? "Copied" : "Copy"}
          </ActionButton>
        }
      >
        <pre className="code-block compact">{sql}</pre>
      </SupportingArtifact>
    );
  }

  if (view.type === "diagnostics") {
    const artifactDiagnostics = resolveArtifactDiagnostics(view.raw, diagnostics);
    if (!artifactDiagnostics) {
      return warnMissingArtifact(id);
    }

    const diagnosticsJson = renderJson(artifactDiagnostics);
    return (
      <SupportingArtifact
        title={view.title || "Runtime diagnostics"}
        actions={
          <ActionButton
            icon={Copy}
            title="Copy diagnostics JSON"
            onClick={(event) => {
              event.preventDefault();
              onCopyArtifact(`${view.id}-json`, diagnosticsJson);
            }}
          >
            {copiedKey === `${view.id}-json` ? "Copied" : "Copy"}
          </ActionButton>
        }
      >
        <pre className="code-block compact">{diagnosticsJson}</pre>
      </SupportingArtifact>
    );
  }

  return warnMissingArtifact(id);
}

export function AnalyticsArtifactRenderer({
  markdown,
  artifacts = [],
  result,
  visualization,
  diagnostics,
  maxPreviewRows = 10,
}) {
  const [activeArtifactId, setActiveArtifactId] = useState("");
  const [copiedKey, setCopiedKey] = useState("");
  const modalTitleId = useId();
  const parts = useMemo(() => splitMarkdownArtifacts(markdown), [markdown]);
  const renderPlan = useMemo(
    () => buildArtifactRenderPlan(parts, artifacts, visualization),
    [parts, artifacts, visualization],
  );
  const fallbackArtifactIds = useMemo(
    () => buildUnreferencedPrimaryArtifactIds(parts, artifacts),
    [parts, artifacts],
  );
  const activeArtifact = activeArtifactId ? findArtifact(artifacts, activeArtifactId) : null;

  async function handleCopy(key, text) {
    const copied = await copyText(text);
    if (!copied) {
      return;
    }
    setCopiedKey(key);
    window.setTimeout(() => setCopiedKey(""), 1200);
  }

  return (
    <>
      {parts.map((part, index) => {
        if (part.type === "artifact") {
          if (renderPlan.skipIds.has(part.id)) {
            return null;
          }

          const resolvedId = renderPlan.aliasById.get(part.id) || part.id;
          return (
            <ArtifactBlock
              key={`artifact-${part.id}-${index}`}
              id={resolvedId}
              artifact={findArtifact(artifacts, resolvedId)}
              result={result}
              visualization={visualization}
              diagnostics={diagnostics}
              maxPreviewRows={maxPreviewRows}
              onOpenArtifact={setActiveArtifactId}
              onCopyArtifact={(key, text) => void handleCopy(key, text)}
              copiedKey={copiedKey}
            />
          );
        }

        return <MarkdownSegment key={`markdown-${index}`}>{part.value}</MarkdownSegment>;
      })}

      {fallbackArtifactIds.map((artifactId) => (
        <ArtifactBlock
          key={`fallback-artifact-${artifactId}`}
          id={artifactId}
          artifact={findArtifact(artifacts, artifactId)}
          result={result}
          visualization={visualization}
          diagnostics={diagnostics}
          maxPreviewRows={maxPreviewRows}
          onOpenArtifact={setActiveArtifactId}
          onCopyArtifact={(key, text) => void handleCopy(key, text)}
          copiedKey={copiedKey}
        />
      ))}

      {activeArtifact && typeof document !== "undefined" ? (
        <AnalyticsArtifactModal
          artifact={activeArtifact}
          result={result}
          visualization={visualization}
          diagnostics={diagnostics}
          titleId={modalTitleId}
          copiedKey={copiedKey}
          onCopyArtifact={(key, text) => void handleCopy(key, text)}
          onClose={() => setActiveArtifactId("")}
        />
      ) : null}
    </>
  );
}
