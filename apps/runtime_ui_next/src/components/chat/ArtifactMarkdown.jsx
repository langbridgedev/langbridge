import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import {
  hasRenderableVisualization,
  normalizeRuntimeArtifactType,
  normalizeTabularResult,
  normalizeVisualizationSpec,
  renderJson,
} from "../../lib/runtimeUi";
import { ChartPreview } from "../ChartPreview";
import { ResultTable } from "../ResultTable";

const ARTIFACT_PLACEHOLDER_PATTERN = /{{\s*artifact:([A-Za-z0-9_.:-]+)\s*}}/g;

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

function splitMarkdownArtifacts(markdown) {
  const text = String(markdown || "");
  if (!text) {
    return [];
  }
  ARTIFACT_PLACEHOLDER_PATTERN.lastIndex = 0;
  const parts = [];
  let lastIndex = 0;
  let match = ARTIFACT_PLACEHOLDER_PATTERN.exec(text);
  while (match) {
    if (match.index > lastIndex) {
      parts.push({
        type: "markdown",
        value: text.slice(lastIndex, match.index),
      });
    }
    parts.push({
      type: "artifact",
      id: String(match[1] || "").trim(),
      value: match[0],
    });
    lastIndex = match.index + match[0].length;
    match = ARTIFACT_PLACEHOLDER_PATTERN.exec(text);
  }
  if (lastIndex < text.length) {
    parts.push({
      type: "markdown",
      value: text.slice(lastIndex),
    });
  }
  ARTIFACT_PLACEHOLDER_PATTERN.lastIndex = 0;
  return parts;
}

function hasTabularPayload(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      (Array.isArray(value.rows) || Array.isArray(value.data) || Array.isArray(value.columns)),
  );
}

function objectValue(value) {
  return value && typeof value === "object" ? value : null;
}

function findArtifact(artifacts, id) {
  const normalizedId = String(id || "").trim();
  if (!normalizedId) {
    return null;
  }
  return (
    (Array.isArray(artifacts) ? artifacts : []).find(
      (artifact) => String(artifact?.id || "").trim() === normalizedId,
    ) || null
  );
}

function artifactId(artifact) {
  return String(artifact?.id || artifact?.artifact_id || artifact?.key || "").trim();
}

function artifactKind(artifact, fallbackId = "") {
  return normalizeRuntimeArtifactType(
    artifact?.type || artifact?.kind || artifact?.source,
    fallbackId || artifactId(artifact),
  );
}

function inferArtifact(id) {
  return {
    id,
    type: normalizeRuntimeArtifactType("", id),
    title: String(id || "artifact").replaceAll("_", " "),
  };
}

function readArtifactReferenceId(artifact) {
  const dataRef = artifact?.data_ref;
  if (typeof dataRef === "string") {
    return dataRef.trim();
  }
  if (dataRef && typeof dataRef === "object") {
    return String(dataRef.artifact_id || dataRef.artifactId || dataRef.id || "").trim();
  }
  return "";
}

function findTableArtifact(artifacts, preferredReference = "") {
  const list = Array.isArray(artifacts) ? artifacts : [];
  const normalizedReference = String(preferredReference || "").trim();
  if (normalizedReference) {
    const referenced = findArtifact(list, normalizedReference);
    if (referenced && artifactKind(referenced) === "table") {
      return referenced;
    }
  }

  return (
    list.find((artifact) => artifactKind(artifact) === "table" && artifactId(artifact) === "primary_result") ||
    list.find((artifact) => artifactKind(artifact) === "table" && artifact?.role === "primary_result") ||
    list.find((artifact) => artifactKind(artifact) === "table") ||
    null
  );
}

function resolveArtifactTableResult(artifact, fallbackResult) {
  const payload = objectValue(artifact?.payload);
  const candidates = [
    objectValue(artifact?.result),
    objectValue(artifact?.table),
    objectValue(artifact?.data),
    objectValue(payload?.result),
    objectValue(payload?.table),
    objectValue(payload?.data),
    payload,
    objectValue(fallbackResult),
  ];
  const candidate = candidates.find(hasTabularPayload);
  return candidate ? normalizeTabularResult(candidate) : null;
}

function looksLikeVisualizationPayload(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      (value.chartType ||
        value.chart_type ||
        value.x ||
        value.x_axis ||
        value.y ||
        value.y_axis ||
        value.measure ||
        value.measures ||
        value.type),
  );
}

function resolveArtifactVisualization(artifact, fallbackVisualization) {
  const payload = objectValue(artifact?.payload);
  const candidates = [
    objectValue(artifact?.visualization),
    objectValue(artifact?.spec),
    objectValue(artifact?.chart),
    objectValue(payload?.visualization),
    objectValue(payload?.spec),
    objectValue(payload?.chart),
    looksLikeVisualizationPayload(payload) ? payload : null,
    objectValue(fallbackVisualization),
  ].filter(Boolean);
  const candidate = candidates.find((item) => looksLikeVisualizationPayload(item));
  return candidate ? normalizeVisualizationSpec(candidate) : null;
}

function buildArtifactRenderPlan(parts, artifacts, fallbackVisualization) {
  const aliasById = new Map();
  const skipIds = new Set();
  const artifactParts = parts.filter((part) => part.type === "artifact");

  artifactParts.forEach((part, index) => {
    const artifact = findArtifact(artifacts, part.id);
    if (!artifact || artifactKind(artifact, part.id) !== "chart") {
      return;
    }
    const visualization = resolveArtifactVisualization(artifact, fallbackVisualization);
    if (visualization?.chartType !== "table") {
      return;
    }
    const referencedTable = findTableArtifact(artifacts, readArtifactReferenceId(artifact));
    if (!referencedTable) {
      return;
    }
    const referencedId = artifactId(referencedTable);
    if (!referencedId || referencedId === part.id) {
      return;
    }
    aliasById.set(part.id, referencedId);

    const laterDuplicate = artifactParts
      .slice(index + 1)
      .some((candidate) => candidate.id === referencedId);
    if (laterDuplicate) {
      skipIds.add(referencedId);
    }
  });

  return { aliasById, skipIds };
}

function artifactFormatting(artifact) {
  const payload = objectValue(artifact?.payload);
  return objectValue(artifact?.formatting) || objectValue(payload?.formatting);
}

function resolveArtifactSql(artifact) {
  const payload = objectValue(artifact?.payload);
  return String(
    artifact?.sql ||
      artifact?.query ||
      artifact?.sql_executable ||
      artifact?.sql_canonical ||
      payload?.sql ||
      payload?.query ||
      payload?.sql_executable ||
      payload?.sql_canonical ||
      "",
  ).trim();
}

function resolveArtifactDiagnostics(artifact, fallbackDiagnostics) {
  const payload = objectValue(artifact?.payload);
  return (
    objectValue(artifact?.diagnostics) ||
    objectValue(payload?.diagnostics) ||
    objectValue(payload) ||
    objectValue(fallbackDiagnostics)
  );
}

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

function MissingArtifact({ id }) {
  return (
    <div className="artifact-markdown-missing" role="note">
      Artifact unavailable: <code>{id}</code>
    </div>
  );
}

function SupportingArtifact({ title, children }) {
  return (
    <details className="artifact-markdown-supporting">
      <summary>{title}</summary>
      {children}
    </details>
  );
}

function ArtifactBlock({
  artifact,
  id,
  result,
  visualization,
  diagnostics,
  maxPreviewRows,
}) {
  const resolvedArtifact = artifact || inferArtifact(id);
  const artifactType = artifactKind(resolvedArtifact, id);
  const title = resolvedArtifact.title || resolvedArtifact.label || id;

  if (artifactType === "chart") {
    const resolvedChartResult = resolveArtifactTableResult(resolvedArtifact, result);
    const formatting = artifactFormatting(resolvedArtifact);
    const chartResult =
      resolvedChartResult && formatting && !resolvedChartResult.formatting
        ? { ...resolvedChartResult, formatting }
        : resolvedChartResult;
    const chartVisualization = resolveArtifactVisualization(resolvedArtifact, visualization);
    if (chartVisualization?.chartType === "table") {
      if (!chartResult) {
        return null;
      }
      return (
        <div className="artifact-markdown-card artifact-markdown-card--table">
          <div className="artifact-markdown-card-head">
            <span>Table</span>
            <strong>{chartVisualization.title || title}</strong>
          </div>
          <ResultTable result={chartResult} maxPreviewRows={maxPreviewRows} />
        </div>
      );
    }
    if (!chartResult || !chartVisualization || !hasRenderableVisualization(chartVisualization)) {
      return <MissingArtifact id={id} />;
    }
    return (
      <div className="artifact-markdown-card artifact-markdown-card--chart">
        <ChartPreview
          title={chartVisualization.title || title}
          result={chartResult}
          metadata={Array.isArray(chartResult?.metadata) ? chartResult.metadata : []}
          visualization={chartVisualization}
          preferredDimension={chartVisualization?.x}
          preferredMeasure={chartVisualization?.y?.[0]}
        />
      </div>
    );
  }

  if (artifactType === "table") {
    const resolvedTableResult = resolveArtifactTableResult(resolvedArtifact, result);
    const formatting = artifactFormatting(resolvedArtifact);
    const tableResult =
      resolvedTableResult && formatting && !resolvedTableResult.formatting
        ? { ...resolvedTableResult, formatting }
        : resolvedTableResult;
    if (!tableResult) {
      return <MissingArtifact id={id} />;
    }
    return (
      <div className="artifact-markdown-card artifact-markdown-card--table">
        <div className="artifact-markdown-card-head">
          <span>Table</span>
          <strong>{title}</strong>
        </div>
        <ResultTable result={tableResult} maxPreviewRows={maxPreviewRows} />
      </div>
    );
  }

  if (artifactType === "sql") {
    const sql = resolveArtifactSql(resolvedArtifact);
    if (!sql) {
      return <MissingArtifact id={id} />;
    }
    return (
      <SupportingArtifact title={title || "Generated SQL"}>
        <pre className="code-block compact">{sql}</pre>
      </SupportingArtifact>
    );
  }

  if (artifactType === "diagnostics") {
    const artifactDiagnostics = resolveArtifactDiagnostics(resolvedArtifact, diagnostics);
    if (!artifactDiagnostics) {
      return <MissingArtifact id={id} />;
    }
    return (
      <SupportingArtifact title={title || "Runtime diagnostics"}>
        <pre className="code-block compact">{renderJson(artifactDiagnostics)}</pre>
      </SupportingArtifact>
    );
  }

  return <MissingArtifact id={id} />;
}

export function ArtifactMarkdown({
  markdown,
  artifacts = [],
  result,
  visualization,
  diagnostics,
  maxPreviewRows = 10,
}) {
  const parts = splitMarkdownArtifacts(markdown);
  const renderPlan = buildArtifactRenderPlan(parts, artifacts, visualization);

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
            />
          );
        }
        return <MarkdownSegment key={`markdown-${index}`}>{part.value}</MarkdownSegment>;
      })}
    </>
  );
}
