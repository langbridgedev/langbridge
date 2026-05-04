import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { formatValue } from "../lib/format";
import {
  deriveRuntimeResultState,
  extractArtifactPlaceholderIds,
  hasRenderableVisualization,
  normalizeAnalystOutcome,
  normalizeRuntimeArtifactType,
  normalizeTabularResult,
  normalizeVisualizationSpec,
} from "../lib/runtimeUi";
import { ChartPreview } from "./ChartPreview";
import { ResultTable } from "./ResultTable";
import { ArtifactMarkdown } from "./chat/ArtifactMarkdown";
import { RunInspector } from "./run-inspector/RunInspector.jsx";

function formatLabel(value) {
  return String(value || "")
    .replaceAll("-", " ")
    .replaceAll("_", " ")
    .trim();
}

function toTitleCase(value) {
  return formatLabel(value).replace(/\b\w/g, (match) => match.toUpperCase());
}

function buildSummaryFallback(state) {
  switch (state.kind) {
    case "empty_result":
      return "The request completed, but no rows matched the current scope.";
    case "needs_clarification":
      return "The runtime needs one more detail before it can continue.";
    case "invalid_request":
      return "The runtime could not act on the request as written.";
    case "query_error":
      return "The runtime could not turn this request into a valid query.";
    case "access_denied":
      return "The runtime blocked this request based on the current access policy.";
    case "execution_failure":
      return "The runtime could not complete this request.";
    case "success_chart":
      return "The runtime returned a structured answer with a visualization and supporting rows.";
    case "success_rows":
      return "The runtime returned structured rows for this request.";
    default:
      return "";
  }
}

function readClarificationText(diagnostics) {
  if (!diagnostics || typeof diagnostics !== "object") {
    return "";
  }
  const direct =
    typeof diagnostics.clarifying_question === "string"
      ? diagnostics.clarifying_question.trim()
      : "";
  if (direct) {
    return direct;
  }
  const aiRun = diagnostics.ai_run && typeof diagnostics.ai_run === "object" ? diagnostics.ai_run : null;
  const routeQuestion =
    typeof aiRun?.diagnostics?.route_decision?.clarification_question === "string"
      ? aiRun.diagnostics.route_decision.clarification_question.trim()
      : "";
  if (routeQuestion) {
    return routeQuestion;
  }
  const planQuestion =
    typeof aiRun?.plan?.clarification_question === "string"
      ? aiRun.plan.clarification_question.trim()
      : "";
  if (planQuestion) {
    return planQuestion;
  }
  const reviewQuestion = Array.isArray(aiRun?.review_decisions)
    ? [...aiRun.review_decisions]
        .reverse()
        .map((item) =>
          typeof item?.clarification_question === "string"
            ? item.clarification_question.trim()
            : "",
        )
        .find(Boolean)
    : "";
  return reviewQuestion || "";
}

function buildStatePills({ result, visualization, diagnostics }) {
  const normalizedResult = result ? normalizeTabularResult(result) : null;
  const normalizedVisualization = normalizeVisualizationSpec(visualization);
  const outcome = normalizeAnalystOutcome(diagnostics);
  const pills = [];

  if (normalizedResult?.rowCount !== undefined && normalizedResult?.rowCount !== null) {
    pills.push(`${Number(normalizedResult.rowCount).toLocaleString()} rows`);
  }
  if (normalizedVisualization?.chartType && normalizedVisualization.chartType !== "table") {
    pills.push(toTitleCase(normalizedVisualization.chartType));
  }
  if (outcome?.retryCount > 0) {
    pills.push(`${outcome.retryCount} retr${outcome.retryCount === 1 ? "y" : "ies"}`);
  }
  if (outcome?.selectedAssetName) {
    pills.push(outcome.selectedAssetName);
  }

  return pills;
}

function artifactPlaceholderMatchesType(id, artifacts, expectedType) {
  const artifact =
    (Array.isArray(artifacts) ? artifacts : []).find(
      (item) => String(item?.id || "").trim() === String(id || "").trim(),
    ) || null;
  const artifactType = normalizeRuntimeArtifactType(
    artifact?.type || artifact?.kind || artifact?.source,
    id,
  );
  return artifactType === expectedType;
}

const SUMMARY_MARKDOWN_COMPONENTS = {
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

export function RuntimeResultPanel({
  summary,
  answerMarkdown = "",
  artifacts = [],
  result,
  visualization,
  diagnostics,
  status = "ready",
  errorMessage = "",
  errorStatus = null,
  maxPreviewRows = 12,
  variant = "default",
}) {
  const normalizedResult = result ? normalizeTabularResult(result) : null;
  const normalizedVisualization = normalizeVisualizationSpec(visualization);
  const state = deriveRuntimeResultState({
    status,
    result: normalizedResult,
    visualization: normalizedVisualization,
    diagnostics,
    errorMessage,
    errorStatus,
  });
  const isChatVariant = variant === "chat";
  const isClarification = state.kind === "needs_clarification";
  const clarificationText = isClarification ? readClarificationText(diagnostics) : "";
  const answerMarkdownText = String(answerMarkdown || "").trim();
  const summaryText =
    String(summary || "").trim() ||
    clarificationText ||
    (isChatVariant && isClarification ? "" : buildSummaryFallback(state));
  const primaryResponseMarkdown = answerMarkdownText || summaryText;
  const normalizedArtifacts = Array.isArray(artifacts) ? artifacts : [];
  const placeholderArtifactIds = extractArtifactPlaceholderIds(primaryResponseMarkdown);
  const suppressSummarySection =
    !answerMarkdownText &&
    state.kind === "needs_clarification" &&
    Boolean(summaryText) &&
    summaryText === String(state.description || "").trim();
  const statePills = buildStatePills({
    result: normalizedResult,
    visualization: normalizedVisualization,
    diagnostics,
  });
  const showChart =
    Boolean(normalizedResult) &&
    Boolean(normalizedVisualization) &&
    state.showChart &&
    hasRenderableVisualization(normalizedVisualization);
  const showTable =
    Boolean(normalizedResult) &&
    (state.showTable || normalizedVisualization?.chartType === "table");
  const compactChatSuccess =
    isChatVariant &&
    (state.kind === "success_rows" || state.kind === "success_chart");
  const hideChatStateCard = isChatVariant && (compactChatSuccess || isClarification);
  const inlineChartArtifactIds = showChart
    ? placeholderArtifactIds.filter((id) =>
        artifactPlaceholderMatchesType(id, normalizedArtifacts, "chart"),
      )
    : [];
  const inlineTableArtifactIds = showTable
    ? placeholderArtifactIds.filter((id) =>
        artifactPlaceholderMatchesType(id, normalizedArtifacts, "table"),
      )
    : [];
  const shouldRenderFallbackArtifacts = !answerMarkdownText;
  const showStandaloneChart =
    shouldRenderFallbackArtifacts && showChart && inlineChartArtifactIds.length === 0;
  const showStandaloneTable =
    shouldRenderFallbackArtifacts &&
    showTable &&
    (!isChatVariant || !showChart) &&
    inlineTableArtifactIds.length === 0;

  return (
    <div className={`runtime-result-stack ${variant === "chat" ? "runtime-result-stack--chat" : ""}`}>
      {!hideChatStateCard ? (
        <div className={`runtime-result-state runtime-result-state--${state.tone}`}>
          <div className="runtime-result-state-copy">
            <span className="runtime-result-state-label">{state.label}</span>
            <strong>{state.title}</strong>
            <p>{state.description}</p>
          </div>
          {statePills.length > 0 ? (
            <div className="runtime-result-state-pills">
              {statePills.map((item) => (
                <span key={item}>{item}</span>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}

      {!suppressSummarySection && primaryResponseMarkdown ? (
        <section className={`runtime-result-section ${isChatVariant ? "runtime-result-section--chat-answer" : ""}`}>
          {!isChatVariant ? (
            <div className="runtime-result-section-head">
              <div>
                <h4>Answer</h4>
                <p>Summary returned by the runtime for this request.</p>
              </div>
            </div>
          ) : null}
          <div className="assistant-summary-card assistant-summary-markdown">
            {isChatVariant ? (
              <ArtifactMarkdown
                markdown={primaryResponseMarkdown}
                artifacts={normalizedArtifacts}
                result={normalizedResult}
                visualization={normalizedVisualization}
                diagnostics={diagnostics}
                maxPreviewRows={maxPreviewRows}
              />
            ) : (
              <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                components={SUMMARY_MARKDOWN_COMPONENTS}
              >
                {primaryResponseMarkdown}
              </ReactMarkdown>
            )}
          </div>
        </section>
      ) : null}

      {showStandaloneChart ? (
        <section className="runtime-result-section">
          <div className="runtime-result-section-head">
            <div>
              <h4>Visualization</h4>
              <p>
                {normalizedVisualization?.subtitle ||
                  `${toTitleCase(normalizedVisualization?.chartType)} preview generated from the returned result.`}
              </p>
            </div>
          </div>
          <ChartPreview
            title={normalizedVisualization?.title}
            result={normalizedResult}
            metadata={Array.isArray(normalizedResult?.metadata) ? normalizedResult.metadata : []}
            visualization={normalizedVisualization}
            preferredDimension={normalizedVisualization?.x}
            preferredMeasure={normalizedVisualization?.y?.[0]}
          />
        </section>
      ) : null}

      {showStandaloneTable ? (
        <section className="runtime-result-section">
          <div className="runtime-result-section-head">
            <div>
              <h4>Underlying rows</h4>
              <p>Preview the tabular result that backs the answer and chart.</p>
            </div>
            {normalizedResult?.duration_ms !== undefined && normalizedResult?.duration_ms !== null ? (
              <span className="runtime-result-section-meta">
                {formatValue(normalizedResult.duration_ms)} ms
              </span>
            ) : null}
          </div>
          <ResultTable result={normalizedResult} maxPreviewRows={maxPreviewRows} />
        </section>
      ) : null}

      {diagnostics && typeof diagnostics === "object" ? (
        <RunInspector diagnostics={diagnostics} />
      ) : null}
    </div>
  );
}
