import { useState } from "react";

import { formatDateTime, formatValue } from "../lib/format";

function formatLabel(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .trim();
}

function toTitleCase(value) {
  return formatLabel(value).replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatOperationStatus(operation) {
  if (!operation || typeof operation !== "object") {
    return "No";
  }
  if (operation.pushed) {
    return "Yes";
  }
  if (operation.supported === false) {
    return "No";
  }
  return "No";
}

function buildSummaryCards(diagnostics) {
  const summary = diagnostics?.summary || {};
  return [
    { label: "Query type", value: toTitleCase(summary.query_type || "sql") },
    { label: "Sources", value: formatValue(summary.source_count || 0) },
    { label: "Stages", value: formatValue(summary.stage_count || 0) },
    { label: "Runtime", value: summary.total_runtime_ms != null ? `${formatValue(summary.total_runtime_ms)} ms` : "Explain only" },
    { label: "Final rows", value: summary.final_rows != null ? formatValue(summary.final_rows) : "n/a" },
    {
      label: "Cache",
      value: `${formatValue(summary.cache_hits || 0)} hit / ${formatValue(summary.cache_misses || 0)} miss / ${formatValue(summary.cache_bypasses || 0)} bypass`,
    },
  ];
}

function renderSqlBlock(label, value) {
  if (!value) {
    return null;
  }
  return (
    <div className="federation-diagnostics-sql-block">
      <span>{label}</span>
      <pre className="code-block compact">{value}</pre>
    </div>
  );
}

function renderOperationCard(label, operation) {
  return (
    <div key={label} className="federation-pushdown-operation-card">
      <span>{label}</span>
      <strong>{formatOperationStatus(operation)}</strong>
      {Array.isArray(operation?.details) && operation.details.length > 0 ? (
        <small>{operation.details.join(", ")}</small>
      ) : null}
      {operation?.reason ? <p>{operation.reason}</p> : null}
    </div>
  );
}

export function FederationDiagnosticsPanel({
  diagnostics,
  title = "Federation diagnostics",
  description = "Inspect the federated plan, stage timings, cache behavior, and pushdown decisions.",
}) {
  const [activeTab, setActiveTab] = useState("plan");

  if (!diagnostics || typeof diagnostics !== "object" || !diagnostics.summary) {
    return null;
  }

  const summary = diagnostics.summary || {};
  const logicalPlan = diagnostics.logical_plan || {};
  const physicalPlan = diagnostics.physical_plan || {};
  const stages = Array.isArray(diagnostics.stages) ? diagnostics.stages : [];
  const cache = diagnostics.cache || {};
  const pushdown = diagnostics.pushdown || {};
  const sources = Array.isArray(diagnostics.sources) ? diagnostics.sources : [];
  const summaryCards = buildSummaryCards(diagnostics);

  return (
    <section className="federation-diagnostics-panel">
      <div className="federation-diagnostics-header">
        <div>
          <h4>{title}</h4>
          <p>{description}</p>
        </div>
        <div className="federation-diagnostics-summary-grid">
          {summaryCards.map((item) => (
            <div key={item.label} className="federation-diagnostics-summary-card">
              <span>{item.label}</span>
              <strong>{item.value}</strong>
            </div>
          ))}
        </div>
      </div>

      <div className="federation-diagnostics-tabs" role="tablist" aria-label="Federation diagnostics tabs">
        {[
          { value: "plan", label: "Plan" },
          { value: "stages", label: "Stages" },
          { value: "cache", label: "Cache" },
          { value: "pushdown", label: "Pushdown" },
        ].map((tab) => (
          <button
            key={tab.value}
            type="button"
            className={`federation-diagnostics-tab ${activeTab === tab.value ? "active" : ""}`.trim()}
            onClick={() => setActiveTab(tab.value)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "plan" ? (
        <div className="federation-diagnostics-body">
          <div className="federation-diagnostics-callout">
            <strong>{summary.full_query_pushdown ? "Full query pushdown" : "Local federation plan"}</strong>
            <span>
              {summary.full_query_pushdown
                ? "The runtime could execute the query remotely as a single source plan."
                : summary.pushdown_reason || "The runtime split the query into remote and local stages."}
            </span>
          </div>
          <div className="federation-diagnostics-grid">
            <div className="federation-diagnostics-section">
              <h5>Logical plan</h5>
              <div className="federation-diagnostics-meta">
                <span>From: {logicalPlan.from_alias || "n/a"}</span>
                <span>Tables: {formatValue((logicalPlan.tables || []).length)}</span>
                <span>Joins: {formatValue((logicalPlan.joins || []).length)}</span>
              </div>
              {renderSqlBlock("Compiled SQL", logicalPlan.sql)}
              {(logicalPlan.tables || []).length > 0 ? (
                <div className="federation-diagnostics-list">
                  {logicalPlan.tables.map((table) => (
                    <div key={`${table.alias}-${table.table_key}`} className="federation-diagnostics-list-card">
                      <strong>{table.alias}</strong>
                      <span>{[table.dataset, table.table_key, table.source_id].filter(Boolean).join(" | ")}</span>
                    </div>
                  ))}
                </div>
              ) : null}
              {(logicalPlan.joins || []).length > 0 ? (
                <div className="federation-diagnostics-list">
                  {logicalPlan.joins.map((join) => (
                    <div key={`${join.left_alias}-${join.right_alias}`} className="federation-diagnostics-list-card">
                      <strong>{`${join.left_alias} ${join.join_type} ${join.right_alias}`}</strong>
                      <span>{join.strategy ? `Strategy: ${toTitleCase(join.strategy)}` : "Strategy pending"}</span>
                      <small>{join.on_sql}</small>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
            <div className="federation-diagnostics-section">
              <h5>Physical plan</h5>
              <div className="federation-diagnostics-meta">
                <span>Plan: {physicalPlan.plan_id || "n/a"}</span>
                <span>Result stage: {physicalPlan.result_stage_id || "n/a"}</span>
                <span>Join order: {Array.isArray(physicalPlan.join_order) ? physicalPlan.join_order.join(" -> ") || "n/a" : "n/a"}</span>
              </div>
              {Array.isArray(physicalPlan.pushdown_reasons) && physicalPlan.pushdown_reasons.length > 0 ? (
                <div className="federation-diagnostics-note-stack">
                  {physicalPlan.pushdown_reasons.map((reason) => (
                    <p key={reason}>{reason}</p>
                  ))}
                </div>
              ) : null}
              <div className="federation-diagnostics-list">
                {(physicalPlan.stages || []).map((stage) => (
                  <div key={stage.stage_id} className="federation-diagnostics-list-card">
                    <strong>{stage.stage_id}</strong>
                    <span>{[toTitleCase(stage.stage_type), stage.source_id].filter(Boolean).join(" | ")}</span>
                    {stage.dependencies?.length ? <small>Depends on: {stage.dependencies.join(", ")}</small> : null}
                    {renderSqlBlock("Remote SQL", stage.remote_sql)}
                    {renderSqlBlock("Local SQL", stage.local_sql)}
                  </div>
                ))}
              </div>
            </div>
          </div>
          {sources.length > 0 ? (
            <div className="federation-diagnostics-section">
              <h5>Sources</h5>
              <div className="federation-diagnostics-list federation-diagnostics-source-grid">
                {sources.map((source) => (
                  <div key={source.source_id} className="federation-diagnostics-list-card">
                    <strong>{source.source_id}</strong>
                    <span>{(source.datasets || []).join(", ") || "n/a"}</span>
                    <small>{`${formatValue(source.stage_count)} stage(s) | ${source.full_query_pushdown ? "full pushdown" : "split plan"}`}</small>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </div>
      ) : null}

      {activeTab === "stages" ? (
        <div className="federation-diagnostics-body">
          <div className="federation-diagnostics-table-wrap">
            <table className="federation-diagnostics-table">
              <thead>
                <tr>
                  <th>Stage</th>
                  <th>Type</th>
                  <th>Source</th>
                  <th>Runtime</th>
                  <th>Source latency</th>
                  <th>Cache</th>
                  <th>Rows</th>
                  <th>Bytes</th>
                </tr>
              </thead>
              <tbody>
                {stages.map((stage) => (
                  <tr key={stage.stage_id}>
                    <td>
                      <strong>{stage.stage_id}</strong>
                      {stage.alias ? <small>{stage.alias}</small> : null}
                    </td>
                    <td>{toTitleCase(stage.stage_type)}</td>
                    <td>{stage.source_id || "local"}</td>
                    <td>{stage.runtime_ms != null ? `${formatValue(stage.runtime_ms)} ms` : "n/a"}</td>
                    <td>{stage.source_elapsed_ms != null ? `${formatValue(stage.source_elapsed_ms)} ms` : "n/a"}</td>
                    <td>{stage.cache?.status ? toTitleCase(stage.cache.status) : "n/a"}</td>
                    <td>{stage.movement?.rows != null ? formatValue(stage.movement.rows) : "n/a"}</td>
                    <td>{stage.movement?.bytes_written != null ? formatValue(stage.movement.bytes_written) : "n/a"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="federation-diagnostics-list">
            {stages.map((stage) => (
              <div key={`${stage.stage_id}-detail`} className="federation-diagnostics-list-card">
                <strong>{stage.stage_id}</strong>
                <span>{[stage.dataset, stage.source_id || "local"].filter(Boolean).join(" | ")}</span>
                <small>
                  {[
                    stage.attempts != null ? `${formatValue(stage.attempts)} attempt(s)` : null,
                    stage.started_at ? `Started ${formatDateTime(stage.started_at)}` : null,
                    stage.finished_at ? `Finished ${formatDateTime(stage.finished_at)}` : null,
                  ]
                    .filter(Boolean)
                    .join(" | ")}
                </small>
                {stage.cache?.reason ? <p>{stage.cache.reason}</p> : null}
                {renderSqlBlock("Remote SQL", stage.remote_sql)}
                {renderSqlBlock("Local SQL", stage.local_sql)}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {activeTab === "cache" ? (
        <div className="federation-diagnostics-body">
          <div className="federation-diagnostics-summary-grid federation-diagnostics-summary-grid--compact">
            <div className="federation-diagnostics-summary-card">
              <span>Hits</span>
              <strong>{formatValue(cache.hits || 0)}</strong>
            </div>
            <div className="federation-diagnostics-summary-card">
              <span>Misses</span>
              <strong>{formatValue(cache.misses || 0)}</strong>
            </div>
            <div className="federation-diagnostics-summary-card">
              <span>Bypasses</span>
              <strong>{formatValue(cache.bypasses || 0)}</strong>
            </div>
            <div className="federation-diagnostics-summary-card">
              <span>Cacheable stages</span>
              <strong>{formatValue(cache.cacheable_stages || 0)}</strong>
            </div>
          </div>
          <div className="federation-diagnostics-list">
            {(cache.stages || []).map((stage) => {
              const stageDetail = stages.find((item) => item.stage_id === stage.stage_id);
              return (
                <div key={stage.stage_id} className="federation-diagnostics-list-card">
                  <strong>{stage.stage_id}</strong>
                  <span>{[stage.source_id || "local", stage.status ? toTitleCase(stage.status) : "Planned"].filter(Boolean).join(" | ")}</span>
                  {stage.reason ? <p>{stage.reason}</p> : null}
                  {Array.isArray(stageDetail?.cache?.inputs) && stageDetail.cache.inputs.length > 0 ? (
                    <div className="federation-diagnostics-chip-list">
                      {stageDetail.cache.inputs.map((input, index) => (
                        <span key={`${stage.stage_id}-${index}`} className="chip subtle">
                          {[
                            input.dataset_name || input.table_key || input.dependency_stage_id,
                            input.cache_policy,
                            input.materialization_mode,
                            input.revision_id,
                          ]
                            .filter(Boolean)
                            .join(" | ")}
                        </span>
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      ) : null}

      {activeTab === "pushdown" ? (
        <div className="federation-diagnostics-body">
          <div className="federation-diagnostics-callout">
            <strong>{pushdown.full_query_pushdown ? "Remote execution plan" : "Split execution plan"}</strong>
            <span>
              {pushdown.full_query_pushdown
                ? "The runtime pushed the full query to a single source."
                : (pushdown.reasons || []).join(" ") || "The runtime kept part of the work local."}
            </span>
          </div>
          <div className="federation-diagnostics-list">
            {(pushdown.stages || []).map((stage) => (
              <div key={stage.stage_id} className="federation-diagnostics-list-card">
                <strong>{stage.stage_id}</strong>
                <span>{[stage.alias, stage.source_id].filter(Boolean).join(" | ")}</span>
                <div className="federation-pushdown-grid">
                  {renderOperationCard("Full query", stage.full_query)}
                  {renderOperationCard("Filter", stage.filter)}
                  {renderOperationCard("Projection", stage.projection)}
                  {renderOperationCard("Aggregation", stage.aggregation)}
                  {renderOperationCard("Limit", stage.limit)}
                  {renderOperationCard("Join", stage.join)}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}
