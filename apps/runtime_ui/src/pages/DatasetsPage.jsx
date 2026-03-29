import { useDeferredValue, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Cable, Database, Layers3, SearchCheck } from "lucide-react";

import { ResultTable } from "../components/ResultTable";
import {
  DetailList,
  PageEmpty,
  Panel,
  SectionTabs,
} from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { fetchDataset, fetchDatasets, previewDataset } from "../lib/runtimeApi";
import {
  formatDateTime,
  formatList,
  formatValue,
  getErrorMessage,
  toSqlAlias,
} from "../lib/format";
import {
  buildItemRef,
  countUniqueValues,
  downloadTextFile,
  normalizeTabularResult,
  resolveItemByRef,
  toCsvText,
} from "../lib/runtimeUi";

export function DatasetsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload } = useAsyncData(fetchDatasets);
  const datasets = Array.isArray(data?.items) ? data.items : [];
  const selected = resolveItemByRef(datasets, params.id);
  const filteredDatasets = datasets.filter((item) => {
    const haystack = [
      item.name,
      item.label,
      item.description,
      item.connector,
      item.semantic_model,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [preview, setPreview] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const [previewLimit, setPreviewLimit] = useState("25");
  const boundConnectorCount = countUniqueValues(datasets, (item) => item.connector);
  const boundSemanticModelCount = countUniqueValues(datasets, (item) => item.semantic_model);
  const schemaColumns = Array.isArray(detail?.columns) ? detail.columns : [];
  const nullableColumns = schemaColumns.filter((column) => column.nullable).length;
  const computedColumns = schemaColumns.filter((column) => column.is_computed).length;
  const policy = detail?.policy && typeof detail.policy === "object" ? detail.policy : null;
  const previewResult = preview ? normalizeTabularResult(preview) : null;

  async function loadDatasetDetail(target = selected) {
    if (!target) {
      setDetail(null);
      setPreview(null);
      return;
    }
    setDetailLoading(true);
    setDetailError("");
    setPreviewLoading(true);
    setPreviewError("");
    try {
      const [detailPayload, previewPayload] = await Promise.all([
        fetchDataset(String(target.id || target.name)),
        previewDataset(String(target.id || target.name), {
          limit: Number(previewLimit) > 0 ? Number(previewLimit) : 25,
        }),
      ]);
      setDetail(detailPayload);
      setPreview(previewPayload);
    } catch (caughtError) {
      const message = getErrorMessage(caughtError);
      setDetail(null);
      setDetailError(message);
      setPreview(null);
      setPreviewError(message);
    } finally {
      setDetailLoading(false);
      setPreviewLoading(false);
    }
  }

  useEffect(() => {
    void loadDatasetDetail(selected);
  }, [selected?.id, selected?.name]);

  return (
    <div className="page-stack">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Datasets</p>
            <h2>{detail?.label || selected?.label || selected?.name || "Dataset inventory"}</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(datasets.length)} datasets</span>
              <span className="chip">{formatValue(boundConnectorCount)} connectors</span>
              <span className="chip">{formatValue(boundSemanticModelCount)} semantic links</span>
              <span className="chip">{formatValue(schemaColumns.length)} columns</span>
            </div>
          </div>
        </div>
      </section>

      <section className="product-search-bar">
        <input
          className="text-input search-input"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Filter datasets by name, connector, or semantic model"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh datasets"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Dataset inventory" className="compact-panel">
          {filteredDatasets.length > 0 ? (
            <div className="stack-list">
              {filteredDatasets.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/datasets/${buildItemRef(item)}`}
                >
                  <strong>{item.label || item.name}</strong>
                  <span>
                    {[item.connector, item.semantic_model].filter(Boolean).join(" | ") || "No bindings"}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty
              title="No datasets found"
              message="Adjust the filter or define datasets in the runtime config."
            />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel
                title={detail?.label || selected.label || selected.name}
                className="compact-panel"
                actions={
                  <div className="panel-actions-inline">
                    <button className="ghost-button" type="button" onClick={() => navigate("/sql")}>
                      Open SQL
                    </button>
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => void loadDatasetDetail()}
                      disabled={detailLoading || previewLoading}
                    >
                      {detailLoading || previewLoading ? "Refreshing..." : "Refresh detail"}
                    </button>
                  </div>
                }
              >
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading dataset detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.connector || "No connector binding"}</span>
                      <span>{detail.semantic_model || "No semantic model binding"}</span>
                      <span>{detail.dataset_type || "runtime dataset"}</span>
                    </div>
                    {Array.isArray(detail.tags) && detail.tags.length > 0 ? (
                      <div className="tag-list">
                        {detail.tags.map((tag) => (
                          <span key={tag} className="tag">
                            #{tag}
                          </span>
                        ))}
                      </div>
                    ) : null}
                    <DetailList
                      items={[
                        { label: "Name", value: formatValue(detail.name) },
                        { label: "SQL alias", value: formatValue(detail.sql_alias) },
                        { label: "Connector", value: formatValue(detail.connector) },
                        { label: "Semantic model", value: formatValue(detail.semantic_model) },
                        { label: "Type", value: formatValue(detail.dataset_type) },
                        { label: "Managed", value: formatValue(detail.managed) },
                        { label: "Tags", value: formatList(detail.tags) },
                      ]}
                    />
                  </>
                ) : (
                  <PageEmpty
                    title="No detail"
                    message="The runtime did not return dataset detail for this item."
                  />
                )}
              </Panel>

              <section className="summary-grid">
                <Panel title="Bindings and execution" eyebrow="Operational">
                  {detail ? (
                    <DetailList
                      items={[
                        { label: "Source kind", value: formatValue(detail.source_kind) },
                        { label: "Storage kind", value: formatValue(detail.storage_kind) },
                        { label: "Storage URI", value: formatValue(detail.storage_uri) },
                        { label: "Table name", value: formatValue(detail.table_name) },
                        { label: "Dialect", value: formatValue(detail.dialect) },
                        {
                          label: "Preview row count",
                          value: formatValue(preview?.rowCount || preview?.row_count_preview),
                        },
                      ]}
                    />
                  ) : (
                    <PageEmpty
                      title="No runtime binding"
                      message="Select a dataset to inspect execution metadata."
                    />
                  )}
                </Panel>

                <Panel title="Schema signals" eyebrow="Columns">
                  {detail ? (
                    <DetailList
                      items={[
                        { label: "Columns", value: formatValue(schemaColumns.length) },
                        { label: "Nullable columns", value: formatValue(nullableColumns) },
                        { label: "Computed columns", value: formatValue(computedColumns) },
                        { label: "Preview limit", value: formatValue(previewLimit) },
                      ]}
                    />
                  ) : (
                    <PageEmpty
                      title="No schema signals"
                      message="Select a dataset to inspect schema detail."
                    />
                  )}
                </Panel>
              </section>

              <Panel title="Dataset workspace" eyebrow="Inspect">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "schema", label: "Schema" },
                    { value: "preview", label: "Preview" },
                    { value: "runtime", label: "Runtime meta" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="detail-card-grid">
                    <article className="detail-card">
                      <strong>Connector binding</strong>
                      <span>{detail?.connector || "None"}</span>
                      {detail?.connector_id ? (
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() =>
                            navigate(`/connectors/${encodeURIComponent(String(detail.connector_id))}`)
                          }
                        >
                          Open connector
                        </button>
                      ) : null}
                    </article>
                    <article className="detail-card">
                      <strong>Semantic binding</strong>
                      <span>{detail?.semantic_model || "Not attached"}</span>
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => navigate("/semantic-models")}
                      >
                        Open semantic models
                      </button>
                    </article>
                    <article className="detail-card">
                      <strong>SQL alias</strong>
                      <span>{detail?.sql_alias || toSqlAlias(detail?.name || selected.name)}</span>
                      <small>Use this alias from the runtime SQL workspace.</small>
                    </article>
                    <article className="detail-card">
                      <strong>Policy posture</strong>
                      <span>
                        {policy ? `${policy.max_rows_preview || "n/a"} preview rows` : "No policy metadata"}
                      </span>
                      <small>Runtime UI intentionally excludes cloud revisioning and governance workflows.</small>
                    </article>
                  </div>
                ) : null}

                {activeTab === "schema" ? (
                  schemaColumns.length > 0 ? (
                    <div className="table-wrap">
                      <table className="result-table">
                        <thead>
                          <tr>
                            <th>Name</th>
                            <th>Type</th>
                            <th>Nullable</th>
                            <th>Computed</th>
                            <th>Description</th>
                          </tr>
                        </thead>
                        <tbody>
                          {schemaColumns.map((column) => (
                            <tr key={column.id || column.name}>
                              <td>{column.name}</td>
                              <td>{formatValue(column.data_type)}</td>
                              <td>{formatValue(column.nullable)}</td>
                              <td>{formatValue(column.is_computed)}</td>
                              <td>{formatValue(column.description)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <PageEmpty
                      title="No column metadata"
                      message="This dataset did not expose column metadata."
                    />
                  )
                ) : null}

                {activeTab === "preview" ? (
                  <>
                    <div className="panel-actions-inline">
                      <input
                        className="text-input narrow-input"
                        type="number"
                        min="1"
                        value={previewLimit}
                        onChange={(event) => setPreviewLimit(event.target.value)}
                      />
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => void loadDatasetDetail()}
                        disabled={previewLoading}
                      >
                        {previewLoading ? "Loading..." : "Run preview"}
                      </button>
                      {previewResult ? (
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() =>
                            downloadTextFile(
                              `${toSqlAlias(detail?.name || selected.name)}-preview.csv`,
                              toCsvText(previewResult),
                              "text/csv;charset=utf-8",
                            )
                          }
                        >
                          Download CSV
                        </button>
                      ) : null}
                    </div>
                    {previewError ? <div className="error-banner">{previewError}</div> : null}
                    {previewLoading ? (
                      <div className="empty-box">Running dataset preview...</div>
                    ) : previewResult ? (
                      <>
                        <div className="inline-notes">
                          <span>Rows: {formatValue(preview.rowCount || preview.row_count_preview)}</span>
                          <span>Limit: {formatValue(preview.effective_limit || previewLimit)}</span>
                          <span>Redaction: {formatValue(preview.redaction_applied)}</span>
                        </div>
                        <ResultTable result={previewResult} maxPreviewRows={12} />
                        {preview.generated_sql ? <pre className="code-block">{preview.generated_sql}</pre> : null}
                      </>
                    ) : (
                      <PageEmpty
                        title="No preview"
                        message="Run a preview to inspect dataset rows from the runtime."
                      />
                    )}
                  </>
                ) : null}

                {activeTab === "runtime" ? (
                  <div className="summary-grid">
                    <Panel title="Policy" eyebrow="Runtime guardrails" className="panel--flat">
                      {policy ? (
                        <DetailList
                          items={[
                            { label: "Max preview rows", value: formatValue(policy.max_rows_preview) },
                            { label: "Max export rows", value: formatValue(policy.max_export_rows) },
                            { label: "Allow DML", value: formatValue(policy.allow_dml) },
                            {
                              label: "Redaction rules",
                              value: formatValue(Object.keys(policy.redaction_rules || {}).length),
                            },
                            {
                              label: "Row filters",
                              value: formatValue((policy.row_filters || []).length),
                            },
                          ]}
                        />
                      ) : (
                        <PageEmpty
                          title="No policy metadata"
                          message="This dataset did not expose runtime policy data."
                        />
                      )}
                    </Panel>
                    <Panel title="Execution" eyebrow="Runtime contracts" className="panel--flat">
                      {detail ? (
                        <>
                          <div className="detail-card">
                            <strong>Relation identity</strong>
                            <pre className="code-block compact">
                              {JSON.stringify(detail.relation_identity || {}, null, 2)}
                            </pre>
                          </div>
                          <div className="detail-card">
                            <strong>Execution capabilities</strong>
                            <pre className="code-block compact">
                              {JSON.stringify(detail.execution_capabilities || {}, null, 2)}
                            </pre>
                          </div>
                        </>
                      ) : (
                        <PageEmpty
                          title="No runtime metadata"
                          message="Select a dataset to inspect runtime execution metadata."
                        />
                      )}
                    </Panel>
                  </div>
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Dataset detail" eyebrow="Runtime">
              <PageEmpty
                title="No dataset selected"
                message="Pick a dataset to inspect its metadata and preview rows."
              />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}
