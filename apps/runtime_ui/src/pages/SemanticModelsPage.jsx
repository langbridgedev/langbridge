import { useDeferredValue, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Database, Layers3, SearchCheck, Sparkles } from "lucide-react";

import {
  DetailList,
  PageEmpty,
  Panel,
  SectionTabs,
} from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { fetchSemanticModel, fetchSemanticModels } from "../lib/runtimeApi";
import { formatList, formatValue, getErrorMessage } from "../lib/format";
import {
  buildItemRef,
  extractSemanticDatasets,
  extractSemanticFields,
  renderJson,
  resolveItemByRef,
} from "../lib/runtimeUi";

export function SemanticModelsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [fieldSearch, setFieldSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const deferredSearch = useDeferredValue(search);
  const deferredFieldSearch = useDeferredValue(fieldSearch);
  const { data, loading, error, reload } = useAsyncData(fetchSemanticModels);
  const models = Array.isArray(data?.items) ? data.items : [];
  const selected = resolveItemByRef(models, params.id);
  const filteredModels = models.filter((item) => {
    const haystack = [
      item.name,
      item.description,
      ...(Array.isArray(item.dataset_names) ? item.dataset_names : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const semanticDatasets = extractSemanticDatasets(detail);
  const semanticFields = extractSemanticFields(detail);
  const filteredSemanticDatasets = semanticDatasets
    .map((dataset) => {
      const searchTerm = String(deferredFieldSearch || "").trim().toLowerCase();
      if (!searchTerm) {
        return dataset;
      }
      return {
        ...dataset,
        dimensions: dataset.dimensions.filter((item) =>
          String(item?.name || "").toLowerCase().includes(searchTerm),
        ),
        measures: dataset.measures.filter((item) =>
          String(item?.name || "").toLowerCase().includes(searchTerm),
        ),
      };
    })
    .filter(
      (dataset) =>
        !deferredFieldSearch ||
        String(dataset.name)
          .toLowerCase()
          .includes(String(deferredFieldSearch).toLowerCase()) ||
        dataset.dimensions.length > 0 ||
        dataset.measures.length > 0,
    );

  useEffect(() => {
    let cancelled = false;

    async function loadDetail() {
      if (!selected) {
        setDetail(null);
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchSemanticModel(String(selected.id || selected.name));
        if (!cancelled) {
          setDetail(payload);
        }
      } catch (caughtError) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadDetail();

    return () => {
      cancelled = true;
    };
  }, [selected?.id, selected?.name]);

  return (
    <div className="page-stack">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Semantic Models</p>
            <h2>{selected?.name || "Semantic inventory"}</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(models.length)} models</span>
              <span className="chip">{formatValue(detail?.dataset_count || semanticDatasets.length)} datasets</span>
              <span className="chip">{formatValue(detail?.dimension_count || semanticFields.dimensions.length)} dimensions</span>
              <span className="chip">{formatValue(detail?.measure_count || semanticFields.measures.length)} measures</span>
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
          placeholder="Filter semantic models by name or dataset"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh semantic models"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Semantic models" className="list-panel compact-panel">
          {filteredModels.length > 0 ? (
            <div className="stack-list">
              {filteredModels.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/semantic-models/${buildItemRef(item)}`}
                >
                  <strong>{item.name}</strong>
                  <span>
                    {[
                      `${item.dataset_count || 0} datasets`,
                      `${item.measure_count || 0} measures`,
                      item.default ? "default" : null,
                    ]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty
              title="No semantic models"
              message="This runtime does not expose semantic model metadata yet."
            />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel title={selected.name} className="compact-panel">
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading semantic model detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.default ? "Default runtime model" : "Secondary model"}</span>
                      <span>
                        {detail.dataset_count || semanticDatasets.length} semantic datasets
                      </span>
                      <span>
                        {detail.measure_count || semanticFields.measures.length} measures
                      </span>
                    </div>
                    <DetailList
                      items={[
                        { label: "Description", value: formatValue(detail.description) },
                        { label: "Default", value: formatValue(detail.default) },
                        { label: "Datasets", value: formatList(detail.dataset_names) },
                        {
                          label: "Dimension count",
                          value: formatValue(detail.dimension_count),
                        },
                        { label: "Measure count", value: formatValue(detail.measure_count) },
                      ]}
                    />
                    <div className="panel-actions-inline">
                      <button className="ghost-button" type="button" onClick={() => navigate("/bi")}>
                        Open BI
                      </button>
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => navigate("/chat")}
                      >
                        Open chat
                      </button>
                    </div>
                  </>
                ) : (
                  <PageEmpty
                    title="No detail"
                    message="The runtime did not return semantic model detail."
                  />
                )}
              </Panel>

              <section className="summary-grid">
                <Panel title="Dataset explorer" eyebrow="Model structure">
                  {semanticDatasets.length > 0 ? (
                    <div className="detail-card-grid">
                      {semanticDatasets.map((item) => (
                        <article key={item.name} className="detail-card">
                          <strong>{item.name}</strong>
                          <span>{item.relationName || "No explicit relation name"}</span>
                          <div className="tag-list">
                            <span className="tag">{item.dimensions.length} dimensions</span>
                            <span className="tag">{item.measures.length} measures</span>
                          </div>
                          <small>
                            {[
                              item.dimensions
                                .slice(0, 3)
                                .map((field) => field.name)
                                .join(", "),
                              item.measures
                                .slice(0, 3)
                                .map((field) => field.name)
                                .join(", "),
                            ]
                              .filter(Boolean)
                              .join(" | ")}
                          </small>
                        </article>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty
                      title="No semantic datasets"
                      message="This model did not expose semantic dataset groups."
                    />
                  )}
                </Panel>

                <Panel title="Field inventory" eyebrow="Dimensions and measures">
                  {semanticFields.dimensions.length > 0 ||
                  semanticFields.measures.length > 0 ? (
                    <div className="field-section-list">
                      <div className="field-group">
                        <div className="field-group-header">
                          <strong>Dimensions</strong>
                          <span>{semanticFields.dimensions.length}</span>
                        </div>
                        <div className="field-pill-list">
                          {semanticFields.dimensions.map((item) => (
                            <span key={item.value} className="field-pill static">
                              {item.label}
                            </span>
                          ))}
                        </div>
                      </div>
                      <div className="field-group">
                        <div className="field-group-header">
                          <strong>Measures</strong>
                          <span>{semanticFields.measures.length}</span>
                        </div>
                        <div className="field-pill-list">
                          {semanticFields.measures.map((item) => (
                            <span key={item.value} className="field-pill static">
                              {item.label}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <PageEmpty
                      title="No fields exposed"
                      message="This model did not expose dimensions or measures."
                    />
                  )}
                </Panel>
              </section>

              <Panel title="Semantic workspace" eyebrow="Inspect">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "datasets", label: "Datasets" },
                    { value: "fields", label: "Fields" },
                    { value: "yaml", label: "YAML" },
                    { value: "json", label: "JSON" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="detail-card-grid">
                    {semanticDatasets.map((item) => (
                      <article key={item.name} className="detail-card">
                        <strong>{item.name}</strong>
                        <span>{item.relationName || "No relation name provided"}</span>
                        <div className="tag-list">
                          <span className="tag">{item.dimensions.length} dimensions</span>
                          <span className="tag">{item.measures.length} measures</span>
                        </div>
                        <small>
                          {[
                            item.dimensions
                              .slice(0, 3)
                              .map((field) => field.name)
                              .join(", "),
                            item.measures
                              .slice(0, 3)
                              .map((field) => field.name)
                              .join(", "),
                          ]
                            .filter(Boolean)
                            .join(" | ")}
                        </small>
                      </article>
                    ))}
                  </div>
                ) : null}

                {activeTab === "datasets" ? (
                  filteredSemanticDatasets.length > 0 ? (
                    <div className="field-section-list">
                      {filteredSemanticDatasets.map((dataset) => (
                        <div key={dataset.name} className="field-group">
                          <div className="field-group-header">
                            <strong>{dataset.name}</strong>
                            <span>{dataset.relationName || "semantic dataset"}</span>
                          </div>
                          <div className="tag-list">
                            <span className="tag">{dataset.dimensions.length} dimensions</span>
                            <span className="tag">{dataset.measures.length} measures</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty
                      title="No semantic datasets"
                      message="This model did not expose semantic dataset groups."
                    />
                  )
                ) : null}

                {activeTab === "fields" ? (
                  <div className="page-stack">
                    <label className="field">
                      <span>Find fields</span>
                      <input
                        className="text-input"
                        type="search"
                        value={fieldSearch}
                        onChange={(event) => setFieldSearch(event.target.value)}
                        placeholder="Filter datasets, dimensions, or measures"
                      />
                    </label>
                    {filteredSemanticDatasets.length > 0 ? (
                      <div className="field-section-list">
                        {filteredSemanticDatasets.map((dataset) => (
                          <div key={`${dataset.name}-fields`} className="field-group">
                            <div className="field-group-header">
                              <strong>{dataset.name}</strong>
                              <span>{dataset.relationName || "semantic dataset"}</span>
                            </div>
                            <div className="field-pill-list">
                              {dataset.dimensions.map((item) => (
                                <span
                                  key={`${dataset.name}-${item.name}-dimension`}
                                  className="field-pill static"
                                >
                                  {item.name}
                                </span>
                              ))}
                              {dataset.measures.map((item) => (
                                <span
                                  key={`${dataset.name}-${item.name}-measure`}
                                  className="field-pill static"
                                >
                                  {item.name}
                                </span>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <PageEmpty title="No fields found" message="Adjust the filter or switch models." />
                    )}
                  </div>
                ) : null}

                {activeTab === "yaml" ? (
                  detail?.content_yaml ? (
                    <pre className="code-block">{detail.content_yaml}</pre>
                  ) : (
                    <PageEmpty
                      title="No YAML available"
                      message="This semantic model did not expose YAML content."
                    />
                  )
                ) : null}

                {activeTab === "json" ? (
                  detail?.content_json ? (
                    <pre className="code-block">{renderJson(detail.content_json)}</pre>
                  ) : (
                    <PageEmpty
                      title="No JSON payload"
                      message="This semantic model did not expose a JSON representation."
                    />
                  )
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Semantic model detail" eyebrow="Runtime">
              <PageEmpty
                title="No model selected"
                message="Pick a semantic model to inspect its runtime definition."
              />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}
