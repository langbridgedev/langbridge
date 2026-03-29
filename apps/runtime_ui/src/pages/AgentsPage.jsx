import { useDeferredValue, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Bot, BrainCircuit, ShieldCheck, Workflow } from "lucide-react";

import { ChartPreview } from "../components/ChartPreview";
import { ResultTable } from "../components/ResultTable";
import {
  DetailList,
  PageEmpty,
  Panel,
  SectionTabs,
} from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { askAgent, fetchAgent, fetchAgents } from "../lib/runtimeApi";
import { formatList, formatValue, getErrorMessage } from "../lib/format";
import {
  buildItemRef,
  hasRenderableVisualization,
  normalizeTabularResult,
  normalizeVisualizationSpec,
  readAgentAllowedConnectors,
  readAgentFeatureFlags,
  readAgentSystemPrompt,
  renderJson,
  resolveItemByRef,
} from "../lib/runtimeUi";

export function AgentsPage() {
  const params = useParams();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const [trialMessage, setTrialMessage] = useState(
    "Summarize the most relevant runtime signals for this workspace.",
  );
  const [trialResponse, setTrialResponse] = useState(null);
  const [trialError, setTrialError] = useState("");
  const [trialRunning, setTrialRunning] = useState(false);
  const deferredSearch = useDeferredValue(search);
  const { data, loading, error, reload } = useAsyncData(fetchAgents);
  const agents = Array.isArray(data?.items) ? data.items : [];
  const selected = resolveItemByRef(agents, params.id);
  const filteredAgents = agents.filter((item) => {
    const haystack = [
      item.name,
      item.description,
      item.llm_connection,
      ...(Array.isArray(item.tools) ? item.tools.map((tool) => tool.name) : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(String(deferredSearch || "").trim().toLowerCase());
  });

  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const totalTools = agents.reduce((sum, item) => sum + Number(item.tool_count || 0), 0);
  const enabledFeatureFlags = readAgentFeatureFlags(detail);
  const allowedConnectors = readAgentAllowedConnectors(detail);
  const systemPrompt = readAgentSystemPrompt(detail);
  const trialResult = trialResponse?.result ? normalizeTabularResult(trialResponse.result) : null;
  const trialVisualization = normalizeVisualizationSpec(trialResponse?.visualization);

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
        const payload = await fetchAgent(String(selected.id || selected.name));
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

  async function handleQuickAsk(event) {
    event.preventDefault();
    if (!selected?.name || !trialMessage.trim()) {
      return;
    }
    setTrialRunning(true);
    setTrialError("");
    setTrialResponse(null);
    try {
      const payload = await askAgent({
        message: trialMessage.trim(),
        agent_name: selected.name,
        title: `Quick run - ${selected.name}`,
      });
      setTrialResponse(payload);
    } catch (caughtError) {
      setTrialError(getErrorMessage(caughtError));
    } finally {
      setTrialRunning(false);
    }
  }

  return (
    <div className="page-stack">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Agents</p>
            <h2>{selected?.name || "Agent inventory"}</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(agents.length)} agents</span>
              <span className="chip">{formatValue(totalTools)} tools</span>
              <span className="chip">{formatValue(detail?.tools?.length || 0)} selected tools</span>
              <span className="chip">{formatValue(allowedConnectors.length)} allowed connectors</span>
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
          placeholder="Filter agents by name, connection, or tool"
        />
        <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh agents"}
        </button>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="split-layout">
        <Panel title="Agent inventory" className="list-panel compact-panel">
          {filteredAgents.length > 0 ? (
            <div className="stack-list">
              {filteredAgents.map((item) => (
                <Link
                  key={item.id || item.name}
                  className={`list-card ${selected?.id === item.id ? "active" : ""}`}
                  to={`/agents/${buildItemRef(item)}`}
                >
                  <strong>{item.name}</strong>
                  <span>
                    {[item.llm_connection, `${item.tool_count || 0} tools`, item.default ? "default" : null]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </Link>
              ))}
            </div>
          ) : (
            <PageEmpty
              title="No agents"
              message="Define runtime agents to use local chat and BI copilots."
            />
          )}
        </Panel>

        <div className="detail-stack">
          {selected ? (
            <>
              <Panel
                title={selected.name}
                className="compact-panel"
                actions={
                  <div className="panel-actions-inline">
                    <button className="ghost-button" type="button" onClick={() => navigate("/chat")}>
                      Open chat
                    </button>
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => setActiveTab("definition")}
                    >
                      View definition
                    </button>
                  </div>
                }
              >
                {detailError ? <div className="error-banner">{detailError}</div> : null}
                {detailLoading ? (
                  <div className="empty-box">Loading agent detail...</div>
                ) : detail ? (
                  <>
                    <div className="inline-notes">
                      <span>{detail.default ? "Default agent" : "Runtime agent"}</span>
                      <span>{detail.llm_connection || "No LLM connection set"}</span>
                      <span>{detail.tools?.length || 0} tools</span>
                    </div>
                    <DetailList
                      items={[
                        { label: "Description", value: formatValue(detail.description) },
                        { label: "LLM connection", value: formatValue(detail.llm_connection) },
                        { label: "Semantic model", value: formatValue(detail.semantic_model) },
                        { label: "Dataset", value: formatValue(detail.dataset) },
                        { label: "Default", value: formatValue(detail.default) },
                      ]}
                    />
                  </>
                ) : (
                  <PageEmpty
                    title="No detail"
                    message="The runtime did not return agent detail."
                  />
                )}
              </Panel>

              <section className="summary-grid">
                <Panel title="Prompt and execution" eyebrow="Behavior">
                  {detail ? (
                    <>
                      <div className="callout">
                        <strong>System prompt</strong>
                        <span>{systemPrompt || "No explicit system prompt exposed by the runtime."}</span>
                      </div>
                      <DetailList
                        items={[
                          {
                            label: "Execution mode",
                            value: formatValue(detail.definition?.execution?.mode),
                          },
                          {
                            label: "Response mode",
                            value: formatValue(detail.definition?.execution?.response_mode),
                          },
                          {
                            label: "Max iterations",
                            value: formatValue(detail.definition?.execution?.max_iterations),
                          },
                          {
                            label: "Output format",
                            value: formatValue(detail.definition?.output?.format),
                          },
                        ]}
                      />
                    </>
                  ) : (
                    <PageEmpty
                      title="No behavior detail"
                      message="Select an agent to inspect prompt and execution posture."
                    />
                  )}
                </Panel>

                <Panel title="Access policy" eyebrow="Guardrails">
                  {detail ? (
                    <>
                      {enabledFeatureFlags.length > 0 ? (
                        <div className="tag-list">
                          {enabledFeatureFlags.map((item) => (
                            <span key={item} className="tag">
                              {item}
                            </span>
                          ))}
                        </div>
                      ) : null}
                      <DetailList
                        items={[
                          { label: "Allowed connectors", value: formatList(allowedConnectors) },
                          {
                            label: "Denied connectors",
                            value: formatList(detail.definition?.access_policy?.denied_connectors),
                          },
                          {
                            label: "Moderation enabled",
                            value: formatValue(detail.definition?.guardrails?.moderation_enabled),
                          },
                          {
                            label: "Parallel tools",
                            value: formatValue(detail.definition?.execution?.allow_parallel_tools),
                          },
                        ]}
                      />
                    </>
                  ) : (
                    <PageEmpty
                      title="No policy detail"
                      message="Select an agent to inspect access policy."
                    />
                  )}
                </Panel>
              </section>

              <Panel title="Agent workspace" eyebrow="Inspect and try">
                <SectionTabs
                  tabs={[
                    { value: "overview", label: "Overview" },
                    { value: "tools", label: "Tools" },
                    { value: "definition", label: "Definition" },
                    { value: "try", label: "Quick ask" },
                  ]}
                  value={activeTab}
                  onChange={setActiveTab}
                />

                {activeTab === "overview" ? (
                  <div className="detail-card-grid">
                    <article className="detail-card">
                      <strong>Semantic context</strong>
                      <span>{detail?.semantic_model || "No semantic model attached"}</span>
                      <small>{detail?.dataset || "No dataset shortcut configured"}</small>
                    </article>
                    <article className="detail-card">
                      <strong>Tool posture</strong>
                      <span>{detail?.tools?.length || 0} attached tools</span>
                      <small>
                        {enabledFeatureFlags.length > 0
                          ? enabledFeatureFlags.join(", ")
                          : "No feature flags exposed"}
                      </small>
                    </article>
                    <article className="detail-card">
                      <strong>Connector access</strong>
                      <span>
                        {allowedConnectors.length > 0
                          ? allowedConnectors.join(", ")
                          : "Unspecified"}
                      </span>
                      <small>
                        Runtime UI intentionally excludes cloud agent-definition editing workflows.
                      </small>
                    </article>
                  </div>
                ) : null}

                {activeTab === "tools" ? (
                  detail?.tools && detail.tools.length > 0 ? (
                    <div className="detail-card-grid">
                      {detail.tools.map((tool) => (
                        <article key={`${tool.name}-${tool.tool_type}`} className="detail-card">
                          <strong>{tool.name}</strong>
                          <span>{tool.tool_type || "runtime tool"}</span>
                          {tool.description ? <small>{tool.description}</small> : null}
                          {tool.config ? (
                            <pre className="code-block compact">{renderJson(tool.config)}</pre>
                          ) : null}
                        </article>
                      ))}
                    </div>
                  ) : (
                    <PageEmpty
                      title="No tools exposed"
                      message="This agent does not currently expose runtime tool metadata."
                    />
                  )
                ) : null}

                {activeTab === "definition" ? (
                  detail?.definition ? (
                    <pre className="code-block">{renderJson(detail.definition)}</pre>
                  ) : (
                    <PageEmpty
                      title="No definition payload"
                      message="The runtime did not expose a definition snapshot for this agent."
                    />
                  )
                ) : null}

                {activeTab === "try" ? (
                  <div className="page-stack">
                    <form className="form-grid" onSubmit={handleQuickAsk}>
                      <label className="field field-full">
                        <span>Prompt</span>
                        <textarea
                          className="textarea-input"
                          value={trialMessage}
                          onChange={(event) => setTrialMessage(event.target.value)}
                          rows={5}
                          disabled={trialRunning}
                        />
                      </label>
                      <div className="page-actions field-full">
                        <button
                          className="primary-button"
                          type="submit"
                          disabled={trialRunning || !trialMessage.trim()}
                        >
                          {trialRunning ? "Running agent..." : "Run agent"}
                        </button>
                        {trialResponse?.thread_id ? (
                          <button
                            className="ghost-button"
                            type="button"
                            onClick={() =>
                              navigate(`/chat/${encodeURIComponent(String(trialResponse.thread_id))}`)
                            }
                          >
                            Open thread
                          </button>
                        ) : null}
                      </div>
                    </form>
                    {trialError ? <div className="error-banner">{trialError}</div> : null}
                    {trialResponse ? (
                      <>
                        <div className="callout">
                          <strong>{trialResponse.summary || "Run completed"}</strong>
                          <span>
                            {trialResponse.thread_id
                              ? `Thread ${trialResponse.thread_id} was created for this quick run.`
                              : "The runtime returned a direct response."}
                          </span>
                        </div>
                        {trialResult ? (
                          <>
                            {trialVisualization &&
                            hasRenderableVisualization(trialResponse.visualization) ? (
                              <ChartPreview
                                title={trialVisualization.title}
                                result={trialResult}
                                visualization={trialVisualization}
                                preferredDimension={trialVisualization.x}
                                preferredMeasure={trialVisualization.y?.[0]}
                              />
                            ) : null}
                            <ResultTable result={trialResult} maxPreviewRows={12} />
                          </>
                        ) : null}
                      </>
                    ) : (
                      <PageEmpty
                        title="No quick run yet"
                        message="Run the selected agent here to inspect its current runtime behavior."
                      />
                    )}
                  </div>
                ) : null}
              </Panel>
            </>
          ) : (
            <Panel title="Agent detail" eyebrow="Runtime">
              <PageEmpty
                title="No agent selected"
                message="Pick an agent to inspect its runtime bindings and definition."
              />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}
