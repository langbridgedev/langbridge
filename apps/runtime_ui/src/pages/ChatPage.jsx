import { useEffect, useMemo, useRef, useState } from "react";
import { Navigate, useNavigate, useParams } from "react-router-dom";
import { ArrowRight, Bot, Edit3, History, RefreshCw, Sparkles } from "lucide-react";

import { ChartPreview } from "../components/ChartPreview";
import { ResultTable } from "../components/ResultTable";
import { DetailList, PageEmpty } from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import {
  askAgent,
  fetchAgents,
  fetchThread,
  fetchThreadMessages,
  updateThread,
} from "../lib/runtimeApi";
import { formatValue, getErrorMessage } from "../lib/format";
import {
  CHAT_STARTERS,
  DEFAULT_CHAT_MESSAGE,
  buildConversationTurns,
  createLocalId,
  formatRelativeTime,
  hasRenderableVisualization,
  normalizeVisualizationSpec,
  renderJson,
} from "../lib/runtimeUi";

export function ChatPage() {
  const navigate = useNavigate();
  const params = useParams();
  const threadId = String(params.threadId || "").trim();
  const agentsState = useAsyncData(fetchAgents);
  const agents = Array.isArray(agentsState.data?.items) ? agentsState.data.items : [];

  const [selectedAgentName, setSelectedAgentName] = useState("");
  const [message, setMessage] = useState(DEFAULT_CHAT_MESSAGE);
  const [thread, setThread] = useState(null);
  const [messages, setMessages] = useState([]);
  const [threadLoading, setThreadLoading] = useState(false);
  const [threadError, setThreadError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const [threadMutationError, setThreadMutationError] = useState("");
  const [renaming, setRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState("");
  const [renamingOpen, setRenamingOpen] = useState(false);
  const [transientTurn, setTransientTurn] = useState(null);
  const selectedAgent = agents.find((item) => item.name === selectedAgentName) || null;
  const turns = useMemo(() => buildConversationTurns(messages, agents), [messages, agents]);
  const displayTurns = useMemo(() => {
    if (!transientTurn) {
      return turns;
    }
    if (turns.some((turn) => String(turn.id) === String(transientTurn.id))) {
      return turns;
    }
    return [...turns, transientTurn];
  }, [turns, transientTurn]);
  const timelineEndRef = useRef(null);
  const readyTurns = displayTurns.filter((turn) => turn.status === "ready");
  const latestArtifactTurn =
    [...readyTurns]
      .reverse()
      .find((turn) => turn.assistantTable || turn.assistantVisualization) || null;
  const lastUpdated =
    readyTurns.length > 0
      ? readyTurns[readyTurns.length - 1].createdAt
      : transientTurn?.createdAt || thread?.updated_at || null;
  const isPending = submitting || displayTurns.some((turn) => turn.status === "pending");

  useEffect(() => {
    if (!threadId) {
      return;
    }
    const storageKey = `runtime-thread-agent:${threadId}`;
    try {
      const stored = window.localStorage.getItem(storageKey);
      if (stored) {
        setSelectedAgentName(stored);
      }
    } catch {}
  }, [threadId]);

  useEffect(() => {
    if (!threadId) {
      return;
    }
    const storageKey = `runtime-thread-agent:${threadId}`;
    try {
      if (selectedAgentName) {
        window.localStorage.setItem(storageKey, selectedAgentName);
      } else {
        window.localStorage.removeItem(storageKey);
      }
    } catch {}
  }, [selectedAgentName, threadId]);

  useEffect(() => {
    if (!selectedAgentName && agents.length > 0) {
      setSelectedAgentName(agents.find((item) => item.default)?.name || agents[0].name);
    }
  }, [agents, selectedAgentName]);

  useEffect(() => {
    if (!threadId || typeof window === "undefined") {
      return;
    }
    const draftKey = `runtime-thread-draft:${threadId}`;
    const storedDraft = window.sessionStorage.getItem(draftKey);
    if (!storedDraft) {
      return;
    }
    setMessage(storedDraft);
    window.sessionStorage.removeItem(draftKey);
  }, [threadId]);

  useEffect(() => {
    let cancelled = false;

    async function loadThreadState() {
      if (!threadId) {
        return;
      }
      setThreadLoading(true);
      setThreadError("");
      try {
        const [threadPayload, messagePayload] = await Promise.all([
          fetchThread(threadId),
          fetchThreadMessages(threadId),
        ]);
        if (cancelled) {
          return;
        }
        setThread(threadPayload);
        setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
        setTransientTurn(null);
        setRenameValue(threadPayload?.title || "");
      } catch (caughtError) {
        if (!cancelled) {
          setThread(null);
          setMessages([]);
          setThreadError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setThreadLoading(false);
        }
      }
    }

    void loadThreadState();

    return () => {
      cancelled = true;
    };
  }, [threadId]);

  useEffect(() => {
    timelineEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [displayTurns.length, submitting]);

  async function handleSubmit(event) {
    event.preventDefault();
    if (!threadId || !selectedAgentName || !message.trim()) {
      return;
    }
    setSubmitting(true);
    setSubmitError("");
    const pendingPrompt = message.trim();
    const pendingTurn = {
      id: createLocalId("pending-turn"),
      prompt: pendingPrompt,
      createdAt: new Date().toISOString(),
      assistantSummary: "",
      assistantTable: null,
      assistantVisualization: null,
      diagnostics: null,
      errorMessage: "",
      agentId: String(selectedAgent?.id || ""),
      agentLabel: selectedAgent?.name || selectedAgentName,
      status: "pending",
    };
    setTransientTurn(pendingTurn);
    try {
      const response = await askAgent({
        message: pendingPrompt,
        agent_name: selectedAgentName,
        thread_id: threadId,
      });
      const resolvedThreadId = String(response.thread_id || threadId).trim();
      setMessage("");
      const [threadPayload, messagePayload] = await Promise.all([
        fetchThread(resolvedThreadId),
        fetchThreadMessages(resolvedThreadId),
      ]);
      setThread(threadPayload);
      setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
      setTransientTurn(null);
      setRenameValue(threadPayload?.title || "");
      if (resolvedThreadId !== threadId) {
        navigate(`/chat/${resolvedThreadId}`);
      }
    } catch (caughtError) {
      setTransientTurn({
        ...pendingTurn,
        status: "error",
        errorMessage: getErrorMessage(caughtError),
      });
      setSubmitError(getErrorMessage(caughtError));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleRenameThread() {
    if (!threadId) {
      return;
    }
    setRenaming(true);
    setThreadMutationError("");
    try {
      const updated = await updateThread(threadId, {
        title: renameValue.trim() || undefined,
      });
      setThread(updated);
      setRenameValue(updated?.title || "");
      setRenamingOpen(false);
    } catch (caughtError) {
      setThreadMutationError(getErrorMessage(caughtError));
    } finally {
      setRenaming(false);
    }
  }

  function handleReuseLastPrompt() {
    const lastPrompt = [...displayTurns].reverse().find((turn) => turn.prompt)?.prompt;
    if (lastPrompt) {
      setMessage(lastPrompt);
    }
  }

  function handleComposerKeyDown(event) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      if (!submitting && selectedAgentName && message.trim()) {
        void handleSubmit(event);
      }
    }
  }

  if (!threadId) {
    return <Navigate to="/chat" replace />;
  }

  const snapshotItems = [
    { label: "Thread title", value: formatValue(thread?.title || `Thread ${threadId.slice(0, 8)}`) },
    { label: "State", value: formatValue(thread?.state || (isPending ? "pending" : "ready")) },
    { label: "Messages", value: formatValue(messages.length) },
    {
      label: "Updated",
      value: lastUpdated ? formatRelativeTime(lastUpdated) : "Awaiting first prompt",
    },
  ];

  return (
    <div className="thread-detail-shell">
      <section className="surface-panel thread-detail-header">
        <div className="thread-detail-copy">
          <div className="thread-detail-meta">
            <span className="tag">Thread</span>
            <span className="thread-detail-id">{threadId}</span>
            <span className={`thread-status-pill ${isPending ? "pending" : "ready"}`}>
              {isPending ? "Generating response" : "Standing by"}
            </span>
          </div>
          <h2>{thread?.title?.trim() || `Thread ${threadId.slice(0, 8)}`}</h2>
        </div>
        <div className="thread-detail-actions">
          <button className="ghost-button" type="button" onClick={() => navigate("/chat")}>
            <History className="button-icon" aria-hidden="true" />
            Thread list
          </button>
          <button
            className="ghost-button"
            type="button"
            onClick={handleReuseLastPrompt}
            disabled={turns.length === 0}
          >
            <RefreshCw className="button-icon" aria-hidden="true" />
            Reuse last prompt
          </button>
          <button
            className="ghost-button"
            type="button"
            onClick={() => setRenamingOpen((current) => !current)}
          >
            <Edit3 className="button-icon" aria-hidden="true" />
            {renamingOpen ? "Close rename" : "Rename"}
          </button>
        </div>
      </section>

      {renamingOpen ? (
        <section className="surface-panel thread-rename-panel">
          <div className="thread-section-head">
            <div>
              <h3>Rename thread</h3>
              <p>Update the working title shown across the runtime chat surface.</p>
            </div>
          </div>
          <div className="form-grid compact">
            <label className="field">
              <span>Thread title</span>
              <input
                className="text-input"
                type="text"
                value={renameValue}
                onChange={(event) => setRenameValue(event.target.value)}
                disabled={renaming}
              />
            </label>
            <div className="page-actions">
              <button
                className="primary-button"
                type="button"
                onClick={() => void handleRenameThread()}
                disabled={renaming}
              >
                {renaming ? "Saving..." : "Save title"}
              </button>
            </div>
          </div>
          {threadMutationError ? <div className="error-banner">{threadMutationError}</div> : null}
        </section>
      ) : null}

      <div className="thread-detail-grid">
        <section className="surface-panel thread-workspace-panel">
          <div className="thread-section-head thread-workspace-head">
            <div>
              <h3>Thread timeline</h3>
              <p>Messages, summaries, tables, and visual artifacts generated for this thread.</p>
            </div>
            <div className="thread-section-status">
              <span className={`thread-live-indicator ${isPending ? "pending" : "ready"}`}>
                <span className="thread-live-dot" aria-hidden="true" />
                {isPending ? "Generating response" : "Standing by"}
              </span>
              <span>
                {lastUpdated
                  ? `Updated ${formatRelativeTime(lastUpdated)}`
                  : "Awaiting first prompt"}
              </span>
            </div>
          </div>

          {threadError ? <div className="error-banner">{threadError}</div> : null}
          {threadLoading ? (
            <div className="empty-box">Loading thread messages...</div>
          ) : displayTurns.length > 0 ? (
            <div className="thread-transcript-scroll">
              <div className="conversation-stack thread-conversation-stack">
                {displayTurns.map((turn) => {
                  const visualization = normalizeVisualizationSpec(turn.assistantVisualization);
                  return (
                    <article key={turn.id} className="conversation-turn-shell">
                      <div className="thread-user-row">
                        <div className="thread-user-bubble">
                          <p>{turn.prompt}</p>
                          <span>{formatRelativeTime(turn.createdAt)}</span>
                        </div>
                      </div>

                      <div className="thread-assistant-row">
                        <div className="thread-assistant-shell">
                          <header className="thread-assistant-meta">
                            <div>
                              <strong>{turn.agentLabel || "Assistant"}</strong>
                              <span>{turn.agentId ? "Agent run" : "Runtime response"}</span>
                            </div>
                            <span className={`message-status-badge ${turn.status}`}>
                              {turn.status}
                            </span>
                          </header>

                          {turn.status === "pending" ? (
                            <div className="thread-runtime-pending">
                              Waiting for the runtime to finish this turn...
                            </div>
                          ) : turn.status === "error" ? (
                            <div className="error-banner">
                              {turn.errorMessage ||
                                "The runtime failed to complete this request."}
                            </div>
                          ) : (
                            <div className="thread-assistant-body">
                              <p className="assistant-summary-card">
                                {turn.assistantSummary || "No summary returned."}
                              </p>
                              {turn.assistantTable ? (
                                <div className="assistant-artifact-stack">
                                  {visualization &&
                                  hasRenderableVisualization(turn.assistantVisualization) ? (
                                    <ChartPreview
                                      title={visualization.title}
                                      result={turn.assistantTable}
                                      visualization={visualization}
                                      preferredDimension={visualization.x}
                                      preferredMeasure={visualization.y?.[0]}
                                    />
                                  ) : null}
                                  <ResultTable result={turn.assistantTable} maxPreviewRows={10} />
                                </div>
                              ) : null}
                              {turn.diagnostics ? (
                                <details className="diagnostics-disclosure">
                                  <summary>Execution diagnostics</summary>
                                  <pre className="code-block compact">{renderJson(turn.diagnostics)}</pre>
                                </details>
                              ) : null}
                            </div>
                          )}
                        </div>
                      </div>
                    </article>
                  );
                })}
                <div ref={timelineEndRef} />
              </div>
            </div>
          ) : (
            <div className="thread-empty-state">
              <Sparkles className="thread-empty-icon" aria-hidden="true" />
              <div>
                <strong>Start the thread</strong>
                <p>Pick an agent and send the first prompt to generate summaries, tables, and charts.</p>
              </div>
            </div>
          )}

          <form className="thread-composer-form" onSubmit={handleSubmit}>
            <div className="thread-composer-topbar">
              <div>
                <h3>Composer</h3>
                <p>Choose an agent, send a prompt, and keep the thread moving.</p>
              </div>
              <div className="thread-composer-actions">
                <select
                  className="select-input thread-agent-select"
                  value={selectedAgentName}
                  onChange={(event) => setSelectedAgentName(event.target.value)}
                  disabled={submitting || agents.length === 0}
                >
                  {agents.map((item) => (
                    <option key={item.id || item.name} value={item.name}>
                      {item.name}
                    </option>
                  ))}
                </select>
                <button className="ghost-button" type="button" onClick={() => navigate("/agents")}>
                  <Bot className="button-icon" aria-hidden="true" />
                  Manage agents
                </button>
              </div>
            </div>

            <label className="field">
              <span>Message</span>
              <textarea
                className="textarea-input thread-composer-input"
                value={message}
                onChange={(event) => setMessage(event.target.value)}
                onKeyDown={handleComposerKeyDown}
                disabled={submitting}
                rows={5}
                placeholder="Shift + Enter for a new line. Describe what you need from this runtime thread..."
              />
            </label>
            <div className="thread-composer-footer">
              <p className="composer-note">Press Enter to send. Use Shift+Enter for a newline.</p>
              <div className="page-actions">
                <button
                  className="ghost-button"
                  type="button"
                  onClick={handleReuseLastPrompt}
                  disabled={turns.length === 0 || submitting}
                >
                  <RefreshCw className="button-icon" aria-hidden="true" />
                  Reuse last prompt
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => setMessage(DEFAULT_CHAT_MESSAGE)}
                  disabled={submitting}
                >
                  Load default prompt
                </button>
                <button
                  className="primary-button"
                  type="submit"
                  disabled={submitting || !selectedAgentName}
                >
                  <ArrowRight className="button-icon" aria-hidden="true" />
                  {submitting ? "Sending..." : "Send prompt"}
                </button>
              </div>
            </div>
          </form>
          {submitError ? <div className="error-banner">{submitError}</div> : null}
        </section>

        <aside className="surface-panel thread-context-rail">
          <div className="thread-rail-section">
            <div className="thread-section-head">
              <div>
                <h3>Thread snapshot</h3>
                <p>Current runtime state for this thread.</p>
              </div>
            </div>
            <DetailList items={snapshotItems} />
          </div>

          <div className="thread-rail-section">
            <div className="thread-section-head">
              <div>
                <h3>Active agent</h3>
                <p>The selected runtime agent for the next turn.</p>
              </div>
            </div>
            {selectedAgent ? (
              <div className="callout">
                <strong>{selectedAgent.name}</strong>
                <span>
                  {[selectedAgent.description, selectedAgent.llm_connection, `${selectedAgent.tool_count || 0} tools`]
                    .filter(Boolean)
                    .join(" | ")}
                </span>
              </div>
            ) : (
              <PageEmpty
                title="No agent selected"
                message="Choose a runtime agent to send the next prompt."
              />
            )}
          </div>

          <div className="thread-rail-section">
            <div className="thread-section-head">
              <div>
                <h3>Starter prompts</h3>
                <p>Load a richer thread prompt without leaving the detail view.</p>
              </div>
            </div>
            <div className="thread-rail-starters">
              {CHAT_STARTERS.map((starter) => (
                <button
                  key={starter}
                  className="starter-button"
                  type="button"
                  onClick={() => setMessage(starter)}
                  disabled={submitting}
                >
                  {starter}
                </button>
              ))}
            </div>
          </div>

          {latestArtifactTurn ? (
            <div className="thread-rail-section">
              <div className="thread-section-head">
                <div>
                  <h3>Latest artifact</h3>
                  <p>Most recent turn that returned structured output.</p>
                </div>
              </div>
              <div className="callout">
                <strong>{latestArtifactTurn.agentLabel || "Assistant"}</strong>
                <span>
                  {[
                    latestArtifactTurn.assistantVisualization ? "Chart available" : null,
                    latestArtifactTurn.assistantTable
                      ? `${latestArtifactTurn.assistantTable.rowCount || 0} rows`
                      : null,
                  ]
                    .filter(Boolean)
                    .join(" | ")}
                </span>
              </div>
            </div>
          ) : null}
        </aside>
      </div>
    </div>
  );
}
