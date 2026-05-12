import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowRight, Bot, MessageSquarePlus } from "lucide-react";

import { useAsyncData } from "../../hooks/useAsyncData";
import {
  createThread,
  fetchAgents,
  fetchThreads,
} from "../../lib/runtimeApi";
import { getErrorMessage, getRuntimeTimestamp } from "../../lib/format";
import {
  AUTO_AGENT_SELECTION_VALUE,
  CHAT_STARTERS,
  RUNTIME_AGENT_MODE_OPTIONS,
  formatRelativeTime,
  formatRuntimeAgentModeLabel,
  normalizeRuntimeAgentSelection,
  normalizeRuntimeAgentMode,
} from "../../lib/runtimeUi";

function buildPromptTitle(prompt) {
  const normalized = String(prompt || "")
    .trim()
    .replace(/\s+/g, " ");
  if (!normalized) {
    return undefined;
  }
  return normalized.slice(0, 80);
}

function getInitialAgentMode() {
  if (typeof window === "undefined") {
    return "auto";
  }
  try {
    return normalizeRuntimeAgentMode(window.localStorage.getItem("runtime-ask-agent-mode"));
  } catch {
    return "auto";
  }
}

export function ChatIndexPage() {
  const navigate = useNavigate();
  const threadsState = useAsyncData(fetchThreads);
  const agentsState = useAsyncData(fetchAgents);
  const threads = Array.isArray(threadsState.data?.items) ? threadsState.data.items : [];
  const agents = Array.isArray(agentsState.data?.items) ? agentsState.data.items : [];
  const sortedThreads = [...threads].sort((left, right) => {
    const leftTime = getRuntimeTimestamp(left.updated_at || left.created_at || 0);
    const rightTime = getRuntimeTimestamp(right.updated_at || right.created_at || 0);
    return rightTime - leftTime;
  });
  const latestThread = sortedThreads[0] || null;
  const [selectedAgentName, setSelectedAgentName] = useState(AUTO_AGENT_SELECTION_VALUE);
  const [selectedAgentMode, setSelectedAgentMode] = useState(getInitialAgentMode);
  const [prompt, setPrompt] = useState("");
  const [asking, setAsking] = useState(false);
  const [creatingThread, setCreatingThread] = useState(false);
  const [mutationError, setMutationError] = useState("");
  const textareaRef = useRef(null);

  useEffect(() => {
    try {
      const stored = window.localStorage.getItem("runtime-ask-agent");
      if (stored) {
        setSelectedAgentName(stored);
      }
    } catch {}
  }, []);

  useEffect(() => {
    if (agents.length === 0) {
      return;
    }
    const normalized = normalizeRuntimeAgentSelection(selectedAgentName, agents);
    if (normalized !== selectedAgentName) {
      setSelectedAgentName(normalized);
    }
  }, [agents, selectedAgentName]);

  useEffect(() => {
    try {
      window.localStorage.setItem("runtime-ask-agent", selectedAgentName);
    } catch {}
  }, [selectedAgentName]);

  useEffect(() => {
    try {
      window.localStorage.setItem("runtime-ask-agent-mode", selectedAgentMode);
    } catch {}
  }, [selectedAgentMode]);

  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) {
      return;
    }
    textarea.style.height = "auto";
    textarea.style.height = `${Math.min(textarea.scrollHeight, 220)}px`;
  }, [prompt]);

  async function handleAsk(event) {
    event.preventDefault();
    if (!prompt.trim()) {
      return;
    }
    await handleCreateThread(prompt.trim(), buildPromptTitle(prompt));
  }

  async function handleCreateThread(seedMessage = "", title) {
    if (seedMessage) {
      setAsking(true);
    }
    setCreatingThread(true);
    setMutationError("");
    try {
      const createdThread = await createThread(title ? { title } : {});
      if (typeof window !== "undefined") {
        if (seedMessage) {
          window.sessionStorage.setItem(`runtime-thread-draft:${createdThread.id}`, seedMessage);
        }
        window.localStorage.setItem(`runtime-thread-agent:${createdThread.id}`, selectedAgentName);
        window.localStorage.setItem(
          `runtime-thread-agent-mode:${createdThread.id}`,
          normalizeRuntimeAgentMode(selectedAgentMode),
        );
      }
      navigate(`/chat/${createdThread.id}`);
      void threadsState.reload();
    } catch (caughtError) {
      setMutationError(getErrorMessage(caughtError));
    } finally {
      if (seedMessage) {
        setAsking(false);
      }
      setCreatingThread(false);
    }
  }

  function handleKeyDown(event) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      if (prompt.trim()) {
        void handleAsk(event);
      }
    }
  }

  return (
    <div className="chat-index-shell chat-home-shell chat-home-shell--assistant">
      <section className="chat-home-assistant-stage">
        {threadsState.error ? <div className="error-banner">{threadsState.error}</div> : null}
        {agentsState.error ? <div className="error-banner">{agentsState.error}</div> : null}
        {mutationError ? <div className="error-banner">{mutationError}</div> : null}

        <div className="chat-home-assistant-actions">
          {latestThread ? (
            <button
              className="ghost-button compact"
              type="button"
              onClick={() => navigate(`/chat/${latestThread.id}`)}
            >
              Continue latest
            </button>
          ) : null}
          <button
            className="ghost-button compact"
            type="button"
            onClick={() => void handleCreateThread()}
            disabled={creatingThread}
          >
            <MessageSquarePlus className="button-icon" aria-hidden="true" />
            {creatingThread ? "Creating..." : "New chat"}
          </button>
          <button className="ghost-button compact" type="button" onClick={() => navigate("/configure/agents")}>
            <Bot className="button-icon" aria-hidden="true" />
            Agents
          </button>
        </div>

        <div className="chat-home-assistant-center">
          <div className="chat-home-copy chat-home-copy--assistant">
            <span className="chat-home-kicker">Langbridge Runtime</span>
            <h3>What can I help you analyse?</h3>
            {/* <p className="chat-home-copy-text">
              Ask a business question, request a chart, or investigate a deeper analytical pattern.
            </p> */}
          </div>

          <form className="chat-home-assistant-composer" onSubmit={handleAsk}>
            <textarea
              ref={textareaRef}
              className="chat-home-assistant-input"
              value={prompt}
              onKeyDown={handleKeyDown}
              onChange={(event) => setPrompt(event.target.value)}
              rows={3}
              disabled={asking}
              aria-label="Question"
              placeholder="Ask about orders, revenue, support load, marketing efficiency, or what changed..."
            />

            <div className="chat-home-assistant-toolbar">
              <div className="chat-home-assistant-controls">
                <label className="chat-home-control-pill">
                  <span>Agent</span>
                  <select
                    className="select-input thread-agent-select"
                    value={selectedAgentName}
                    onChange={(event) => setSelectedAgentName(event.target.value)}
                    disabled={asking || agents.length === 0}
                  >
                    <option value={AUTO_AGENT_SELECTION_VALUE}>Auto-select</option>
                    {agents.map((item) => (
                      <option key={item.id || item.name} value={item.name}>
                        {item.name}
                      </option>
                    ))}
                  </select>
                </label>

                <div className="chat-home-mode-selector" aria-label="Agent mode">
                  {RUNTIME_AGENT_MODE_OPTIONS.map((mode) => (
                    <button
                      key={mode.value}
                      className={`thread-mode-option ${
                        selectedAgentMode === mode.value ? "active" : ""
                      }`}
                      type="button"
                      onClick={() => setSelectedAgentMode(normalizeRuntimeAgentMode(mode.value))}
                      disabled={asking}
                      aria-pressed={selectedAgentMode === mode.value}
                      title={mode.hint}
                    >
                      {mode.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className="chat-home-assistant-submit">
                <span style={{marginRight: "10px"}} className="chat-home-selected-mode">
                  {formatRuntimeAgentModeLabel(selectedAgentMode)}
                </span>
                <button
                  className="thread-composer-send chat-home-send-button"
                  type="submit"
                  disabled={asking || agents.length === 0 || !prompt.trim()}
                  aria-label={asking ? "Asking runtime" : "Ask runtime"}
                  title={asking ? "Asking runtime..." : "Ask runtime"}
                >
                  <ArrowRight className="button-icon" aria-hidden="true" />
                </button>
              </div>
            </div>
          </form>

          <div className="chat-home-suggestion-grid" aria-label="Example prompts">
            {CHAT_STARTERS.map((starter) => (
              <button
                key={starter}
                className="chat-home-suggestion"
                type="button"
                onClick={() => setPrompt(starter)}
                disabled={asking}
              >
                {starter}
              </button>
            ))}
          </div>
        </div>
      </section>

      {sortedThreads.length > 0 ? (
        <section className="chat-home-history chat-home-history--assistant">
          <div className="chat-home-history-head">
            <h3>Recent chats</h3>
            <p>
              {latestThread
                ? `Latest updated ${formatRelativeTime(latestThread.updated_at || latestThread.created_at)}`
                : "Pick up where you left off."}
            </p>
          </div>
          <div className="chat-home-history-strip">
            {sortedThreads.slice(0, 6).map((thread) => (
              <button
                key={thread.id}
                className="chat-home-history-item"
                type="button"
                onClick={() => navigate(`/chat/${thread.id}`)}
              >
                <strong>{thread.title || `Thread ${String(thread.id).slice(0, 8)}`}</strong>
                <span>
                  {thread.updated_at
                    ? `Updated ${formatRelativeTime(thread.updated_at)}`
                    : `Created ${formatRelativeTime(thread.created_at)}`}
                </span>
              </button>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}
