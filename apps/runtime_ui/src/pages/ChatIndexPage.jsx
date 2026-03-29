import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowRight, MessageSquareText, Plus, RefreshCw, Trash2 } from "lucide-react";

import { useAsyncData } from "../hooks/useAsyncData";
import { createThread, deleteThread, fetchAgents, fetchThreads } from "../lib/runtimeApi";
import { formatValue, getErrorMessage } from "../lib/format";
import { CHAT_STARTERS, formatRelativeTime } from "../lib/runtimeUi";

export function ChatIndexPage() {
  const navigate = useNavigate();
  const threadsState = useAsyncData(fetchThreads);
  const agentsState = useAsyncData(fetchAgents);
  const threads = Array.isArray(threadsState.data?.items) ? threadsState.data.items : [];
  const agents = Array.isArray(agentsState.data?.items) ? agentsState.data.items : [];
  const sortedThreads = [...threads].sort((left, right) => {
    const leftTime = new Date(left.updated_at || left.created_at || 0).getTime();
    const rightTime = new Date(right.updated_at || right.created_at || 0).getTime();
    return rightTime - leftTime;
  });
  const latestThread = sortedThreads[0] || null;
  const [creatingThread, setCreatingThread] = useState(false);
  const [deletingThreadId, setDeletingThreadId] = useState("");
  const [mutationError, setMutationError] = useState("");

  async function handleCreateThread(seedMessage = "") {
    setCreatingThread(true);
    setMutationError("");
    try {
      const createdThread = await createThread({});
      if (seedMessage && typeof window !== "undefined") {
        window.sessionStorage.setItem(`runtime-thread-draft:${createdThread.id}`, seedMessage);
      }
      await threadsState.reload();
      navigate(`/chat/${createdThread.id}`);
    } catch (caughtError) {
      setMutationError(getErrorMessage(caughtError));
    } finally {
      setCreatingThread(false);
    }
  }

  async function handleDeleteThread(threadId) {
    setDeletingThreadId(String(threadId));
    setMutationError("");
    try {
      await deleteThread(threadId);
      await threadsState.reload();
    } catch (caughtError) {
      setMutationError(getErrorMessage(caughtError));
    } finally {
      setDeletingThreadId("");
    }
  }

  return (
    <div className="chat-index-shell">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Threads</p>
            <h2>Thread workspace</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(threads.length)} threads</span>
              <span className="chip">{formatValue(agents.length)} agents</span>
              <span className="chip">
                {latestThread
                  ? formatRelativeTime(latestThread.updated_at || latestThread.created_at)
                  : "No activity"}
              </span>
            </div>
          </div>
          <div className="product-command-bar-actions">
            <button
              className="ghost-button"
              type="button"
              onClick={() => void threadsState.reload()}
              disabled={threadsState.loading}
            >
              <RefreshCw className="button-icon" aria-hidden="true" />
              Refresh
            </button>
          </div>
        </div>
        <div className="thread-index-actions">
          <button
            className="primary-button"
            type="button"
            onClick={() => void handleCreateThread()}
            disabled={creatingThread}
          >
            <Plus className="button-icon" aria-hidden="true" />
            {creatingThread ? "Creating..." : "New thread"}
          </button>
        </div>
      </section>

      <div className="thread-index-layout">
        <section className="surface-panel thread-index-list">
          <div className="thread-section-head">
            <div>
              <h3>Recent threads</h3>
            </div>
          </div>

          {threadsState.error ? <div className="error-banner">{threadsState.error}</div> : null}
          {mutationError ? <div className="error-banner">{mutationError}</div> : null}

          {threadsState.loading ? (
            <div className="empty-box">Loading threads...</div>
          ) : sortedThreads.length > 0 ? (
            <div className="thread-index-cards">
              {sortedThreads.map((thread) => (
                <article key={thread.id} className="thread-index-card">
                  <button
                    className="thread-index-card-main"
                    type="button"
                    onClick={() => navigate(`/chat/${thread.id}`)}
                  >
                    <span className="thread-link-avatar">
                      {String(thread.title || thread.id || "th")
                        .slice(0, 2)
                        .toUpperCase()}
                    </span>
                    <span className="thread-link-copy">
                      <strong>{thread.title || `Thread ${String(thread.id).slice(0, 8)}`}</strong>
                      <span>
                        {formatValue(thread.state)} |{" "}
                        {thread.updated_at
                          ? `Updated ${formatRelativeTime(thread.updated_at)}`
                          : `Created ${formatRelativeTime(thread.created_at)}`}
                      </span>
                    </span>
                    <ArrowRight className="thread-link-arrow" aria-hidden="true" />
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => void handleDeleteThread(thread.id)}
                    disabled={deletingThreadId === String(thread.id)}
                  >
                    <Trash2 className="button-icon" aria-hidden="true" />
                    {deletingThreadId === String(thread.id) ? "Deleting..." : "Delete"}
                  </button>
                </article>
              ))}
            </div>
          ) : (
            <div className="thread-empty-state">
              <MessageSquareText className="thread-empty-icon" aria-hidden="true" />
              <div>
                <strong>No threads found</strong>
                <p>Start a new thread to see it appear here.</p>
              </div>
              <button
                className="primary-button"
                type="button"
                onClick={() => void handleCreateThread()}
                disabled={creatingThread}
              >
                <Plus className="button-icon" aria-hidden="true" />
                Start a thread
              </button>
            </div>
          )}
        </section>

        <aside className="surface-panel thread-index-rail">
          <div className="thread-section-head">
            <div>
              <h3>Prompt starters</h3>
            </div>
          </div>
          <div className="thread-rail-starters">
            {CHAT_STARTERS.map((starter) => (
              <button
                key={starter}
                className="starter-button"
                type="button"
                onClick={() => void handleCreateThread(starter)}
                disabled={creatingThread}
              >
                <strong>Start with this prompt</strong>
                <span>{starter}</span>
              </button>
            ))}
          </div>
        </aside>
      </div>
    </div>
  );
}
