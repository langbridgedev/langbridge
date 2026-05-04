import { useEffect, useMemo, useRef, useState } from "react";
import { Navigate, useNavigate, useParams } from "react-router-dom";
import { Sparkles } from "lucide-react";

import { ChatComposer } from "../../components/chat/ChatComposer";
import { ChatTopBar } from "../../components/chat/ChatTopBar";
import { ConversationTimeline } from "../../components/chat/ConversationTimeline";
import { useAsyncData } from "../../hooks/useAsyncData";
import {
  createAgentRun,
  fetchAgents,
  fetchThread,
  fetchThreadMessages,
  streamRuntimeJob,
  updateThread,
} from "../../lib/runtimeApi";
import { getErrorMessage } from "../../lib/format";
import {
  CHAT_STARTERS,
  buildConversationTurns,
  createLocalId,
  formatRuntimeAgentModeLabel,
  formatRelativeTime,
  normalizeRuntimeAgentMode,
} from "../../lib/runtimeUi";

function buildJobStorageKey(threadId) {
  return `runtime-thread-job:${threadId}`;
}

function readStoredJobState(threadId) {
  if (!threadId || typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.sessionStorage.getItem(buildJobStorageKey(threadId));
    if (!raw) {
      return null;
    }
    const payload = JSON.parse(raw);
    const jobId = String(payload?.jobId || payload?.job_id || "").trim();
    if (!jobId) {
      return null;
    }
    return {
      jobId,
      lastSequence: Number(payload?.lastSequence || payload?.last_sequence || 0) || 0,
      terminal: Boolean(payload?.terminal),
    };
  } catch {
    return null;
  }
}

function writeStoredJobState(threadId, payload) {
  if (!threadId || typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.setItem(
      buildJobStorageKey(threadId),
      JSON.stringify({
        jobId: payload?.jobId || "",
        lastSequence: Number(payload?.lastSequence || 0) || 0,
        terminal: Boolean(payload?.terminal),
      }),
    );
  } catch {}
}

function clearStoredJobState(threadId) {
  if (!threadId || typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.removeItem(buildJobStorageKey(threadId));
  } catch {}
}

function normalizeProgressEvent(event) {
  const details =
    event?.details && typeof event.details === "object" ? event.details : null;
  const diagnostics =
    details?.diagnostics && typeof details.diagnostics === "object"
      ? details.diagnostics
      : null;
  const clarifyingQuestion =
    (typeof details?.clarifying_question === "string" && details.clarifying_question.trim()) ||
    (typeof diagnostics?.clarifying_question === "string" && diagnostics.clarifying_question.trim()) ||
    "";
  const answer =
    typeof details?.answer === "string" && details.answer.trim() ? details.answer.trim() : "";
  const summary =
    typeof details?.summary === "string" && details.summary.trim() ? details.summary.trim() : "";
  const eventMessage =
    clarifyingQuestion ||
    answer ||
    summary ||
    (typeof event?.message === "string" ? event.message : "");
  return {
    sequence: Number(event?.sequence || 0),
    id: event?.id || "",
    event: event?.event || "run.progress",
    stage: event?.stage || "planning",
    status: event?.status || "in_progress",
    message: eventMessage,
    timestamp: event?.timestamp || new Date().toISOString(),
    source: event?.source || "",
    rawEventType: event?.raw_event_type || "",
    jobId: event?.job_id || "",
    jobType: event?.job_type || "agent.run",
    terminal: Boolean(event?.terminal),
    details,
  };
}

function buildPendingTurn({ prompt, agentId, agentLabel, agentMode }) {
  const now = new Date().toISOString();
  return {
    id: createLocalId("pending-turn"),
    prompt,
    agentMode: normalizeRuntimeAgentMode(agentMode),
    createdAt: now,
    assistantSummary: "Connecting to the runtime stream.",
    assistantTable: null,
    assistantVisualization: null,
    diagnostics: null,
    errorMessage: "",
    errorStatus: null,
    agentId,
    agentLabel,
    status: "pending",
    liveStage: "planning",
    progressEvents: [
      {
        sequence: 0,
        event: "client.connecting",
        stage: "planning",
        status: "in_progress",
        message: "Connecting to the runtime stream.",
        timestamp: now,
        source: "client",
        rawEventType: "client.connecting",
      },
    ],
  };
}

function buildResumedPendingTurn({ turn, agentLabel }) {
  const createdAt = turn?.createdAt || new Date().toISOString();
  return {
    id: turn?.id || createLocalId("pending-turn"),
    prompt: turn?.prompt || "",
    agentMode: normalizeRuntimeAgentMode(turn?.agentMode),
    createdAt,
    assistantSummary: "Reconnecting to the runtime stream.",
    assistantTable: null,
    assistantVisualization: null,
    diagnostics: null,
    errorMessage: "",
    errorStatus: null,
    agentId: String(turn?.agentId || ""),
    agentLabel: turn?.agentLabel || agentLabel || null,
    status: "pending",
    liveStage: "planning",
    progressEvents: [
      {
        sequence: 0,
        event: "client.reconnecting",
        stage: "planning",
        status: "in_progress",
        message: "Reconnecting to the runtime stream.",
        timestamp: createdAt,
        source: "client",
        rawEventType: "client.reconnecting",
      },
    ],
  };
}

function applyStreamEventToTurn(turn, event) {
  if (!turn) {
    return turn;
  }
  const progressEvents = Array.isArray(turn.progressEvents) ? [...turn.progressEvents] : [];
  const normalizedEvent = normalizeProgressEvent(event);
  const sequence = normalizedEvent.sequence;
  if (!progressEvents.some((item) => Number(item.sequence || 0) === sequence)) {
    progressEvents.push(normalizedEvent);
  }
  const latestStage = normalizedEvent.stage || progressEvents[progressEvents.length - 1]?.stage || "planning";
  return {
    ...turn,
    assistantSummary: normalizedEvent.message || turn.assistantSummary,
    status: normalizedEvent.terminal ? (normalizedEvent.status === "failed" ? "error" : "pending") : "pending",
    errorMessage: normalizedEvent.terminal && normalizedEvent.status === "failed" ? normalizedEvent.message || "" : "",
    liveStage: latestStage,
    progressEvents,
  };
}

export function ChatPage() {
  const navigate = useNavigate();
  const params = useParams();
  const threadId = String(params.threadId || "").trim();
  const agentsState = useAsyncData(fetchAgents);
  const agents = Array.isArray(agentsState.data?.items) ? agentsState.data.items : [];

  const [selectedAgentName, setSelectedAgentName] = useState("");
  const [message, setMessage] = useState("");
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
  const [pendingDraftMessage, setPendingDraftMessage] = useState("");
  const [selectedAgentMode, setSelectedAgentMode] = useState("auto");
  const selectedAgent = agents.find((item) => item.name === selectedAgentName) || null;
  const turns = useMemo(() => buildConversationTurns(messages, agents), [messages, agents]);
  const displayTurns = useMemo(() => {
    if (!transientTurn) {
      return turns;
    }
    if (turns.some((turn) => String(turn.id) === String(transientTurn.id))) {
      return turns.map((turn) =>
        String(turn.id) === String(transientTurn.id) ? { ...turn, ...transientTurn } : turn,
      );
    }
    return [...turns, transientTurn];
  }, [turns, transientTurn]);
  const timelineEndRef = useRef(null);
  const latestTurnRef = useRef(null);
  const streamAbortRef = useRef(null);
  const activeJobRef = useRef(null);
  const resumedJobKeyRef = useRef("");
  const initialThreadAnchorRef = useRef(false);
  const previousTailSignatureRef = useRef("");
  const readyTurns = displayTurns.filter((turn) => turn.status === "ready");
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
    const storageKey = `runtime-thread-agent-mode:${threadId}`;
    try {
      const stored = window.localStorage.getItem(storageKey);
      if (stored) {
        setSelectedAgentMode(normalizeRuntimeAgentMode(stored));
      } else {
        setSelectedAgentMode("auto");
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
    if (!threadId) {
      return;
    }
    const storageKey = `runtime-thread-agent-mode:${threadId}`;
    try {
      window.localStorage.setItem(storageKey, selectedAgentMode);
    } catch {}
  }, [selectedAgentMode, threadId]);

  useEffect(() => {
    if (agents.length === 0) {
      return;
    }
    const hasSelectedAgent = agents.some((item) => item.name === selectedAgentName);
    if (!selectedAgentName || !hasSelectedAgent) {
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
    setPendingDraftMessage(storedDraft);
    window.sessionStorage.removeItem(draftKey);
  }, [threadId]);

  useEffect(() => {
    activeJobRef.current = readStoredJobState(threadId);
    resumedJobKeyRef.current = "";
    initialThreadAnchorRef.current = false;
    previousTailSignatureRef.current = "";
  }, [threadId]);

  useEffect(() => {
    let cancelled = false;

    async function loadThreadState() {
      if (!threadId) {
        return;
      }
      setThreadLoading(true);
      setThreadError("");
      setThread(null);
      setMessages([]);
      setTransientTurn(null);
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
    if (
      !pendingDraftMessage ||
      threadLoading ||
      submitting ||
      !selectedAgentName ||
      !threadId
    ) {
      return;
    }
    setPendingDraftMessage("");
    void submitPrompt(pendingDraftMessage);
  }, [pendingDraftMessage, selectedAgentMode, selectedAgentName, submitting, threadId, threadLoading]);

  useEffect(() => {
    if (threadLoading || displayTurns.length === 0) {
      return;
    }
    const latestTurn = displayTurns[displayTurns.length - 1];
    const tailSignature = `${latestTurn?.id || ""}:${latestTurn?.status || ""}:${displayTurns.length}:${submitting ? "submitting" : "idle"}`;
    if (tailSignature === previousTailSignatureRef.current) {
      return;
    }
    previousTailSignatureRef.current = tailSignature;

    const shouldInitialAnchor = !initialThreadAnchorRef.current;
    const anchorTarget = shouldInitialAnchor ? latestTurnRef.current : timelineEndRef.current;
    if (!anchorTarget) {
      return;
    }

    const frame = window.requestAnimationFrame(() => {
      anchorTarget.scrollIntoView({
        behavior: shouldInitialAnchor ? "auto" : "smooth",
        block: shouldInitialAnchor ? "start" : "end",
      });
      if (shouldInitialAnchor) {
        initialThreadAnchorRef.current = true;
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [displayTurns, submitting, threadLoading]);

  useEffect(() => {
    return () => {
      if (streamAbortRef.current) {
        streamAbortRef.current.abort();
      }
    };
  }, []);

  function persistJobState(event, resolvedThreadId = threadId) {
    const normalizedEvent = normalizeProgressEvent(event);
    const jobId =
      normalizedEvent.jobId ||
      activeJobRef.current?.jobId ||
      "";
    if (!jobId) {
      return;
    }
    const nextState = {
      jobId,
      lastSequence: Math.max(
        normalizedEvent.sequence,
        Number(activeJobRef.current?.lastSequence || 0),
      ),
      terminal: Boolean(normalizedEvent.terminal),
    };
    activeJobRef.current = nextState;
    writeStoredJobState(resolvedThreadId, nextState);
  }

  function clearActiveJobState(resolvedThreadId = threadId) {
    activeJobRef.current = null;
    resumedJobKeyRef.current = "";
    clearStoredJobState(resolvedThreadId);
  }

  async function reloadThreadState(resolvedThreadId = threadId) {
    const [threadPayload, messagePayload] = await Promise.all([
      fetchThread(resolvedThreadId),
      fetchThreadMessages(resolvedThreadId),
    ]);
    setThread(threadPayload);
    setMessages(Array.isArray(messagePayload?.items) ? messagePayload.items : []);
    setRenameValue(threadPayload?.title || "");
    return {
      threadPayload,
      messagePayload,
    };
  }

  async function finalizeStreamedJob({
    resolvedThreadId = threadId,
    promptValue,
    fallbackTurn,
    streamedEvents,
  }) {
    const terminalEvent = [...streamedEvents].reverse().find((event) => event?.terminal);
    const streamedProgressEvents = streamedEvents
      .filter((event) => !event?.terminal)
      .map((event) => normalizeProgressEvent(event));
    const { threadPayload, messagePayload } = await reloadThreadState(resolvedThreadId);
    const nextTurns = buildConversationTurns(
      Array.isArray(messagePayload?.items) ? messagePayload.items : [],
      agents,
    );
    const lastTurn = nextTurns[nextTurns.length - 1] || null;
    const canonicalTurnCompleted = Boolean(
      lastTurn &&
      lastTurn.prompt === promptValue &&
      lastTurn.status !== "pending",
    );
    const canonicalJobFinished = threadPayload?.state !== "processing";

    if (terminalEvent?.terminal || canonicalTurnCompleted || canonicalJobFinished) {
      clearActiveJobState(resolvedThreadId);
    }

    if (!terminalEvent && !canonicalTurnCompleted && !canonicalJobFinished) {
      const streamErrorMessage = "The runtime stream ended before the job completed.";
      setTransientTurn({
        ...(lastTurn && lastTurn.prompt === promptValue ? lastTurn : fallbackTurn),
        status: "error",
        errorMessage: streamErrorMessage,
        progressEvents:
          streamedProgressEvents.length > 0 ? streamedProgressEvents : fallbackTurn.progressEvents,
        assistantSummary:
          streamedProgressEvents[streamedProgressEvents.length - 1]?.message ||
          fallbackTurn.assistantSummary,
      });
      setSubmitError(streamErrorMessage);
      return;
    }

    if (
      terminalEvent?.status === "failed" &&
      lastTurn &&
      lastTurn.status === "pending" &&
      lastTurn.prompt === promptValue
    ) {
      setTransientTurn({
        ...lastTurn,
        status: "error",
        errorMessage: terminalEvent.message || "Job failed.",
        progressEvents: streamedProgressEvents,
        assistantSummary: terminalEvent.message || lastTurn.assistantSummary,
      });
      return;
    }

    setTransientTurn(null);
  }

  useEffect(() => {
    if (threadLoading || submitting || !threadId || !thread) {
      return;
    }

    if (thread.state !== "processing") {
      clearActiveJobState(threadId);
      return;
    }

    const threadJobId = String(thread?.metadata?.active_job_id || "").trim();
    const storedJob = readStoredJobState(threadId);
    const activeJob = threadJobId
      ? {
          jobId: threadJobId,
          lastSequence: storedJob?.jobId === threadJobId ? Number(storedJob?.lastSequence || 0) : 0,
          terminal: false,
        }
      : storedJob?.jobId
        ? storedJob
        : null;
    if (!activeJob?.jobId) {
      return;
    }

    const pendingTurn = [...turns].reverse().find((turn) => turn.status === "pending");
    if (!pendingTurn) {
      return;
    }

    const resumeKey = `${threadId}:${activeJob.jobId}:${activeJob.lastSequence}`;
    if (resumedJobKeyRef.current === resumeKey) {
      return;
    }
    resumedJobKeyRef.current = resumeKey;
    activeJobRef.current = activeJob;
    writeStoredJobState(threadId, activeJob);
    setTransientTurn((current) =>
      current && current.status === "pending"
        ? current
        : buildResumedPendingTurn({
            turn: pendingTurn,
            agentLabel: selectedAgentName,
          }),
    );
    setSubmitError("");
    setSubmitting(true);
    const controller = new AbortController();
    streamAbortRef.current = controller;
    const streamedEvents = [];

    void (async () => {
      try {
        await streamRuntimeJob(activeJob.jobId, {
          afterSequence: activeJob.lastSequence,
          signal: controller.signal,
          onEvent: (event) => {
            streamedEvents.push(event);
            persistJobState(event, threadId);
            setTransientTurn((current) => applyStreamEventToTurn(current, event));
          },
        });
        await finalizeStreamedJob({
          resolvedThreadId: threadId,
          promptValue: pendingTurn.prompt,
          fallbackTurn: buildResumedPendingTurn({
            turn: pendingTurn,
            agentLabel: selectedAgentName,
          }),
          streamedEvents,
        });
      } catch (caughtError) {
        if (caughtError?.name === "AbortError") {
          return;
        }
        try {
          const { threadPayload } = await reloadThreadState(threadId);
          if (threadPayload?.state !== "processing") {
            clearActiveJobState(threadId);
            setTransientTurn(null);
            return;
          }
        } catch {}
        setSubmitError(getErrorMessage(caughtError));
      } finally {
        if (streamAbortRef.current === controller) {
          streamAbortRef.current = null;
        }
        setSubmitting(false);
      }
    })();
  }, [selectedAgentName, submitting, thread, threadId, threadLoading, turns]);

  async function submitPrompt(promptValue) {
    if (!threadId || !selectedAgentName || !String(promptValue || "").trim()) {
      return;
    }
    if (streamAbortRef.current) {
      streamAbortRef.current.abort();
    }
    setSubmitting(true);
    setSubmitError("");
    const pendingPrompt = String(promptValue || "").trim();
    const pendingTurn = buildPendingTurn({
      prompt: pendingPrompt,
      agentId: String(selectedAgent?.id || ""),
      agentLabel: selectedAgent?.name || selectedAgentName,
      agentMode: selectedAgentMode,
    });
    setTransientTurn(pendingTurn);
    setMessage("");
    const controller = new AbortController();
    streamAbortRef.current = controller;
    const streamedEvents = [];
    try {
      const queued = await createAgentRun({
        message: pendingPrompt,
        agent_name: selectedAgentName,
        thread_id: threadId,
        agent_mode: selectedAgentMode,
      });
      const activeJob = {
        jobId: String(queued?.job_id || ""),
        lastSequence: 0,
        terminal: false,
      };
      if (!activeJob.jobId) {
        throw new Error("The runtime did not return an agent run job id.");
      }
      activeJobRef.current = activeJob;
      writeStoredJobState(threadId, activeJob);
      await streamRuntimeJob(activeJob.jobId, {
        signal: controller.signal,
        onEvent: (event) => {
          streamedEvents.push(event);
          persistJobState(event, threadId);
          setTransientTurn((current) => applyStreamEventToTurn(current, event));
        },
      });
      await finalizeStreamedJob({
        resolvedThreadId: threadId,
        promptValue: pendingPrompt,
        fallbackTurn: pendingTurn,
        streamedEvents,
      });
    } catch (caughtError) {
      if (caughtError?.name === "AbortError") {
        return;
      }
      const activeJob = activeJobRef.current;
      if (activeJob?.jobId && !activeJob?.terminal) {
        try {
          await streamRuntimeJob(activeJob.jobId, {
            afterSequence: activeJob.lastSequence,
            signal: controller.signal,
            onEvent: (event) => {
              streamedEvents.push(event);
              persistJobState(event, threadId);
              setTransientTurn((current) => applyStreamEventToTurn(current, event));
            },
          });
          await finalizeStreamedJob({
            resolvedThreadId: threadId,
            promptValue: pendingPrompt,
            fallbackTurn: pendingTurn,
            streamedEvents,
          });
          return;
        } catch (resumeError) {
          caughtError = resumeError;
        }
      }
      setTransientTurn({
        ...pendingTurn,
        status: "error",
        errorMessage: getErrorMessage(caughtError),
        errorStatus: caughtError?.status || null,
      });
      setMessage(pendingPrompt);
      setSubmitError(getErrorMessage(caughtError));
    } finally {
      if (streamAbortRef.current === controller) {
        streamAbortRef.current = null;
      }
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

  function handleCancelRenameThread() {
    setRenamingOpen(false);
    setThreadMutationError("");
    setRenameValue(thread?.title || "");
  }

  if (!threadId) {
    return <Navigate to="/chat" replace />;
  }
  const threadTitle = thread?.title?.trim() || `Thread ${threadId.slice(0, 8)}`;

  return (
    <div className="thread-detail-shell thread-detail-shell--chat">
      <ChatTopBar
        threadTitle={threadTitle}
        isPending={isPending}
        selectedAgentName={selectedAgent?.name || selectedAgentName}
        selectedAgentModeLabel={formatRuntimeAgentModeLabel(selectedAgentMode)}
        onBack={() => navigate("/chat")}
        renamingOpen={renamingOpen}
        onToggleRename={() => {
          setThreadMutationError("");
          if (renamingOpen) {
            setRenameValue(thread?.title || "");
          }
          setRenamingOpen(!renamingOpen);
        }}
        renameValue={renameValue}
        onRenameValueChange={setRenameValue}
        onRenameSubmit={handleRenameThread}
        onCancelRename={handleCancelRenameThread}
        renaming={renaming}
        renameError={threadMutationError}
      />

      <section className="thread-chat-stage">
        {threadError ? <div className="error-banner">{threadError}</div> : null}

        <div className="thread-chat-context">
          <span className="chip">
            {selectedAgent ? `Using ${selectedAgent.name}` : "Choose an agent"}
          </span>
          <span className="chip">
            {lastUpdated ? `Updated ${formatRelativeTime(lastUpdated)}` : "New thread"}
          </span>
          <span className={`chip thread-mode-chip ${selectedAgentMode !== "auto" ? "active" : ""}`}>
            Mode: {formatRuntimeAgentModeLabel(selectedAgentMode)}
          </span>
        </div>

        {threadLoading ? (
          <div className="thread-loading-inline" role="status" aria-live="polite">
            <span className="thread-loading-dot" aria-hidden="true" />
            Loading thread messages
          </div>
        ) : null}
        {!threadLoading && displayTurns.length > 0 ? (
          <ConversationTimeline
            turns={displayTurns}
            latestTurnRef={latestTurnRef}
            timelineEndRef={timelineEndRef}
          />
        ) : null}
        {!threadLoading && displayTurns.length === 0 ? (
          <div className="thread-empty-state thread-empty-state--chat">
            <Sparkles className="thread-empty-icon" aria-hidden="true" />
            <div>
              <strong>Start with a question</strong>
              <p>Choose an agent, ask a question, and the runtime will answer here without extra workspace chrome.</p>
            </div>
            <div className="thread-suggestion-strip thread-suggestion-strip--empty">
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
        ) : null}

        <ChatComposer
          agents={agents}
          selectedAgentName={selectedAgentName}
          onSelectedAgentNameChange={setSelectedAgentName}
          selectedAgentMode={selectedAgentMode}
          onSelectedAgentModeChange={(value) => setSelectedAgentMode(normalizeRuntimeAgentMode(value))}
          message={message}
          onMessageChange={setMessage}
          submitting={submitting}
          onSubmit={() => submitPrompt(message)}
        />
        {submitError ? <div className="error-banner">{submitError}</div> : null}
      </section>
    </div>
  );
}
