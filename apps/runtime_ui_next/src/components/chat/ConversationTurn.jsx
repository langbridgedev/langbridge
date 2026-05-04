import { RuntimeResultPanel } from "../RuntimeResultPanel";
import { formatRelativeTime, formatRuntimeAgentModeLabel } from "../../lib/runtimeUi";
import { RuntimeActivityDisclosure } from "./RuntimeActivityDisclosure";

export function ConversationTurn({ turn, turnRef = null }) {
  const progressEvents = Array.isArray(turn.progressEvents) ? turn.progressEvents : [];
  const latestProgressEvent = progressEvents[progressEvents.length - 1] || null;
  const latestStage = latestProgressEvent?.stage || turn.liveStage || "planning";
  const latestStatus = latestProgressEvent?.status || "in_progress";
  const isPending = turn.status === "pending";
  const isReady = turn.status === "ready";
  const isError = turn.status === "error";

  return (
    <article className="conversation-turn-shell" ref={turnRef}>
      <div className="thread-user-row">
        <div className="thread-user-bubble">
          <p>{turn.prompt}</p>
          <div className="thread-user-meta">
            <span className={`thread-user-mode-pill ${turn.agentMode !== "auto" ? "active" : ""}`}>
              {formatRuntimeAgentModeLabel(turn.agentMode)}
            </span>
            <span>{formatRelativeTime(turn.createdAt)}</span>
          </div>
        </div>
      </div>

      <div className="thread-assistant-row">
        <div className="thread-assistant-shell">
          <header className="thread-assistant-meta">
            <div>
              <strong>{turn.agentLabel || "Langbridge Runtime"}</strong>
            </div>
            <span className={`message-status-badge ${turn.status}`}>
              {isReady ? "responded" : turn.status}
            </span>
          </header>

          <div className="thread-assistant-body">
            {isPending ? (
              <RuntimeActivityDisclosure
                progressEvents={progressEvents}
                latestProgressEvent={latestProgressEvent}
                latestStage={latestStage}
                latestStatus={latestStatus}
                createdAt={turn.createdAt}
                summary={turn.assistantSummary}
              />
            ) : null}
            {!isPending ? (
              <RuntimeResultPanel
                summary={turn.assistantSummary}
                answerMarkdown={turn.assistantAnswerMarkdown}
                artifacts={turn.assistantArtifacts}
                result={turn.assistantTable}
                visualization={turn.assistantVisualization}
                diagnostics={turn.diagnostics}
                status={turn.status}
                errorMessage={turn.errorMessage}
                errorStatus={turn.errorStatus}
                maxPreviewRows={10}
                variant="chat"
              />
            ) : null}
            {isError && !turn.assistantTable && !turn.assistantSummary ? (
              <div className="error-banner">{turn.errorMessage || "Run failed."}</div>
            ) : null}
          </div>
        </div>
      </div>
    </article>
  );
}
