import { formatRelativeTime } from "../../lib/runtimeUi";
import { formatStageTitle, getProgressIcon } from "./chatRuntimeProgress";

export function RuntimeActivityDisclosure({
  progressEvents,
  latestProgressEvent,
  latestStage,
  latestStatus,
  createdAt,
  summary,
}) {
  const events = Array.isArray(progressEvents) ? progressEvents.slice().reverse() : [];
  const LatestStageIcon = getProgressIcon(latestStage, latestStatus);

  return (
    <div className="thread-progress-inline">
      <div className="thread-progress-current">
        <div className="thread-progress-current-top">
          <span className={`thread-progress-status ${latestStatus}`}>
            <span className="thread-progress-live-dot" aria-hidden="true" />
            <LatestStageIcon className="thread-progress-stage-icon" aria-hidden="true" />
            {formatStageTitle(latestStage)}
          </span>
          <span className="thread-progress-current-time">
            {formatRelativeTime(latestProgressEvent?.timestamp || createdAt)}
          </span>
        </div>
        <p>{summary || "Runtime is still working on this run."}</p>
      </div>
      {events.length > 1 ? (
        <details className="thread-thinking-disclosure">
          <summary>
            <span>Runtime activity</span>
            <span>{events.length} updates</span>
          </summary>
          <div className="thread-thinking-list">
            {events.slice(0, 8).map((event) => {
              const ProgressIcon = getProgressIcon(event.stage, event.status);
              return (
                <div
                  key={`${event.sequence}-${event.rawEventType || event.event}`}
                  className={`thread-thinking-item ${event.status || "in_progress"}`}
                >
                  <div className="thread-thinking-item-top">
                    <div className="thread-thinking-item-stage">
                      <ProgressIcon className="thread-progress-stage-icon" aria-hidden="true" />
                      <strong>{formatStageTitle(event.stage)}</strong>
                    </div>
                    <span>{formatRelativeTime(event.timestamp)}</span>
                  </div>
                  <p>{event.message}</p>
                  {event.source || event.rawEventType ? (
                    <div className="thread-thinking-item-meta">
                      {event.source ? <span>{event.source}</span> : null}
                      {event.rawEventType ? <span>{event.rawEventType}</span> : null}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </details>
      ) : null}
    </div>
  );
}
