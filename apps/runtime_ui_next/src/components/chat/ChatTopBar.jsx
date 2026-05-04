import { Check, Edit3, History, X } from "lucide-react";

export function ChatTopBar({
  threadTitle,
  isPending,
  selectedAgentName,
  selectedAgentModeLabel,
  onBack,
  renamingOpen,
  onToggleRename,
  renameValue,
  onRenameValueChange,
  onRenameSubmit,
  onCancelRename,
  renaming,
  renameError,
}) {
  function handleRenameSubmit(event) {
    event.preventDefault();
    void onRenameSubmit();
  }

  return (
    <header className="thread-chat-header thread-chat-header--modern">
      <div className="thread-chat-title thread-chat-title--modern">
        <span className={`thread-status-dot ${isPending ? "pending" : "ready"}`} aria-hidden="true" />
        {renamingOpen ? (
          <form className="thread-title-edit-form" onSubmit={handleRenameSubmit}>
            <input
              className="thread-title-edit-input"
              type="text"
              value={renameValue}
              onChange={(event) => onRenameValueChange(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Escape") {
                  event.preventDefault();
                  onCancelRename();
                }
              }}
              disabled={renaming}
              aria-label="Thread title"
              autoFocus
            />
            <button
              className="thread-title-edit-action"
              type="submit"
              disabled={renaming}
              aria-label="Save thread title"
              title="Save thread title"
            >
              <Check className="button-icon" aria-hidden="true" />
            </button>
            <button
              className="thread-title-edit-action"
              type="button"
              onClick={onCancelRename}
              disabled={renaming}
              aria-label="Cancel rename"
              title="Cancel rename"
            >
              <X className="button-icon" aria-hidden="true" />
            </button>
            {renameError ? <span className="thread-title-edit-error">{renameError}</span> : null}
          </form>
        ) : (
          <div>
            <h1>{threadTitle}</h1>
            <p>
              {selectedAgentName ? selectedAgentName : "No agent selected"} - {selectedAgentModeLabel}
            </p>
          </div>
        )}
      </div>
      <div className="thread-chat-actions thread-chat-actions--modern">
        <span className={`thread-status-pill ${isPending ? "pending" : "ready"}`}>
          {isPending ? "Running" : "Ready"}
        </span>
        <button className="ghost-button compact" type="button" onClick={onBack}>
          <History className="button-icon" aria-hidden="true" />
          Threads
        </button>
        <button
          className="ghost-button compact"
          type="button"
          onClick={onToggleRename}
          aria-label={renamingOpen ? "Close rename" : "Rename thread"}
          disabled={renaming}
        >
          <Edit3 className="button-icon" aria-hidden="true" />
          {renamingOpen ? "Editing" : "Rename"}
        </button>
      </div>
    </header>
  );
}
