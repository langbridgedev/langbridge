import { ArrowRight, Plus } from "lucide-react";
import { useEffect, useRef } from "react";

import { RUNTIME_AGENT_MODE_OPTIONS } from "../../lib/runtimeUi";

export function ChatComposer({
  agents,
  selectedAgentName,
  onSelectedAgentNameChange,
  selectedAgentMode,
  onSelectedAgentModeChange,
  message,
  onMessageChange,
  submitting,
  onSubmit,
}) {
  const textareaRef = useRef(null);
  const canSubmit = Boolean(!submitting && selectedAgentName && String(message || "").trim());

  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) {
      return;
    }
    textarea.style.height = "auto";
    textarea.style.height = `${Math.min(textarea.scrollHeight, 180)}px`;
  }, [message]);

  function handleSubmit(event) {
    event.preventDefault();
    if (canSubmit) {
      void onSubmit();
    }
  }

  function handleKeyDown(event) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      if (canSubmit) {
        void onSubmit();
      }
    }
  }

  return (
    <form className="thread-composer-form thread-composer-form--chat" onSubmit={handleSubmit}>
      <div className="thread-composer-input-shell thread-composer-input-shell--modern">
        <textarea
          ref={textareaRef}
          className="text-input thread-composer-input thread-composer-input--textarea"
          value={message}
          onChange={(event) => onMessageChange(event.target.value)}
          onKeyDown={handleKeyDown}
          disabled={submitting}
          rows={1}
          aria-label="Message"
          placeholder="Ask Langbridge to analyze, explain, visualize, or investigate..."
        />
        <button
          className="thread-composer-send"
          type="submit"
          disabled={!canSubmit}
          aria-label={submitting ? "Sending message" : "Send message"}
          title={submitting ? "Sending..." : "Send"}
        >
          <ArrowRight className="button-icon" aria-hidden="true" />
        </button>
      </div>

      <div className="thread-composer-meta thread-composer-meta--chat thread-composer-meta--modern">
        <label className="thread-chat-agent-field thread-chat-agent-field--inline">
          <span>Agent</span>
          <select
            className="select-input thread-agent-select"
            value={selectedAgentName}
            onChange={(event) => onSelectedAgentNameChange(event.target.value)}
            disabled={submitting || agents.length === 0}
          >
            {agents.map((item) => (
              <option key={item.id || item.name} value={item.name}>
                {item.name}
              </option>
            ))}
          </select>
        </label>

        <label className="thread-chat-agent-field thread-chat-agent-field--inline thread-chat-mode-field">
          <span>Mode</span>
          <select
            className="select-input thread-agent-select thread-mode-select"
            value={selectedAgentMode}
            onChange={(event) => onSelectedAgentModeChange(event.target.value)}
            disabled={submitting}
            aria-label="Agent mode"
          >
            {RUNTIME_AGENT_MODE_OPTIONS.map((mode) => (
              <option key={mode.value} value={mode.value}>
                {mode.label}
              </option>
            ))}
          </select>
        </label>

        <button
          className={`thread-composer-utility thread-composer-utility--text ${
            selectedAgentMode === "research" ? "active" : ""
          }`}
          type="button"
          onClick={() =>
            onSelectedAgentModeChange(selectedAgentMode === "research" ? "auto" : "research")
          }
          disabled={submitting}
          aria-label={
            selectedAgentMode === "research"
              ? "Switch back to auto mode"
              : "Quick switch to research mode"
          }
          aria-pressed={selectedAgentMode === "research"}
          title={
            selectedAgentMode === "research"
              ? "Research mode selected"
              : "Quick switch to research mode"
          }
        >
          <Plus className="button-icon" aria-hidden="true" />
          <span>Research</span>
        </button>
      </div>
    </form>
  );
}
