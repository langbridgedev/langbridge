import { Copy, ExternalLink, Play } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { buildAgentTestPayload, normalizeAgentWorkspace } from "../../features/configuration/agentModel.js";
import { RUNTIME_AGENT_MODE_OPTIONS, formatRuntimeAgentModeLabel, normalizeAssistantArtifacts } from "../../lib/runtimeUi.js";
import { runAgentConfigurationTest } from "../../services/configurationService.js";
import { classNames } from "../../utils/classNames.js";
import { RuntimeResultPanel } from "../RuntimeResultPanel.jsx";
import { ManagementPill } from "../resources/ManagementPill.jsx";
import { Modal } from "../ui/Modal.jsx";

const tabLabels = {
  overview: "Overview",
  analyst: "Analyst setup",
  instructions: "Instructions",
  test: "Test agent",
  actions: "Actions",
};

const visibleTabs = ["overview", "analyst", "instructions", "test", "actions"];

export function AgentWorkspaceModal({
  resource,
  detailLoading = false,
  detailError = "",
  actions = [],
  onClose,
  onAction,
}) {
  const [activeTab, setActiveTab] = useState("overview");
  const [submittingAction, setSubmittingAction] = useState("");
  const [actionError, setActionError] = useState("");
  const [actionResult, setActionResult] = useState(null);
  const agent = useMemo(() => normalizeAgentWorkspace(resource || {}), [resource]);
  const title = agent.name || resource?.name || "Agent details";

  useEffect(() => {
    setActiveTab("overview");
    setActionError("");
    setActionResult(null);
  }, [resource?.id]);

  async function handleAction(action) {
    if (action.disabled) {
      return;
    }
    setActionError("");
    setSubmittingAction(action.id);
    try {
      const payload = await onAction(action.id);
      setActionResult({ label: action.label, payload });
    } catch (caughtError) {
      setActionError(caughtError?.message || `Unable to run ${action.label}.`);
    } finally {
      setSubmittingAction("");
    }
  }

  return (
    <Modal title={title} onClose={onClose}>
      <article className="resource-workspace agent-workspace">
        <header className="resource-workspace-head agent-workspace-head">
          <div>
            <p className="eyebrow">Analyst agent</p>
            <h3>{title}</h3>
            <span>{agent.description || "Config-managed runtime analyst profile."}</span>
          </div>
          <ManagementPill mode={agent.management} />
        </header>

        <nav className="resource-workspace-tabs" aria-label="Agent workspace tabs">
          {visibleTabs.map((tab) => (
            <button
              key={tab}
              className={classNames(activeTab === tab && "active")}
              type="button"
              onClick={() => setActiveTab(tab)}
            >
              {tabLabels[tab]}
            </button>
          ))}
        </nav>

        {detailError ? <div className="resource-error">{detailError}</div> : null}
        {actionError ? <div className="resource-error">{actionError}</div> : null}
        {detailLoading ? <div className="resource-loading">Loading runtime detail...</div> : null}

        {activeTab === "overview" ? <AgentOverview agent={agent} /> : null}
        {activeTab === "analyst" ? <AnalystSetup agent={agent} /> : null}
        {activeTab === "instructions" ? <InstructionPanel agent={agent} /> : null}
        {activeTab === "test" ? <AgentTestPanel agent={agent} /> : null}
        {activeTab === "actions" ? (
          <section className="resource-actions-panel">
            <div className="resource-readonly-notice compact">
              <strong>Config-managed agent</strong>
              <span>Agents are loaded from runtime configuration in this phase. Editing and deletion are intentionally disabled.</span>
            </div>
            <div className="resource-action-grid">
              {actions.map((action) => (
                <button
                  key={action.id}
                  className="resource-action-card"
                  type="button"
                  disabled={Boolean(submittingAction) || action.disabled}
                  onClick={() => void handleAction(action)}
                >
                  <strong>{submittingAction === action.id ? "Running..." : action.label}</strong>
                  <span>{action.description}</span>
                </button>
              ))}
            </div>
            {actionResult ? (
              <div className="agent-action-result">
                <strong>{actionResult.label}</strong>
                <span>Action completed.</span>
              </div>
            ) : null}
          </section>
        ) : null}
      </article>
    </Modal>
  );
}

function AgentOverview({ agent }) {
  const cards = [
    {
      label: "LLM",
      value: agent.llm.connection || agent.llm.model || "Default runtime LLM",
      detail: [agent.llm.provider, agent.llm.reasoningEffort].filter(Boolean).join(" | "),
    },
    {
      label: "Query policy",
      value: labelize(agent.analystScope.queryPolicy),
      detail: agent.analystScope.allowSourceScope ? "Source scope allowed" : "Governed scopes only",
    },
    {
      label: "Semantic models",
      value: String(agent.analystScope.semanticModels.length),
      detail: firstOrFallback(agent.analystScope.semanticModels, "No semantic model scope"),
    },
    {
      label: "Datasets",
      value: String(agent.analystScope.datasets.length),
      detail: firstOrFallback(agent.analystScope.datasets, "No dataset scope"),
    },
    {
      label: "Tools",
      value: String(agent.tools.length),
      detail: firstOrFallback(agent.tools.map((tool) => tool.name), "No tools advertised"),
    },
    {
      label: "Status",
      value: agent.default ? "Default" : "Ready",
      detail: "Config-managed runtime profile",
    },
  ];

  return (
    <div className="config-resource-detail">
      <div className="agent-overview-grid">
        {cards.map((card) => (
          <article key={card.label} className="agent-overview-card">
            <span>{card.label}</span>
            <strong>{card.value}</strong>
            <small>{card.detail}</small>
          </article>
        ))}
      </div>

      <div className="resource-state-grid">
        <AgentDetailBlock title="Runtime identity">
          <DefinitionRows
            rows={[
              ["Agent id", agent.id || "n/a"],
              ["Name", agent.name],
              ["Description", agent.description || "n/a"],
              ["Default", agent.default ? "Yes" : "No"],
              ["Management", agent.management],
            ]}
          />
        </AgentDetailBlock>
        <AgentDetailBlock title="LLM setup">
          <DefinitionRows
            rows={[
              ["Connection", agent.llm.connection || "Runtime default"],
              ["Provider", agent.llm.provider || "n/a"],
              ["Model", agent.llm.model || "n/a"],
              ["Reasoning", agent.llm.reasoningEffort || "n/a"],
              ["Max tokens", agent.llm.maxCompletionTokens ?? "n/a"],
            ]}
          />
        </AgentDetailBlock>
      </div>
    </div>
  );
}

function AnalystSetup({ agent }) {
  return (
    <div className="config-resource-detail">
      <div className="resource-state-grid">
        <AgentDetailBlock title="Query behavior">
          <DefinitionRows
            rows={[
              ["Query policy", labelize(agent.analystScope.queryPolicy)],
              ["Source scope", agent.analystScope.allowSourceScope ? "Allowed" : "Disabled"],
              ["Research", agent.research.enabled ? "Enabled" : "Disabled"],
              ["Web search", agent.webSearch.enabled ? "Enabled" : "Disabled"],
            ]}
          />
        </AgentDetailBlock>
        <AgentDetailBlock title="Execution limits">
          <DefinitionRows
            rows={[
              ["Max iterations", agent.execution.maxIterations ?? "n/a"],
              ["Max replans", agent.execution.maxReplans ?? "n/a"],
              ["Max step retries", agent.execution.maxStepRetries ?? "n/a"],
              ["Evidence rounds", agent.execution.maxEvidenceRounds ?? "n/a"],
              ["Governed attempts", agent.execution.maxGovernedAttempts ?? "n/a"],
            ]}
          />
        </AgentDetailBlock>
      </div>

      <AgentDetailBlock title="Semantic model scope">
        <PillList items={agent.analystScope.semanticModels} empty="No semantic models configured." />
      </AgentDetailBlock>

      <AgentDetailBlock title="Dataset scope">
        <PillList items={agent.analystScope.datasets} empty="No datasets configured." />
      </AgentDetailBlock>

      <AgentDetailBlock title="Tools">
        {agent.tools.length > 0 ? (
          <div className="agent-tool-list">
            {agent.tools.map((tool, index) => (
              <article key={`${tool.name}-${index}`} className="agent-tool-card">
                <strong>{tool.name}</strong>
                <span>{tool.description || tool.kind || "Runtime tool"}</span>
              </article>
            ))}
          </div>
        ) : (
          <div className="connector-empty-note">No tool metadata was returned for this agent.</div>
        )}
      </AgentDetailBlock>

      <AgentDetailBlock title="Connector access">
        <div className="agent-access-grid">
          <div>
            <strong>Allowed</strong>
            <PillList items={agent.access.allowedConnectors} empty="All connectors unless denied." />
          </div>
          <div>
            <strong>Denied</strong>
            <PillList items={agent.access.deniedConnectors} empty="No denied connectors." />
          </div>
        </div>
      </AgentDetailBlock>
    </div>
  );
}

function InstructionPanel({ agent }) {
  const prompts = [
    ["System", agent.prompts.system],
    ["User", agent.prompts.user],
    ["Planning", agent.prompts.planning],
    ["Presentation", agent.prompts.presentation],
    ["Response format", agent.prompts.responseFormat],
  ];

  return (
    <div className="config-resource-detail">
      {prompts.map(([label, value]) => (
        <PromptBlock key={label} label={label} value={value} />
      ))}
    </div>
  );
}

function PromptBlock({ label, value }) {
  const [copied, setCopied] = useState(false);
  const text = String(value || "").trim();

  async function copyPrompt() {
    if (
      !text ||
      typeof navigator === "undefined" ||
      !navigator.clipboard ||
      typeof navigator.clipboard.writeText !== "function"
    ) {
      return;
    }
    await navigator.clipboard.writeText(text);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  }

  return (
    <section className="agent-prompt-block">
      <header>
        <div>
          <span>{label}</span>
          <strong>{text ? `${label} prompt` : "Not configured"}</strong>
        </div>
        <button type="button" disabled={!text} onClick={() => void copyPrompt()}>
          <Copy className="button-icon" aria-hidden="true" />
          {copied ? "Copied" : "Copy"}
        </button>
      </header>
      {text ? <pre>{text}</pre> : <p>No {label.toLowerCase()} prompt returned for this agent.</p>}
    </section>
  );
}

function AgentTestPanel({ agent }) {
  const [message, setMessage] = useState("Which semantic models and datasets can you use?");
  const [agentMode, setAgentMode] = useState("auto");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);
  const canRun = Boolean(agent.name && message.trim() && !running);

  async function handleSubmit(event) {
    event.preventDefault();
    setError("");
    setRunning(true);
    try {
      const payload = buildAgentTestPayload({ agent, message, agentMode });
      setResult(await runAgentConfigurationTest(agent, payload));
    } catch (caughtError) {
      setError(caughtError?.message || "Unable to run agent test.");
    } finally {
      setRunning(false);
    }
  }

  const normalizedResult = result ? normalizeAgentTestResult(result) : null;

  return (
    <div className="config-resource-detail">
      <form className="agent-test-form" onSubmit={handleSubmit}>
        <div className="connector-form-section-head">
          <div>
            <h4>Run a test prompt</h4>
            <p>Uses the live runtime ask endpoint and the selected config-managed agent.</p>
          </div>
          <span className="semantic-count-pill">{formatRuntimeAgentModeLabel(agentMode)}</span>
        </div>
        <label className="connector-field connector-field--full">
          <span>Prompt</span>
          <textarea
            value={message}
            disabled={running}
            onChange={(event) => setMessage(event.target.value)}
            placeholder="Ask a scoped analyst question..."
          />
        </label>
        <div className="agent-test-controls">
          <label className="connector-field">
            <span>Mode</span>
            <select
              value={agentMode}
              disabled={running}
              onChange={(event) => setAgentMode(event.target.value)}
            >
              {RUNTIME_AGENT_MODE_OPTIONS.map((mode) => (
                <option key={mode.value} value={mode.value}>{mode.label}</option>
              ))}
            </select>
          </label>
          <button type="submit" disabled={!canRun}>
            <Play className="button-icon" aria-hidden="true" />
            {running ? "Running..." : "Run test"}
          </button>
        </div>
      </form>

      {error ? <div className="resource-error">{error}</div> : null}

      {normalizedResult ? (
        <section className="agent-test-result">
          <header>
            <div>
              <span>Test result</span>
              <strong>{normalizedResult.threadId ? "Runtime thread created" : "Runtime response"}</strong>
            </div>
            {normalizedResult.threadId ? (
              <a href={`/chat/${encodeURIComponent(normalizedResult.threadId)}`}>
                Open in chat
                <ExternalLink className="button-icon" aria-hidden="true" />
              </a>
            ) : null}
          </header>
          <RuntimeResultPanel
            summary=""
            answerMarkdown={normalizedResult.answerMarkdown}
            artifacts={normalizedResult.artifacts}
            diagnostics={normalizedResult.diagnostics}
            status={normalizedResult.status}
            errorMessage={normalizedResult.errorMessage}
            maxPreviewRows={8}
            variant="chat"
          />
        </section>
      ) : null}
    </div>
  );
}

function AgentDetailBlock({ title, children }) {
  return (
    <section className="resource-detail-block agent-detail-block">
      <h4>{title}</h4>
      {children}
    </section>
  );
}

function DefinitionRows({ rows }) {
  return (
    <dl className="resource-definition-list">
      {rows.map(([label, value]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>{formatValue(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

function PillList({ items, empty }) {
  const values = Array.isArray(items) ? items.filter(Boolean) : [];
  if (values.length === 0) {
    return <div className="connector-empty-note">{empty}</div>;
  }
  return (
    <div className="resource-chip-row">
      {values.map((item) => (
        <span key={item}>{item}</span>
      ))}
    </div>
  );
}

function normalizeAgentTestResult(result) {
  const content = result && typeof result === "object" ? result : {};
  const error = content.error && typeof content.error === "object" ? content.error : null;
  return {
    threadId: String(content.thread_id || ""),
    answerMarkdown: String(content.answer_markdown || content.answer || "").trim(),
    artifacts: normalizeAssistantArtifacts({ artifacts: content.artifacts }),
    diagnostics: content.diagnostics && typeof content.diagnostics === "object" ? content.diagnostics : null,
    status: error ? "error" : "ready",
    errorMessage: String(error?.message || error?.detail || ""),
  };
}

function firstOrFallback(items, fallback) {
  const values = Array.isArray(items) ? items.filter(Boolean) : [];
  if (values.length === 0) {
    return fallback;
  }
  return values.length === 1 ? values[0] : `${values[0]} +${values.length - 1}`;
}

function labelize(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatValue(value) {
  if (value === undefined || value === null || value === "") {
    return "n/a";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "number") {
    return value.toLocaleString();
  }
  return String(value);
}
