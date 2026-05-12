import { normalizeRuntimeAgentMode } from "../../lib/runtimeUi.js";

export function normalizeAgentWorkspace(resourceOrPayload = {}) {
  const raw = resourceOrPayload?.rawPayload || resourceOrPayload || {};
  const definition = objectValue(raw.definition);
  const availability = objectValue(definition.availability || raw.availability);
  const dataScope = objectValue(definition.data_scope || raw.data_scope);
  const capabilities = objectValue(definition.capabilities || raw.capabilities);
  const research = objectValue(capabilities.research);
  const webSearch = objectValue(capabilities.web_search);
  const instructions = objectValue(definition.instructions || raw.instructions);
  const llm = objectValue(definition.llm || raw.llm);
  const orchestration = objectValue(definition.orchestration || raw.orchestration);
  const effectiveAccess = objectValue(definition.effective_access || raw.effective_access);
  const tools = toArray(raw.tools || definition.tools);
  const semanticModels = toStringList(raw.semantic_models || dataScope.semantic_models);
  const datasets = toStringList(raw.datasets || dataScope.datasets);

  return {
    id: String(raw.id || resourceOrPayload?.id || ""),
    name: String(raw.name || resourceOrPayload?.name || "agent").trim(),
    description: String(raw.description || resourceOrPayload?.description || "").trim(),
    status: raw.default ? "Default" : "Ready",
    default: Boolean(raw.default),
    management: resourceOrPayload?.management || "config_managed",
    availability: {
      runtime: availability.runtime !== false,
      mcp: Boolean(availability.mcp),
    },
    llm: {
      connection: stringValue(raw.llm_connection || llm.llm_connection),
      provider: stringValue(llm.provider),
      model: stringValue(llm.model),
      temperature: numberOrNull(llm.temperature),
      reasoningEffort: stringValue(llm.reasoning_effort),
      maxCompletionTokens: numberOrNull(llm.max_completion_tokens),
    },
    dataScope: {
      semanticModels,
      datasets,
      queryPolicy: stringValue(dataScope.query_policy || "semantic_preferred"),
    },
    capabilities: {
      sourceSql: Boolean(capabilities.source_sql),
      research: {
        enabled: Boolean(research.enabled),
        extendedThinking: Boolean(research.extended_thinking),
        maxSources: numberOrNull(research.max_sources),
        requireSources: Boolean(research.require_sources),
      },
      webSearch: {
        enabled: Boolean(webSearch.enabled),
        provider: stringValue(webSearch.provider),
        allowedDomains: toStringList(webSearch.allowed_domains),
        requireAllowedDomain: Boolean(webSearch.require_allowed_domain),
        maxResults: numberOrNull(webSearch.max_results),
        timeboxSeconds: numberOrNull(webSearch.timebox_seconds),
      },
    },
    instructions: {
      system: stringValue(instructions.system),
      user: stringValue(instructions.user || raw.instructions),
      planning: stringValue(instructions.planning),
      presentation: stringValue(instructions.presentation),
      responseFormat: stringValue(instructions.response_format),
    },
    orchestration: {
      policy: stringValue(orchestration.policy || "balanced_governed"),
    },
    effectiveAccess: {
      connectors: toStringList(effectiveAccess.connectors),
    },
    tools: tools.map(normalizeTool),
    raw,
  };
}

export function buildAgentTestPayload({ agent, message, agentMode = "auto", title = "" }) {
  const agentName = String(agent?.name || agent?.rawPayload?.name || "").trim();
  const prompt = String(message || "").trim();
  if (!agentName) {
    throw new Error("Agent name is required.");
  }
  if (!prompt) {
    throw new Error("Enter a test prompt before running the agent.");
  }
  return {
    agent_name: agentName,
    message: prompt,
    agent_mode: normalizeRuntimeAgentMode(agentMode),
    title: String(title || `Configuration test: ${agentName}`).trim(),
    metadata_json: {
      source: "runtime_ui_next.configuration.agent_test",
    },
  };
}

export function agentWorkspaceStats(agent) {
  const model = normalizeAgentWorkspace(agent);
  return {
    semanticModels: model.dataScope.semanticModels.length,
    datasets: model.dataScope.datasets.length,
    capabilities: [
      model.capabilities.sourceSql,
      model.capabilities.research.enabled,
      model.capabilities.webSearch.enabled,
      model.availability.mcp,
    ].filter(Boolean).length,
    instructions: Object.values(model.instructions).filter(Boolean).length,
  };
}

function normalizeTool(tool) {
  if (!tool || typeof tool !== "object" || Array.isArray(tool)) {
    return { name: String(tool || "tool"), description: "", kind: "" };
  }
  return {
    name: stringValue(tool.name || tool.label || tool.id || tool.tool_name || "tool"),
    description: stringValue(tool.description),
    kind: stringValue(tool.kind || tool.type || tool.task_kind || tool.tool_type),
  };
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function toArray(value) {
  if (Array.isArray(value)) {
    return value.filter((item) => item !== undefined && item !== null && item !== "");
  }
  if (value === undefined || value === null || value === "") {
    return [];
  }
  return [value];
}

function toStringList(value) {
  return toArray(value).map((item) => String(item).trim()).filter(Boolean);
}

function stringValue(value) {
  return value === undefined || value === null ? "" : String(value).trim();
}

function numberOrNull(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}
