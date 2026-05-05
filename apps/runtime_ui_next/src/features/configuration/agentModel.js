import { normalizeRuntimeAgentMode } from "../../lib/runtimeUi.js";

export function normalizeAgentWorkspace(resourceOrPayload = {}) {
  const raw = resourceOrPayload?.rawPayload || resourceOrPayload || {};
  const definition = objectValue(raw.definition);
  const analystScope = objectValue(definition.analyst_scope || raw.analyst_scope);
  const prompts = normalizePrompts(definition.prompts || raw.prompts, raw.instructions);
  const llmScope = objectValue(definition.llm_scope || raw.llm_scope);
  const execution = objectValue(definition.execution || raw.execution);
  const researchScope = objectValue(definition.research_scope || raw.research_scope);
  const webSearchScope = objectValue(definition.web_search_scope || raw.web_search_scope);
  const access = objectValue(definition.access || raw.access);
  const tools = toArray(raw.tools || definition.tools);
  const semanticModels = toStringList(raw.semantic_models || analystScope.semantic_models);
  const datasets = toStringList(raw.datasets || analystScope.datasets);

  return {
    id: String(raw.id || resourceOrPayload?.id || ""),
    name: String(raw.name || resourceOrPayload?.name || "agent").trim(),
    description: String(raw.description || resourceOrPayload?.description || "").trim(),
    status: raw.default ? "Default" : "Ready",
    default: Boolean(raw.default),
    management: resourceOrPayload?.management || "config_managed",
    llm: {
      connection: stringValue(raw.llm_connection || llmScope.llm_connection),
      provider: stringValue(llmScope.provider),
      model: stringValue(llmScope.model),
      temperature: numberOrNull(llmScope.temperature),
      reasoningEffort: stringValue(llmScope.reasoning_effort),
      maxCompletionTokens: numberOrNull(llmScope.max_completion_tokens),
    },
    analystScope: {
      semanticModels,
      datasets,
      queryPolicy: stringValue(analystScope.query_policy || "semantic_preferred"),
      allowSourceScope: Boolean(analystScope.allow_source_scope),
    },
    prompts,
    execution: {
      maxIterations: numberOrNull(execution.max_iterations),
      maxReplans: numberOrNull(execution.max_replans),
      maxStepRetries: numberOrNull(execution.max_step_retries),
      maxEvidenceRounds: numberOrNull(execution.max_evidence_rounds),
      maxGovernedAttempts: numberOrNull(execution.max_governed_attempts),
      finalReviewEnabled: execution.final_review_enabled,
    },
    research: {
      enabled: Boolean(researchScope.enabled),
      extendedThinking: Boolean(researchScope.extended_thinking_enabled ?? researchScope.extended_thinking),
      maxSources: numberOrNull(researchScope.max_sources),
      requireSources: Boolean(researchScope.require_sources),
    },
    webSearch: {
      enabled: Boolean(webSearchScope.enabled),
      provider: stringValue(webSearchScope.provider),
      allowedDomains: toStringList(webSearchScope.allowed_domains),
      requireAllowedDomain: Boolean(webSearchScope.require_allowed_domain),
      maxResults: numberOrNull(webSearchScope.max_results),
      timeboxSeconds: numberOrNull(webSearchScope.timebox_seconds),
    },
    access: {
      allowedConnectors: toStringList(access.allowed_connectors),
      deniedConnectors: toStringList(access.denied_connectors),
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
    semanticModels: model.analystScope.semanticModels.length,
    datasets: model.analystScope.datasets.length,
    tools: model.tools.length,
    prompts: Object.values(model.prompts).filter(Boolean).length,
  };
}

function normalizePrompts(value, fallbackUserPrompt = "") {
  const prompts = objectValue(value);
  return {
    system: stringValue(prompts.system_prompt || prompts.system),
    user: stringValue(prompts.user_prompt || prompts.user || fallbackUserPrompt),
    planning: stringValue(prompts.planning_prompt || prompts.planning),
    presentation: stringValue(prompts.presentation_prompt || prompts.presentation),
    responseFormat: stringValue(prompts.response_format_prompt || prompts.response_format),
  };
}

function normalizeTool(tool) {
  if (!tool || typeof tool !== "object" || Array.isArray(tool)) {
    return { name: String(tool || "tool"), description: "", kind: "" };
  }
  return {
    name: stringValue(tool.name || tool.label || tool.id || tool.tool_name || "tool"),
    description: stringValue(tool.description),
    kind: stringValue(tool.kind || tool.type || tool.task_kind),
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
