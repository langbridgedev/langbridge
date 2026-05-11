export const LLM_PROVIDER_OPTIONS = [
  { value: "openai", label: "OpenAI" },
  { value: "azure", label: "Azure OpenAI" },
  { value: "anthropic", label: "Anthropic" },
  { value: "ollama", label: "Ollama" },
];

export const STRUCTURED_OUTPUT_OPTIONS = [
  { value: "auto", label: "Auto" },
  { value: "native", label: "Native only" },
  { value: "prompt", label: "Prompt fallback" },
];

export function buildLLMConnectionFormState(resource = null) {
  const payload = resource?.rawPayload || {};
  const configuration = { ...(payload.configuration || {}) };
  return {
    name: payload.name || "",
    provider: String(payload.provider || "openai").toLowerCase(),
    model: payload.model || "",
    description: payload.description || "",
    apiKey: "",
    baseUrl: payload.base_url || configuration.base_url || "",
    structuredOutputs: configuration.structured_outputs || payload.structured_outputs || "auto",
    isActive: Boolean(payload.is_active ?? true),
    default: Boolean(payload.default),
    configurationText: formatConfigurationJson(stripPromotedConfiguration(configuration)),
  };
}

export function isRuntimeManagedLLMConnection(resource) {
  return resource?.management === "runtime_managed";
}

export function buildLLMConnectionPayload(formState, { mode = "create" } = {}) {
  const provider = String(formState.provider || "").trim().toLowerCase();
  const name = String(formState.name || "").trim();
  const model = String(formState.model || "").trim();
  const apiKey = String(formState.apiKey || "").trim();
  const configuration = buildConfiguration(formState);

  if (!model) {
    throw new Error("Model is required.");
  }
  if (mode === "create") {
    if (!name) {
      throw new Error("Name is required.");
    }
    if (!provider) {
      throw new Error("Provider is required.");
    }
    if (provider !== "ollama" && !apiKey) {
      throw new Error("API key is required for non-Ollama providers.");
    }
  }

  const payload = {
    description: String(formState.description || "").trim() || null,
    model,
    configuration,
    is_active: Boolean(formState.isActive),
    default: Boolean(formState.default),
  };

  if (apiKey) {
    payload.api_key = apiKey;
  }

  if (mode === "create") {
    payload.name = name;
    payload.provider = provider;
    if (!apiKey) {
      payload.api_key = "";
    }
  }

  return payload;
}

function buildConfiguration(formState) {
  const configuration = parseConfigurationJson(formState.configurationText);
  const baseUrl = String(formState.baseUrl || "").trim();
  const structuredOutputs = String(formState.structuredOutputs || "auto").trim().toLowerCase() || "auto";

  if (baseUrl) {
    configuration.base_url = baseUrl;
  } else {
    delete configuration.base_url;
  }
  configuration.structured_outputs = structuredOutputs;
  return configuration;
}

function parseConfigurationJson(text) {
  const raw = String(text || "").trim();
  if (!raw) {
    return {};
  }
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("Advanced configuration must be a JSON object.");
    }
    return { ...parsed };
  } catch (error) {
    if (error?.message === "Advanced configuration must be a JSON object.") {
      throw error;
    }
    throw new Error("Advanced configuration must be valid JSON.");
  }
}

function stripPromotedConfiguration(configuration) {
  const next = { ...(configuration || {}) };
  delete next.base_url;
  delete next.structured_outputs;
  return next;
}

function formatConfigurationJson(value) {
  return JSON.stringify(value || {}, null, 2);
}
