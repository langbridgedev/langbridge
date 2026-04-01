function buildError(message, status, payload) {
  const error = new Error(message);
  error.status = status;
  error.payload = payload;
  return error;
}

async function parseResponse(response) {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return response.json();
  }
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function runtimeRequest(path, options = {}) {
  const headers = new Headers(options.headers || {});
  headers.set("Accept", "application/json");
  const hasBody = options.body !== undefined && options.body !== null;
  const isFormData = typeof FormData !== "undefined" && options.body instanceof FormData;
  if (hasBody && !isFormData && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(path, {
    credentials: "include",
    ...options,
    headers,
  });
  const payload = await parseResponse(response);
  if (!response.ok) {
    const message =
      payload?.detail ||
      payload?.message ||
      (typeof payload === "string" ? payload : "") ||
      `Runtime request failed with status ${response.status}`;
    throw buildError(message, response.status, payload);
  }
  return payload;
}

export function fetchAuthBootstrapStatus() {
  return runtimeRequest("/api/runtime/v1/auth/bootstrap");
}

export function bootstrapAdmin(payload) {
  return runtimeRequest("/api/runtime/v1/auth/bootstrap", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function login(payload) {
  return runtimeRequest("/api/runtime/v1/auth/login", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function logout() {
  return runtimeRequest("/api/runtime/v1/auth/logout", {
    method: "POST",
  });
}

export function fetchAuthMe() {
  return runtimeRequest("/api/runtime/v1/auth/me");
}

export function fetchActors() {
  return runtimeRequest("/api/runtime/v1/actors");
}

export function createActor(payload) {
  return runtimeRequest("/api/runtime/v1/actors", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function updateActor(actorId, payload) {
  return runtimeRequest(`/api/runtime/v1/actors/${encodeURIComponent(actorId)}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export function resetActorPassword(actorId, payload) {
  return runtimeRequest(`/api/runtime/v1/actors/${encodeURIComponent(actorId)}/reset-password`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function fetchRuntimeInfo() {
  return runtimeRequest("/api/runtime/v1/info");
}

export function fetchRuntimeSummary() {
  return runtimeRequest("/api/runtime/ui/v1/summary");
}

export function fetchConnectors() {
  return runtimeRequest("/api/runtime/v1/connectors");
}

export function fetchConnectorTypes() {
  return runtimeRequest("/api/runtime/v1/connector/types");
}

export function fetchConnectorTypeConfig(connectorType) {
  return runtimeRequest(
    `/api/runtime/v1/connector/type/${encodeURIComponent(connectorType)}/config`,
  );
}

export function fetchConnector(connectorName) {
  return runtimeRequest(`/api/runtime/v1/connectors/${encodeURIComponent(connectorName)}`);
}

export function createConnector(payload) {
  return runtimeRequest("/api/runtime/v1/connectors", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function updateConnector(connectorName, payload) {
  return runtimeRequest(`/api/runtime/v1/connectors/${encodeURIComponent(connectorName)}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export function deleteConnector(connectorName) {
  return runtimeRequest(`/api/runtime/v1/connectors/${encodeURIComponent(connectorName)}`, {
    method: "DELETE",
  });
}

export function fetchConnectorResources(connectorName) {
  return runtimeRequest(
    `/api/runtime/v1/connectors/${encodeURIComponent(connectorName)}/sync/resources`,
  );
}

export function fetchConnectorStates(connectorName) {
  return runtimeRequest(
    `/api/runtime/v1/connectors/${encodeURIComponent(connectorName)}/sync/states`,
  );
}

export function runConnectorSync(connectorName, payload) {
  return runtimeRequest(`/api/runtime/v1/connectors/${encodeURIComponent(connectorName)}/sync`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function fetchDatasets() {
  return runtimeRequest("/api/runtime/v1/datasets");
}

export function createDataset(payload) {
  return runtimeRequest("/api/runtime/v1/datasets", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function updateDataset(datasetRef, payload) {
  return runtimeRequest(`/api/runtime/v1/datasets/${encodeURIComponent(datasetRef)}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export function deleteDataset(datasetRef) {
  return runtimeRequest(`/api/runtime/v1/datasets/${encodeURIComponent(datasetRef)}`, {
    method: "DELETE",
  });
}

export function fetchDataset(datasetRef) {
  return runtimeRequest(`/api/runtime/v1/datasets/${encodeURIComponent(datasetRef)}`);
}

export function previewDataset(datasetRef, payload = { limit: 25 }) {
  return runtimeRequest(`/api/runtime/v1/datasets/${encodeURIComponent(datasetRef)}/preview`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function fetchSemanticModels() {
  return runtimeRequest("/api/runtime/v1/semantic-models");
}

export function createSemanticModel(payload) {
  return runtimeRequest("/api/runtime/v1/semantic-models", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function updateSemanticModel(modelRef, payload) {
  return runtimeRequest(`/api/runtime/v1/semantic-models/${encodeURIComponent(modelRef)}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export function deleteSemanticModel(modelRef) {
  return runtimeRequest(`/api/runtime/v1/semantic-models/${encodeURIComponent(modelRef)}`, {
    method: "DELETE",
  });
}

export function fetchSemanticModel(modelRef) {
  return runtimeRequest(`/api/runtime/v1/semantic-models/${encodeURIComponent(modelRef)}`);
}

export function querySemantic(payload) {
  return runtimeRequest("/api/runtime/v1/semantic/query", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function querySql(payload) {
  return runtimeRequest("/api/runtime/v1/sql/query", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function fetchAgents() {
  return runtimeRequest("/api/runtime/v1/agents");
}

export function fetchAgent(agentRef) {
  return runtimeRequest(`/api/runtime/v1/agents/${encodeURIComponent(agentRef)}`);
}

export function askAgent(payload) {
  return runtimeRequest("/api/runtime/v1/agents/ask", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function createThread(payload = {}) {
  return runtimeRequest("/api/runtime/v1/threads", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function fetchThreads() {
  return runtimeRequest("/api/runtime/v1/threads");
}

export function fetchThread(threadId) {
  return runtimeRequest(`/api/runtime/v1/threads/${encodeURIComponent(threadId)}`);
}

export function updateThread(threadId, payload) {
  return runtimeRequest(`/api/runtime/v1/threads/${encodeURIComponent(threadId)}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export function deleteThread(threadId) {
  return runtimeRequest(`/api/runtime/v1/threads/${encodeURIComponent(threadId)}`, {
    method: "DELETE",
  });
}

export function fetchThreadMessages(threadId) {
  return runtimeRequest(`/api/runtime/v1/threads/${encodeURIComponent(threadId)}/messages`);
}
