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

export async function runtimeRequest(path, options = {}) {
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

export function parseSseEventBlock(block) {
  const lines = String(block || "").split(/\r?\n/);
  let eventName = "message";
  let eventId = "";
  const dataLines = [];

  lines.forEach((line) => {
    if (!line || line.startsWith(":")) {
      return;
    }
    if (line.startsWith("event:")) {
      eventName = line.slice(6).trim() || eventName;
      return;
    }
    if (line.startsWith("id:")) {
      eventId = line.slice(3).trim();
      return;
    }
    if (line.startsWith("data:")) {
      dataLines.push(line.slice(5).trimStart());
    }
  });

  if (dataLines.length === 0) {
    return null;
  }

  const dataText = dataLines.join("\n");
  let payload = dataText;
  try {
    payload = JSON.parse(dataText);
  } catch {}

  if (payload && typeof payload === "object" && !Array.isArray(payload)) {
    return eventId ? { id: eventId, event: eventName, ...payload } : { event: eventName, ...payload };
  }
  return eventId ? { id: eventId, event: eventName, data: payload } : { event: eventName, data: payload };
}

async function readSseResponse(response, { onEvent } = {}) {
  if (!response.ok) {
    const errorPayload = await parseResponse(response);
    const message =
      errorPayload?.detail ||
      errorPayload?.message ||
      (typeof errorPayload === "string" ? errorPayload : "") ||
      `Runtime stream failed with status ${response.status}`;
    throw buildError(message, response.status, errorPayload);
  }

  const events = [];
  let buffer = "";
  const flushBuffer = () => {
    const chunks = buffer.split(/\r?\n\r?\n/);
    buffer = chunks.pop() || "";
    chunks.forEach((chunk) => {
      const parsed = parseSseEventBlock(chunk);
      if (!parsed) {
        return;
      }
      events.push(parsed);
      if (typeof onEvent === "function") {
        onEvent(parsed);
      }
    });
  };

  if (!response.body || typeof response.body.getReader !== "function") {
    buffer = await response.text();
    flushBuffer();
    if (buffer.trim()) {
      const parsed = parseSseEventBlock(buffer);
      if (parsed) {
        events.push(parsed);
        if (typeof onEvent === "function") {
          onEvent(parsed);
        }
      }
    }
    return events;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
    flushBuffer();
    if (done) {
      break;
    }
  }
  if (buffer.trim()) {
    const parsed = parseSseEventBlock(buffer);
    if (parsed) {
      events.push(parsed);
      if (typeof onEvent === "function") {
        onEvent(parsed);
      }
    }
  }
  return events;
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

export function fetchDatasetSync(datasetRef) {
  return runtimeRequest(`/api/runtime/v1/datasets/${encodeURIComponent(datasetRef)}/sync`);
}

export async function runDatasetSync(datasetRef, payload, options = {}) {
  const queued = await runtimeRequest(`/api/runtime/v1/datasets/${encodeURIComponent(datasetRef)}/sync`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
  if (!queued?.job_id || options.waitForCompletion === false) {
    return queued;
  }
  if (typeof options.onQueued === "function") {
    options.onQueued(queued);
  }
  const job = await waitForRuntimeJob(queued.job_id, options);
  return syncResultFromJob(queued, job);
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

export async function runSqlQuery(payload, options = {}) {
  const queued = await runtimeRequest("/api/runtime/v1/sql/query/jobs", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  if (!queued?.job_id || options.waitForCompletion === false) {
    return queued;
  }
  if (typeof options.onQueued === "function") {
    options.onQueued(queued);
  }
  const job = await waitForRuntimeJob(queued.job_id, options);
  return sqlResultFromJob(queued, job, payload);
}

export function fetchAgents() {
  return runtimeRequest("/api/runtime/v1/agents");
}

export function fetchAgent(agentRef) {
  return runtimeRequest(`/api/runtime/v1/agents/${encodeURIComponent(agentRef)}`);
}

export function createAgentRun(payload) {
  return runtimeRequest("/api/runtime/v1/agents/run", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function fetchRuntimeJob(jobId) {
  return runtimeRequest(`/api/runtime/v1/jobs/${encodeURIComponent(jobId)}`);
}

export function cancelRuntimeJob(jobId, payload = {}) {
  return runtimeRequest(`/api/runtime/v1/jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function streamRuntimeJob(jobId, options = {}) {
  const { signal, onEvent, afterSequence } = options;
  const query = Number.isFinite(Number(afterSequence)) && Number(afterSequence) > 0
    ? `?after_sequence=${encodeURIComponent(String(Number(afterSequence)))}`
    : "";
  const response = await fetch(
    `/api/runtime/v1/jobs/${encodeURIComponent(jobId)}/stream${query}`,
    {
      method: "GET",
      credentials: "include",
      headers: {
        Accept: "text/event-stream",
      },
      signal,
    },
  );
  return readSseResponse(response, { onEvent });
}

export async function waitForRuntimeJob(jobId, options = {}) {
  const {
    signal,
    onEvent,
    pollIntervalMs = 750,
    timeoutMs = 120_000,
  } = options;

  try {
    await streamRuntimeJob(jobId, { signal, onEvent });
  } catch (caughtError) {
    if (signal?.aborted) {
      throw caughtError;
    }
  }

  const deadline = Date.now() + Math.max(0, Number(timeoutMs) || 0);
  let job = await fetchRuntimeJob(jobId);
  while (!isTerminalRuntimeJob(job)) {
    if (Date.now() >= deadline) {
      throw buildError(`Runtime job ${jobId} did not complete before timeout.`, 408, job);
    }
    await sleep(Math.max(100, Number(pollIntervalMs) || 750), signal);
    job = await fetchRuntimeJob(jobId);
  }
  return job;
}

function isTerminalRuntimeJob(job) {
  return ["succeeded", "failed", "cancelled"].includes(
    String(job?.status || "").trim().toLowerCase(),
  );
}

function syncResultFromJob(queued, job) {
  const status = String(job?.status || "").trim().toLowerCase();
  if (status === "failed" || status === "cancelled") {
    const message =
      job?.error?.message ||
      job?.status_message ||
      `Dataset sync job ${status}.`;
    throw buildError(message, status === "cancelled" ? 409 : 500, job);
  }
  const result = job?.result && typeof job.result === "object" ? job.result : {};
  return {
    ...queued,
    ...result,
    status: result.status || status || queued.status,
    job_id: queued.job_id || job?.id,
    job_type: queued.job_type || job?.job_type,
    job,
  };
}

function sqlResultFromJob(queued, job, requestPayload = {}) {
  const status = String(job?.status || "").trim().toLowerCase();
  if (status === "failed" || status === "cancelled") {
    const message =
      job?.error?.message ||
      job?.status_message ||
      `SQL query job ${status}.`;
    throw buildError(message, status === "cancelled" ? 409 : 500, job);
  }
  const result = job?.result && typeof job.result === "object" ? job.result : {};
  const resultTable = findRuntimeJobArtifact(job, "result_table");
  const diagnostics = findRuntimeJobArtifact(job, "sql_diagnostics");
  const tableData = resultTable?.data && typeof resultTable.data === "object" ? resultTable.data : {};
  const diagnosticsData = diagnostics?.data && typeof diagnostics.data === "object" ? diagnostics.data : {};
  const rows = Array.isArray(result.rows)
    ? result.rows
    : Array.isArray(tableData.rows)
      ? tableData.rows
      : [];
  const columns = Array.isArray(result.columns)
    ? result.columns
    : Array.isArray(tableData.columns)
      ? tableData.columns
      : [];
  return {
    ...queued,
    ...result,
    status: result.status || status || queued.status,
    job_id: queued.job_id || job?.id,
    job_type: queued.job_type || job?.job_type,
    query_scope: result.query_scope || queued.query_scope || requestPayload.query_scope,
    query: result.query || requestPayload.query,
    generated_sql: result.generated_sql || diagnosticsData.generated_sql,
    rows,
    columns,
    row_count_preview:
      result.row_count_preview ??
      tableData.row_count_preview ??
      rows.length,
    total_rows_estimate: result.total_rows_estimate ?? diagnosticsData.total_rows_estimate,
    bytes_scanned: result.bytes_scanned ?? diagnosticsData.bytes_scanned,
    duration_ms: result.duration_ms ?? diagnosticsData.duration_ms,
    redaction_applied: Boolean(result.redaction_applied ?? diagnosticsData.redaction_applied),
    federation_diagnostics:
      result.federation_diagnostics || diagnosticsData.federation_diagnostics || null,
    artifacts: Array.isArray(job?.artifacts) ? job.artifacts : [],
    job,
  };
}

function findRuntimeJobArtifact(job, artifactKey) {
  if (!Array.isArray(job?.artifacts)) {
    return null;
  }
  return job.artifacts.find((artifact) => artifact?.artifact_key === artifactKey) || null;
}

function sleep(ms, signal) {
  if (signal?.aborted) {
    return Promise.reject(signal.reason || new Error("Operation aborted."));
  }
  return new Promise((resolve, reject) => {
    let timeout;
    const onAbort = () => {
      clearTimeout(timeout);
      reject(signal.reason || new Error("Operation aborted."));
    };
    timeout = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    if (!signal) {
      return;
    }
    signal.addEventListener("abort", onAbort, { once: true });
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
