import { apiFetch, resolveApiUrl } from '../http';

import type {
  SqlAssistRequestPayload,
  SqlAssistResponsePayload,
  SqlCancelRequestPayload,
  SqlCancelResponsePayload,
  SqlExecuteRequestPayload,
  SqlExecuteResponsePayload,
  SqlHistoryPayload,
  SqlJobRecord,
  SqlJobResultsPayload,
  SqlSavedQueryCreatePayload,
  SqlSavedQueryListPayload,
  SqlSavedQueryRecord,
  SqlSavedQueryUpdatePayload,
  SqlWorkspacePolicyRecord,
  SqlWorkspacePolicyUpdatePayload,
} from './types';

const SQL_BASE_PATH = '/api/v1/sql';

function requiredWorkspaceId(workspaceId: string): string {
  if (!workspaceId) {
    throw new Error('Workspace id is required.');
  }
  return workspaceId;
}

export async function executeSql(payload: SqlExecuteRequestPayload): Promise<SqlExecuteResponsePayload> {
  return apiFetch<SqlExecuteResponsePayload>(`${SQL_BASE_PATH}/execute`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function cancelSqlJob(payload: SqlCancelRequestPayload): Promise<SqlCancelResponsePayload> {
  return apiFetch<SqlCancelResponsePayload>(`${SQL_BASE_PATH}/cancel`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function fetchSqlJob(workspaceId: string, sqlJobId: string): Promise<SqlJobRecord> {
  requiredWorkspaceId(workspaceId);
  if (!sqlJobId) {
    throw new Error('SQL job id is required.');
  }
  const params = new URLSearchParams({ workspace_id: workspaceId });
  return apiFetch<SqlJobRecord>(`${SQL_BASE_PATH}/jobs/${encodeURIComponent(sqlJobId)}?${params.toString()}`);
}

export async function fetchSqlJobResults(
  workspaceId: string,
  sqlJobId: string,
  cursor?: string | null,
  pageSize = 100,
): Promise<SqlJobResultsPayload> {
  requiredWorkspaceId(workspaceId);
  if (!sqlJobId) {
    throw new Error('SQL job id is required.');
  }
  const params = new URLSearchParams({
    workspace_id: workspaceId,
    page_size: String(pageSize),
  });
  if (cursor) {
    params.set('cursor', cursor);
  }
  return apiFetch<SqlJobResultsPayload>(
    `${SQL_BASE_PATH}/jobs/${encodeURIComponent(sqlJobId)}/results?${params.toString()}`,
  );
}

export async function downloadSqlJobResults(
  workspaceId: string,
  sqlJobId: string,
  format: 'csv' | 'parquet',
): Promise<Blob> {
  requiredWorkspaceId(workspaceId);
  if (!sqlJobId) {
    throw new Error('SQL job id is required.');
  }
  const params = new URLSearchParams({
    workspace_id: workspaceId,
    format,
  });
  const response = await fetch(
    resolveApiUrl(`${SQL_BASE_PATH}/jobs/${encodeURIComponent(sqlJobId)}/results/download?${params.toString()}`),
    {
      credentials: 'include',
    },
  );
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Unable to export ${format.toUpperCase()} results.`);
  }
  return response.blob();
}

export async function fetchSqlHistory(
  workspaceId: string,
  scope: 'user' | 'workspace' = 'user',
  limit = 100,
): Promise<SqlHistoryPayload> {
  requiredWorkspaceId(workspaceId);
  const params = new URLSearchParams({
    workspace_id: workspaceId,
    scope,
    limit: String(limit),
  });
  return apiFetch<SqlHistoryPayload>(`${SQL_BASE_PATH}/history?${params.toString()}`);
}

export async function createSavedSqlQuery(payload: SqlSavedQueryCreatePayload): Promise<SqlSavedQueryRecord> {
  return apiFetch<SqlSavedQueryRecord>(`${SQL_BASE_PATH}/saved`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function listSavedSqlQueries(workspaceId: string): Promise<SqlSavedQueryListPayload> {
  requiredWorkspaceId(workspaceId);
  const params = new URLSearchParams({ workspace_id: workspaceId });
  return apiFetch<SqlSavedQueryListPayload>(`${SQL_BASE_PATH}/saved?${params.toString()}`);
}

export async function getSavedSqlQuery(workspaceId: string, savedQueryId: string): Promise<SqlSavedQueryRecord> {
  requiredWorkspaceId(workspaceId);
  if (!savedQueryId) {
    throw new Error('Saved query id is required.');
  }
  const params = new URLSearchParams({ workspace_id: workspaceId });
  return apiFetch<SqlSavedQueryRecord>(
    `${SQL_BASE_PATH}/saved/${encodeURIComponent(savedQueryId)}?${params.toString()}`,
  );
}

export async function updateSavedSqlQuery(
  savedQueryId: string,
  payload: SqlSavedQueryUpdatePayload,
): Promise<SqlSavedQueryRecord> {
  if (!savedQueryId) {
    throw new Error('Saved query id is required.');
  }
  return apiFetch<SqlSavedQueryRecord>(`${SQL_BASE_PATH}/saved/${encodeURIComponent(savedQueryId)}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function deleteSavedSqlQuery(workspaceId: string, savedQueryId: string): Promise<void> {
  requiredWorkspaceId(workspaceId);
  if (!savedQueryId) {
    throw new Error('Saved query id is required.');
  }
  const params = new URLSearchParams({ workspace_id: workspaceId });
  await apiFetch<void>(`${SQL_BASE_PATH}/saved/${encodeURIComponent(savedQueryId)}?${params.toString()}`, {
    method: 'DELETE',
    skipJsonParse: true,
  });
}

export async function fetchSqlWorkspacePolicy(workspaceId: string): Promise<SqlWorkspacePolicyRecord> {
  requiredWorkspaceId(workspaceId);
  const params = new URLSearchParams({ workspace_id: workspaceId });
  return apiFetch<SqlWorkspacePolicyRecord>(`${SQL_BASE_PATH}/policies?${params.toString()}`);
}

export async function updateSqlWorkspacePolicy(
  payload: SqlWorkspacePolicyUpdatePayload,
): Promise<SqlWorkspacePolicyRecord> {
  return apiFetch<SqlWorkspacePolicyRecord>(`${SQL_BASE_PATH}/policies`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function assistSql(payload: SqlAssistRequestPayload): Promise<SqlAssistResponsePayload> {
  return apiFetch<SqlAssistResponsePayload>(`${SQL_BASE_PATH}/assist`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function fetchConnectorSchemas(organizationId: string, connectorId: string): Promise<{ schemas: string[] }> {
  if (!organizationId || !connectorId) {
    throw new Error('Organization and connector ids are required.');
  }
  return apiFetch<{ schemas: string[] }>(
    `/api/v1/connectors/${encodeURIComponent(organizationId)}/${encodeURIComponent(connectorId)}/source/schemas`,
  );
}

export async function fetchConnectorTables(
  organizationId: string,
  connectorId: string,
  schema: string,
): Promise<{ schema: string; tables: string[] }> {
  if (!organizationId || !connectorId || !schema) {
    throw new Error('Organization, connector, and schema are required.');
  }
  return apiFetch<{ schema: string; tables: string[] }>(
    `/api/v1/connectors/${encodeURIComponent(organizationId)}/${encodeURIComponent(connectorId)}/source/schema/${encodeURIComponent(schema)}`,
  );
}

export async function fetchConnectorColumns(
  organizationId: string,
  connectorId: string,
  schema: string,
  table: string,
): Promise<{ name: string; columns: Record<string, { name: string; type: string }> }> {
  if (!organizationId || !connectorId || !schema || !table) {
    throw new Error('Organization, connector, schema, and table are required.');
  }
  return apiFetch<{ name: string; columns: Record<string, { name: string; type: string }> }>(
    `/api/v1/connectors/${encodeURIComponent(organizationId)}/${encodeURIComponent(connectorId)}/source/schema/${encodeURIComponent(schema)}/table/${encodeURIComponent(table)}/columns`,
  );
}

export type {
  SqlAssistMode,
  SqlDialect,
  SqlExecutionMode,
  SqlJobStatus,
  SqlExecuteRequestPayload,
  SqlExecuteResponsePayload,
  SqlCancelRequestPayload,
  SqlCancelResponsePayload,
  SqlColumnMetadata,
  SqlJobArtifact,
  SqlJobRecord,
  SqlJobResultsPayload,
  SqlHistoryPayload,
  SqlSavedQueryCreatePayload,
  SqlSavedQueryUpdatePayload,
  SqlSavedQueryRecord,
  SqlSavedQueryListPayload,
  SqlWorkspacePolicyRecord,
  SqlWorkspacePolicyUpdatePayload,
  SqlAssistRequestPayload,
  SqlAssistResponsePayload,
} from './types';
