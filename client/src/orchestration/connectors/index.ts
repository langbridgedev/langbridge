import { apiFetch } from '../http';
import type {
  ConnectorCatalogResponse,
  ConnectorConfigSchema,
  ConnectorResourceListResponse,
  ConnectorResponse,
  ConnectorSyncHistoryResponse,
  ConnectorSyncRequestPayload,
  ConnectorSyncStartResponse,
  ConnectorSyncStateListResponse,
  ConnectorTestResponse,
  CreateConnectorPayload,
  UpdateConnectorPayload,
} from './types';

const BASE_PATH = '/api/v1/connectors';

function requireOrganizationId(organizationId: string): string {
  if (!organizationId) {
    throw new Error('Organization id is required.');
  }
  return organizationId;
}

function basePath(organizationId: string): string {
  return `${BASE_PATH}/${requireOrganizationId(organizationId)}`;
}

export async function fetchConnectorTypes(organizationId: string): Promise<string[]> {
  return apiFetch<string[]>(`${basePath(organizationId)}/schemas/type`);
}

export async function fetchConnectorSchema(
  organizationId: string,
  type: string,
): Promise<ConnectorConfigSchema> {
  const normalized = type.trim();
  if (!normalized) {
    throw new Error('Connector type is required.');
  }
  return apiFetch<ConnectorConfigSchema>(
    `${basePath(organizationId)}/schema/${encodeURIComponent(normalized)}`,
  );
}

export async function createConnector(
  organizationId: string,
  payload: CreateConnectorPayload,
): Promise<ConnectorResponse> {
  return apiFetch<ConnectorResponse>(basePath(organizationId), {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function fetchConnectors(organizationId: string): Promise<ConnectorResponse[]> {
  return apiFetch<ConnectorResponse[]>(basePath(organizationId));
}

export async function fetchConnector(
  organizationId: string,
  connectorId: string,
): Promise<ConnectorResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  return apiFetch<ConnectorResponse>(`${basePath(organizationId)}/${encodeURIComponent(connectorId)}`);
}

export async function testConnector(
  organizationId: string,
  connectorId: string,
): Promise<ConnectorTestResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  return apiFetch<ConnectorTestResponse>(
    `${basePath(organizationId)}/${encodeURIComponent(connectorId)}/test`,
    {
      method: 'POST',
    },
  );
}

export async function fetchConnectorResources(
  organizationId: string,
  connectorId: string,
): Promise<ConnectorResourceListResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  return apiFetch<ConnectorResourceListResponse>(
    `${basePath(organizationId)}/${encodeURIComponent(connectorId)}/resources`,
  );
}

export async function syncConnector(
  organizationId: string,
  connectorId: string,
  payload: ConnectorSyncRequestPayload,
): Promise<ConnectorSyncStartResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  return apiFetch<ConnectorSyncStartResponse>(
    `${basePath(organizationId)}/${encodeURIComponent(connectorId)}/sync`,
    {
      method: 'POST',
      body: JSON.stringify(payload),
    },
  );
}

export async function fetchConnectorSyncState(
  organizationId: string,
  connectorId: string,
): Promise<ConnectorSyncStateListResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  return apiFetch<ConnectorSyncStateListResponse>(
    `${basePath(organizationId)}/${encodeURIComponent(connectorId)}/sync-state`,
  );
}

export async function fetchConnectorSyncHistory(
  organizationId: string,
  connectorId: string,
  limit = 20,
): Promise<ConnectorSyncHistoryResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  const params = new URLSearchParams({
    limit: String(Math.max(1, Math.min(100, Math.floor(limit)))),
  });
  return apiFetch<ConnectorSyncHistoryResponse>(
    `${basePath(organizationId)}/${encodeURIComponent(connectorId)}/sync-history?${params.toString()}`,
  );
}

export async function updateConnector(
  organizationId: string,
  connectorId: string,
  payload: UpdateConnectorPayload,
): Promise<ConnectorResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  return apiFetch<ConnectorResponse>(`${basePath(organizationId)}/${encodeURIComponent(connectorId)}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function deleteConnector(
  organizationId: string,
  connectorId: string,
): Promise<void> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  await apiFetch<void>(`${basePath(organizationId)}/${encodeURIComponent(connectorId)}`, {
    method: 'DELETE',
    skipJsonParse: true,
  });
}

export async function fetchConnectorCatalog(
  organizationId: string,
  connectorId: string,
  options?: {
    search?: string;
    includeSchemas?: string[];
    excludeSchemas?: string[];
    includeSystemSchemas?: boolean;
    includeColumns?: boolean;
    limit?: number;
    offset?: number;
  },
): Promise<ConnectorCatalogResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required.');
  }
  const params = new URLSearchParams();
  if (options?.search?.trim()) {
    params.set('search', options.search.trim());
  }
  for (const schema of options?.includeSchemas || []) {
    if (schema.trim()) {
      params.append('include_schemas', schema.trim());
    }
  }
  for (const schema of options?.excludeSchemas || []) {
    if (schema.trim()) {
      params.append('exclude_schemas', schema.trim());
    }
  }
  if (options?.includeSystemSchemas) {
    params.set('include_system_schemas', 'true');
  }
  if (options?.includeColumns === false) {
    params.set('include_columns', 'false');
  }
  if (typeof options?.limit === 'number' && Number.isFinite(options.limit)) {
    params.set('limit', String(Math.max(1, Math.min(1000, Math.floor(options.limit)))));
  }
  if (typeof options?.offset === 'number' && Number.isFinite(options.offset)) {
    params.set('offset', String(Math.max(0, Math.floor(options.offset))));
  }
  const query = params.toString();
  return apiFetch<ConnectorCatalogResponse>(
    `${basePath(organizationId)}/${encodeURIComponent(connectorId)}/catalog${query ? `?${query}` : ''}`,
  );
}

export type {
  ConnectorCatalogColumn,
  ConnectorCatalogResponse,
  ConnectorCatalogSchema,
  ConnectorCatalogTable,
  ConnectorResource,
  ConnectorResourceListResponse,
  ConnectorSyncHistoryItem,
  ConnectorSyncHistoryResponse,
  ConnectorSyncMode,
  ConnectorSyncRequestPayload,
  ConnectorSyncStartResponse,
  ConnectorSyncState,
  ConnectorSyncStateListResponse,
  ConnectorSyncStatus,
  ConnectorTestResponse,
  ConnectorConfigEntry,
  ConnectorConfigSchema,
  ConnectorResponse,
  CreateConnectorPayload,
  UpdateConnectorPayload,
} from './types';
