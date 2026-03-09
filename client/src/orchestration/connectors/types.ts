export interface ConnectorConfigEntry {
  field: string;
  value?: unknown;
  label?: string | null;
  required: boolean;
  default?: string | null;
  description: string;
  type: string;
  valueList?: string[] | null;
}

export interface ConnectorConfigSchema {
  name: string;
  description: string;
  version: string;
  label: string;
  icon: string;
  connectorType: string;
  config: ConnectorConfigEntry[];
}

export interface ConnectorResponse {
  id?: string;
  name: string;
  description?: string | null;
  version?: string | null;
  label?: string | null;
  icon?: string | null;
  connectorType?: string | null;
  pluginMetadata?: Record<string, unknown> | null;
  organizationId?: string | null;
  projectId?: string | null;
  config?: Record<string, unknown> | null;
  catalogSummary?: {
    schemaCount: number;
    tableCount: number;
    columnCount: number;
  } | null;
}

export interface ConnectorConfigPayload {
  config: Record<string, unknown>;
  [key: string]: unknown;
}

export interface CreateConnectorPayload {
  name: string;
  description?: string;
  version?: string;
  label?: string;
  icon?: string;
  connectorType: string;
  organizationId?: string;
  projectId?: string;
  config: ConnectorConfigPayload;
}

export interface UpdateConnectorPayload {
  name?: string;
  description?: string;
  version?: string;
  label?: string;
  icon?: string;
  connectorType?: string;
  organizationId: string;
  projectId?: string;
  config?: ConnectorConfigPayload;
}

export interface ConnectorCatalogColumn {
  name: string;
  type: string;
  nullable?: boolean | null;
  primaryKey?: boolean;
}

export interface ConnectorCatalogTable {
  schema: string;
  name: string;
  fullyQualifiedName: string;
  columns: ConnectorCatalogColumn[];
}

export interface ConnectorCatalogSchema {
  name: string;
  tables: ConnectorCatalogTable[];
}

export interface ConnectorCatalogResponse {
  connectorId: string;
  schemas: ConnectorCatalogSchema[];
  schemaCount: number;
  tableCount: number;
  columnCount: number;
  offset: number;
  limit: number;
  hasMore: boolean;
}

export type ConnectorSyncMode = 'FULL_REFRESH' | 'INCREMENTAL' | 'WEBHOOK_ASSISTED';
export type ConnectorSyncStatus = 'never_synced' | 'running' | 'succeeded' | 'failed';

export interface ConnectorTestResponse {
  status: string;
  message: string;
}

export interface ConnectorResource {
  name: string;
  label?: string | null;
  primaryKey?: string | null;
  parentResource?: string | null;
  cursorField?: string | null;
  incrementalCursorField?: string | null;
  supportsIncremental: boolean;
  defaultSyncMode: ConnectorSyncMode;
  status: ConnectorSyncStatus;
  lastCursor?: string | null;
  lastSyncAt?: string | null;
  datasetIds: string[];
  datasetNames: string[];
  recordsSynced?: number | null;
}

export interface ConnectorResourceListResponse {
  connectorId: string;
  items: ConnectorResource[];
}

export interface ConnectorSyncRequestPayload {
  resources: string[];
  syncMode: ConnectorSyncMode;
  forceFullRefresh?: boolean;
}

export interface ConnectorSyncStartResponse {
  jobId: string;
  jobStatus: string;
}

export interface ConnectorSyncState {
  id: string;
  workspaceId: string;
  connectionId: string;
  connectorType: string;
  resourceName: string;
  syncMode: ConnectorSyncMode;
  lastCursor?: string | null;
  lastSyncAt?: string | null;
  state: Record<string, unknown>;
  status: ConnectorSyncStatus;
  errorMessage?: string | null;
  recordsSynced: number;
  bytesSynced?: number | null;
  createdAt: string;
  updatedAt: string;
  datasetIds: string[];
}

export interface ConnectorSyncStateListResponse {
  connectionId: string;
  items: ConnectorSyncState[];
}

export interface ConnectorSyncHistoryItem {
  jobId: string;
  status: string;
  progress: number;
  statusMessage?: string | null;
  createdAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  error?: Record<string, unknown> | null;
  payload: Record<string, unknown>;
}

export interface ConnectorSyncHistoryResponse {
  connectionId: string;
  items: ConnectorSyncHistoryItem[];
}
