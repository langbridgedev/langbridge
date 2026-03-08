export type SqlExecutionMode = 'single' | 'federated';
export type SqlDialect =
  | 'tsql'
  | 'postgres'
  | 'mysql'
  | 'snowflake'
  | 'redshift'
  | 'bigquery'
  | 'oracle'
  | 'sqlite';
export type SqlJobStatus =
  | 'queued'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'cancelled'
  | 'awaiting_approval';

export interface SqlExecuteRequestPayload {
  workspaceId: string;
  projectId?: string | null;
  connectionId?: string | null;
  federated?: boolean;
  query: string;
  queryDialect?: SqlDialect;
  params?: Record<string, unknown>;
  requestedLimit?: number;
  requestedTimeoutSeconds?: number;
  explain?: boolean;
  federatedDatasets?: Array<{ alias: string; datasetId: string }>;
}

export interface SqlExecuteResponsePayload {
  sqlJobId: string;
  expensiveQuery: boolean;
  warnings: string[];
}

export interface SqlCancelRequestPayload {
  sqlJobId: string;
  workspaceId: string;
}

export interface SqlCancelResponsePayload {
  accepted: boolean;
  status: SqlJobStatus;
}

export interface SqlColumnMetadata {
  name: string;
  type?: string | null;
}

export interface SqlJobArtifact {
  id: string;
  format: string;
  mimeType: string;
  rowCount: number;
  byteSize?: number | null;
  storageReference: string;
  createdAt: string;
}

export interface SqlJobRecord {
  id: string;
  workspaceId: string;
  projectId?: string | null;
  userId: string;
  connectionId?: string | null;
  executionMode: SqlExecutionMode;
  status: SqlJobStatus;
  queryHash: string;
  isExplain: boolean;
  isFederated: boolean;
  requestedLimit?: number | null;
  enforcedLimit: number;
  requestedTimeoutSeconds?: number | null;
  enforcedTimeoutSeconds: number;
  rowCountPreview: number;
  totalRowsEstimate?: number | null;
  bytesScanned?: number | null;
  durationMs?: number | null;
  redactionApplied: boolean;
  warning?: Record<string, unknown> | null;
  error?: Record<string, unknown> | null;
  correlationId?: string | null;
  createdAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  artifacts: SqlJobArtifact[];
}

export interface SqlJobResultsPayload {
  sqlJobId: string;
  status: SqlJobStatus;
  columns: SqlColumnMetadata[];
  rows: Array<Record<string, unknown>>;
  rowCountPreview: number;
  totalRowsEstimate?: number | null;
  nextCursor?: string | null;
  artifacts: SqlJobArtifact[];
}

export interface SqlHistoryPayload {
  items: SqlJobRecord[];
}

export interface SqlSavedQueryRecord {
  id: string;
  workspaceId: string;
  projectId?: string | null;
  createdBy: string;
  updatedBy: string;
  connectionId?: string | null;
  name: string;
  description?: string | null;
  query: string;
  queryHash: string;
  tags: string[];
  defaultParams: Record<string, unknown>;
  isShared: boolean;
  lastSqlJobId?: string | null;
  lastResultArtifactId?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface SqlSavedQueryCreatePayload {
  workspaceId: string;
  projectId?: string | null;
  connectionId?: string | null;
  name: string;
  description?: string | null;
  query: string;
  tags?: string[];
  defaultParams?: Record<string, unknown>;
  isShared?: boolean;
  lastSqlJobId?: string | null;
}

export interface SqlSavedQueryUpdatePayload {
  workspaceId: string;
  projectId?: string | null;
  connectionId?: string | null;
  name?: string;
  description?: string | null;
  query?: string;
  tags?: string[];
  defaultParams?: Record<string, unknown>;
  isShared?: boolean;
  lastSqlJobId?: string | null;
}

export interface SqlSavedQueryListPayload {
  items: SqlSavedQueryRecord[];
}

export interface SqlWorkspacePolicyBounds {
  maxPreviewRowsUpperBound: number;
  maxExportRowsUpperBound: number;
  maxRuntimeSecondsUpperBound: number;
  maxConcurrencyUpperBound: number;
}

export interface SqlWorkspacePolicyRecord {
  workspaceId: string;
  maxPreviewRows: number;
  maxExportRows: number;
  maxRuntimeSeconds: number;
  maxConcurrency: number;
  allowDml: boolean;
  allowFederation: boolean;
  allowedSchemas: string[];
  allowedTables: string[];
  defaultDatasource?: string | null;
  budgetLimitBytes?: number | null;
  bounds: SqlWorkspacePolicyBounds;
  updatedAt?: string | null;
}

export interface SqlWorkspacePolicyUpdatePayload {
  workspaceId: string;
  maxPreviewRows?: number;
  maxExportRows?: number;
  maxRuntimeSeconds?: number;
  maxConcurrency?: number;
  allowDml?: boolean;
  allowFederation?: boolean;
  allowedSchemas?: string[];
  allowedTables?: string[];
  defaultDatasource?: string | null;
  budgetLimitBytes?: number | null;
}

export type SqlAssistMode = 'generate' | 'fix' | 'explain' | 'lint';

export interface SqlAssistRequestPayload {
  workspaceId: string;
  connectionId?: string | null;
  mode: SqlAssistMode;
  prompt: string;
  query?: string;
}

export interface SqlAssistResponsePayload {
  mode: SqlAssistMode;
  suggestion: string;
  warnings: string[];
}
