export type DatasetType = 'TABLE' | 'SQL' | 'FEDERATED' | 'FILE';
export type DatasetStatus = 'draft' | 'published';
export type DatasetSortDirection = 'asc' | 'desc';

export interface DatasetColumn {
  id?: string;
  datasetId?: string;
  name: string;
  dataType: string;
  nullable: boolean;
  description?: string | null;
  isAllowed: boolean;
  isComputed: boolean;
  expression?: string | null;
  ordinalPosition?: number;
}

export interface DatasetPolicy {
  maxRowsPreview: number;
  maxExportRows: number;
  redactionRules: Record<string, string>;
  rowFilters: string[];
  allowDml: boolean;
}

export interface DatasetPolicyDefaults {
  maxPreviewRows?: number;
  maxExportRows?: number;
  allowDml?: boolean;
  redactionRules?: Record<string, string>;
}

export interface DatasetStats {
  rowCountEstimate?: number | null;
  bytesEstimate?: number | null;
  lastProfiledAt?: string | null;
}

export interface DatasetRecord {
  id: string;
  workspaceId: string;
  projectId?: string | null;
  connectionId?: string | null;
  name: string;
  description?: string | null;
  tags: string[];
  datasetType: DatasetType;
  dialect?: string | null;
  catalogName?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  sqlText?: string | null;
  referencedDatasetIds: string[];
  federatedPlan?: Record<string, unknown> | null;
  fileConfig?: Record<string, unknown> | null;
  status: DatasetStatus;
  revisionId?: string | null;
  columns: DatasetColumn[];
  policy: DatasetPolicy;
  stats: DatasetStats;
  createdAt: string;
  updatedAt: string;
}

export interface DatasetListResponse {
  items: DatasetRecord[];
  total: number;
}

export interface DatasetCatalogItem {
  id: string;
  name: string;
  datasetType: DatasetType;
  tags: string[];
  columns: DatasetColumn[];
  updatedAt: string;
}

export interface DatasetCatalogResponse {
  workspaceId: string;
  items: DatasetCatalogItem[];
}

export interface DatasetUsageResponse {
  semanticModels: Array<Record<string, unknown>>;
  dashboards: Array<Record<string, unknown>>;
  savedQueries: Array<Record<string, unknown>>;
}

export interface DatasetPreviewSortItem {
  column: string;
  direction: DatasetSortDirection;
}

export interface DatasetPreviewRequestPayload {
  workspaceId: string;
  projectId?: string | null;
  limit?: number;
  filters?: Record<string, unknown>;
  sort?: DatasetPreviewSortItem[];
  userContext?: Record<string, unknown>;
}

export interface DatasetPreviewColumn {
  name: string;
  dataType?: string | null;
}

export interface DatasetPreviewResponse {
  jobId: string;
  status: string;
  datasetId: string;
  columns: DatasetPreviewColumn[];
  rows: Array<Record<string, unknown>>;
  rowCountPreview: number;
  effectiveLimit: number;
  redactionApplied: boolean;
  durationMs?: number | null;
  bytesScanned?: number | null;
  error?: string | null;
}

export interface DatasetProfileRequestPayload {
  workspaceId: string;
  projectId?: string | null;
  userContext?: Record<string, unknown>;
}

export interface DatasetProfileResponse {
  jobId: string;
  status: string;
  datasetId: string;
  rowCountEstimate?: number | null;
  bytesEstimate?: number | null;
  distinctCounts: Record<string, number>;
  nullRates: Record<string, number>;
  profiledAt?: string | null;
  error?: string | null;
}

export interface DatasetCreatePayload {
  workspaceId: string;
  projectId?: string | null;
  name: string;
  description?: string | null;
  tags?: string[];
  datasetType: DatasetType;
  connectionId?: string | null;
  dialect?: string | null;
  catalogName?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  sqlText?: string | null;
  referencedDatasetIds?: string[];
  federatedPlan?: Record<string, unknown> | null;
  fileConfig?: Record<string, unknown> | null;
  columns?: DatasetColumn[];
  policy?: Partial<DatasetPolicy>;
  status?: DatasetStatus;
}

export interface DatasetSelectionColumnPayload {
  name: string;
  dataType?: string | null;
  nullable?: boolean | null;
}

export interface DatasetTableSelectionPayload {
  schema: string;
  table: string;
  columns: DatasetSelectionColumnPayload[];
}

export interface DatasetEnsurePayload {
  workspaceId: string;
  projectId?: string | null;
  connectionId: string;
  schema: string;
  table: string;
  columns: DatasetSelectionColumnPayload[];
  name?: string;
  namingTemplate?: string;
  policyDefaults?: DatasetPolicyDefaults;
  tags?: string[];
}

export interface DatasetEnsureResponse {
  datasetId: string;
  created: boolean;
  name: string;
}

export interface DatasetBulkCreatePayload {
  workspaceId: string;
  projectId?: string | null;
  connectionId: string;
  selections: DatasetTableSelectionPayload[];
  namingTemplate?: string;
  policyDefaults?: DatasetPolicyDefaults;
  tags?: string[];
  profileAfterCreate?: boolean;
}

export interface DatasetBulkCreateStartResponse {
  jobId: string;
  jobStatus: string;
}

export interface DatasetUpdatePayload {
  workspaceId: string;
  projectId?: string | null;
  name?: string;
  description?: string | null;
  tags?: string[];
  dialect?: string | null;
  catalogName?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  sqlText?: string | null;
  referencedDatasetIds?: string[];
  federatedPlan?: Record<string, unknown> | null;
  fileConfig?: Record<string, unknown> | null;
  columns?: DatasetColumn[];
  policy?: Partial<DatasetPolicy>;
  status?: DatasetStatus;
}
