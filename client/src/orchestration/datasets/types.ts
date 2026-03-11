export type DatasetType = 'TABLE' | 'SQL' | 'FEDERATED' | 'FILE';
export type DatasetStatus = 'draft' | 'published';
export type DatasetSourceKind = 'database' | 'saas' | 'api' | 'file' | 'virtual';
export type DatasetStorageKind = 'table' | 'parquet' | 'csv' | 'json' | 'view' | 'virtual';
export type DatasetSortDirection = 'asc' | 'desc';
export type DatasetLineageNodeType =
  | 'connection'
  | 'source_table'
  | 'api_resource'
  | 'file_resource'
  | 'dataset'
  | 'semantic_model'
  | 'unified_semantic_model'
  | 'saved_query'
  | 'dashboard';
export type DatasetLineageEdgeType =
  | 'DERIVES_FROM'
  | 'REFERENCES'
  | 'GENERATED_BY'
  | 'FEEDS'
  | 'MATERIALIZES_FROM';

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

export interface DatasetExecutionCapabilities {
  supportsStructuredScan: boolean;
  supportsSqlFederation: boolean;
  supportsFilterPushdown: boolean;
  supportsProjectionPushdown: boolean;
  supportsAggregationPushdown: boolean;
  supportsJoinPushdown: boolean;
  supportsMaterialization: boolean;
  supportsSemanticModeling: boolean;
}

export interface DatasetRelationIdentity {
  canonicalReference: string;
  relationName: string;
  qualifiedName?: string | null;
  catalogName?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  storageUri?: string | null;
  datasetId?: string | null;
  connectorId?: string | null;
  sourceKind: DatasetSourceKind;
  storageKind: DatasetStorageKind;
}

export interface DatasetRecord {
  id: string;
  workspaceId: string;
  projectId?: string | null;
  connectionId?: string | null;
  ownerId?: string | null;
  name: string;
  sqlAlias: string;
  description?: string | null;
  tags: string[];
  datasetType: DatasetType;
  sourceKind: DatasetSourceKind;
  connectorKind?: string | null;
  storageKind: DatasetStorageKind;
  dialect?: string | null;
  storageUri?: string | null;
  catalogName?: string | null;
  schemaName?: string | null;
  tableName?: string | null;
  sqlText?: string | null;
  referencedDatasetIds: string[];
  federatedPlan?: Record<string, unknown> | null;
  fileConfig?: Record<string, unknown> | null;
  relationIdentity: DatasetRelationIdentity;
  executionCapabilities: DatasetExecutionCapabilities;
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
  sqlAlias: string;
  datasetType: DatasetType;
  sourceKind: DatasetSourceKind;
  connectorKind?: string | null;
  storageKind: DatasetStorageKind;
  relationIdentity: DatasetRelationIdentity;
  executionCapabilities: DatasetExecutionCapabilities;
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
  unifiedSemanticModels: Array<Record<string, unknown>>;
  dependentDatasets: Array<Record<string, unknown>>;
  dashboards: Array<Record<string, unknown>>;
  savedQueries: Array<Record<string, unknown>>;
}

export interface DatasetVersionSummary {
  id: string;
  datasetId: string;
  revisionNumber: number;
  revisionHash?: string | null;
  createdAt: string;
  createdBy?: string | null;
  changeSummary?: string | null;
  status?: DatasetStatus | null;
  isCurrent: boolean;
}

export interface DatasetVersion extends DatasetVersionSummary {
  definitionSnapshot: Record<string, unknown>;
  schemaSnapshot: Array<Record<string, unknown>>;
  policySnapshot: Record<string, unknown>;
  sourceBindingsSnapshot: Array<Record<string, unknown>>;
  executionCharacteristicsSnapshot?: Record<string, unknown> | null;
}

export interface DatasetVersionListResponse {
  items: DatasetVersionSummary[];
}

export interface DatasetVersionFieldDiff {
  field: string;
  changeType: string;
  before?: unknown;
  after?: unknown;
}

export interface DatasetSchemaColumnDiff {
  columnName: string;
  changeType: string;
  before?: Record<string, unknown> | null;
  after?: Record<string, unknown> | null;
}

export interface DatasetVersionDiffResponse {
  datasetId: string;
  fromRevisionId: string;
  toRevisionId: string;
  fromRevisionNumber: number;
  toRevisionNumber: number;
  summary: string[];
  definitionChanges: DatasetVersionFieldDiff[];
  policyChanges: DatasetVersionFieldDiff[];
  sourceBindingChanges: DatasetVersionFieldDiff[];
  executionChanges: DatasetVersionFieldDiff[];
  schemaChanges: DatasetSchemaColumnDiff[];
}

export interface DatasetRestorePayload {
  workspaceId: string;
  projectId?: string | null;
  revisionId: string;
  changeSummary?: string | null;
}

export interface DatasetLineageNode {
  nodeType: DatasetLineageNodeType;
  nodeId: string;
  label: string;
  direction: string;
  metadata: Record<string, unknown>;
}

export interface DatasetLineageEdge {
  sourceType: DatasetLineageNodeType;
  sourceId: string;
  targetType: DatasetLineageNodeType;
  targetId: string;
  edgeType: DatasetLineageEdgeType;
  metadata: Record<string, unknown>;
}

export interface DatasetLineageResponse {
  datasetId: string;
  nodes: DatasetLineageNode[];
  edges: DatasetLineageEdge[];
  upstreamCount: number;
  downstreamCount: number;
}

export interface DatasetImpactItem {
  nodeType: DatasetLineageNodeType;
  nodeId: string;
  label: string;
  direct: boolean;
  metadata: Record<string, unknown>;
}

export interface DatasetImpactResponse {
  datasetId: string;
  totalDownstreamAssets: number;
  directDependents: DatasetImpactItem[];
  dependentDatasets: DatasetImpactItem[];
  semanticModels: DatasetImpactItem[];
  unifiedSemanticModels: DatasetImpactItem[];
  savedQueries: DatasetImpactItem[];
  dashboards: DatasetImpactItem[];
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
  storageUri?: string | null;
  sqlText?: string | null;
  referencedDatasetIds?: string[];
  federatedPlan?: Record<string, unknown> | null;
  fileConfig?: Record<string, unknown> | null;
  columns?: DatasetColumn[];
  policy?: Partial<DatasetPolicy>;
  status?: DatasetStatus;
  changeSummary?: string | null;
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

export interface DatasetCsvIngestResponse {
  datasetId: string;
  jobId: string;
  jobStatus: string;
  storageUri: string;
}

export interface DatasetUpdatePayload {
  workspaceId: string;
  projectId?: string | null;
  connectionId?: string | null;
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
  changeSummary?: string | null;
}
