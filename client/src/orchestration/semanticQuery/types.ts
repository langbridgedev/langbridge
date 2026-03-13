export interface SemanticDimensionPayload {
  name: string;
  full_path?: string;
  type: string;
  primary_key?: boolean;
  alias?: string | null;
  description?: string | null;
  synonyms?: string[] | null;
  vectorized?: boolean;
  vector_reference?: string | null;
  vector_index?: Record<string, unknown> | null;
}

export interface SemanticMeasurePayload {
  name: string;
  full_path?: string;
  type: string;
  description?: string | null;
  aggregation?: string | null;
  synonyms?: string[] | null;
}

export interface SemanticFilterPayload {
  condition: string;
  description?: string | null;
  synonyms?: string[] | null;
}

export interface SemanticDatasetPayload {
  dataset_id?: string | null;
  relation_name?: string | null;
  schema_name?: string | null;
  catalog_name?: string | null;
  schema?: string | null;
  name?: string | null;
  description?: string | null;
  synonyms?: string[] | null;
  dimensions?: SemanticDimensionPayload[] | null;
  measures?: SemanticMeasurePayload[] | null;
  filters?: Record<string, SemanticFilterPayload> | null;
}

export type SemanticTablePayload = SemanticDatasetPayload;

export interface SemanticRelationshipPayload {
  name: string;
  type: string;
  source_dataset?: string | null;
  source_field?: string | null;
  target_dataset?: string | null;
  target_field?: string | null;
  operator?: string | null;
  from_?: string | null;
  to?: string | null;
  join_on?: string | null;
}

export interface SemanticMetricPayload {
  description?: string | null;
  expression: string;
}

export interface SemanticModelPayload {
  version: string;
  name?: string | null;
  connector?: string | null;
  dialect?: string | null;
  description?: string | null;
  tags?: string[] | null;
  datasets?: Record<string, SemanticDatasetPayload> | null;
  tables?: Record<string, SemanticTablePayload> | null;
  relationships?: SemanticRelationshipPayload[] | null;
  metrics?: Record<string, SemanticMetricPayload> | null;
}

export interface SemanticQueryMetaResponse {
  id: string;
  name: string;
  description?: string | null;
  connectorId?: string | null;
  organizationId: string;
  projectId?: string | null;
  semanticModel: SemanticModelPayload;
}

export interface SemanticQueryFilter {
  member?: string;
  dimension?: string;
  measure?: string;
  timeDimension?: string;
  operator: string;
  values?: string[];
}

export interface SemanticQueryTimeDimension {
  dimension: string;
  granularity?: string;
  dateRange?: string | string[];
  compareDateRange?: string | string[];
}

export type SemanticQueryOrder =
  | Record<string, 'asc' | 'desc'>
  | Array<Record<string, 'asc' | 'desc'>>;

export interface SemanticQueryPayload {
  measures?: string[];
  dimensions?: string[];
  timeDimensions?: SemanticQueryTimeDimension[];
  filters?: SemanticQueryFilter[];
  segments?: string[];
  order?: SemanticQueryOrder;
  limit?: number;
  offset?: number;
  timezone?: string;
}

export interface SemanticQueryRequestPayload {
  organizationId: string;
  projectId?: string | null;
  semanticModelId: string;
  query: SemanticQueryPayload;
}

export interface SemanticQueryResponse {
  id: string;
  organizationId: string;
  projectId?: string | null;
  semanticModelId: string;
  data: Array<Record<string, unknown>>;
  annotations: Array<Record<string, unknown>>;
  metadata?: Array<Record<string, unknown>>;
}

export interface SemanticQueryJobResponse {
  jobId: string;
  jobStatus: string;
}

export interface UnifiedSemanticSourceModelPayload {
  id: string;
  alias: string;
  name?: string | null;
  description?: string | null;
}

export interface UnifiedSemanticRelationshipPayload {
  name?: string | null;
  sourceSemanticModelId: string;
  sourceField: string;
  targetSemanticModelId: string;
  targetField: string;
  operator?: string | null;
  relationshipType?: string;
}

export interface UnifiedSemanticMetricPayload {
  expression: string;
  description?: string | null;
}

export interface UnifiedSemanticQueryRequestPayload {
  organizationId: string;
  projectId?: string | null;
  connectorId?: string | null;
  semanticModelIds: string[];
  sourceModels?: UnifiedSemanticSourceModelPayload[];
  relationships?: UnifiedSemanticRelationshipPayload[];
  metrics?: Record<string, UnifiedSemanticMetricPayload>;
  query: SemanticQueryPayload;
}

export interface UnifiedSemanticQueryMetaRequestPayload {
  organizationId: string;
  projectId?: string | null;
  connectorId?: string | null;
  semanticModelIds: string[];
  sourceModels?: UnifiedSemanticSourceModelPayload[];
  relationships?: UnifiedSemanticRelationshipPayload[];
  metrics?: Record<string, UnifiedSemanticMetricPayload>;
}

export interface UnifiedSemanticQueryMetaResponse {
  connectorId?: string | null;
  organizationId: string;
  projectId?: string | null;
  semanticModelIds: string[];
  semanticModel: SemanticModelPayload;
}

export interface UnifiedSemanticQueryResponse {
  id: string;
  organizationId: string;
  projectId?: string | null;
  connectorId: string;
  semanticModelIds: string[];
  data: Array<Record<string, unknown>>;
  annotations: Array<Record<string, unknown>>;
  metadata?: Array<Record<string, unknown>>;
}
