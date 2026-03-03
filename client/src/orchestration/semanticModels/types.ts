export interface SemanticDimension {
  name: string;
  expression?: string | null;
  type: string;
  primaryKey?: boolean;
  alias?: string | null;
  description?: string | null;
  synonyms?: string[] | null;
  vectorized?: boolean;
}

export interface SemanticMeasure {
  name: string;
  expression: string | null;
  type: string;
  description?: string | null;
  aggregation?: string | null;
  synonyms?: string[] | null;
}

export interface SemanticFilter {
  condition: string;
  description?: string | null;
  synonyms?: string[] | null;
}

export interface SemanticTable {
  schema: string;
  name: string;
  description?: string | null;
  synonyms?: string[] | null;
  dimensions?: SemanticDimension[] | null;
  measures?: SemanticMeasure[] | null;
  filters?: Record<string, SemanticFilter> | null;
}

export interface SemanticRelationship {
  name: string;
  from: string;
  to: string;
  type: string;
  joinOn: string;
}

export interface SemanticMetric {
  description?: string | null;
  expression: string;
}

export interface SemanticModel {
  version: string;
  connector?: string | null;
  description?: string | null;
  tables: Record<string, SemanticTable>;
  relationships?: SemanticRelationship[] | null;
  metrics?: Record<string, SemanticMetric> | null;
}

export interface SemanticModelRecord {
  id: string;
  organizationId: string;
  projectId?: string | null;
  connectorId: string;
  name: string;
  description?: string | null;
  contentYaml: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateSemanticModelPayload {
  organizationId: string;
  projectId?: string | null;
  connectorId: string;
  name: string;
  description?: string;
  autoGenerate?: boolean;
  modelYaml?: string;
}

export interface UpdateSemanticModelPayload {
  projectId?: string | null;
  connectorId?: string;
  name?: string;
  description?: string;
  autoGenerate?: boolean;
  modelYaml?: string;
}

export interface SemanticModelCatalogColumn {
  name: string;
  type: string;
  nullable?: boolean | null;
  primaryKey?: boolean;
}

export interface SemanticModelCatalogTable {
  schema: string;
  name: string;
  fullyQualifiedName: string;
  columns: SemanticModelCatalogColumn[];
}

export interface SemanticModelCatalogSchema {
  name: string;
  tables: SemanticModelCatalogTable[];
}

export interface SemanticModelCatalogResponse {
  connectorId: string;
  schemas: SemanticModelCatalogSchema[];
  tableCount: number;
  columnCount: number;
}

export interface SemanticModelSelectionGeneratePayload {
  connectorId: string;
  selectedTables: string[];
  selectedColumns: Record<string, string[]>;
  includeSampleValues?: boolean;
  description?: string;
}

export interface SemanticModelSelectionGenerateResponse {
  yamlText: string;
  warnings: string[];
}

export interface SemanticModelAgenticJobCreatePayload {
  connectorId: string;
  projectId?: string | null;
  name: string;
  description?: string;
  filename?: string;
  selectedTables: string[];
  selectedColumns: Record<string, string[]>;
  questionPrompts: string[];
  includeSampleValues?: boolean;
}

export interface SemanticModelAgenticJobCreateResponse {
  jobId: string;
  jobStatus: string;
  semanticModelId: string;
}

export type SemanticModelKind = 'all' | 'standard' | 'unified';
