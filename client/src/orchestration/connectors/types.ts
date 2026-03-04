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
