import type { ConnectorResponse } from '@/orchestration/connectors/types';
import type { DatasetCatalogItem } from '@/orchestration/datasets/types';

import {
  parseFederatedDirectiveBindings,
  supportsStructuredFederatedDataset,
} from './federation';
import type {
  SqlSavedQueryRecord,
  SqlSelectedDataset,
  SqlWorkbenchMode,
} from './types';

const DIRECT_SQL_CONNECTOR_TYPES = new Set([
  'SQLSERVER',
  'MSSQL',
  'POSTGRES',
  'MYSQL',
  'MARIADB',
  'SNOWFLAKE',
  'REDSHIFT',
  'BIGQUERY',
  'ORACLE',
  'SQLITE',
]);

export function isDirectSqlConnector(connector: ConnectorResponse): boolean {
  const family = String(connector.pluginMetadata?.connectorFamily || '').toUpperCase();
  const connectorType = String(connector.connectorType || '').toUpperCase();
  return family === 'DATABASE' && DIRECT_SQL_CONNECTOR_TYPES.has(connectorType);
}

export function getDirectSqlConnectors(connectors: ConnectorResponse[]): ConnectorResponse[] {
  return connectors.filter((connector) => isDirectSqlConnector(connector));
}

export function getDatasetWorkbenchCatalogItems(
  datasets: DatasetCatalogItem[],
): DatasetCatalogItem[] {
  return datasets.filter((dataset) => supportsStructuredFederatedDataset(dataset));
}

export function groupDatasetWorkbenchItems(datasets: DatasetCatalogItem[]): Array<{
  key: string;
  label: string;
  items: DatasetCatalogItem[];
}> {
  const groups = new Map<string, DatasetCatalogItem[]>();
  datasets.forEach((dataset) => {
    const sourceKind = String(dataset.sourceKind || 'dataset');
    const connectorKind = String(dataset.connectorKind || '').trim().toLowerCase();
    const key = connectorKind || sourceKind;
    const items = groups.get(key) || [];
    items.push(dataset);
    groups.set(key, items);
  });
  return Array.from(groups.entries())
    .map(([key, items]) => ({
      key,
      label: key.replaceAll('_', ' '),
      items: [...items].sort((left, right) => left.name.localeCompare(right.name)),
    }))
    .sort((left, right) => left.label.localeCompare(right.label));
}

export function inferSavedQueryWorkbenchMode(saved: SqlSavedQueryRecord): SqlWorkbenchMode {
  if (saved.workbenchMode === 'dataset' || saved.workbenchMode === 'direct_sql') {
    return saved.workbenchMode;
  }
  const selectedDatasets = resolveSavedQuerySelectedDatasets(saved);
  return selectedDatasets.length > 0 ? 'dataset' : 'direct_sql';
}

export function resolveSavedQuerySelectedDatasets(
  saved: Pick<SqlSavedQueryRecord, 'query' | 'selectedDatasets'>,
): SqlSelectedDataset[] {
  if (saved.selectedDatasets.length > 0) {
    return saved.selectedDatasets;
  }
  return parseFederatedDirectiveBindings(saved.query).map((binding) => ({
    alias: binding.alias,
    sqlAlias: binding.alias,
    datasetId: binding.datasetId,
  }));
}
