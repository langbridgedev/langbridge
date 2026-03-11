import { describe, expect, it } from 'vitest';

import type { ConnectorResponse } from '@/orchestration/connectors/types';
import type { DatasetCatalogItem } from '@/orchestration/datasets/types';

import {
  getDatasetWorkbenchCatalogItems,
  getDirectSqlConnectors,
  inferSavedQueryWorkbenchMode,
  resolveSavedQuerySelectedDatasets,
} from './workbench';

function dataset(overrides: Partial<DatasetCatalogItem>): DatasetCatalogItem {
  return {
    id: 'dataset-1',
    name: 'orders',
    sqlAlias: 'orders',
    datasetType: 'TABLE',
    sourceKind: 'database',
    connectorKind: 'postgres',
    storageKind: 'table',
    relationIdentity: {
      canonicalReference: 'public.orders',
      relationName: 'orders',
      sourceKind: 'database',
      storageKind: 'table',
    },
    executionCapabilities: {
      supportsStructuredScan: true,
      supportsSqlFederation: true,
      supportsFilterPushdown: true,
      supportsProjectionPushdown: true,
      supportsAggregationPushdown: true,
      supportsJoinPushdown: true,
      supportsMaterialization: true,
      supportsSemanticModeling: true,
    },
    tags: [],
    columns: [],
    updatedAt: '2026-03-11T00:00:00Z',
    ...overrides,
  };
}

function connector(overrides: Partial<ConnectorResponse>): ConnectorResponse {
  return {
    id: 'connector-1',
    name: 'Warehouse',
    connectorType: 'POSTGRES',
    pluginMetadata: {
      connectorFamily: 'DATABASE',
    },
    ...overrides,
  };
}

describe('sql workbench helpers', () => {
  it('filters dataset catalog to structured federation datasets', () => {
    const items = getDatasetWorkbenchCatalogItems([
      dataset({ id: 'a' }),
      dataset({
        id: 'b',
        datasetType: 'FEDERATED',
        storageKind: 'virtual',
        executionCapabilities: {
          supportsStructuredScan: false,
          supportsSqlFederation: false,
          supportsFilterPushdown: false,
          supportsProjectionPushdown: false,
          supportsAggregationPushdown: false,
          supportsJoinPushdown: false,
          supportsMaterialization: false,
          supportsSemanticModeling: false,
        },
      }),
    ]);

    expect(items.map((item) => item.id)).toEqual(['a']);
  });

  it('filters direct SQL connectors to real SQL databases', () => {
    const items = getDirectSqlConnectors([
      connector({ id: 'postgres', connectorType: 'POSTGRES' }),
      connector({ id: 'shopify', connectorType: 'SHOPIFY', pluginMetadata: { connectorFamily: 'API' } }),
      connector({ id: 'mongo', connectorType: 'MONGODB' }),
    ]);

    expect(items.map((item) => item.id)).toEqual(['postgres']);
  });

  it('prefers explicit saved query dataset metadata and falls back to legacy directives', () => {
    const explicit = resolveSavedQuerySelectedDatasets({
      query: 'SELECT * FROM shop.public.orders',
      selectedDatasets: [{ alias: 'shop', sqlAlias: 'dataset_1', datasetId: 'dataset-1', datasetName: 'Orders' }],
    });
    const legacy = resolveSavedQuerySelectedDatasets({
      query: '-- langbridge:federated-source alias=crm dataset_id=dataset-9\nSELECT * FROM crm.public.accounts',
      selectedDatasets: [],
    });

    expect(explicit).toEqual([{ alias: 'shop', sqlAlias: 'dataset_1', datasetId: 'dataset-1', datasetName: 'Orders' }]);
    expect(legacy).toEqual([{ alias: 'crm', sqlAlias: 'crm', datasetId: 'dataset-9' }]);
  });

  it('infers saved query mode from explicit or legacy dataset selections', () => {
    expect(
      inferSavedQueryWorkbenchMode({
        id: 'saved-1',
        workspaceId: 'workspace-1',
        createdBy: 'user-1',
        updatedBy: 'user-1',
        workbenchMode: 'dataset',
        connectionId: null,
        selectedDatasets: [{ alias: 'shop', sqlAlias: 'dataset_1', datasetId: 'dataset-1' }],
        name: 'Dataset query',
        query: 'SELECT * FROM shop.public.orders',
        queryHash: 'hash',
        tags: [],
        defaultParams: {},
        isShared: false,
        createdAt: '2026-03-11T00:00:00Z',
        updatedAt: '2026-03-11T00:00:00Z',
      },
    )).toBe('dataset');

    expect(
      inferSavedQueryWorkbenchMode({
        id: 'saved-2',
        workspaceId: 'workspace-1',
        createdBy: 'user-1',
        updatedBy: 'user-1',
        workbenchMode: 'direct_sql',
        connectionId: 'connector-1',
        selectedDatasets: [],
        name: 'Direct query',
        query: 'SELECT * FROM public.orders',
        queryHash: 'hash',
        tags: [],
        defaultParams: {},
        isShared: false,
        createdAt: '2026-03-11T00:00:00Z',
        updatedAt: '2026-03-11T00:00:00Z',
      },
    )).toBe('direct_sql');
  });
});
