import { describe, expect, it } from 'vitest';

import {
  injectFederatedDirectives,
  parseFederatedDirectiveBindings,
  shouldDefaultFederatedMode,
} from '@/orchestration/sql/federation';

describe('sql federation helpers', () => {
  it('defaults federated mode on when a structured federatable dataset exists', () => {
    expect(
      shouldDefaultFederatedMode({
        allowFederation: true,
        datasets: [
          {
            id: 'dataset-1',
            name: 'shopify_orders',
            datasetType: 'FILE',
            sourceKind: 'saas',
            connectorKind: 'shopify',
            storageKind: 'parquet',
            relationIdentity: {
              canonicalReference: 'dataset:dataset-1',
              relationName: 'shopify_orders',
              sourceKind: 'saas',
              storageKind: 'parquet',
            },
            executionCapabilities: {
              supportsStructuredScan: true,
              supportsSqlFederation: true,
              supportsFilterPushdown: true,
              supportsProjectionPushdown: true,
              supportsAggregationPushdown: true,
              supportsJoinPushdown: false,
              supportsMaterialization: true,
              supportsSemanticModeling: true,
            },
            tags: [],
            columns: [],
            updatedAt: '2026-03-08T00:00:00Z',
          },
        ],
      }),
    ).toBe(true);
  });

  it('round-trips federated dataset directives in saved query text', () => {
    const query = injectFederatedDirectives('SELECT * FROM shop.orders', [
      { alias: 'shop', datasetId: 'dataset-1' },
      { alias: 'crm', datasetId: 'dataset-2' },
    ]);

    expect(parseFederatedDirectiveBindings(query)).toEqual([
      { alias: 'shop', datasetId: 'dataset-1' },
      { alias: 'crm', datasetId: 'dataset-2' },
    ]);
  });
});
