import { describe, expect, it } from 'vitest';

import {
  buildUnifiedModelPersistencePayload,
  collectUnifiedSourceDatasetIds,
} from './page';

describe('Unified semantic model persistence helpers', () => {
  it('collects unique source dataset ids from selected semantic models', () => {
    expect(
      collectUnifiedSourceDatasetIds([
        {
          id: 'model-1',
          organizationId: 'org-1',
          name: 'Sales',
          contentYaml: '',
          createdAt: '',
          updatedAt: '',
          sourceDatasetIds: ['dataset-1', 'dataset-2'],
        },
        {
          id: 'model-2',
          organizationId: 'org-1',
          name: 'Marketing',
          contentYaml: '',
          createdAt: '',
          updatedAt: '',
          sourceDatasetIds: ['dataset-2', 'dataset-3'],
        },
      ]),
    ).toEqual(['dataset-1', 'dataset-2', 'dataset-3']);
  });

  it('builds a save payload without requiring connector lineage', () => {
    expect(
      buildUnifiedModelPersistencePayload({
        selectedUnifiedModel: null,
        selectedSourceModels: [
          {
            id: 'model-1',
            organizationId: 'org-1',
            name: 'Sales',
            contentYaml: '',
            createdAt: '',
            updatedAt: '',
            connectorId: null,
            sourceDatasetIds: ['dataset-1'],
          },
          {
            id: 'model-2',
            organizationId: 'org-1',
            name: 'Marketing',
            contentYaml: '',
            createdAt: '',
            updatedAt: '',
            sourceDatasetIds: ['dataset-2'],
          },
        ],
        selectedProjectId: 'proj-1',
        formState: {
          name: '',
          description: 'Cross-domain metrics',
          version: '1.0',
        },
        modelYaml: 'name: unified',
        payloadName: 'unified',
      }),
    ).toEqual({
      projectId: 'proj-1',
      connectorId: undefined,
      name: 'unified',
      description: 'Cross-domain metrics',
      modelYaml: 'name: unified',
      autoGenerate: false,
      sourceDatasetIds: ['dataset-1', 'dataset-2'],
    });
  });
});
