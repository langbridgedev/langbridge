import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import DataConnectionsPage from './page';

const mockPush = vi.fn();
const mockCreateConnector = vi.fn();
const mockFetchConnectorCatalog = vi.fn();
const mockFetchConnectorSchema = vi.fn();
const mockFetchConnectorTypes = vi.fn();
const mockFetchOrganizationEnvironmentSettings = vi.fn();
const mockSetOrganizationEnvironmentSetting = vi.fn();
const mockBulkCreateDatasets = vi.fn();
const mockFetchAgentJobState = vi.fn();

vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: mockPush,
  }),
}));

vi.mock('@/context/workspaceScope', () => ({
  useWorkspaceScope: () => ({
    organizations: [
      {
        id: 'org-1',
        name: 'Primary Org',
        projects: [{ id: 'proj-1', name: 'Analytics' }],
      },
    ],
    loading: false,
    selectedOrganizationId: 'org-1',
    selectedProjectId: 'proj-1',
    setSelectedOrganizationId: vi.fn(),
  }),
}));

vi.mock('@/orchestration/connectors', () => ({
  createConnector: (...args: unknown[]) => mockCreateConnector(...args),
  fetchConnectorCatalog: (...args: unknown[]) => mockFetchConnectorCatalog(...args),
  fetchConnectorSchema: (...args: unknown[]) => mockFetchConnectorSchema(...args),
  fetchConnectorTypes: (...args: unknown[]) => mockFetchConnectorTypes(...args),
}));

vi.mock('@/orchestration/organizations', () => ({
  fetchOrganizationEnvironmentSettings: (...args: unknown[]) =>
    mockFetchOrganizationEnvironmentSettings(...args),
  setOrganizationEnvironmentSetting: (...args: unknown[]) =>
    mockSetOrganizationEnvironmentSetting(...args),
}));

vi.mock('@/orchestration/datasets', () => ({
  bulkCreateDatasets: (...args: unknown[]) => mockBulkCreateDatasets(...args),
}));

vi.mock('@/orchestration/jobs', () => ({
  fetchAgentJobState: (...args: unknown[]) => mockFetchAgentJobState(...args),
}));

describe('DataConnectionsPage dataset bootstrap flow', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    mockFetchConnectorTypes.mockResolvedValue(['POSTGRES']);
    mockFetchConnectorSchema.mockResolvedValue({
      name: 'Postgres connector',
      description: 'Connect to Postgres.',
      version: '1',
      label: 'Postgres',
      icon: 'database',
      connectorType: 'POSTGRES',
      config: [],
    });
    mockCreateConnector.mockResolvedValue({
      id: 'conn-1',
      name: 'Postgres connector',
      connectorType: 'POSTGRES',
      organizationId: 'org-1',
      projectId: 'proj-1',
    });
    mockFetchOrganizationEnvironmentSettings.mockResolvedValue([]);
    mockSetOrganizationEnvironmentSetting.mockResolvedValue(undefined);
    mockFetchConnectorCatalog.mockResolvedValue({
      connectorId: 'conn-1',
      schemas: [
        {
          name: 'public',
          tables: [
            {
              schema: 'public',
              name: 'orders',
              fullyQualifiedName: 'public.orders',
              columns: [
                { name: 'order_id', type: 'integer', nullable: false },
                { name: 'amount', type: 'decimal', nullable: false },
              ],
            },
          ],
        },
      ],
      schemaCount: 1,
      tableCount: 1,
      columnCount: 2,
      offset: 0,
      limit: 1000,
      hasMore: false,
    });
    mockBulkCreateDatasets.mockResolvedValue({ jobId: 'job-1', jobStatus: 'queued' });
    mockFetchAgentJobState.mockResolvedValue({
      status: 'succeeded',
      progress: 100,
      finalResponse: { result: { created_count: 1, reused_count: 0, items: [] } },
    });
  });

  async function createConnectorAndOpenModal(): Promise<void> {
    render(<DataConnectionsPage params={{ organizationId: 'org-1' }} />);
    const user = userEvent.setup();

    const connectorCard = await screen.findByRole('button', { name: /postgres/i });
    await user.click(connectorCard);

    const createButton = await screen.findByRole('button', { name: /Create connector/i });
    await user.click(createButton);

    await screen.findByRole('heading', { name: /Connection added/i });
  }

  it('shows post-connection dataset modal after connector create', async () => {
    await createConnectorAndOpenModal();
    expect(
      screen.getByText(/Would you like to create datasets from this connection\?/i),
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Guided dataset generation/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Auto-generate all datasets/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Skip for now/i })).toBeInTheDocument();
  });

  it('navigates dataset wizard steps from prompt continue', async () => {
    await createConnectorAndOpenModal();
    const user = userEvent.setup();

    await user.click(screen.getByRole('button', { name: /^Continue$/i }));

    await screen.findByRole('heading', { name: /Create datasets from connection/i });
    await waitFor(() => {
      expect(screen.getByText('1. Scope')).toBeInTheDocument();
    });

    await user.click(screen.getByRole('button', { name: /^Next$/i }));
    await waitFor(() => {
      expect(screen.getByText('2. Select tables')).toBeInTheDocument();
    });

    await user.click(screen.getByRole('button', { name: /^Next$/i }));
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /Select all columns/i })).toBeInTheDocument();
    });

    await user.click(screen.getByRole('button', { name: /^Next$/i }));
    await waitFor(() => {
      expect(screen.getByText(/Naming template/i)).toBeInTheDocument();
    });
  });
});
