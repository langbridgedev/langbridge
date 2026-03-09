'use client';

import { useCallback, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Activity,
  Bot,
  BrainCircuit,
  Database,
  FileCode2,
  LineChart,
  MessageSquareText,
  RefreshCw,
  Sparkles,
  Table2,
  Workflow,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { formatRelativeDate } from '@/lib/utils';
import {
  fetchConnectors,
  fetchConnectorSyncHistory,
  type ConnectorResponse,
  type ConnectorSyncHistoryItem,
} from '@/orchestration/connectors';
import { listDashboards, type DashboardRecord } from '@/orchestration/dashboards';
import { listDatasets, type DatasetListResponse } from '@/orchestration/datasets';
import { ApiError } from '@/orchestration/http';
import { fetchAgentDefinitions, type AgentDefinition } from '@/orchestration/agents';
import { listSemanticModels, type SemanticModelRecord } from '@/orchestration/semanticModels';
import { fetchSqlHistory, type SqlJobRecord } from '@/orchestration/sql';
import { listThreads, type Thread } from '@/orchestration/threads';

import type {
  DashboardActivityItem,
  DashboardEntryCard,
  DashboardExecutionItem,
  DashboardExecutionSummaryMetric,
  DashboardOnboardingStep,
  DashboardOverviewMetric,
  DashboardQuickAction,
  DashboardStatusTone,
} from '../types';
import { EntryCardGrid } from './EntryCardGrid';
import { ExecutionStatusPanel } from './ExecutionStatusPanel';
import { QuickActionPanel } from './QuickActionPanel';
import { RecentActivityPanel } from './RecentActivityPanel';
import { WorkspaceOnboardingPanel } from './WorkspaceOnboardingPanel';
import { WorkspaceOverview } from './WorkspaceOverview';

const ACTIVE_SQL_STATUSES = new Set(['queued', 'running', 'awaiting_approval']);
const ACTIVE_SYNC_STATUSES = new Set(['running']);
const TERMINAL_SYNC_STATUSES = new Set(['succeeded', 'failed']);

type SqlHistoryFeed = {
  items: SqlJobRecord[];
  scope: 'workspace' | 'user';
};

type ConnectorExecutionRecord = ConnectorSyncHistoryItem & {
  connectorId: string;
  connectorName: string;
};

function formatCount(value: number): string {
  return value.toLocaleString();
}

function formatStatus(value: string): string {
  return value
    .split(/[_-]/g)
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ');
}

function toTimestamp(...values: Array<string | null | undefined>): number {
  for (const value of values) {
    if (!value) {
      continue;
    }
    const parsed = new Date(value).getTime();
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return 0;
}

function formatTimestamp(value?: string | null): string {
  if (!value) {
    return 'No recent activity';
  }
  return formatRelativeDate(value) || new Date(value).toLocaleString();
}

function resolveStatusTone(status: string): DashboardStatusTone {
  if (status === 'succeeded' || status === 'connected' || status === 'published' || status === 'ready') {
    return 'success';
  }
  if (status === 'failed' || status === 'cancelled' || status === 'error') {
    return 'destructive';
  }
  if (status === 'awaiting_approval' || status === 'pending') {
    return 'warning';
  }
  return 'secondary';
}

function hasConnectorId(connector: ConnectorResponse): connector is ConnectorResponse & { id: string } {
  return typeof connector.id === 'string' && connector.id.length > 0;
}

export function DashboardCards() {
  const {
    selectedOrganizationId,
    selectedOrganization,
    selectedProject,
    selectedProjectId,
  } = useWorkspaceScope();

  const connectionsBasePath = selectedOrganizationId ? `/datasources/${selectedOrganizationId}` : '/datasources';
  const datasetsBasePath = selectedOrganizationId ? `/datasets/${selectedOrganizationId}` : '/datasets';
  const semanticModelsBasePath = selectedOrganizationId
    ? `/semantic-model/${selectedOrganizationId}`
    : '/semantic-model';
  const sqlBasePath = selectedOrganizationId ? `/sql/${selectedOrganizationId}` : '/sql';
  const biBasePath = selectedOrganizationId ? `/bi/${selectedOrganizationId}` : '/bi';
  const agentsBasePath = selectedOrganizationId ? `/agents/${selectedOrganizationId}/definitions` : '/agents';
  const threadsBasePath = selectedOrganizationId ? `/chat/${selectedOrganizationId}` : '/chat';

  const connectorsQuery = useQuery<ConnectorResponse[]>({
    queryKey: ['dashboard-connectors', selectedOrganizationId],
    enabled: Boolean(selectedOrganizationId),
    queryFn: () => fetchConnectors(selectedOrganizationId || ''),
  });

  const datasetsQuery = useQuery<DatasetListResponse>({
    queryKey: ['dashboard-datasets', selectedOrganizationId, selectedProjectId],
    enabled: Boolean(selectedOrganizationId),
    queryFn: () =>
      listDatasets(selectedOrganizationId || '', {
        projectId: selectedProjectId || undefined,
      }),
  });

  const semanticModelsQuery = useQuery<SemanticModelRecord[]>({
    queryKey: ['dashboard-semantic-models', selectedOrganizationId, selectedProjectId],
    enabled: Boolean(selectedOrganizationId),
    queryFn: () => listSemanticModels(selectedOrganizationId || '', selectedProjectId || undefined, 'all'),
  });

  const agentDefinitionsQuery = useQuery<AgentDefinition[]>({
    queryKey: ['dashboard-agent-definitions', selectedOrganizationId],
    enabled: Boolean(selectedOrganizationId),
    queryFn: () => fetchAgentDefinitions(selectedOrganizationId || ''),
  });

  const threadsQuery = useQuery<Thread[]>({
    queryKey: ['dashboard-threads', selectedOrganizationId],
    enabled: Boolean(selectedOrganizationId),
    queryFn: () => listThreads(selectedOrganizationId || ''),
  });

  const dashboardsQuery = useQuery<DashboardRecord[]>({
    queryKey: ['dashboard-bi-dashboards', selectedOrganizationId, selectedProjectId],
    enabled: Boolean(selectedOrganizationId),
    queryFn: () => listDashboards(selectedOrganizationId || '', selectedProjectId || undefined),
  });

  const sqlHistoryQuery = useQuery<SqlHistoryFeed>({
    queryKey: ['dashboard-sql-history', selectedOrganizationId],
    enabled: Boolean(selectedOrganizationId),
    refetchInterval: 15000,
    queryFn: async () => {
      if (!selectedOrganizationId) {
        return { items: [], scope: 'user' as const };
      }
      try {
        const history = await fetchSqlHistory(selectedOrganizationId, 'workspace', 12);
        return { items: history.items, scope: 'workspace' as const };
      } catch (error) {
        if (error instanceof ApiError && error.status === 403) {
          const history = await fetchSqlHistory(selectedOrganizationId, 'user', 12);
          return { items: history.items, scope: 'user' as const };
        }
        throw error;
      }
    },
  });

  const connectorSyncHistoryQuery = useQuery<ConnectorExecutionRecord[]>({
    queryKey: [
      'dashboard-connector-sync-history',
      selectedOrganizationId,
      (connectorsQuery.data ?? []).filter(hasConnectorId).map((connector) => connector.id).join(','),
    ],
    enabled: Boolean(selectedOrganizationId) && (connectorsQuery.data ?? []).some(hasConnectorId),
    refetchInterval: 15000,
    queryFn: async () => {
      if (!selectedOrganizationId) {
        return [];
      }

      const connectors = (connectorsQuery.data ?? []).filter(hasConnectorId);
      const responses = await Promise.allSettled(
        connectors.map(async (connector) => {
          const history = await fetchConnectorSyncHistory(selectedOrganizationId, connector.id, 3);
          return history.items.map((item) => ({
            ...item,
            connectorId: connector.id,
            connectorName: connector.name,
          }));
        }),
      );

      return responses
        .flatMap((result) => (result.status === 'fulfilled' ? result.value : []))
        .sort((left, right) =>
          toTimestamp(right.finishedAt, right.startedAt, right.createdAt)
          - toTimestamp(left.finishedAt, left.startedAt, left.createdAt),
        );
    },
  });

  const handleRefreshAll = useCallback(async () => {
    await Promise.allSettled([
      connectorsQuery.refetch(),
      datasetsQuery.refetch(),
      semanticModelsQuery.refetch(),
      agentDefinitionsQuery.refetch(),
      threadsQuery.refetch(),
      dashboardsQuery.refetch(),
      sqlHistoryQuery.refetch(),
      connectorSyncHistoryQuery.refetch(),
    ]);
  }, [
    agentDefinitionsQuery,
    connectorSyncHistoryQuery,
    connectorsQuery,
    dashboardsQuery,
    datasetsQuery,
    semanticModelsQuery,
    sqlHistoryQuery,
    threadsQuery,
  ]);

  const connectionCount = connectorsQuery.data?.length ?? 0;
  const datasetCount = datasetsQuery.data?.total ?? 0;
  const semanticModelCount = semanticModelsQuery.data?.length ?? 0;
  const agentCount = agentDefinitionsQuery.data?.length ?? 0;
  const threadCount = threadsQuery.data?.length ?? 0;
  const dashboardCount = dashboardsQuery.data?.length ?? 0;
  const recentQueryCount = sqlHistoryQuery.data?.items.length ?? 0;
  const federatedDatasetCount =
    datasetsQuery.data?.items.filter((dataset) => dataset.executionCapabilities.supportsSqlFederation).length ?? 0;

  const activeSqlJobs = useMemo(
    () => (sqlHistoryQuery.data?.items ?? []).filter((item) => ACTIVE_SQL_STATUSES.has(item.status)),
    [sqlHistoryQuery.data?.items],
  );
  const pendingApprovals = useMemo(
    () => activeSqlJobs.filter((item) => item.status === 'awaiting_approval').length,
    [activeSqlJobs],
  );
  const activeSyncJobs = useMemo(
    () => (connectorSyncHistoryQuery.data ?? []).filter((item) => ACTIVE_SYNC_STATUSES.has(item.status)),
    [connectorSyncHistoryQuery.data],
  );

  const statusTone: DashboardStatusTone = !selectedOrganizationId
    ? 'secondary'
    : connectorsQuery.isError
        || datasetsQuery.isError
        || semanticModelsQuery.isError
        || agentDefinitionsQuery.isError
        || sqlHistoryQuery.isError
      ? 'warning'
      : activeSqlJobs.length > 0 || activeSyncJobs.length > 0
        ? 'success'
        : connectionCount === 0 && datasetCount === 0 && semanticModelCount === 0 && agentCount === 0
          ? 'warning'
          : 'secondary';

  const statusLabel = !selectedOrganizationId
    ? 'Select scope'
    : activeSqlJobs.length > 0 || activeSyncJobs.length > 0
      ? 'Live'
      : connectionCount === 0 && datasetCount === 0 && semanticModelCount === 0 && agentCount === 0
        ? 'Onboarding'
        : 'Operational';

  const statusDescription = !selectedOrganizationId
    ? 'Pick an organization to load live workspace state.'
    : activeSqlJobs.length > 0 || activeSyncJobs.length > 0
      ? 'Queries or sync jobs are running across the active workspace scope.'
      : connectionCount === 0 && datasetCount === 0 && semanticModelCount === 0 && agentCount === 0
        ? 'The workspace is ready for its first connection, dataset, and semantic model.'
        : 'The workspace has active assets and is ready for the next analysis step.';

  const quickActions: DashboardQuickAction[] = [
    {
      href: selectedOrganizationId ? `${connectionsBasePath}/create` : '/datasources/create',
      label: 'Add data source',
      description: 'Register a warehouse, SaaS API, or file-backed connector.',
      icon: Database,
      emphasis: 'primary',
    },
    {
      href: selectedOrganizationId ? `${datasetsBasePath}?create=1` : '/datasets',
      label: 'Create dataset',
      description: 'Publish a governed table, SQL, or file dataset.',
      icon: Table2,
    },
    {
      href: selectedOrganizationId ? `${semanticModelsBasePath}/create` : '/semantic-model/create',
      label: 'Build semantic model',
      description: 'Define metrics, joins, and business logic for reuse.',
      icon: BrainCircuit,
    },
    {
      href: sqlBasePath,
      label: 'Open SQL workspace',
      description: 'Run native and federated SQL with history and guardrails.',
      icon: FileCode2,
    },
    {
      href: biBasePath,
      label: 'Launch BI Studio',
      description: 'Turn semantic queries into dashboards and visual layouts.',
      icon: LineChart,
    },
    {
      href: selectedOrganizationId ? `${agentsBasePath}/create` : '/agents/definitions/create',
      label: 'Create agent',
      description: 'Configure an analytics agent to work across your data.',
      icon: Bot,
    },
  ];

  const overviewMetrics: DashboardOverviewMetric[] = [
    {
      label: 'Data connections',
      value: connectorsQuery.isLoading ? '...' : formatCount(connectionCount),
      detail: connectionCount === 0 ? 'No sources registered yet.' : 'Connected sources ready to power datasets.',
      icon: Database,
    },
    {
      label: 'Datasets',
      value: datasetsQuery.isLoading ? '...' : formatCount(datasetCount),
      detail: datasetCount === 0 ? 'No governed datasets published yet.' : 'Published assets available to query and model.',
      icon: Table2,
    },
    {
      label: 'Semantic models',
      value: semanticModelsQuery.isLoading ? '...' : formatCount(semanticModelCount),
      detail:
        semanticModelCount === 0
          ? 'No semantic layers defined yet.'
          : 'Business logic and reusable metrics are available.',
      icon: BrainCircuit,
    },
    {
      label: 'Agents',
      value: agentDefinitionsQuery.isLoading ? '...' : formatCount(agentCount),
      detail: agentCount === 0 ? 'No analytics agents configured yet.' : 'Agent definitions are ready to orchestrate.',
      icon: Bot,
    },
    {
      label: sqlHistoryQuery.data?.scope === 'workspace' ? 'Recent queries' : 'Your recent queries',
      value: sqlHistoryQuery.isLoading ? '...' : formatCount(recentQueryCount),
      detail:
        recentQueryCount === 0
          ? 'No SQL executions have been recorded yet.'
          : 'Recent execution history keeps the workspace feeling live.',
      icon: Activity,
    },
  ];

  const showOnboarding =
    selectedOrganizationId != null
    && selectedOrganizationId.length > 0
    && connectionCount === 0
    && datasetCount === 0
    && semanticModelCount === 0
    && agentCount === 0
    && threadCount === 0;

  const onboardingSteps: DashboardOnboardingStep[] = [
    {
      id: 'connections',
      href: selectedOrganizationId ? `${connectionsBasePath}/create` : '/datasources/create',
      title: 'Add a data connection',
      description: 'Bring in a database, SaaS API, or file-backed source.',
      completed: connectionCount > 0,
    },
    {
      id: 'datasets',
      href: selectedOrganizationId ? `${datasetsBasePath}?create=1` : '/datasets',
      title: 'Create a dataset',
      description: 'Publish a governed asset for SQL, federation, and BI.',
      completed: datasetCount > 0,
    },
    {
      id: 'semantic-models',
      href: selectedOrganizationId ? `${semanticModelsBasePath}/create` : '/semantic-model/create',
      title: 'Define a semantic model',
      description: 'Capture joins, measures, and business terminology once.',
      completed: semanticModelCount > 0,
    },
    {
      id: 'agents',
      href: threadsBasePath,
      title: 'Launch an agent workflow',
      description: 'Start a thread to investigate and orchestrate analytics work.',
      completed: threadCount > 0,
    },
  ];

  const recentActivityItems = useMemo<DashboardActivityItem[]>(() => {
    if (!selectedOrganizationId) {
      return [];
    }

    const items: Array<DashboardActivityItem & { sortKey: number }> = [];

    for (const thread of threadsQuery.data ?? []) {
      items.push({
        id: `thread-${thread.id}`,
        href: `${threadsBasePath}/${thread.id}`,
        title: thread.title?.trim() || `Thread ${thread.id.slice(0, 8)}`,
        description: 'Resume an analytics investigation thread.',
        kindLabel: 'Thread',
        timestampLabel: formatTimestamp(thread.updatedAt ?? thread.createdAt),
        icon: MessageSquareText,
        statusLabel: formatStatus(thread.status),
        statusTone: resolveStatusTone(thread.status),
        sortKey: toTimestamp(thread.updatedAt, thread.createdAt),
      });
    }

    for (const dataset of datasetsQuery.data?.items ?? []) {
      items.push({
        id: `dataset-${dataset.id}`,
        href: `${datasetsBasePath}/${dataset.id}`,
        title: dataset.name,
        description: dataset.description || `${formatStatus(dataset.datasetType)} dataset ready for governed access.`,
        kindLabel: 'Dataset',
        timestampLabel: formatTimestamp(dataset.updatedAt),
        icon: Table2,
        statusLabel: formatStatus(dataset.status),
        statusTone: resolveStatusTone(dataset.status),
        sortKey: toTimestamp(dataset.updatedAt, dataset.createdAt),
      });
    }

    for (const model of semanticModelsQuery.data ?? []) {
      items.push({
        id: `semantic-model-${model.id}`,
        href: `${semanticModelsBasePath}/${model.id}`,
        title: model.name,
        description: model.description || 'Semantic model available for BI, agents, and governed query flows.',
        kindLabel: 'Semantic model',
        timestampLabel: formatTimestamp(model.updatedAt),
        icon: BrainCircuit,
        sortKey: toTimestamp(model.updatedAt, model.createdAt),
      });
    }

    for (const agent of agentDefinitionsQuery.data ?? []) {
      items.push({
        id: `agent-${agent.id}`,
        href: `${agentsBasePath}/${agent.id}`,
        title: agent.name,
        description: agent.description || 'Agent definition ready for operational analytics workflows.',
        kindLabel: 'Agent',
        timestampLabel: formatTimestamp(agent.updatedAt),
        icon: Bot,
        statusLabel: agent.isActive ? 'Active' : 'Inactive',
        statusTone: agent.isActive ? 'success' : 'secondary',
        sortKey: toTimestamp(agent.updatedAt, agent.createdAt),
      });
    }

    for (const job of sqlHistoryQuery.data?.items ?? []) {
      items.push({
        id: `sql-job-${job.id}`,
        href: `${sqlBasePath}?jobId=${job.id}`,
        title: `SQL job ${job.id.slice(0, 8)}`,
        description: `${formatStatus(job.executionMode)} execution${job.isFederated ? ' across federated sources' : ''}.`,
        kindLabel: 'Query',
        timestampLabel: formatTimestamp(job.startedAt ?? job.createdAt),
        icon: FileCode2,
        statusLabel: formatStatus(job.status),
        statusTone: resolveStatusTone(job.status),
        sortKey: toTimestamp(job.finishedAt, job.startedAt, job.createdAt),
      });
    }

    return items.sort((left, right) => right.sortKey - left.sortKey).slice(0, 8);
  }, [
    agentDefinitionsQuery.data,
    agentsBasePath,
    datasetsBasePath,
    datasetsQuery.data?.items,
    semanticModelsBasePath,
    semanticModelsQuery.data,
    selectedOrganizationId,
    sqlBasePath,
    sqlHistoryQuery.data?.items,
    threadsBasePath,
    threadsQuery.data,
  ]);

  const executionItems = useMemo<DashboardExecutionItem[]>(() => {
    if (!selectedOrganizationId) {
      return [];
    }

    const items: Array<DashboardExecutionItem & { sortKey: number; isActive: boolean }> = [
      ...activeSqlJobs.map((job) => ({
        id: `active-sql-${job.id}`,
        href: `${sqlBasePath}?jobId=${job.id}`,
        title: `SQL job ${job.id.slice(0, 8)}`,
        description: `${formatStatus(job.executionMode)} execution${job.bytesScanned ? ` - ${formatCount(job.bytesScanned)} bytes scanned` : ''}`,
        sourceLabel: 'SQL workspace',
        timestampLabel: formatTimestamp(job.startedAt ?? job.createdAt),
        statusLabel: formatStatus(job.status),
        statusTone: resolveStatusTone(job.status),
        progress: job.status === 'running' ? 60 : job.status === 'queued' ? 15 : job.status === 'awaiting_approval' ? 45 : null,
        sortKey: toTimestamp(job.finishedAt, job.startedAt, job.createdAt),
        isActive: true,
      })),
      ...activeSyncJobs.map((job) => ({
        id: `active-sync-${job.jobId}`,
        href: `${connectionsBasePath}/${job.connectorId}`,
        title: job.connectorName,
        description: job.statusMessage || 'Connector sync is running.',
        sourceLabel: 'Connector sync',
        timestampLabel: formatTimestamp(job.startedAt ?? job.createdAt),
        statusLabel: formatStatus(job.status),
        statusTone: resolveStatusTone(job.status),
        progress: job.progress,
        sortKey: toTimestamp(job.finishedAt, job.startedAt, job.createdAt),
        isActive: true,
      })),
    ];

    if (items.length >= 5) {
      return items.sort((left, right) => right.sortKey - left.sortKey).slice(0, 5);
    }

    const recentSqlFallback = (sqlHistoryQuery.data?.items ?? [])
      .filter((job) => !ACTIVE_SQL_STATUSES.has(job.status))
      .map((job) => ({
        id: `recent-sql-${job.id}`,
        href: `${sqlBasePath}?jobId=${job.id}`,
        title: `SQL job ${job.id.slice(0, 8)}`,
        description:
          job.durationMs != null
            ? `${formatStatus(job.executionMode)} execution completed in ${formatCount(job.durationMs)} ms.`
            : `${formatStatus(job.executionMode)} execution finished.`,
        sourceLabel: 'SQL workspace',
        timestampLabel: formatTimestamp(job.finishedAt ?? job.startedAt ?? job.createdAt),
        statusLabel: formatStatus(job.status),
        statusTone: resolveStatusTone(job.status),
        progress: null,
        sortKey: toTimestamp(job.finishedAt, job.startedAt, job.createdAt),
        isActive: false,
      }));

    const recentSyncFallback = (connectorSyncHistoryQuery.data ?? [])
      .filter((job) => TERMINAL_SYNC_STATUSES.has(job.status))
      .map((job) => ({
        id: `recent-sync-${job.jobId}`,
        href: `${connectionsBasePath}/${job.connectorId}`,
        title: job.connectorName,
        description: job.statusMessage || 'Connector sync completed.',
        sourceLabel: 'Connector sync',
        timestampLabel: formatTimestamp(job.finishedAt ?? job.startedAt ?? job.createdAt),
        statusLabel: formatStatus(job.status),
        statusTone: resolveStatusTone(job.status),
        progress: null,
        sortKey: toTimestamp(job.finishedAt, job.startedAt, job.createdAt),
        isActive: false,
      }));

    return [...items, ...recentSqlFallback, ...recentSyncFallback]
      .sort((left, right) => right.sortKey - left.sortKey)
      .slice(0, 5);
  }, [
    activeSqlJobs,
    activeSyncJobs,
    connectionsBasePath,
    connectorSyncHistoryQuery.data,
    selectedOrganizationId,
    sqlBasePath,
    sqlHistoryQuery.data?.items,
  ]);

  const executionSummary: DashboardExecutionSummaryMetric[] = [
    {
      label: 'Live SQL jobs',
      value: sqlHistoryQuery.isLoading ? '...' : formatCount(activeSqlJobs.length),
      detail: activeSqlJobs.length > 0 ? 'Running or queued in the SQL workspace.' : 'No active SQL work right now.',
    },
    {
      label: 'Connector syncs',
      value: connectorSyncHistoryQuery.isLoading ? '...' : formatCount(activeSyncJobs.length),
      detail:
        activeSyncJobs.length > 0
          ? 'Sync jobs are refreshing source data into datasets.'
          : 'No connector syncs are currently running.',
    },
    {
      label: 'Approvals needed',
      value: sqlHistoryQuery.isLoading ? '...' : formatCount(pendingApprovals),
      detail:
        pendingApprovals > 0
          ? 'Queries are waiting on execution approval.'
          : 'No SQL approvals are blocking work.',
    },
  ];

  const entryCards: DashboardEntryCard[] = [
    {
      href: connectionsBasePath,
      title: 'Data Connections',
      description: 'Manage warehouses, APIs, and file-backed sources that feed the workspace.',
      cta: 'Open connections',
      metric: `${formatCount(connectionCount)} registered`,
      icon: Database,
    },
    {
      href: datasetsBasePath,
      title: 'Datasets',
      description: 'Curate governed datasets for federation, semantic modeling, and operational analytics.',
      cta: 'Open datasets',
      metric: `${formatCount(datasetCount)} datasets`,
      icon: Table2,
    },
    {
      href: semanticModelsBasePath,
      title: 'Semantic Models',
      description: 'Define shared measures, joins, and business language once for every downstream surface.',
      cta: 'Open models',
      metric: `${formatCount(semanticModelCount)} models`,
      icon: BrainCircuit,
    },
    {
      href: sqlBasePath,
      title: 'SQL Workspace',
      description: 'Run native and federated SQL with history, policies, and reusable saved queries.',
      cta: 'Open SQL',
      metric: `${formatCount(recentQueryCount)} recent queries`,
      icon: FileCode2,
    },
    {
      href: biBasePath,
      title: 'BI Studio',
      description: 'Turn semantic models into operational dashboards and shared visual analysis.',
      cta: 'Launch BI Studio',
      metric: `${formatCount(dashboardCount)} dashboards`,
      icon: LineChart,
    },
    {
      href: agentsBasePath,
      title: 'Agents',
      description: 'Configure analytics agents and orchestrated workflows across structured and unstructured data.',
      cta: 'Open agents',
      metric: `${formatCount(agentCount)} agents`,
      icon: Bot,
    },
  ];

  const workspaceScopeLabel = selectedProject
    ? `${selectedOrganization?.name ?? 'Workspace'} / ${selectedProject.name}`
    : selectedOrganization?.name ?? 'Select a workspace scope';

  const isRefreshing = [
    connectorsQuery.isFetching,
    datasetsQuery.isFetching,
    semanticModelsQuery.isFetching,
    agentDefinitionsQuery.isFetching,
    threadsQuery.isFetching,
    dashboardsQuery.isFetching,
    sqlHistoryQuery.isFetching,
    connectorSyncHistoryQuery.isFetching,
  ].some(Boolean);

  return (
    <div className="relative flex min-h-full flex-col">
      <div className="pointer-events-none absolute inset-0 -z-10">
        <div className="absolute -left-24 top-0 h-72 w-72 rounded-full bg-[radial-gradient(circle,_var(--accent-soft),_transparent_70%)] blur-3xl" />
        <div className="absolute right-[-120px] top-16 h-80 w-80 rounded-full bg-[radial-gradient(circle,_rgba(56,189,248,0.14),_transparent_72%)] blur-3xl" />
      </div>

      <div className="space-y-6 px-6 pb-16 pt-8 text-[color:var(--text-secondary)] sm:px-10 lg:px-14">
        <section className="grid gap-4 xl:grid-cols-[minmax(0,1.55fr)_minmax(340px,0.95fr)]">
          <div className="surface-panel relative overflow-hidden rounded-[32px] p-8 shadow-soft">
            <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(16,163,127,0.18),transparent_42%),radial-gradient(circle_at_bottom_right,rgba(56,189,248,0.12),transparent_38%)]" />
            <div className="relative z-10 space-y-6">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="space-y-3">
                  <div className="inline-flex items-center gap-2 rounded-full border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-[color:var(--text-muted)]">
                    <Sparkles className="h-3.5 w-3.5 text-[color:var(--accent)]" aria-hidden="true" />
                    Command Center
                  </div>
                  <div className="space-y-2">
                    <p className="text-sm font-medium text-[color:var(--text-muted)]">{workspaceScopeLabel}</p>
                    <h2 className="text-3xl font-semibold text-[color:var(--text-primary)] sm:text-4xl">
                      Agentic analytics, operated as a workspace.
                    </h2>
                    <p className="max-w-3xl text-sm leading-7 text-[color:var(--text-secondary)] sm:text-base">
                      Connect data, create datasets, model shared semantics, run federated SQL, launch BI dashboards,
                      and orchestrate agents from a single operational surface.
                    </p>
                  </div>
                </div>

                <Button variant="outline" size="sm" onClick={() => void handleRefreshAll()} isLoading={isRefreshing}>
                  <RefreshCw className="h-4 w-4" aria-hidden="true" />
                  Refresh
                </Button>
              </div>

              <div className="flex flex-wrap gap-2 text-xs">
                <span className="rounded-full border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-3 py-1">
                  {formatCount(federatedDatasetCount)} federation-ready datasets
                </span>
                <span className="rounded-full border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-3 py-1">
                  {formatCount(dashboardCount)} dashboards
                </span>
                <span className="rounded-full border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-3 py-1">
                  {formatCount(threadCount)} investigation threads
                </span>
              </div>

              <QuickActionPanel actions={quickActions} />
            </div>
          </div>

          <WorkspaceOverview
            metrics={overviewMetrics}
            statusLabel={statusLabel}
            statusDescription={statusDescription}
            statusTone={statusTone}
          />
        </section>

        {showOnboarding ? <WorkspaceOnboardingPanel steps={onboardingSteps} /> : null}

        <section className="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(360px,0.9fr)]">
          <RecentActivityPanel items={recentActivityItems} />
          <ExecutionStatusPanel summary={executionSummary} items={executionItems} />
        </section>

        <section className="space-y-4">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
                Platform surfaces
              </p>
              <h2 className="mt-2 text-2xl font-semibold text-[color:var(--text-primary)]">
                Navigation entry points for the analytics stack
              </h2>
            </div>
            <div className="flex items-center gap-2 rounded-full border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-3 py-1 text-xs text-[color:var(--text-muted)]">
              <Workflow className="h-4 w-4" aria-hidden="true" />
              Use these surfaces to move from ingestion to orchestration.
            </div>
          </div>

          <EntryCardGrid items={entryCards} />
        </section>
      </div>
    </div>
  );
}
