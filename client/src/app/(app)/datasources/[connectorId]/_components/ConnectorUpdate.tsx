'use client';

import { JSX, useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  ArrowLeft,
  CheckCircle2,
  Clock3,
  History,
  Play,
  RefreshCw,
  Save,
  ShieldCheck,
  Square,
  Trash2,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Skeleton } from '@/components/ui/skeleton';
import { Textarea } from '@/components/ui/textarea';
import { useToast } from '@/components/ui/toast';
import { cancelAgentJob, fetchAgentJobState, type AgentJobState } from '@/orchestration/jobs';
import { ApiError } from '@/orchestration/http';
import {
  deleteConnector,
  fetchConnector,
  fetchConnectorResources,
  fetchConnectorSyncHistory,
  fetchConnectorSyncState,
  syncConnector,
  testConnector,
  updateConnector,
  type ConnectorResource,
  type ConnectorSyncMode,
  type ConnectorSyncState,
  type ConnectorResponse,
  type UpdateConnectorPayload,
} from '@/orchestration/connectors';

interface ConnectorUpdateProps {
  connectorId: string;
  organizationId: string;
}

const connectorQueryKey = (organizationId: string, connectorId: string) =>
  ['connector', organizationId, connectorId] as const;

const resourceQueryKey = (organizationId: string, connectorId: string) =>
  ['connector-resources', organizationId, connectorId] as const;

const syncStateQueryKey = (organizationId: string, connectorId: string) =>
  ['connector-sync-state', organizationId, connectorId] as const;

const syncHistoryQueryKey = (organizationId: string, connectorId: string) =>
  ['connector-sync-history', organizationId, connectorId] as const;

const JOB_TERMINAL_STATUSES = new Set(['succeeded', 'failed', 'cancelled']);

function resolveError(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Something went wrong. Please try again.';
}

function formatTimestamp(value?: string | null): string {
  if (!value) {
    return 'Never';
  }
  return new Date(value).toLocaleString();
}

function formatStatus(value: string): string {
  return value.replaceAll('_', ' ').replaceAll('-', ' ');
}

function buildStateMap(items: ConnectorSyncState[] | undefined): Record<string, ConnectorSyncState> {
  const stateByResource: Record<string, ConnectorSyncState> = {};
  for (const item of items || []) {
    stateByResource[item.resourceName] = item;
  }
  return stateByResource;
}

export function ConnectorUpdate({ connectorId, organizationId }: ConnectorUpdateProps): JSX.Element {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [configText, setConfigText] = useState('');
  const [localError, setLocalError] = useState<string | null>(null);
  const [selectedResources, setSelectedResources] = useState<string[]>([]);
  const [syncMode, setSyncMode] = useState<ConnectorSyncMode>('INCREMENTAL');
  const [forceFullRefresh, setForceFullRefresh] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [handledJobId, setHandledJobId] = useState<string | null>(null);

  const connectorQuery = useQuery({
    queryKey: connectorQueryKey(organizationId, connectorId),
    queryFn: () => fetchConnector(organizationId, connectorId),
    enabled: Boolean(organizationId && connectorId),
  });

  const resourcesQuery = useQuery({
    queryKey: resourceQueryKey(organizationId, connectorId),
    queryFn: () => fetchConnectorResources(organizationId, connectorId),
    enabled: Boolean(organizationId && connectorId),
  });

  const syncStateQuery = useQuery({
    queryKey: syncStateQueryKey(organizationId, connectorId),
    queryFn: () => fetchConnectorSyncState(organizationId, connectorId),
    enabled: Boolean(organizationId && connectorId),
  });

  const syncHistoryQuery = useQuery({
    queryKey: syncHistoryQueryKey(organizationId, connectorId),
    queryFn: () => fetchConnectorSyncHistory(organizationId, connectorId, 15),
    enabled: Boolean(organizationId && connectorId),
  });

  const activeJobQuery = useQuery<AgentJobState>({
    queryKey: ['connector-sync-job', organizationId, activeJobId],
    queryFn: () => fetchAgentJobState(organizationId, activeJobId || ''),
    enabled: Boolean(organizationId && activeJobId),
    refetchInterval: (query) => {
      const status = (query.state.data as AgentJobState | undefined)?.status;
      return status && JOB_TERMINAL_STATUSES.has(status) ? false : 1500;
    },
  });

  const connector = connectorQuery.data;
  const resources = useMemo(
    () => resourcesQuery.data?.items || [],
    [resourcesQuery.data?.items],
  );
  const stateByResource = useMemo(
    () => buildStateMap(syncStateQuery.data?.items),
    [syncStateQuery.data?.items],
  );

  useEffect(() => {
    if (!connector) {
      return;
    }
    setName(connector.name);
    setDescription(connector.description ?? '');
    setConfigText(JSON.stringify(connector.config ?? {}, null, 2));
  }, [connector]);

  useEffect(() => {
    if (!resources.length) {
      return;
    }
    setSelectedResources((current) => {
      const available = new Set(resources.map((resource) => resource.name));
      const retained = current.filter((value) => available.has(value));
      if (retained.length > 0) {
        return retained;
      }
      return resources.map((resource) => resource.name);
    });
  }, [resources]);

  useEffect(() => {
    const latestJob = syncHistoryQuery.data?.items[0];
    if (!latestJob || activeJobId) {
      return;
    }
    if (!JOB_TERMINAL_STATUSES.has(latestJob.status)) {
      setActiveJobId(latestJob.jobId);
    }
  }, [activeJobId, syncHistoryQuery.data]);

  useEffect(() => {
    const job = activeJobQuery.data;
    if (!job || !activeJobId || handledJobId === activeJobId) {
      return;
    }
    if (!JOB_TERMINAL_STATUSES.has(job.status)) {
      return;
    }

    void Promise.all([
      queryClient.invalidateQueries({ queryKey: resourceQueryKey(organizationId, connectorId) }),
      queryClient.invalidateQueries({ queryKey: syncStateQueryKey(organizationId, connectorId) }),
      queryClient.invalidateQueries({ queryKey: syncHistoryQueryKey(organizationId, connectorId) }),
      queryClient.invalidateQueries({ queryKey: ['datasets-list', organizationId] }),
    ]);

    if (job.status === 'succeeded') {
      toast({
        title: 'Sync completed',
        description: job.finalResponse?.summary || 'Connector sync finished successfully.',
      });
    } else if (job.status === 'cancelled') {
      toast({
        title: 'Sync cancelled',
        description: job.error?.message || 'Connector sync was cancelled.',
      });
    } else {
      toast({
        title: 'Sync failed',
        description: resolveError(job.error?.message || job.error),
        variant: 'destructive',
      });
    }
    setHandledJobId(activeJobId);
  }, [
    activeJobId,
    activeJobQuery.data,
    connectorId,
    handledJobId,
    organizationId,
    queryClient,
    toast,
  ]);

  const updateMutation = useMutation({
    mutationFn: (payload: UpdateConnectorPayload) =>
      updateConnector(organizationId, connectorId, payload),
    onSuccess: (updatedConnector: ConnectorResponse) => {
      queryClient.setQueryData(connectorQueryKey(organizationId, connectorId), updatedConnector);
      void queryClient.invalidateQueries({ queryKey: ['connectors', organizationId] });
      toast({
        title: 'Connector saved',
        description: `"${updatedConnector.name}" has been updated.`,
      });
    },
    onError: (error: unknown) => {
      toast({
        title: 'Update failed',
        description: resolveError(error),
        variant: 'destructive',
      });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteConnector(organizationId, connectorId),
    onSuccess: () => {
      queryClient.removeQueries({ queryKey: connectorQueryKey(organizationId, connectorId) });
      void queryClient.invalidateQueries({ queryKey: ['connectors', organizationId] });
      toast({
        title: 'Connection deleted',
        description: 'The connection has been removed.',
      });
      router.push(`/datasources/${organizationId}`);
    },
    onError: (error: unknown) => {
      toast({
        title: 'Delete failed',
        description: resolveError(error),
        variant: 'destructive',
      });
    },
  });

  const testMutation = useMutation({
    mutationFn: () => testConnector(organizationId, connectorId),
    onSuccess: (result) => {
      toast({
        title: 'Connection validated',
        description: result.message,
      });
    },
    onError: (error: unknown) => {
      toast({
        title: 'Connection test failed',
        description: resolveError(error),
        variant: 'destructive',
      });
    },
  });

  const syncMutation = useMutation({
    mutationFn: () =>
      syncConnector(organizationId, connectorId, {
        resources: selectedResources,
        syncMode,
        forceFullRefresh,
      }),
    onSuccess: (result) => {
      setHandledJobId(null);
      setActiveJobId(result.jobId);
      toast({
        title: 'Sync queued',
        description: `Job ${result.jobId} is now running.`,
      });
      void Promise.all([
        queryClient.invalidateQueries({ queryKey: syncHistoryQueryKey(organizationId, connectorId) }),
        queryClient.invalidateQueries({ queryKey: syncStateQueryKey(organizationId, connectorId) }),
      ]);
    },
    onError: (error: unknown) => {
      toast({
        title: 'Sync failed to start',
        description: resolveError(error),
        variant: 'destructive',
      });
    },
  });

  const cancelJobMutation = useMutation({
    mutationFn: (jobId: string) => cancelAgentJob(organizationId, jobId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['connector-sync-job', organizationId, activeJobId] }),
        queryClient.invalidateQueries({ queryKey: syncHistoryQueryKey(organizationId, connectorId) }),
        queryClient.invalidateQueries({ queryKey: syncStateQueryKey(organizationId, connectorId) }),
        queryClient.invalidateQueries({ queryKey: resourceQueryKey(organizationId, connectorId) }),
      ]);
      toast({
        title: 'Cancellation requested',
        description: 'The sync job was marked for cancellation.',
      });
    },
    onError: (error: unknown) => {
      toast({
        title: 'Unable to cancel sync',
        description: resolveError(error),
        variant: 'destructive',
      });
    },
  });

  const selectedResourceDefinitions = resources.filter((resource) =>
    selectedResources.includes(resource.name),
  );
  const selectedResourceCount = selectedResources.length;
  const selectedHasNonIncremental = selectedResourceDefinitions.some(
    (resource) => !resource.supportsIncremental,
  );
  const lastSyncAt = syncStateQuery.data?.items
    ?.map((item) => item.lastSyncAt)
    .filter((value): value is string => Boolean(value))
    .sort()
    .at(-1);
  const runningResourceCount = Object.values(stateByResource).filter(
    (item) => item.status === 'running',
  ).length;

  const handleSubmit: React.FormEventHandler<HTMLFormElement> = (event) => {
    event.preventDefault();
    if (!organizationId) {
      setLocalError('Select an organization scope before saving changes.');
      return;
    }
    setLocalError(null);

    let parsedConfig: Record<string, unknown> | undefined;
    const trimmed = configText.trim();
    if (trimmed) {
      try {
        parsedConfig = JSON.parse(trimmed) as Record<string, unknown>;
      } catch {
        setLocalError('Connector configuration must be valid JSON.');
        return;
      }
    }

    const payload: UpdateConnectorPayload = {
      organizationId,
    };

    if (name.trim() && name.trim() !== connector?.name) {
      payload.name = name.trim();
    }
    payload.description = description.trim() || undefined;
    if (connector?.connectorType) {
      payload.connectorType = connector.connectorType;
    }
    if (connector?.projectId) {
      payload.projectId = connector.projectId;
    }
    if (parsedConfig) {
      payload.config = { config: parsedConfig };
    }

    updateMutation.mutate(payload);
  };

  const handleDelete = () => {
    if (deleteMutation.isPending) {
      return;
    }
    const confirmed = window.confirm('Delete this connection? This action cannot be undone.');
    if (!confirmed) {
      return;
    }
    deleteMutation.mutate();
  };

  const handleResourceToggle = (resourceName: string, checked: boolean) => {
    setSelectedResources((current) => {
      if (checked) {
        return current.includes(resourceName) ? current : [...current, resourceName];
      }
      return current.filter((value) => value !== resourceName);
    });
  };

  if (connectorQuery.isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <div className="space-y-4">
          <Skeleton className="h-10 w-full max-w-md" />
          <Skeleton className="h-24 w-full" />
        </div>
      </div>
    );
  }

  if (connectorQuery.isError || !connector) {
    return (
      <div className="space-y-4 text-sm text-[color:var(--text-muted)]">
        <p>We couldn&apos;t load that connector.</p>
        <Button variant="outline" size="sm" onClick={() => connectorQuery.refetch()}>
          Try again
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="gap-2 text-[color:var(--text-secondary)] hover:text-[color:var(--text-primary)]"
            onClick={() => router.push(`/datasources/${organizationId}`)}
          >
            <ArrowLeft className="h-4 w-4" aria-hidden="true" />
            Back to connections
          </Button>
          <div>
            <h1 className="text-xl font-semibold text-[color:var(--text-primary)]">{connector.name}</h1>
            <p className="text-sm text-[color:var(--text-muted)]">{connector.connectorType ?? 'Unknown type'}</p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => testMutation.mutate()}
            isLoading={testMutation.isPending}
          >
            <ShieldCheck className="mr-2 h-4 w-4" aria-hidden="true" />
            Test connection
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => {
              void connectorQuery.refetch();
              void resourcesQuery.refetch();
              void syncStateQuery.refetch();
              void syncHistoryQuery.refetch();
            }}
            disabled={
              connectorQuery.isFetching
              || resourcesQuery.isFetching
              || syncStateQuery.isFetching
              || syncHistoryQuery.isFetching
            }
          >
            <RefreshCw className="mr-2 h-4 w-4" aria-hidden="true" />
            Refresh
          </Button>
        </div>
      </div>

      <section className="grid gap-4 md:grid-cols-4">
        <StatusCard
          title="Resources"
          value={String(resources.length)}
          detail={`${selectedResourceCount} selected for next run`}
          icon={<CheckCircle2 className="h-4 w-4" aria-hidden="true" />}
        />
        <StatusCard
          title="Last sync"
          value={lastSyncAt ? formatTimestamp(lastSyncAt) : 'Never'}
          detail={runningResourceCount > 0 ? `${runningResourceCount} resource(s) running` : 'No active syncs'}
          icon={<Clock3 className="h-4 w-4" aria-hidden="true" />}
        />
        <StatusCard
          title="Datasets produced"
          value={String(resources.reduce((count, resource) => count + resource.datasetIds.length, 0))}
          detail="Managed dataset outputs"
          icon={<History className="h-4 w-4" aria-hidden="true" />}
        />
        <StatusCard
          title="Mode"
          value={forceFullRefresh ? 'Forced full refresh' : formatStatus(syncMode)}
          detail={selectedHasNonIncremental && syncMode === 'INCREMENTAL'
            ? 'Unsupported resources fall back to full refresh.'
            : 'Checkpoint state is retained per resource.'}
          icon={<Play className="h-4 w-4" aria-hidden="true" />}
        />
      </section>

      <section className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="space-y-1">
            <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Sync control</h2>
            <p className="text-sm text-[color:var(--text-muted)]">
              Run full refresh or incremental syncs into managed datasets. Incremental state is stored per resource.
            </p>
          </div>
          <Button
            type="button"
            onClick={() => syncMutation.mutate()}
            isLoading={syncMutation.isPending}
            disabled={selectedResources.length === 0}
          >
            <Play className="mr-2 h-4 w-4" aria-hidden="true" />
            Sync now
          </Button>
        </div>

        <div className="mt-5 grid gap-4 lg:grid-cols-[220px_220px_minmax(0,1fr)]">
          <div className="space-y-2">
            <Label htmlFor="sync-mode">Sync mode</Label>
            <select
              id="sync-mode"
              className="w-full rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-2 text-sm"
              value={syncMode}
              onChange={(event) => setSyncMode(event.target.value as ConnectorSyncMode)}
            >
              <option value="INCREMENTAL">Incremental</option>
              <option value="FULL_REFRESH">Full refresh</option>
            </select>
          </div>

          <label className="flex items-center gap-3 rounded-2xl border border-[color:var(--panel-border)] px-4 py-3 text-sm">
            <input
              type="checkbox"
              checked={forceFullRefresh}
              onChange={(event) => setForceFullRefresh(event.target.checked)}
            />
            Force full refresh for this run
          </label>

          <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-3 text-sm">
            {selectedHasNonIncremental && syncMode === 'INCREMENTAL' ? (
              <p>
                Some selected resources do not expose an incremental cursor. They will run as full refresh while
                incremental-capable resources continue using checkpoints.
              </p>
            ) : (
              <p>Select one or more resources, then start a sync. Child tables are normalized into managed datasets automatically.</p>
            )}
          </div>
        </div>

        {activeJobQuery.data ? (
          <div className="mt-5 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-sm font-medium text-[color:var(--text-primary)]">
                  Active job: {activeJobQuery.data.id}
                </p>
                <p className="text-xs text-[color:var(--text-muted)]">
                  {formatStatus(activeJobQuery.data.status)} | {activeJobQuery.data.progress}% complete
                </p>
              </div>
              <div className="flex flex-wrap items-center justify-end gap-3">
                <p className="text-sm text-[color:var(--text-secondary)]">
                  {activeJobQuery.data.events.at(-1)?.message || activeJobQuery.data.finalResponse?.summary || 'Sync in progress'}
                </p>
                {!JOB_TERMINAL_STATUSES.has(activeJobQuery.data.status) ? (
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => cancelJobMutation.mutate(activeJobQuery.data.id)}
                    isLoading={cancelJobMutation.isPending}
                    disabled={cancelJobMutation.isPending}
                  >
                    <Square className="mr-2 h-4 w-4" aria-hidden="true" />
                    Cancel sync
                  </Button>
                ) : null}
              </div>
            </div>
          </div>
        ) : null}
      </section>

      <section className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Resources</h2>
            <p className="text-sm text-[color:var(--text-muted)]">
              Discover available API resources, select sync scope, and inspect the datasets produced for each resource.
            </p>
          </div>
          <Button type="button" variant="outline" size="sm" onClick={() => resourcesQuery.refetch()}>
            <RefreshCw className="mr-2 h-4 w-4" aria-hidden="true" />
            Rediscover resources
          </Button>
        </div>

        <div className="mt-5 space-y-3">
          {resourcesQuery.isLoading ? (
            <div className="space-y-3">
              {Array.from({ length: 3 }).map((_, index) => (
                <Skeleton key={index} className="h-28 w-full rounded-2xl" />
              ))}
            </div>
          ) : resources.length === 0 ? (
            <p className="text-sm text-[color:var(--text-muted)]">No resources were discovered for this connector.</p>
          ) : (
            resources.map((resource) => (
              <ResourceCard
                key={resource.name}
                resource={resource}
                state={stateByResource[resource.name]}
                selected={selectedResources.includes(resource.name)}
                onToggle={handleResourceToggle}
                onOpenDataset={(datasetId) => router.push(`/datasets/${organizationId}/${datasetId}`)}
              />
            ))
          )}
        </div>
      </section>

      <section className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
        <form
          onSubmit={handleSubmit}
          className="space-y-6 rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft"
        >
          <div>
            <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Connection settings</h2>
            <p className="text-sm text-[color:var(--text-muted)]">
              Update runtime configuration without changing the sync API surface.
            </p>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="connector-name">Name</Label>
              <Input
                id="connector-name"
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="Warehouse name"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="connector-type">Connector type</Label>
              <Input id="connector-type" value={connector.connectorType ?? 'Unknown'} readOnly />
            </div>

            <div className="space-y-2 md:col-span-2">
              <Label htmlFor="connector-description">Description</Label>
              <Textarea
                id="connector-description"
                value={description}
                onChange={(event) => setDescription(event.target.value)}
                placeholder="Short summary of this data source"
                rows={3}
              />
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="connector-config">Configuration (JSON)</Label>
            <Textarea
              id="connector-config"
              value={configText}
              onChange={(event) => setConfigText(event.target.value)}
              rows={12}
              className="font-mono text-xs"
            />
            <p className="text-xs text-[color:var(--text-muted)]">
              Update credentials or connection details. These values map directly to the connector runtime configuration.
            </p>
          </div>

          {localError ? <p className="text-sm text-rose-600">{localError}</p> : null}

          <div className="flex flex-wrap items-center gap-3">
            <Button
              type="submit"
              className="gap-2"
              disabled={updateMutation.isPending}
              isLoading={updateMutation.isPending}
              loadingText="Saving..."
            >
              <Save className="h-4 w-4" aria-hidden="true" />
              Save changes
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => {
                setName(connector.name);
                setDescription(connector.description ?? '');
                setConfigText(JSON.stringify(connector.config ?? {}, null, 2));
                setLocalError(null);
              }}
            >
              Reset
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="gap-2 border-rose-300 text-rose-700 hover:bg-rose-50"
              onClick={handleDelete}
              isLoading={deleteMutation.isPending}
              disabled={updateMutation.isPending}
            >
              <Trash2 className="h-4 w-4" aria-hidden="true" />
              Delete connection
            </Button>
          </div>
        </form>

        <section className="space-y-4 rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
          <div>
            <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Run history</h2>
            <p className="text-sm text-[color:var(--text-muted)]">
              Recent connector sync runs and their outcomes.
            </p>
          </div>

          <div className="space-y-3">
            {syncHistoryQuery.isLoading ? (
              Array.from({ length: 4 }).map((_, index) => (
                <Skeleton key={index} className="h-20 w-full rounded-2xl" />
              ))
            ) : syncHistoryQuery.data?.items.length ? (
              syncHistoryQuery.data.items.map((item) => (
                <div
                  key={item.jobId}
                  className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-3"
                >
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-sm font-medium text-[color:var(--text-primary)]">{formatStatus(item.status)}</p>
                    <div className="flex items-center gap-2">
                      <p className="text-xs text-[color:var(--text-muted)]">{item.progress}%</p>
                      {!JOB_TERMINAL_STATUSES.has(item.status) ? (
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          onClick={() => cancelJobMutation.mutate(item.jobId)}
                          isLoading={cancelJobMutation.isPending && activeJobId === item.jobId}
                          disabled={cancelJobMutation.isPending}
                        >
                          Cancel
                        </Button>
                      ) : null}
                    </div>
                  </div>
                  <p className="mt-1 text-xs text-[color:var(--text-muted)]">{item.statusMessage || 'No status message'}</p>
                  <p className="mt-2 text-xs text-[color:var(--text-muted)]">
                    Started: {formatTimestamp(item.startedAt || item.createdAt)}
                  </p>
                </div>
              ))
            ) : (
              <p className="text-sm text-[color:var(--text-muted)]">No syncs have been run for this connector yet.</p>
            )}
          </div>
        </section>
      </section>
    </div>
  );
}

function StatusCard({
  title,
  value,
  detail,
  icon,
}: {
  title: string;
  value: string;
  detail: string;
  icon: JSX.Element;
}): JSX.Element {
  return (
    <div className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-5 shadow-soft">
      <div className="flex items-center justify-between gap-2 text-[color:var(--text-muted)]">
        <p className="text-xs font-semibold uppercase tracking-[0.18em]">{title}</p>
        {icon}
      </div>
      <p className="mt-3 text-lg font-semibold text-[color:var(--text-primary)]">{value}</p>
      <p className="mt-2 text-xs text-[color:var(--text-muted)]">{detail}</p>
    </div>
  );
}

function ResourceCard({
  resource,
  state,
  selected,
  onToggle,
  onOpenDataset,
}: {
  resource: ConnectorResource;
  state?: ConnectorSyncState;
  selected: boolean;
  onToggle: (resourceName: string, checked: boolean) => void;
  onOpenDataset: (datasetId: string) => void;
}): JSX.Element {
  const datasetLinks = resource.datasetIds.map((datasetId, index) => ({
    id: datasetId,
    name: resource.datasetNames[index] || datasetId,
  }));

  return (
    <label className="block rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <input
            type="checkbox"
            className="mt-1"
            checked={selected}
            onChange={(event) => onToggle(resource.name, event.target.checked)}
          />
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <p className="text-sm font-semibold text-[color:var(--text-primary)]">{resource.label || resource.name}</p>
              <span className="rounded-full border border-[color:var(--panel-border)] px-2 py-1 text-[10px] uppercase tracking-[0.16em] text-[color:var(--text-muted)]">
                {formatStatus(state?.status || resource.status)}
              </span>
              {resource.supportsIncremental ? (
                <span className="rounded-full border border-emerald-300 px-2 py-1 text-[10px] uppercase tracking-[0.16em] text-emerald-700">
                  incremental
                </span>
              ) : (
                <span className="rounded-full border border-[color:var(--panel-border)] px-2 py-1 text-[10px] uppercase tracking-[0.16em] text-[color:var(--text-muted)]">
                  full refresh
                </span>
              )}
            </div>
            <p className="mt-1 text-xs text-[color:var(--text-muted)]">
              Primary key: {resource.primaryKey || 'n/a'} | Cursor: {resource.incrementalCursorField || 'n/a'}
            </p>
            <p className="mt-1 text-xs text-[color:var(--text-muted)]">
              Last sync: {formatTimestamp(state?.lastSyncAt || resource.lastSyncAt)} | Records synced:{' '}
              {state?.recordsSynced ?? resource.recordsSynced ?? 0}
            </p>
            {state?.errorMessage ? (
              <p className="mt-2 text-xs text-rose-600">{state.errorMessage}</p>
            ) : null}
          </div>
        </div>

        <div className="max-w-md space-y-2 text-right">
          <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[color:var(--text-muted)]">
            Datasets produced
          </p>
          {datasetLinks.length ? (
            <div className="flex flex-wrap justify-end gap-2">
              {datasetLinks.map((dataset) => (
                <button
                  key={dataset.id}
                  type="button"
                  onClick={() => onOpenDataset(dataset.id)}
                  className="rounded-full border border-[color:var(--panel-border)] px-3 py-1 text-xs text-[color:var(--text-primary)] transition hover:bg-[color:var(--panel-alt)]"
                >
                  {dataset.name}
                </button>
              ))}
            </div>
          ) : (
            <p className="text-xs text-[color:var(--text-muted)]">No datasets materialized yet.</p>
          )}
        </div>
      </div>
    </label>
  );
}
