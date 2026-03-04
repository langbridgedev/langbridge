'use client';

import { JSX, useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, Play, RefreshCw, Save, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Textarea } from '@/components/ui/textarea';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { ApiError } from '@/orchestration/http';
import {
  deleteDataset,
  fetchPreviewDatasetJob,
  fetchProfileDatasetJob,
  fetchDatasetUsage,
  getDataset,
  previewDataset,
  profileDataset,
  updateDataset,
  type DatasetColumn,
  type DatasetPreviewResponse,
  type DatasetProfileResponse,
  type DatasetRecord,
} from '@/orchestration/datasets';

type DatasetDetailPageProps = {
  params: { organizationId: string; datasetId: string };
};

type EditablePolicy = {
  maxRowsPreview: string;
  maxExportRows: string;
  allowDml: boolean;
  redactionRules: string;
  rowFilters: string;
};

const JOB_TERMINAL_STATUSES = new Set(['succeeded', 'failed', 'cancelled']);
const JOB_POLL_INTERVAL_MS = 1200;
const JOB_POLL_TIMEOUT_MS = 60_000;

export default function DatasetDetailPage({ params }: DatasetDetailPageProps): JSX.Element {
  const { organizationId, datasetId } = params;
  const router = useRouter();
  const queryClient = useQueryClient();
  const { selectedOrganizationId, selectedProjectId, setSelectedOrganizationId } = useWorkspaceScope();

  const [previewLimit, setPreviewLimit] = useState('100');
  const [previewResult, setPreviewResult] = useState<DatasetPreviewResponse | null>(null);
  const [profileResult, setProfileResult] = useState<DatasetProfileResponse | null>(null);
  const [policyEditor, setPolicyEditor] = useState<EditablePolicy | null>(null);
  const [metaEditor, setMetaEditor] = useState<{ name: string; description: string; tags: string }>({
    name: '',
    description: '',
    tags: '',
  });
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (organizationId && organizationId !== selectedOrganizationId) {
      setSelectedOrganizationId(organizationId);
    }
  }, [organizationId, selectedOrganizationId, setSelectedOrganizationId]);

  const datasetQuery = useQuery<DatasetRecord>({
    queryKey: ['dataset-detail', organizationId, datasetId],
    queryFn: () => getDataset(datasetId, organizationId),
    enabled: Boolean(organizationId && datasetId),
  });

  const usageQuery = useQuery({
    queryKey: ['dataset-usage', organizationId, datasetId],
    queryFn: () => fetchDatasetUsage(datasetId, organizationId),
    enabled: Boolean(organizationId && datasetId),
  });

  useEffect(() => {
    const dataset = datasetQuery.data;
    if (!dataset) {
      return;
    }
    setMetaEditor({
      name: dataset.name,
      description: dataset.description || '',
      tags: dataset.tags.join(', '),
    });
    setPolicyEditor({
      maxRowsPreview: String(dataset.policy.maxRowsPreview),
      maxExportRows: String(dataset.policy.maxExportRows),
      allowDml: dataset.policy.allowDml,
      redactionRules: JSON.stringify(dataset.policy.redactionRules || {}, null, 2),
      rowFilters: (dataset.policy.rowFilters || []).join('\n'),
    });
  }, [datasetQuery.data]);

  const saveMutation = useMutation({
    mutationFn: async () => {
      if (!policyEditor) {
        throw new Error('Dataset policy is not loaded.');
      }
      const redactionRules = JSON.parse(policyEditor.redactionRules || '{}') as Record<string, string>;
      const rowFilters = policyEditor.rowFilters
        .split('\n')
        .map((item) => item.trim())
        .filter(Boolean);

      return updateDataset(datasetId, {
        workspaceId: organizationId,
        projectId: selectedProjectId || undefined,
        name: metaEditor.name.trim(),
        description: metaEditor.description.trim() || undefined,
        tags: metaEditor.tags
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean),
        policy: {
          maxRowsPreview: Math.max(1, Number(policyEditor.maxRowsPreview) || 1),
          maxExportRows: Math.max(1, Number(policyEditor.maxExportRows) || 1),
          allowDml: policyEditor.allowDml,
          redactionRules,
          rowFilters,
        },
      });
    },
    onSuccess: async () => {
      setError(null);
      await queryClient.invalidateQueries({ queryKey: ['dataset-detail', organizationId, datasetId] });
    },
    onError: (mutationError: unknown) => {
      setError(resolveError(mutationError));
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteDataset(datasetId, organizationId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['datasets-list', organizationId] });
      router.push(`/datasets/${organizationId}`);
    },
    onError: (mutationError: unknown) => {
      setError(resolveError(mutationError));
    },
  });

  const previewMutation = useMutation({
    mutationFn: async () => {
      const queued = await previewDataset(datasetId, {
        workspaceId: organizationId,
        projectId: selectedProjectId || undefined,
        limit: Math.max(1, Number(previewLimit) || 100),
      });
      return pollDatasetPreviewJob(datasetId, queued.jobId, organizationId);
    },
    onSuccess: (result) => {
      setPreviewResult(result);
      setError(null);
    },
    onError: (mutationError: unknown) => {
      setError(resolveError(mutationError));
    },
  });

  const profileMutation = useMutation({
    mutationFn: async () => {
      const queued = await profileDataset(datasetId, {
        workspaceId: organizationId,
        projectId: selectedProjectId || undefined,
      });
      return pollDatasetProfileJob(datasetId, queued.jobId, organizationId);
    },
    onSuccess: (result) => {
      setProfileResult(result);
      setError(null);
      void queryClient.invalidateQueries({ queryKey: ['dataset-detail', organizationId, datasetId] });
    },
    onError: (mutationError: unknown) => {
      setError(resolveError(mutationError));
    },
  });

  const dataset = datasetQuery.data;
  const previewColumns = useMemo(
    () => previewResult?.columns?.map((column) => column.name) || [],
    [previewResult],
  );

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Button variant="ghost" size="sm" onClick={() => router.push(`/datasets/${organizationId}`)}>
          <ArrowLeft className="mr-2 h-4 w-4" /> Back to datasets
        </Button>
        <div className="flex gap-2">
          <Button variant="ghost" size="sm" onClick={() => datasetQuery.refetch()}>
            <RefreshCw className="mr-2 h-4 w-4" /> Refresh
          </Button>
          <Button variant="destructive" size="sm" onClick={() => deleteMutation.mutate()} isLoading={deleteMutation.isPending}>
            <Trash2 className="mr-2 h-4 w-4" /> Delete
          </Button>
        </div>
      </div>

      {datasetQuery.isLoading ? <p className="text-sm">Loading dataset...</p> : null}
      {datasetQuery.isError ? <p className="text-sm text-rose-500">{resolveError(datasetQuery.error)}</p> : null}

      {dataset ? (
        <>
          <header className="surface-panel rounded-3xl p-6 shadow-soft space-y-4">
            <div className="grid gap-4 md:grid-cols-3">
              <div className="space-y-1 md:col-span-2">
                <Label>Name</Label>
                <Input
                  value={metaEditor.name}
                  onChange={(event) => setMetaEditor((current) => ({ ...current, name: event.target.value }))}
                />
              </div>
              <div className="space-y-1">
                <Label>Type</Label>
                <Input value={dataset.datasetType} readOnly />
              </div>
              <div className="space-y-1 md:col-span-2">
                <Label>Description</Label>
                <Textarea
                  rows={3}
                  value={metaEditor.description}
                  onChange={(event) => setMetaEditor((current) => ({ ...current, description: event.target.value }))}
                />
              </div>
              <div className="space-y-1">
                <Label>Tags</Label>
                <Input
                  value={metaEditor.tags}
                  onChange={(event) => setMetaEditor((current) => ({ ...current, tags: event.target.value }))}
                />
              </div>
            </div>
            <Button onClick={() => saveMutation.mutate()} isLoading={saveMutation.isPending}>
              <Save className="mr-2 h-4 w-4" /> Save dataset
            </Button>
            {error ? <p className="text-sm text-rose-600">{error}</p> : null}
          </header>

          <section className="surface-panel rounded-3xl p-6 shadow-soft">
            <Tabs defaultValue="schema" className="space-y-4">
              <TabsList>
                <TabsTrigger value="schema">Schema</TabsTrigger>
                <TabsTrigger value="policies">Policies</TabsTrigger>
                <TabsTrigger value="preview">Preview</TabsTrigger>
                <TabsTrigger value="used-by">Used by</TabsTrigger>
              </TabsList>

              <TabsContent value="schema" className="space-y-3">
                <p className="text-sm">Columns defined in this dataset.</p>
                <div className="overflow-x-auto rounded-xl border border-[color:var(--panel-border)]">
                  <table className="min-w-full text-left text-sm">
                    <thead className="bg-[color:var(--panel-alt)] text-xs uppercase tracking-wide text-[color:var(--text-muted)]">
                      <tr>
                        <th className="px-3 py-2">Column</th>
                        <th className="px-3 py-2">Type</th>
                        <th className="px-3 py-2">Allowed</th>
                        <th className="px-3 py-2">Computed</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dataset.columns.map((column: DatasetColumn) => (
                        <tr key={column.id || column.name} className="border-t border-[color:var(--panel-border)]">
                          <td className="px-3 py-2">{column.name}</td>
                          <td className="px-3 py-2">{column.dataType}</td>
                          <td className="px-3 py-2">{column.isAllowed ? 'yes' : 'no'}</td>
                          <td className="px-3 py-2">{column.isComputed ? 'yes' : 'no'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </TabsContent>

              <TabsContent value="policies" className="space-y-4">
                {policyEditor ? (
                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="space-y-1">
                      <Label>Max preview rows</Label>
                      <Input
                        value={policyEditor.maxRowsPreview}
                        onChange={(event) =>
                          setPolicyEditor((current) =>
                            current ? { ...current, maxRowsPreview: event.target.value } : current,
                          )
                        }
                      />
                    </div>
                    <div className="space-y-1">
                      <Label>Max export rows</Label>
                      <Input
                        value={policyEditor.maxExportRows}
                        onChange={(event) =>
                          setPolicyEditor((current) =>
                            current ? { ...current, maxExportRows: event.target.value } : current,
                          )
                        }
                      />
                    </div>
                    <label className="flex items-center gap-2 text-sm md:col-span-2">
                      <input
                        type="checkbox"
                        checked={policyEditor.allowDml}
                        onChange={(event) =>
                          setPolicyEditor((current) =>
                            current ? { ...current, allowDml: event.target.checked } : current,
                          )
                        }
                      />
                      Allow DML
                    </label>
                    <div className="space-y-1 md:col-span-2">
                      <Label>Redaction rules (JSON map)</Label>
                      <Textarea
                        rows={6}
                        className="font-mono text-xs"
                        value={policyEditor.redactionRules}
                        onChange={(event) =>
                          setPolicyEditor((current) =>
                            current ? { ...current, redactionRules: event.target.value } : current,
                          )
                        }
                      />
                    </div>
                    <div className="space-y-1 md:col-span-2">
                      <Label>Row filters (one SQL predicate per line)</Label>
                      <Textarea
                        rows={4}
                        className="font-mono text-xs"
                        value={policyEditor.rowFilters}
                        onChange={(event) =>
                          setPolicyEditor((current) =>
                            current ? { ...current, rowFilters: event.target.value } : current,
                          )
                        }
                      />
                    </div>
                  </div>
                ) : null}
                <div className="flex flex-wrap gap-3">
                  <Button onClick={() => saveMutation.mutate()} isLoading={saveMutation.isPending}>
                    <Save className="mr-2 h-4 w-4" /> Save policies
                  </Button>
                  <Button variant="outline" onClick={() => profileMutation.mutate()} isLoading={profileMutation.isPending}>
                    Profile dataset
                  </Button>
                </div>
                {profileResult ? (
                  <div className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3 text-sm">
                    <p>Row estimate: {profileResult.rowCountEstimate ?? 'n/a'}</p>
                    <p>Bytes estimate: {profileResult.bytesEstimate ?? 'n/a'}</p>
                    <p>Profiled at: {profileResult.profiledAt ? new Date(profileResult.profiledAt).toLocaleString() : 'n/a'}</p>
                  </div>
                ) : null}
              </TabsContent>

              <TabsContent value="preview" className="space-y-3">
                <div className="flex flex-wrap items-end gap-3">
                  <div className="space-y-1">
                    <Label>Limit</Label>
                    <Input value={previewLimit} onChange={(event) => setPreviewLimit(event.target.value)} className="w-[120px]" />
                  </div>
                  <Button onClick={() => previewMutation.mutate()} isLoading={previewMutation.isPending}>
                    <Play className="mr-2 h-4 w-4" /> Run preview
                  </Button>
                </div>

                {previewResult?.error ? <p className="text-sm text-rose-600">{previewResult.error}</p> : null}
                {previewResult ? (
                  <div className="space-y-2">
                    <p className="text-xs text-[color:var(--text-muted)]">
                      Rows: {previewResult.rowCountPreview} | Duration: {previewResult.durationMs ?? 'n/a'} ms | Bytes: {previewResult.bytesScanned ?? 'n/a'}
                    </p>
                    <div className="overflow-x-auto rounded-xl border border-[color:var(--panel-border)]">
                      <table className="min-w-full text-left text-sm">
                        <thead className="bg-[color:var(--panel-alt)] text-xs uppercase tracking-wide text-[color:var(--text-muted)]">
                          <tr>
                            {previewColumns.map((column) => (
                              <th key={column} className="px-3 py-2">{column}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {previewResult.rows.map((row, rowIndex) => (
                            <tr key={rowIndex} className="border-t border-[color:var(--panel-border)]">
                              {previewColumns.map((column) => (
                                <td key={`${rowIndex}-${column}`} className="px-3 py-2">
                                  {formatCell(row[column])}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-[color:var(--text-muted)]">Run preview to inspect rows.</p>
                )}
              </TabsContent>

              <TabsContent value="used-by" className="space-y-3">
                {usageQuery.isLoading ? <p className="text-sm">Loading usage...</p> : null}
                {usageQuery.isError ? <p className="text-sm text-rose-600">{resolveError(usageQuery.error)}</p> : null}
                {!usageQuery.isLoading && !usageQuery.isError ? (
                  <div className="space-y-3">
                    <div>
                      <p className="text-sm font-semibold text-[color:var(--text-primary)]">Semantic models</p>
                      <ul className="mt-2 space-y-2">
                        {(usageQuery.data?.semanticModels || []).map((model, index) => (
                          <li key={index} className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3 text-sm">
                            {String(model.name || model.id || 'Unnamed model')}
                          </li>
                        ))}
                        {(usageQuery.data?.semanticModels || []).length === 0 ? (
                          <li className="text-sm text-[color:var(--text-muted)]">No semantic models currently reference this dataset.</li>
                        ) : null}
                      </ul>
                    </div>
                    <div>
                      <p className="text-sm font-semibold text-[color:var(--text-primary)]">Dashboards</p>
                      <p className="text-sm text-[color:var(--text-muted)]">{(usageQuery.data?.dashboards || []).length} linked dashboards.</p>
                    </div>
                    <div>
                      <p className="text-sm font-semibold text-[color:var(--text-primary)]">Saved queries</p>
                      <p className="text-sm text-[color:var(--text-muted)]">{(usageQuery.data?.savedQueries || []).length} linked queries.</p>
                    </div>
                  </div>
                ) : null}
              </TabsContent>
            </Tabs>
          </section>
        </>
      ) : null}
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value == null) {
    return '';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

function resolveError(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Something went wrong while processing the request.';
}

async function pollDatasetPreviewJob(
  datasetId: string,
  jobId: string,
  workspaceId: string,
): Promise<DatasetPreviewResponse> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < JOB_POLL_TIMEOUT_MS) {
    const state = await fetchPreviewDatasetJob(datasetId, jobId, workspaceId);
    if (JOB_TERMINAL_STATUSES.has(state.status)) {
      return state;
    }
    await sleep(JOB_POLL_INTERVAL_MS);
  }
  throw new Error('Dataset preview timed out while waiting for job completion.');
}

async function pollDatasetProfileJob(
  datasetId: string,
  jobId: string,
  workspaceId: string,
): Promise<DatasetProfileResponse> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < JOB_POLL_TIMEOUT_MS) {
    const state = await fetchProfileDatasetJob(datasetId, jobId, workspaceId);
    if (JOB_TERMINAL_STATUSES.has(state.status)) {
      return state;
    }
    await sleep(JOB_POLL_INTERVAL_MS);
  }
  throw new Error('Dataset profile timed out while waiting for job completion.');
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
