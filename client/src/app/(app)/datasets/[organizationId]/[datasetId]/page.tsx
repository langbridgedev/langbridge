'use client';

import { JSX, useEffect, useMemo, useState, type Dispatch, type SetStateAction } from 'react';
import { useRouter } from 'next/navigation';
import { useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { AlertTriangle, ArrowLeft, History, Play, RefreshCw, RotateCcw, Save, Trash2 } from 'lucide-react';

import { ErrorPanel } from '@/components/ErrorPanel';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Textarea } from '@/components/ui/textarea';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { toDisplayError, type DisplayError } from '@/lib/errors';
import {
  deleteDataset,
  fetchDatasetDiff,
  fetchDatasetImpact,
  fetchDatasetLineage,
  fetchDatasetVersion,
  fetchDatasetVersions,
  fetchPreviewDatasetJob,
  fetchProfileDatasetJob,
  getDataset,
  previewDataset,
  profileDataset,
  restoreDataset,
  updateDataset,
  type DatasetColumn,
  type DatasetImpactItem,
  type DatasetLineageNode,
  type DatasetPreviewResponse,
  type DatasetProfileResponse,
  type DatasetVersionSummary,
} from '@/orchestration/datasets';

type Props = { params: { organizationId: string; datasetId: string } };
type EditablePolicy = { maxRowsPreview: string; maxExportRows: string; allowDml: boolean; redactionRules: string; rowFilters: string };

const JOB_TERMINAL_STATUSES = new Set(['succeeded', 'failed', 'cancelled']);
const JOB_POLL_INTERVAL_MS = 1200;
const JOB_POLL_TIMEOUT_MS = 60_000;

export default function DatasetDetailPage({ params }: Props): JSX.Element {
  const { organizationId, datasetId } = params;
  const router = useRouter();
  const queryClient = useQueryClient();
  const { selectedOrganizationId, selectedProjectId, setSelectedOrganizationId } = useWorkspaceScope();
  const [previewLimit, setPreviewLimit] = useState('100');
  const [previewResult, setPreviewResult] = useState<DatasetPreviewResponse | null>(null);
  const [profileResult, setProfileResult] = useState<DatasetProfileResponse | null>(null);
  const [policyEditor, setPolicyEditor] = useState<EditablePolicy | null>(null);
  const [metaEditor, setMetaEditor] = useState({ name: '', description: '', tags: '' });
  const [selectedVersionId, setSelectedVersionId] = useState('');
  const [diffFromRevisionId, setDiffFromRevisionId] = useState('');
  const [diffToRevisionId, setDiffToRevisionId] = useState('');
  const [restoreRevisionId, setRestoreRevisionId] = useState('');
  const [restoreOpen, setRestoreOpen] = useState(false);
  const [pageError, setPageError] = useState<DisplayError | null>(null);
  const [previewError, setPreviewError] = useState<DisplayError | null>(null);

  useEffect(() => {
    if (organizationId && organizationId !== selectedOrganizationId) setSelectedOrganizationId(organizationId);
  }, [organizationId, selectedOrganizationId, setSelectedOrganizationId]);

  const datasetQuery = useQuery({ queryKey: ['dataset-detail', organizationId, datasetId], queryFn: () => getDataset(datasetId, organizationId), enabled: Boolean(organizationId && datasetId) });
  const versionsQuery = useQuery({ queryKey: ['dataset-versions', organizationId, datasetId], queryFn: () => fetchDatasetVersions(datasetId, organizationId), enabled: Boolean(organizationId && datasetId) });
  const versionQuery = useQuery({ queryKey: ['dataset-version', organizationId, datasetId, selectedVersionId], queryFn: () => fetchDatasetVersion(datasetId, selectedVersionId, organizationId), enabled: Boolean(selectedVersionId) });
  const diffQuery = useQuery({ queryKey: ['dataset-diff', organizationId, datasetId, diffFromRevisionId, diffToRevisionId], queryFn: () => fetchDatasetDiff(datasetId, organizationId, diffFromRevisionId, diffToRevisionId), enabled: Boolean(diffFromRevisionId && diffToRevisionId && diffFromRevisionId !== diffToRevisionId) });
  const lineageQuery = useQuery({ queryKey: ['dataset-lineage', organizationId, datasetId], queryFn: () => fetchDatasetLineage(datasetId, organizationId), enabled: Boolean(organizationId && datasetId) });
  const impactQuery = useQuery({ queryKey: ['dataset-impact', organizationId, datasetId], queryFn: () => fetchDatasetImpact(datasetId, organizationId), enabled: Boolean(organizationId && datasetId) });

  useEffect(() => {
    const dataset = datasetQuery.data;
    if (!dataset) return;
    setMetaEditor({ name: dataset.name, description: dataset.description || '', tags: dataset.tags.join(', ') });
    setPolicyEditor({ maxRowsPreview: String(dataset.policy.maxRowsPreview), maxExportRows: String(dataset.policy.maxExportRows), allowDml: dataset.policy.allowDml, redactionRules: JSON.stringify(dataset.policy.redactionRules || {}, null, 2), rowFilters: (dataset.policy.rowFilters || []).join('\n') });
  }, [datasetQuery.data]);

  useEffect(() => {
    const versions = versionsQuery.data?.items || [];
    if (!versions.length) return;
    const current = versions.find((item) => item.isCurrent) || versions[0];
    setSelectedVersionId((value) => value || current.id);
    setDiffToRevisionId((value) => value || current.id);
    setDiffFromRevisionId((value) => value || versions[versions.length - 1]?.id || current.id);
  }, [versionsQuery.data]);

  const versions = versionsQuery.data?.items || [];
  const currentVersion = versions.find((item) => item.isCurrent) || versions[0];
  const connectorSyncMeta = useMemo(
    () => extractConnectorSyncMetadata(datasetQuery.data?.fileConfig),
    [datasetQuery.data?.fileConfig],
  );
  const previewColumns = useMemo(() => previewResult?.columns.map((column) => column.name) || [], [previewResult]);
  const upstreamNodes = (lineageQuery.data?.nodes || []).filter((node) => node.direction === 'upstream');
  const downstreamNodes = (lineageQuery.data?.nodes || []).filter((node) => node.direction === 'downstream');

  const saveMutation = useMutation({
    mutationFn: async () => {
      if (!policyEditor) throw new Error('Dataset policy is not loaded.');
      return updateDataset(datasetId, {
        workspaceId: organizationId,
        projectId: selectedProjectId || undefined,
        name: metaEditor.name.trim(),
        description: metaEditor.description.trim() || undefined,
        tags: metaEditor.tags.split(',').map((item) => item.trim()).filter(Boolean),
        policy: { maxRowsPreview: Math.max(1, Number(policyEditor.maxRowsPreview) || 1), maxExportRows: Math.max(1, Number(policyEditor.maxExportRows) || 1), allowDml: policyEditor.allowDml, redactionRules: JSON.parse(policyEditor.redactionRules || '{}'), rowFilters: policyEditor.rowFilters.split('\n').map((item) => item.trim()).filter(Boolean) },
        changeSummary: 'Dataset metadata updated from dataset detail page.',
      });
    },
    onSuccess: async () => { setPageError(null); await refreshAll(queryClient, organizationId, datasetId); },
    onError: (mutationError: unknown) => setPageError(toDisplayError(mutationError)),
  });
  const restoreMutation = useMutation({
    mutationFn: () => restoreDataset(datasetId, { workspaceId: organizationId, projectId: selectedProjectId || undefined, revisionId: restoreRevisionId, changeSummary: `Restore dataset from revision ${restoreRevisionId}.` }),
    onSuccess: async () => { setRestoreOpen(false); setPageError(null); await refreshAll(queryClient, organizationId, datasetId); },
    onError: (mutationError: unknown) => setPageError(toDisplayError(mutationError)),
  });
  const deleteMutation = useMutation({
    mutationFn: () => deleteDataset(datasetId, organizationId),
    onSuccess: async () => { await queryClient.invalidateQueries({ queryKey: ['datasets-list', organizationId] }); router.push(`/datasets/${organizationId}`); },
    onError: (mutationError: unknown) => setPageError(toDisplayError(mutationError)),
  });
  const previewMutation = useMutation({
    mutationFn: async () => { const queued = await previewDataset(datasetId, { workspaceId: organizationId, projectId: selectedProjectId || undefined, limit: Math.max(1, Number(previewLimit) || 100) }); return pollDatasetPreviewJob(datasetId, queued.jobId, organizationId); },
    onSuccess: (result) => { setPreviewResult(result); setPreviewError(null); },
    onError: (mutationError: unknown) => {
      setPreviewResult(null);
      setPreviewError(toDisplayError(mutationError, 'dataset.preview'));
    },
  });
  const profileMutation = useMutation({
    mutationFn: async () => { const queued = await profileDataset(datasetId, { workspaceId: organizationId, projectId: selectedProjectId || undefined }); return pollDatasetProfileJob(datasetId, queued.jobId, organizationId); },
    onSuccess: async (result) => { setProfileResult(result); setPageError(null); await queryClient.invalidateQueries({ queryKey: ['dataset-detail', organizationId, datasetId] }); },
    onError: (mutationError: unknown) => setPageError(toDisplayError(mutationError)),
  });

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Button variant="ghost" size="sm" onClick={() => router.push(`/datasets/${organizationId}`)}><ArrowLeft className="mr-2 h-4 w-4" /> Back to datasets</Button>
        <div className="flex gap-2">
          <Button variant="ghost" size="sm" onClick={() => void refreshAll(queryClient, organizationId, datasetId)}><RefreshCw className="mr-2 h-4 w-4" /> Refresh</Button>
          <Button variant="destructive" size="sm" onClick={() => deleteMutation.mutate()} isLoading={deleteMutation.isPending}><Trash2 className="mr-2 h-4 w-4" /> Delete</Button>
        </div>
      </div>
      {datasetQuery.isLoading ? <p className="text-sm">Loading dataset...</p> : null}
      {datasetQuery.isError ? <ErrorPanel {...toDisplayError(datasetQuery.error)} /> : null}

      {datasetQuery.data ? <div className="surface-panel space-y-4 rounded-3xl p-6 shadow-soft">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="flex flex-wrap items-center gap-2 text-xs uppercase tracking-[0.18em] text-[color:var(--text-muted)]">
              <span>{datasetQuery.data.datasetType}</span>
              <span className="rounded-full border border-[color:var(--panel-border)] px-2 py-1">{datasetQuery.data.status}</span>
              {currentVersion ? <span className="rounded-full border border-[color:var(--panel-border)] px-2 py-1">Revision {currentVersion.revisionNumber}</span> : null}
            </div>
            <h1 className="mt-2 text-2xl font-semibold text-[color:var(--text-primary)]">{datasetQuery.data.name}</h1>
            <p className="mt-2 max-w-3xl text-sm">{currentVersion?.changeSummary || 'Governed dataset asset with version history and lineage.'}</p>
          </div>
          <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-3 text-sm">{datasetQuery.data.revisionId || 'n/a'}</div>
        </div>
        {(impactQuery.data?.totalDownstreamAssets || 0) > 0 ? <div className="rounded-2xl border border-amber-300/60 bg-amber-100/60 px-4 py-3 text-sm text-amber-900"><div className="flex items-start gap-2"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" /><div><p className="font-medium">{impactQuery.data?.totalDownstreamAssets} downstream assets depend on this dataset.</p><p className="mt-1">Review lineage and impact before restoring or changing the definition.</p></div></div></div> : null}
        {connectorSyncMeta ? <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-4 text-sm"><div className="flex flex-wrap items-center justify-between gap-3"><div><p className="text-xs font-semibold uppercase tracking-[0.18em] text-[color:var(--text-muted)]">Managed connector dataset</p><p className="mt-1 text-[color:var(--text-primary)]">{connectorSyncMeta.connectorType || 'API connector'} · {connectorSyncMeta.resourceName || 'resource'}</p><p className="mt-1 text-[color:var(--text-muted)]">Mode: {connectorSyncMeta.syncMode || 'n/a'} | Last sync: {formatTimestamp(connectorSyncMeta.lastSyncAt)}</p></div>{datasetQuery.data.connectionId ? <Button variant="outline" size="sm" onClick={() => router.push(`/datasources/${organizationId}/${datasetQuery.data.connectionId}`)}>Open connector</Button> : null}</div></div> : null}
        <div className="grid gap-4 md:grid-cols-3">
          <div className="space-y-1 md:col-span-2"><Label>Name</Label><Input value={metaEditor.name} onChange={(event) => setMetaEditor((current) => ({ ...current, name: event.target.value }))} /></div>
          <div className="space-y-1"><Label>Type</Label><Input value={datasetQuery.data.datasetType} readOnly /></div>
          <div className="space-y-1 md:col-span-2"><Label>Description</Label><Textarea rows={3} value={metaEditor.description} onChange={(event) => setMetaEditor((current) => ({ ...current, description: event.target.value }))} /></div>
          <div className="space-y-1"><Label>Tags</Label><Input value={metaEditor.tags} onChange={(event) => setMetaEditor((current) => ({ ...current, tags: event.target.value }))} /></div>
        </div>
        <div className="flex flex-wrap gap-3">
          <Button onClick={() => saveMutation.mutate()} isLoading={saveMutation.isPending}><Save className="mr-2 h-4 w-4" /> Save dataset</Button>
          <Button variant="outline" disabled={!currentVersion} onClick={() => { setRestoreRevisionId(currentVersion?.id || ''); setRestoreOpen(true); }}><RotateCcw className="mr-2 h-4 w-4" /> Restore revision</Button>
        </div>
        {pageError ? <ErrorPanel {...pageError} /> : null}
      </div> : null}

      {datasetQuery.data ? <section className="surface-panel rounded-3xl p-6 shadow-soft">
        <Tabs defaultValue="schema" className="space-y-4">
          <TabsList className="flex w-full flex-wrap justify-start gap-1">
            <TabsTrigger value="schema">Schema</TabsTrigger>
            <TabsTrigger value="policies">Policies</TabsTrigger>
            <TabsTrigger value="preview">Preview</TabsTrigger>
            <TabsTrigger value="versions">Versions</TabsTrigger>
            <TabsTrigger value="lineage">Lineage</TabsTrigger>
          </TabsList>
          <TabsContent value="schema"><SimpleSchemaTable columns={datasetQuery.data.columns} /></TabsContent>
          <TabsContent value="policies"><PolicyEditorView editor={policyEditor} onChange={setPolicyEditor} onSave={() => saveMutation.mutate()} savePending={saveMutation.isPending} onProfile={() => profileMutation.mutate()} profilePending={profileMutation.isPending} profileResult={profileResult} /></TabsContent>
          <TabsContent value="preview"><PreviewView previewLimit={previewLimit} setPreviewLimit={setPreviewLimit} previewMutation={previewMutation} previewResult={previewResult} previewColumns={previewColumns} previewError={previewError} /></TabsContent>
          <TabsContent value="versions"><VersionsView versions={versions} selectedVersionId={selectedVersionId} setSelectedVersionId={setSelectedVersionId} diffFromRevisionId={diffFromRevisionId} diffToRevisionId={diffToRevisionId} setDiffFromRevisionId={setDiffFromRevisionId} setDiffToRevisionId={setDiffToRevisionId} diffData={diffQuery.data} versionData={versionQuery.data} onRestore={(revisionId) => { setRestoreRevisionId(revisionId); setRestoreOpen(true); }} /></TabsContent>
          <TabsContent value="lineage"><LineageView upstreamNodes={upstreamNodes} downstreamNodes={downstreamNodes} impact={impactQuery.data} /></TabsContent>
        </Tabs>
      </section> : null}

      <Dialog open={restoreOpen} onOpenChange={setRestoreOpen}>
        <DialogContent>
          <DialogHeader><DialogTitle>Restore dataset revision</DialogTitle><DialogDescription>This creates a new current revision from an older snapshot and keeps the full audit trail.</DialogDescription></DialogHeader>
          <div className="space-y-3">
            <Label>Revision to restore</Label>
            <select className="w-full rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-2 text-sm" value={restoreRevisionId} onChange={(event) => setRestoreRevisionId(event.target.value)}>
              {versions.map((version) => <option key={version.id} value={version.id}>Revision {version.revisionNumber}</option>)}
            </select>
          </div>
          <DialogFooter><Button variant="outline" onClick={() => setRestoreOpen(false)}>Cancel</Button><Button onClick={() => restoreMutation.mutate()} isLoading={restoreMutation.isPending}><RotateCcw className="mr-2 h-4 w-4" /> Restore revision</Button></DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

async function refreshAll(queryClient: QueryClient, organizationId: string, datasetId: string): Promise<void> {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['dataset-detail', organizationId, datasetId] }),
    queryClient.invalidateQueries({ queryKey: ['dataset-versions', organizationId, datasetId] }),
    queryClient.invalidateQueries({ queryKey: ['dataset-version', organizationId, datasetId] }),
    queryClient.invalidateQueries({ queryKey: ['dataset-diff', organizationId, datasetId] }),
    queryClient.invalidateQueries({ queryKey: ['dataset-lineage', organizationId, datasetId] }),
    queryClient.invalidateQueries({ queryKey: ['dataset-impact', organizationId, datasetId] }),
  ]);
}

function SimpleSchemaTable({ columns }: { columns: DatasetColumn[] }): JSX.Element {
  return (
    <div className="space-y-3">
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
            {columns.map((column) => (
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
    </div>
  );
}

function PolicyEditorView({
  editor,
  onChange,
  onSave,
  savePending,
  onProfile,
  profilePending,
  profileResult,
}: {
  editor: EditablePolicy | null;
  onChange: Dispatch<SetStateAction<EditablePolicy | null>>;
  onSave: () => void;
  savePending: boolean;
  onProfile: () => void;
  profilePending: boolean;
  profileResult: DatasetProfileResponse | null;
}): JSX.Element {
  return (
    <div className="space-y-4">
      {editor ? (
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-1">
            <Label>Max preview rows</Label>
            <Input value={editor.maxRowsPreview} onChange={(event) => onChange((current) => current ? { ...current, maxRowsPreview: event.target.value } : current)} />
          </div>
          <div className="space-y-1">
            <Label>Max export rows</Label>
            <Input value={editor.maxExportRows} onChange={(event) => onChange((current) => current ? { ...current, maxExportRows: event.target.value } : current)} />
          </div>
          <label className="flex items-center gap-2 text-sm md:col-span-2">
            <input type="checkbox" checked={editor.allowDml} onChange={(event) => onChange((current) => current ? { ...current, allowDml: event.target.checked } : current)} />
            Allow DML
          </label>
          <div className="space-y-1 md:col-span-2">
            <Label>Redaction rules (JSON map)</Label>
            <Textarea rows={6} className="font-mono text-xs" value={editor.redactionRules} onChange={(event) => onChange((current) => current ? { ...current, redactionRules: event.target.value } : current)} />
          </div>
          <div className="space-y-1 md:col-span-2">
            <Label>Row filters (one SQL predicate per line)</Label>
            <Textarea rows={4} className="font-mono text-xs" value={editor.rowFilters} onChange={(event) => onChange((current) => current ? { ...current, rowFilters: event.target.value } : current)} />
          </div>
        </div>
      ) : null}
      <div className="flex flex-wrap gap-3">
        <Button onClick={onSave} isLoading={savePending}><Save className="mr-2 h-4 w-4" /> Save policies</Button>
        <Button variant="outline" onClick={onProfile} isLoading={profilePending}>Profile dataset</Button>
      </div>
      {profileResult ? (
        <div className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3 text-sm">
          <p>Row estimate: {profileResult.rowCountEstimate ?? 'n/a'}</p>
          <p>Bytes estimate: {profileResult.bytesEstimate ?? 'n/a'}</p>
          <p>Profiled at: {profileResult.profiledAt ? formatTimestamp(profileResult.profiledAt) : 'n/a'}</p>
        </div>
      ) : null}
    </div>
  );
}

function PreviewView({
  previewLimit,
  setPreviewLimit,
  previewMutation,
  previewResult,
  previewColumns,
  previewError,
}: {
  previewLimit: string;
  setPreviewLimit: Dispatch<SetStateAction<string>>;
  previewMutation: { mutate: () => void; isPending: boolean };
  previewResult: DatasetPreviewResponse | null;
  previewColumns: string[];
  previewError: DisplayError | null;
}): JSX.Element {
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-end gap-3">
        <div className="space-y-1">
          <Label>Limit</Label>
          <Input value={previewLimit} onChange={(event) => setPreviewLimit(event.target.value)} className="w-[120px]" />
        </div>
        <Button onClick={() => previewMutation.mutate()} isLoading={previewMutation.isPending}>
          <Play className="mr-2 h-4 w-4" /> Run preview
        </Button>
      </div>

      {previewError ? <ErrorPanel {...previewError} /> : null}
      {!previewError && previewResult?.error ? (
        <ErrorPanel {...toDisplayError(new Error(previewResult.error), 'dataset.preview')} />
      ) : null}
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
                      <td key={`${rowIndex}-${column}`} className="px-3 py-2">{formatCell(row[column])}</td>
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
    </div>
  );
}

function VersionsView({
  versions,
  selectedVersionId,
  setSelectedVersionId,
  diffFromRevisionId,
  diffToRevisionId,
  setDiffFromRevisionId,
  setDiffToRevisionId,
  diffData,
  versionData,
  onRestore,
}: {
  versions: DatasetVersionSummary[];
  selectedVersionId: string;
  setSelectedVersionId: Dispatch<SetStateAction<string>>;
  diffFromRevisionId: string;
  diffToRevisionId: string;
  setDiffFromRevisionId: Dispatch<SetStateAction<string>>;
  setDiffToRevisionId: Dispatch<SetStateAction<string>>;
  diffData: Record<string, unknown> | null | undefined;
  versionData: Record<string, unknown> | null | undefined;
  onRestore: (revisionId: string) => void;
}): JSX.Element {
  return (
    <div className="grid gap-4 lg:grid-cols-[320px_minmax(0,1fr)]">
      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <History className="h-4 w-4 text-[color:var(--text-primary)]" />
          <h2 className="text-sm font-semibold text-[color:var(--text-primary)]">Revision history</h2>
        </div>
        <div className="space-y-2">
          {versions.map((version) => (
            <button
              key={version.id}
              type="button"
              className={`w-full rounded-2xl border p-3 text-left transition ${
                selectedVersionId === version.id
                  ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)]'
                  : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)]'
              }`}
              onClick={() => setSelectedVersionId(version.id)}
            >
              <div className="flex items-center justify-between gap-2">
                <span className="font-medium text-[color:var(--text-primary)]">v{version.revisionNumber}</span>
                {version.isCurrent ? <span className="rounded-full border border-[color:var(--panel-border)] px-2 py-1 text-xs">Current</span> : null}
              </div>
              <p className="mt-1 text-sm">{version.changeSummary || 'No summary provided.'}</p>
              <p className="mt-2 text-xs text-[color:var(--text-muted)]">{formatTimestamp(version.createdAt)}</p>
            </button>
          ))}
        </div>
      </div>

      <div className="space-y-4">
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-1">
            <Label>Compare from</Label>
            <select className="w-full rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-2 text-sm" value={diffFromRevisionId} onChange={(event) => setDiffFromRevisionId(event.target.value)}>
              {versions.map((version) => (
                <option key={version.id} value={version.id}>v{version.revisionNumber}</option>
              ))}
            </select>
          </div>
          <div className="space-y-1">
            <Label>Compare to</Label>
            <select className="w-full rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-2 text-sm" value={diffToRevisionId} onChange={(event) => setDiffToRevisionId(event.target.value)}>
              {versions.map((version) => (
                <option key={version.id} value={version.id}>v{version.revisionNumber}</option>
              ))}
            </select>
          </div>
        </div>

        <div className="grid gap-4 xl:grid-cols-2">
          <section className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <h3 className="font-semibold text-[color:var(--text-primary)]">Revision snapshot</h3>
                <p className="text-xs text-[color:var(--text-muted)]">
                  {versionData && typeof versionData.createdAt === 'string' ? formatTimestamp(versionData.createdAt) : 'Select a revision to inspect.'}
                </p>
              </div>
              {versionData && typeof versionData.id === 'string' ? (
                <Button variant="outline" size="sm" onClick={() => onRestore(versionData.id)}>
                  <RotateCcw className="mr-2 h-4 w-4" /> Restore
                </Button>
              ) : null}
            </div>
            {versionData ? (
              <div className="mt-4 space-y-4 text-sm">
                <SnapshotBlock title="Definition snapshot" payload={(versionData as { definitionSnapshot?: unknown }).definitionSnapshot} />
                <SnapshotBlock title="Policy snapshot" payload={(versionData as { policySnapshot?: unknown }).policySnapshot} />
                <SnapshotBlock title="Source bindings" payload={(versionData as { sourceBindingsSnapshot?: unknown }).sourceBindingsSnapshot} />
                <SnapshotBlock title="Schema snapshot" payload={(versionData as { schemaSnapshot?: unknown }).schemaSnapshot} />
              </div>
            ) : (
              <p className="mt-3 text-sm text-[color:var(--text-muted)]">Select a revision to inspect.</p>
            )}
          </section>

          <section className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4">
            <h3 className="font-semibold text-[color:var(--text-primary)]">Revision diff</h3>
            {diffData ? (
              <div className="mt-4 space-y-4">
                <SnapshotBlock title="Summary" payload={(diffData as { summary?: unknown }).summary || []} />
                <DiffSection title="Schema changes" items={((diffData as { schemaChanges?: Array<Record<string, unknown>> }).schemaChanges) || []} />
                <DiffSection title="Definition changes" items={((diffData as { definitionChanges?: Array<Record<string, unknown>> }).definitionChanges) || []} />
                <DiffSection title="Policy changes" items={((diffData as { policyChanges?: Array<Record<string, unknown>> }).policyChanges) || []} />
                <DiffSection title="Source changes" items={((diffData as { sourceBindingChanges?: Array<Record<string, unknown>> }).sourceBindingChanges) || []} />
                <DiffSection title="Execution changes" items={((diffData as { executionChanges?: Array<Record<string, unknown>> }).executionChanges) || []} />
              </div>
            ) : (
              <p className="mt-3 text-sm text-[color:var(--text-muted)]">Choose two different revisions to compare.</p>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

function LineageView({
  upstreamNodes,
  downstreamNodes,
  impact,
}: {
  upstreamNodes: DatasetLineageNode[];
  downstreamNodes: DatasetLineageNode[];
  impact: { totalDownstreamAssets?: number; directDependents?: DatasetImpactItem[]; dependentDatasets?: DatasetImpactItem[]; semanticModels?: DatasetImpactItem[]; unifiedSemanticModels?: DatasetImpactItem[]; savedQueries?: DatasetImpactItem[]; dashboards?: DatasetImpactItem[] } | undefined;
}): JSX.Element {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-3">
        <MetricCard label="Upstream assets" value={String(upstreamNodes.length)} />
        <MetricCard label="Downstream assets" value={String(downstreamNodes.length)} />
        <MetricCard label="Impact scope" value={String(impact?.totalDownstreamAssets ?? 0)} />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <section className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4">
          <h3 className="font-semibold text-[color:var(--text-primary)]">Upstream lineage</h3>
          <p className="mt-1 text-sm text-[color:var(--text-muted)]">Connections, source tables, files, and datasets this asset derives from.</p>
          <LineageList nodes={upstreamNodes} emptyLabel="No upstream lineage registered." />
        </section>
        <section className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4">
          <h3 className="font-semibold text-[color:var(--text-primary)]">Downstream lineage</h3>
          <p className="mt-1 text-sm text-[color:var(--text-muted)]">Semantic models, unified models, saved queries, dashboards, and dependent datasets.</p>
          <LineageList nodes={downstreamNodes} emptyLabel="No downstream lineage registered." />
        </section>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <ImpactSection title="Direct dependents" items={impact?.directDependents || []} />
        <ImpactSection title="Dependent datasets" items={impact?.dependentDatasets || []} />
        <ImpactSection title="Semantic models" items={impact?.semanticModels || []} />
        <ImpactSection title="Unified semantic models" items={impact?.unifiedSemanticModels || []} />
        <ImpactSection title="Saved queries" items={impact?.savedQueries || []} />
        <ImpactSection title="Dashboards" items={impact?.dashboards || []} />
      </div>
    </div>
  );
}

function LineageList({
  nodes,
  emptyLabel,
}: {
  nodes: DatasetLineageNode[];
  emptyLabel: string;
}): JSX.Element {
  if (!nodes.length) {
    return <p className="mt-3 text-sm text-[color:var(--text-muted)]">{emptyLabel}</p>;
  }

  return (
    <div className="mt-4 space-y-3">
      {nodes.map((node) => (
        <div key={`${node.nodeType}-${node.nodeId}`} className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <p className="font-medium text-[color:var(--text-primary)]">{node.label}</p>
              <p className="mt-1 text-xs uppercase tracking-wide text-[color:var(--text-muted)]">{formatNodeType(node.nodeType)}</p>
            </div>
            <span className="text-xs text-[color:var(--text-muted)]">{node.direction}</span>
          </div>
          {Object.keys(node.metadata || {}).length ? (
            <pre className="mt-3 overflow-x-auto rounded-xl bg-[color:var(--panel-bg)] p-3 text-xs text-[color:var(--text-secondary)]">
              {JSON.stringify(node.metadata, null, 2)}
            </pre>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function ImpactSection({
  title,
  items,
}: {
  title: string;
  items: DatasetImpactItem[];
}): JSX.Element {
  return (
    <section className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4">
      <h3 className="font-semibold text-[color:var(--text-primary)]">{title}</h3>
      {items.length ? (
        <div className="mt-3 space-y-2">
          {items.map((item) => (
            <div key={`${item.nodeType}-${item.nodeId}`} className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <p className="font-medium text-[color:var(--text-primary)]">{item.label}</p>
                  <p className="mt-1 text-xs uppercase tracking-wide text-[color:var(--text-muted)]">{formatNodeType(item.nodeType)}</p>
                </div>
                <span className="rounded-full border border-[color:var(--panel-border)] px-2 py-1 text-xs">
                  {item.direct ? 'Direct' : 'Indirect'}
                </span>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="mt-3 text-sm text-[color:var(--text-muted)]">No assets found.</p>
      )}
    </section>
  );
}

function SnapshotBlock({
  title,
  payload,
}: {
  title: string;
  payload: unknown;
}): JSX.Element {
  return (
    <div className="space-y-2">
      <p className="text-sm font-semibold text-[color:var(--text-primary)]">{title}</p>
      <pre className="overflow-x-auto rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3 text-xs text-[color:var(--text-secondary)]">
        {JSON.stringify(payload, null, 2)}
      </pre>
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: string }): JSX.Element {
  return (
    <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4">
      <p className="text-xs uppercase tracking-wide text-[color:var(--text-muted)]">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-[color:var(--text-primary)]">{value}</p>
    </div>
  );
}

function DiffSection({
  title,
  items,
}: {
  title: string;
  items: Array<Record<string, unknown>>;
}): JSX.Element {
  return (
    <div className="space-y-2">
      <p className="text-sm font-semibold text-[color:var(--text-primary)]">{title}</p>
      {items.length ? (
        <div className="space-y-2">
          {items.map((item, index) => {
            const label = String(item.columnName || item.field || `Change ${index + 1}`);
            const changeType = String(item.changeType || 'changed');
            return (
              <div key={`${title}-${label}-${changeType}-${index}`} className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3 text-sm">
                <p className="font-medium text-[color:var(--text-primary)]">{label} · {changeType}</p>
                {'before' in item ? <p className="mt-2 text-xs text-[color:var(--text-muted)]">Before: {formatCell(item.before)}</p> : null}
                {'after' in item ? <p className="text-xs text-[color:var(--text-muted)]">After: {formatCell(item.after)}</p> : null}
              </div>
            );
          })}
        </div>
      ) : (
        <p className="text-sm text-[color:var(--text-muted)]">No changes.</p>
      )}
    </div>
  );
}

function extractConnectorSyncMetadata(
  fileConfig: Record<string, unknown> | null | undefined,
): {
  connectorType?: string | null;
  resourceName?: string | null;
  syncMode?: string | null;
  lastSyncAt?: string | null;
} | null {
  if (!fileConfig || typeof fileConfig !== 'object') {
    return null;
  }
  const syncMeta = fileConfig.connectorSync;
  if (!syncMeta || typeof syncMeta !== 'object') {
    return null;
  }
  const payload = syncMeta as Record<string, unknown>;
  return {
    connectorType: typeof payload.connectorType === 'string' ? payload.connectorType : null,
    resourceName: typeof payload.resourceName === 'string' ? payload.resourceName : null,
    syncMode: typeof payload.syncMode === 'string' ? payload.syncMode : null,
    lastSyncAt: typeof payload.lastSyncAt === 'string' ? payload.lastSyncAt : null,
  };
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) {
    return 'n/a';
  }
  return new Date(value).toLocaleString();
}

function formatNodeType(value: string): string {
  return value.replaceAll('_', ' ');
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

async function pollDatasetPreviewJob(datasetId: string, jobId: string, workspaceId: string): Promise<DatasetPreviewResponse> {
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

async function pollDatasetProfileJob(datasetId: string, jobId: string, workspaceId: string): Promise<DatasetProfileResponse> {
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
