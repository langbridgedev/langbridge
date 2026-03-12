'use client';

import { Bot, ChevronDown, Search, Sparkles } from 'lucide-react';
import { useRouter, useSearchParams } from 'next/navigation';
import { JSX, useEffect, useMemo, useState } from 'react';
import yaml from 'js-yaml';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { createClientId } from '@/lib/utils';
import { fetchDatasetCatalog, type DatasetCatalogItem } from '@/orchestration/datasets';
import { ApiError } from '@/orchestration/http';
import { fetchAgentJobState } from '@/orchestration/jobs';
import { createSemanticModel, deleteSemanticModel, fetchSemanticModel, generateSemanticModelYamlFromSelection, startAgenticSemanticModelJob, updateSemanticModel } from '@/orchestration/semanticModels';
import type { SemanticDimension, SemanticMeasure, SemanticRelationship } from '@/orchestration/semanticModels/types';

type Props = { params: { organizationId: string } };
type Dim = SemanticDimension & { id: string };
type Mea = SemanticMeasure & { id: string };
type Ds = { id: string; datasetId: string; datasetName: string; key: string; description: string; dimensions: Dim[]; measures: Mea[] };
type Rel = SemanticRelationship & { id: string };
type CreateMode = 'select' | 'auto' | 'manual' | 'agentic';

const FIELD_TYPES = ['string', 'integer', 'decimal', 'float', 'boolean', 'date'];
const REL_TYPES = ['many_to_one', 'one_to_many', 'one_to_one', 'many_to_many', 'inner', 'left'];

export default function SemanticModelCreatePage({ params }: Props): JSX.Element {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { selectedProjectId, setSelectedOrganizationId } = useWorkspaceScope();
  const organizationId = params.organizationId;
  const modelId = searchParams.get('modelId') ?? '';
  const isEdit = modelId.length > 0;
  const requestedMode = searchParams.get('mode') ?? '';
  const createMode: CreateMode = isEdit
    ? 'manual'
    : requestedMode === 'auto' || requestedMode === 'manual' || requestedMode === 'agentic'
      ? requestedMode
      : 'select';
  const autoMode = createMode === 'auto';
  const projectId = selectedProjectId || undefined;

  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [agenticPromptText, setAgenticPromptText] = useState('revenue by segment\nmonthly trend\ncustomer retention');
  const [includeSampleValues, setIncludeSampleValues] = useState(false);
  const [catalog, setCatalog] = useState<DatasetCatalogItem[]>([]);
  const [datasetSearch, setDatasetSearch] = useState('');
  const [datasets, setDatasets] = useState<Ds[]>([]);
  const [expandedDatasetIds, setExpandedDatasetIds] = useState<string[]>([]);
  const [relationships, setRelationships] = useState<Rel[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [agenticSubmitting, setAgenticSubmitting] = useState(false);
  const [agenticJobId, setAgenticJobId] = useState<string | null>(null);
  const [agenticJobStatus, setAgenticJobStatus] = useState<string | null>(null);
  const [agenticProgress, setAgenticProgress] = useState(0);
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (organizationId) setSelectedOrganizationId(organizationId);
  }, [organizationId, setSelectedOrganizationId]);

  useEffect(() => {
    let cancelled = false;
    async function load(): Promise<void> {
      setLoading(true);
      try {
        const cat = await fetchDatasetCatalog(organizationId, projectId);
        if (cancelled) return;
        setCatalog(cat.items || []);
        if (!isEdit) return;
        const model = await fetchSemanticModel(modelId, organizationId);
        if (cancelled) return;
        setName(model.name || '');
        setDescription(model.description || '');
        const parsed = parseYaml(model.contentYaml, cat.items || []);
        setDatasets(parsed.datasets);
        setExpandedDatasetIds(parsed.datasets.map((dataset) => dataset.id));
        setRelationships(parsed.relationships);
      } catch (cause) {
        if (!cancelled) setError(msg(cause, 'Unable to load semantic model builder.'));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void load();
    return () => {
      cancelled = true;
    };
  }, [isEdit, modelId, organizationId, projectId]);

  useEffect(() => {
    setError(null);
    setNotice(null);
    if (createMode !== 'agentic') {
      setAgenticJobId(null);
      setAgenticJobStatus(null);
      setAgenticProgress(0);
    }
  }, [createMode]);

  const selectedIds = useMemo(() => new Set(datasets.map((d) => d.datasetId)), [datasets]);
  const expandedIds = useMemo(() => new Set(expandedDatasetIds), [expandedDatasetIds]);
  const promptCount = useMemo(() => splitPrompts(agenticPromptText).length, [agenticPromptText]);
  const filteredCatalog = useMemo(() => {
    const search = datasetSearch.trim().toLowerCase();
    const visible = !search
      ? catalog
      : catalog.filter((item) => {
          const fields = (item.columns || []).map((column) => column.name.toLowerCase());
          return (
            item.name.toLowerCase().includes(search) ||
            item.sqlAlias.toLowerCase().includes(search) ||
            fields.some((field) => field.includes(search))
          );
        });
    return [...visible].sort((left, right) => {
      const selectedDelta = Number(selectedIds.has(right.id)) - Number(selectedIds.has(left.id));
      if (selectedDelta !== 0) return selectedDelta;
      return left.name.localeCompare(right.name);
    });
  }, [catalog, datasetSearch, selectedIds]);
  const yamlPreview = useMemo(() => yaml.dump(toModel(name, description, datasets, relationships), { noRefs: true, sortKeys: false }), [name, description, datasets, relationships]);

  const toggleDataset = (item: DatasetCatalogItem): void => {
    setError(null);
    setNotice(null);
    if (selectedIds.has(item.id)) {
      const next = datasets.filter((d) => d.datasetId !== item.id);
      const keys = new Set(next.map((d) => d.key));
      setDatasets(next);
      setExpandedDatasetIds((current) => current.filter((id) => next.some((dataset) => dataset.id === id)));
      setRelationships((current) => current.filter((r) => keys.has(r.sourceDataset) && keys.has(r.targetDataset)));
      return;
    }
    const nextDataset = fromCatalog(item, datasets.map((d) => d.key));
    setDatasets((current) => [...current, nextDataset]);
    setExpandedDatasetIds((current) => [...current, nextDataset.id]);
  };

  const patchDataset = (id: string, fn: (d: Ds) => Ds): void => {
    const current = datasets.find((d) => d.id === id);
    if (!current) return;
    const next = fn(current);
    setDatasets((all) => all.map((d) => (d.id === id ? next : d)));
    if (current.key !== next.key) {
      setRelationships((all) => all.map((r) => ({
        ...r,
        sourceDataset: r.sourceDataset === current.key ? next.key : r.sourceDataset,
        targetDataset: r.targetDataset === current.key ? next.key : r.targetDataset,
      })));
    }
  };
  const toggleDatasetExpanded = (id: string): void => {
    setExpandedDatasetIds((current) => (current.includes(id) ? current.filter((value) => value !== id) : [...current, id]));
  };

  const addRelationship = (): void => {
    if (datasets.length < 2) return setError('Select at least two datasets before adding a relationship.');
    const [left, right] = datasets;
    setRelationships((current) => [...current, { id: createClientId(), name: `${left.key}_to_${right.key}`, sourceDataset: left.key, sourceField: left.dimensions[0]?.name || '', targetDataset: right.key, targetField: right.dimensions[0]?.name || '', type: 'many_to_one' }]);
  };

  const refreshDraft = async (): Promise<void> => {
    if (datasets.length === 0) return setError('Select at least one dataset before generating.');
    setGenerating(true);
    setError(null);
    setNotice(null);
    try {
      const resp = await generateSemanticModelYamlFromSelection(organizationId, {
        datasetIds: datasets.map((d) => d.datasetId),
        selectedFields: Object.fromEntries(datasets.map((d) => [d.datasetId, [...d.dimensions.map((f) => f.name), ...d.measures.map((f) => f.name)]])),
        description: description || undefined,
      });
      const parsed = parseYaml(resp.yamlText, catalog);
      setDatasets(parsed.datasets);
      setExpandedDatasetIds(parsed.datasets.map((dataset) => dataset.id));
      setRelationships(parsed.relationships);
      if (resp.warnings.length) setNotice(resp.warnings.join(' '));
    } catch (cause) {
      setError(msg(cause, 'Unable to generate semantic model draft.'));
    } finally {
      setGenerating(false);
    }
  };

  const save = async (): Promise<void> => {
    if (!name.trim()) return setError('Semantic model name is required.');
    if (datasets.length === 0) return setError('Select at least one dataset before saving.');
    setSaving(true);
    setError(null);
    try {
      const payload = {
        projectId,
        connectorId: undefined,
        name: name.trim(),
        description: description.trim() || undefined,
        modelYaml: yamlPreview,
        autoGenerate: false,
        sourceDatasetIds: datasets.map((d) => d.datasetId),
      };
      if (isEdit) await updateSemanticModel(modelId, organizationId, payload);
      else await createSemanticModel(organizationId, { organizationId, ...payload });
      router.push(`/semantic-model/${organizationId}`);
    } catch (cause) {
      setError(msg(cause, 'Unable to save semantic model.'));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (): Promise<void> => {
    if (!isEdit) return;
    setDeleting(true);
    try {
      await deleteSemanticModel(modelId, organizationId);
      router.push(`/semantic-model/${organizationId}`);
    } catch (cause) {
      setError(msg(cause, 'Unable to delete semantic model.'));
    } finally {
      setDeleting(false);
    }
  };

  const startAgentic = async (): Promise<void> => {
    const prompts = splitPrompts(agenticPromptText);
    if (!name.trim()) return setError('Semantic model name is required.');
    if (datasets.length === 0) return setError('Select at least one dataset before starting the agentic builder.');
    if (prompts.length < 3 || prompts.length > 10) return setError('Provide between 3 and 10 prompts for the agentic builder.');

    setAgenticSubmitting(true);
    setError(null);
    setNotice('Agentic semantic model generation queued. You will be redirected into the editor when it completes.');
    try {
      const start = await startAgenticSemanticModelJob(organizationId, {
        projectId,
        name: name.trim(),
        description: description.trim() || undefined,
        datasetIds: datasets.map((dataset) => dataset.datasetId),
        questionPrompts: prompts,
        includeSampleValues,
      });
      setAgenticJobId(start.jobId);
      setAgenticJobStatus(start.jobStatus);

      let terminalStatus: string | null = start.jobStatus;
      for (let attempt = 0; attempt < 180; attempt += 1) {
        const state = await fetchAgentJobState(organizationId, start.jobId);
        setAgenticProgress(state.progress || 0);
        setAgenticJobStatus(state.status || null);
        if (isTerminalJobStatus(state.status)) {
          terminalStatus = state.status;
          break;
        }
        await sleep(2000);
      }

      if (terminalStatus !== 'succeeded') {
        throw new Error('Agentic semantic model generation did not finish successfully.');
      }
      router.push(`/semantic-model/${organizationId}/create?modelId=${start.semanticModelId}`);
    } catch (cause) {
      setNotice(null);
      setError(msg(cause, 'Unable to start agentic semantic model generation.'));
    } finally {
      setAgenticSubmitting(false);
    }
  };

  const builderTitle = isEdit
    ? 'Edit semantic model'
    : autoMode
      ? 'Create autogenerated semantic model'
      : 'Create semantic model';
  const builderSubtitle = autoMode
    ? 'Select governed datasets, generate the first draft automatically, then refine fields and relationships before saving.'
    : 'Build the model from governed datasets, then define guided relationships with source and target field dropdowns.';

  if (loading) return <main className="p-8 text-sm text-[color:var(--text-muted)]">Loading semantic model builder...</main>;

  if (!isEdit && createMode === 'select') {
    return (
      <PageFrame>
        <section className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft backdrop-blur">
          <div className="space-y-3">
            <Badge variant="secondary">Semantic model creation</Badge>
            <h1 className="text-3xl font-semibold">Choose how to start</h1>
            <p className="max-w-3xl text-sm text-[color:var(--text-muted)]">
              Keep the dataset-first architecture, but choose whether to begin from a generated draft, build manually, or let the agentic flow expand the model from example questions.
            </p>
          </div>
        </section>

        <section className="grid gap-6 lg:grid-cols-3">
          <CreateModeCard
            badge="Autogenerated"
            title="Seed a draft from datasets"
            description="Pick governed datasets first, then start from an automatically generated semantic draft that you can refine."
            action="Start autogenerated flow"
            icon={<Sparkles className="h-5 w-5 text-[color:var(--accent)]" />}
            onClick={() => router.push(`/semantic-model/${organizationId}/create?mode=auto`)}
          />
          <CreateModeCard
            badge="Manual"
            title="Open the manual builder"
            description="Select datasets and define dimensions, measures, calculated fields, and relationships directly."
            action="Start manual flow"
            onClick={() => router.push(`/semantic-model/${organizationId}/create?mode=manual`)}
          />
          <CreateModeCard
            badge="Agentic"
            title="Use the agentic builder"
            description="Create a dataset-backed draft, then let Langbridge enrich it using example analytical prompts."
            action="Start agentic flow"
            icon={<Bot className="h-5 w-5 text-[color:var(--accent)]" />}
            onClick={() => router.push(`/semantic-model/${organizationId}/create?mode=agentic`)}
          />
        </section>
      </PageFrame>
    );
  }

  if (!isEdit && createMode === 'agentic') {
    return (
      <PageFrame>
        <section className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft backdrop-blur">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="secondary">Dataset-first semantic model</Badge>
                <Badge variant="secondary">Agentic</Badge>
              </div>
              <h1 className="text-3xl font-semibold">Create semantic model with agentics</h1>
              <p className="max-w-3xl text-sm text-[color:var(--text-muted)]">
                Start from governed datasets, then let the agentic flow refine the semantic model from the analytical prompts you provide.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button variant="outline" onClick={() => router.push(`/semantic-model/${organizationId}/create`)}>Modes</Button>
              <Button variant="outline" onClick={() => router.push(`/semantic-model/${organizationId}/create?mode=manual`)}>Manual builder</Button>
              <Button onClick={() => void startAgentic()} disabled={agenticSubmitting}>{agenticSubmitting ? 'Running agentic flow...' : 'Start agentic builder'}</Button>
            </div>
          </div>
          <div className="mt-5 grid gap-4 md:grid-cols-2">
            <div className="space-y-2"><Label>Name</Label><Input value={name} onChange={(e) => setName(e.target.value)} placeholder="Executive revenue model" /></div>
            <div className="space-y-2"><Label>Description</Label><Input value={description} onChange={(e) => setDescription(e.target.value)} placeholder="Dataset-backed semantic layer for revenue and customers" /></div>
          </div>
          {notice ? <p className="mt-4 rounded-2xl border border-emerald-500/20 bg-[color:var(--success-soft)] px-4 py-3 text-sm text-[color:var(--text-secondary)]">{notice}</p> : null}
          {error ? <p className="mt-4 rounded-2xl border border-rose-500/20 bg-[color:var(--danger-soft)] px-4 py-3 text-sm text-[color:var(--text-secondary)]">{error}</p> : null}
        </section>

        <section className="grid gap-6 lg:grid-cols-[1.04fr_0.96fr]">
          <div className="space-y-6">
            <Card title="Agentic prompts" subtitle="Provide 3 to 10 example questions so the agent knows what this semantic model must answer well.">
              <div className="space-y-4">
                <div className="flex items-center justify-between gap-3 text-xs text-[color:var(--text-muted)]">
                  <span>One prompt per line</span>
                  <span>{promptCount} prompts</span>
                </div>
                <Textarea value={agenticPromptText} onChange={(event) => setAgenticPromptText(event.target.value)} placeholder={'revenue by segment\nmonthly trend\ncustomer retention'} className="min-h-[220px] bg-[color:var(--surface-muted)]" />
                <label className="flex items-center gap-3 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-3 text-sm text-[color:var(--text-secondary)]">
                  <input type="checkbox" checked={includeSampleValues} onChange={(event) => setIncludeSampleValues(event.target.checked)} className="h-4 w-4 rounded border-[color:var(--border-strong)]" />
                  Include sample values when the agent drafts semantic fields
                </label>
              </div>
            </Card>

            <DatasetSelectionCard
              title="Source datasets"
              subtitle="Choose the governed datasets that should anchor the agentic semantic model draft."
              catalog={catalog}
              datasetSearch={datasetSearch}
              onDatasetSearchChange={setDatasetSearch}
              filteredCatalog={filteredCatalog}
              selectedIds={selectedIds}
              onToggleDataset={toggleDataset}
            />
          </div>

          <div className="space-y-6">
            <Card title="Selected datasets" subtitle="The agentic builder will start from these dataset bindings before refining the model.">
              <div className="space-y-3">{datasets.length ? datasets.map((d) => <div key={d.id} className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"><div className="flex items-center justify-between"><div><p className="text-sm font-semibold">{d.key}</p><p className="text-xs text-[color:var(--text-muted)]">{d.datasetName}</p></div><div className="text-xs text-[color:var(--text-muted)]">{d.dimensions.length} dimensions / {d.measures.length} measures</div></div></div>) : <Empty text="Select one or more datasets to start the agentic draft." />}</div>
            </Card>
            <Card title="Guidance" subtitle="Keep prompts concrete so the agent can infer dimensions, measures, and naming cleanly.">
              <div className="space-y-3 text-sm text-[color:var(--text-secondary)]">
                <p>Use prompts that reflect the analyses people actually want to run, not generic descriptions.</p>
                <p>Include a mix of metric, trend, and breakdown questions so the agent sees the shape of the model you want.</p>
                <p>When the job completes, the generated model opens in the standard editor for review and cleanup.</p>
              </div>
            </Card>
            {agenticJobStatus ? (
              <Card title="Job status" subtitle="The agentic worker updates progress here while the draft is being refined.">
                <div className="space-y-3">
                  <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-3">
                    <div className="flex items-center justify-between gap-3 text-sm">
                      <span className="font-medium capitalize">{agenticJobStatus}</span>
                      <span className="text-[color:var(--text-muted)]">{agenticProgress}%</span>
                    </div>
                    <div className="mt-3 h-2 overflow-hidden rounded-full bg-[color:var(--surface-muted)]">
                      <div className="h-full rounded-full bg-[color:var(--accent)] transition-[width]" style={{ width: `${Math.max(4, agenticProgress)}%` }} />
                    </div>
                    {agenticJobId ? <p className="mt-3 text-xs text-[color:var(--text-muted)]">Job ID: {agenticJobId}</p> : null}
                  </div>
                </div>
              </Card>
            ) : null}
          </div>
        </section>
      </PageFrame>
    );
  }

  return (
    <PageFrame>
        <section className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft backdrop-blur">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="secondary">Dataset-first semantic model</Badge>
                {!isEdit && autoMode ? <Badge variant="secondary">Autogenerated</Badge> : null}
                {!isEdit && !autoMode ? <Badge variant="secondary">Manual</Badge> : null}
              </div>
              <h1 className="text-3xl font-semibold">{builderTitle}</h1>
              <p className="max-w-3xl text-sm text-[color:var(--text-muted)]">{builderSubtitle}</p>
            </div>
            <div className="flex flex-wrap gap-2">
              {!isEdit ? <Button variant="outline" onClick={() => router.push(`/semantic-model/${organizationId}/create`)}>Modes</Button> : null}
              <Button variant="outline" onClick={() => router.push(`/semantic-model/${organizationId}`)}>Back</Button>
              <Button variant="outline" onClick={() => void refreshDraft()} disabled={generating || datasets.length === 0}>{generating ? 'Refreshing...' : autoMode ? 'Generate draft from datasets' : 'Auto-populate from datasets'}</Button>
              <Button onClick={() => void save()} disabled={saving}>{saving ? 'Saving...' : isEdit ? 'Update model' : 'Save model'}</Button>
            </div>
          </div>
          <div className="mt-5 grid gap-4 md:grid-cols-2">
            <div className="space-y-2"><Label>Name</Label><Input value={name} onChange={(e) => setName(e.target.value)} placeholder="Executive revenue model" /></div>
            <div className="space-y-2"><Label>Description</Label><Input value={description} onChange={(e) => setDescription(e.target.value)} placeholder="Dataset-backed semantic layer for revenue and customers" /></div>
          </div>
          {!isEdit && autoMode ? <p className="mt-4 rounded-2xl border border-[color:var(--accent)] bg-[color:var(--accent-soft)] px-4 py-3 text-sm text-[color:var(--text-secondary)]">Select governed datasets first, then use the generated draft as your starting point for semantic cleanup.</p> : null}
          {notice ? <p className="mt-4 rounded-2xl border border-emerald-500/20 bg-[color:var(--success-soft)] px-4 py-3 text-sm text-[color:var(--text-secondary)]">{notice}</p> : null}
          {error ? <p className="mt-4 rounded-2xl border border-rose-500/20 bg-[color:var(--danger-soft)] px-4 py-3 text-sm text-[color:var(--text-secondary)]">{error}</p> : null}
        </section>

        <section className="grid gap-6 lg:grid-cols-[1.08fr_0.92fr]">
          <div className="space-y-6">
            <DatasetSelectionCard
              title="Source datasets"
              subtitle="Choose the governed datasets that feed this semantic model."
              catalog={catalog}
              datasetSearch={datasetSearch}
              onDatasetSearchChange={setDatasetSearch}
              filteredCatalog={filteredCatalog}
              selectedIds={selectedIds}
              onToggleDataset={toggleDataset}
            />

            <Card title="Semantic datasets" subtitle="Define dimensions, measures, and calculated fields on top of the selected datasets.">
              <div className="space-y-5">
                {datasets.map((d) => (
                  <div key={d.id} className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                    <button type="button" onClick={() => toggleDatasetExpanded(d.id)} className="flex w-full items-center justify-between gap-3 text-left">
                      <div className="flex min-w-0 items-center gap-3">
                        <ChevronDown className={`h-4 w-4 shrink-0 text-[color:var(--text-muted)] transition-transform ${expandedIds.has(d.id) ? 'rotate-0' : '-rotate-90'}`} />
                        <div className="min-w-0">
                          <p className="truncate text-sm font-semibold">{d.key}</p>
                          <p className="truncate text-xs text-[color:var(--text-muted)]">{d.datasetName}</p>
                        </div>
                      </div>
                      <div className="shrink-0 text-right text-xs text-[color:var(--text-muted)]">{d.dimensions.length} dimensions / {d.measures.length} measures</div>
                    </button>
                    {expandedIds.has(d.id) ? (
                      <div className="mt-4 border-t border-[color:var(--panel-border)] pt-4">
                        <div className="grid gap-3 md:grid-cols-2">
                          <div className="space-y-2"><Label>Semantic key</Label><Input value={d.key} onChange={(e) => patchDataset(d.id, (current) => ({ ...current, key: sanitize(e.target.value) || current.key }))} /></div>
                          <div className="space-y-2"><Label>Source dataset</Label><Input value={d.datasetName} disabled /></div>
                        </div>
                        <div className="mt-3 space-y-2"><Label>Description</Label><Input value={d.description} onChange={(e) => patchDataset(d.id, (current) => ({ ...current, description: e.target.value }))} /></div>
                        <FieldSection title="Dimensions" action="Add dimension" onAdd={() => patchDataset(d.id, (c) => ({ ...c, dimensions: [...c.dimensions, { id: createClientId(), name: '', expression: '', type: 'string', primaryKey: false }] }))}>
                          {d.dimensions.map((f) => (
                            <FieldRow key={f.id} name={f.name} type={f.type} expression={f.expression || ''} onType={(value) => patchDataset(d.id, (c) => ({ ...c, dimensions: c.dimensions.map((x) => x.id === f.id ? { ...x, type: value } : x) }))} onName={(value) => patchDataset(d.id, (c) => ({ ...c, dimensions: c.dimensions.map((x) => x.id === f.id ? { ...x, name: value } : x) }))} onExpression={(value) => patchDataset(d.id, (c) => ({ ...c, dimensions: c.dimensions.map((x) => x.id === f.id ? { ...x, expression: value } : x) }))} onRemove={() => patchDataset(d.id, (c) => ({ ...c, dimensions: c.dimensions.filter((x) => x.id !== f.id) }))} />
                          ))}
                        </FieldSection>
                        <FieldSection title="Measures" action="Add measure" onAdd={() => patchDataset(d.id, (c) => ({ ...c, measures: [...c.measures, { id: createClientId(), name: '', expression: '', type: 'decimal', aggregation: 'sum' }] }))}>
                          {d.measures.map((f) => (
                            <FieldRow key={f.id} name={f.name} type={f.type} expression={f.expression || ''} onType={(value) => patchDataset(d.id, (c) => ({ ...c, measures: c.measures.map((x) => x.id === f.id ? { ...x, type: value } : x) }))} onName={(value) => patchDataset(d.id, (c) => ({ ...c, measures: c.measures.map((x) => x.id === f.id ? { ...x, name: value } : x) }))} onExpression={(value) => patchDataset(d.id, (c) => ({ ...c, measures: c.measures.map((x) => x.id === f.id ? { ...x, expression: value } : x) }))} onRemove={() => patchDataset(d.id, (c) => ({ ...c, measures: c.measures.filter((x) => x.id !== f.id) }))} />
                          ))}
                        </FieldSection>
                      </div>
                    ) : null}
                  </div>
                ))}
                {datasets.length === 0 ? <Empty text="Select one or more datasets to start defining semantic fields." /> : null}
              </div>
            </Card>

            <Card title="Relationships" subtitle="Create joins with guided dataset and field dropdowns.">
              <div className="mb-4 flex justify-end"><Button variant="outline" onClick={addRelationship} disabled={datasets.length < 2}>Add relationship</Button></div>
              <div className="space-y-4">
                {relationships.map((r) => {
                  const left = datasets.find((d) => d.key === r.sourceDataset);
                  const right = datasets.find((d) => d.key === r.targetDataset);
                  return (
                    <div key={r.id} className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                      <div className="grid gap-3 xl:grid-cols-5">
                        <Input value={r.name} onChange={(e) => setRelationships((all) => all.map((x) => x.id === r.id ? { ...x, name: e.target.value } : x))} placeholder="orders_to_customers" />
                        <SelectBox value={r.sourceDataset} onChange={(value) => setRelationships((all) => all.map((x) => x.id === r.id ? { ...x, sourceDataset: value, sourceField: '' } : x))} placeholder="Source dataset" options={datasets.map((d) => d.key)} />
                        <SelectBox value={r.sourceField} onChange={(value) => setRelationships((all) => all.map((x) => x.id === r.id ? { ...x, sourceField: value } : x))} placeholder="Source field" options={(left?.dimensions || []).map((f) => f.name).filter(Boolean)} />
                        <SelectBox value={r.targetDataset} onChange={(value) => setRelationships((all) => all.map((x) => x.id === r.id ? { ...x, targetDataset: value, targetField: '' } : x))} placeholder="Target dataset" options={datasets.map((d) => d.key)} />
                        <SelectBox value={r.targetField} onChange={(value) => setRelationships((all) => all.map((x) => x.id === r.id ? { ...x, targetField: value } : x))} placeholder="Target field" options={(right?.dimensions || []).map((f) => f.name).filter(Boolean)} />
                      </div>
                      <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                        <SelectBox value={r.type} onChange={(value) => setRelationships((all) => all.map((x) => x.id === r.id ? { ...x, type: value } : x))} placeholder="Relationship type" options={REL_TYPES} />
                        <p className="text-sm text-[color:var(--text-muted)]">{r.sourceDataset && r.sourceField && r.targetDataset && r.targetField ? `${r.sourceDataset}.${r.sourceField} = ${r.targetDataset}.${r.targetField}` : 'Select both datasets and fields to generate the relationship expression.'}</p>
                        <Button variant="ghost" onClick={() => setRelationships((all) => all.filter((x) => x.id !== r.id))}>Remove</Button>
                      </div>
                    </div>
                  );
                })}
                {relationships.length === 0 ? <Empty text="No relationships yet. Add one to guide multi-dataset analysis." /> : null}
              </div>
            </Card>
          </div>

          <div className="space-y-6">
            <Card title="Outline" subtitle="The datasets and relationships included in this semantic model.">
              <div className="space-y-3">{datasets.length ? datasets.map((d) => <div key={d.id} className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"><div className="flex items-center justify-between"><div><p className="text-sm font-semibold">{d.key}</p><p className="text-xs text-[color:var(--text-muted)]">{d.datasetName}</p></div><div className="text-xs text-[color:var(--text-muted)]">{d.dimensions.length} dimensions / {d.measures.length} measures</div></div></div>) : <Empty text="No datasets selected yet." />}</div>
            </Card>
            <Card title="Generated YAML" subtitle="Saved directly from the dataset-backed builder state.">
              <Textarea value={yamlPreview} readOnly className="min-h-[720px] bg-[color:var(--surface-muted)] font-mono text-xs" />
            </Card>
            {isEdit ? <Card title="Delete semantic model" subtitle="Remove this semantic model and its lineage registration."><Button variant="destructive" onClick={() => void remove()} disabled={deleting}>{deleting ? 'Deleting...' : 'Delete model'}</Button></Card> : null}
          </div>
        </section>
    </PageFrame>
  );
}

function PageFrame({ children }: { children: JSX.Element | JSX.Element[] }): JSX.Element {
  return (
    <main className="relative min-h-screen overflow-hidden bg-[color:var(--shell-bg)] px-4 py-8 text-[color:var(--text-primary)] transition-colors">
      <div className="pointer-events-none absolute inset-0 -z-20 bg-[radial-gradient(circle_at_top_left,_var(--accent-soft),_transparent_36%),radial-gradient(circle_at_top_right,_rgba(59,130,246,0.14),_transparent_32%)]" />
      <div className="mx-auto flex max-w-6xl flex-col gap-6">{children}</div>
    </main>
  );
}

function CreateModeCard({
  badge,
  title,
  description,
  action,
  onClick,
  icon,
}: {
  badge: string;
  title: string;
  description: string;
  action: string;
  onClick: () => void;
  icon?: JSX.Element;
}): JSX.Element {
  return (
    <section className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
      <div className="flex items-start justify-between gap-3">
        <Badge variant="secondary">{badge}</Badge>
        {icon}
      </div>
      <h2 className="mt-5 text-xl font-semibold">{title}</h2>
      <p className="mt-2 text-sm text-[color:var(--text-muted)]">{description}</p>
      <Button className="mt-6" onClick={onClick}>{action}</Button>
    </section>
  );
}

function Card({ title, subtitle, children }: { title: string; subtitle: string; children: JSX.Element | JSX.Element[] }): JSX.Element {
  return <section className="rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft"><h2 className="text-xl font-semibold">{title}</h2><p className="mt-1 text-sm text-[color:var(--text-muted)]">{subtitle}</p><div className="mt-5">{children}</div></section>;
}

function Empty({ text }: { text: string }): JSX.Element {
  return <p className="rounded-2xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-6 text-sm text-[color:var(--text-muted)]">{text}</p>;
}

function DatasetSelectionCard({
  title,
  subtitle,
  catalog,
  datasetSearch,
  onDatasetSearchChange,
  filteredCatalog,
  selectedIds,
  onToggleDataset,
}: {
  title: string;
  subtitle: string;
  catalog: DatasetCatalogItem[];
  datasetSearch: string;
  onDatasetSearchChange: (value: string) => void;
  filteredCatalog: DatasetCatalogItem[];
  selectedIds: Set<string>;
  onToggleDataset: (item: DatasetCatalogItem) => void;
}): JSX.Element {
  return (
    <Card title={title} subtitle={subtitle}>
      <div className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="relative min-w-[240px] flex-1">
            <Search className="pointer-events-none absolute left-3 top-3 h-4 w-4 text-[color:var(--text-muted)]" />
            <Input value={datasetSearch} onChange={(event) => onDatasetSearchChange(event.target.value)} placeholder="Search datasets by name, alias, or field" className="pl-9" />
          </div>
          <p className="text-xs text-[color:var(--text-muted)]">{filteredCatalog.length} of {catalog.length} datasets</p>
        </div>
        <div className="max-h-[420px] space-y-3 overflow-y-auto pr-1">
          {filteredCatalog.map((item) => (
            <button key={item.id} type="button" onClick={() => onToggleDataset(item)} className={`w-full rounded-2xl border px-4 py-4 text-left shadow-sm transition-colors ${selectedIds.has(item.id) ? 'border-[color:var(--accent)] bg-[color:var(--accent-soft)] text-[color:var(--text-primary)]' : 'border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] text-[color:var(--text-primary)] hover:border-[color:var(--border-strong)] hover:bg-[color:var(--panel-bg)]'}`}>
              <div className="flex items-center justify-between gap-3"><div><p className="text-sm font-semibold">{item.name}</p><p className={`text-xs ${selectedIds.has(item.id) ? 'text-[color:var(--text-secondary)]' : 'text-[color:var(--text-muted)]'}`}>{item.sqlAlias}</p></div><span className={`text-xs ${selectedIds.has(item.id) ? 'text-[color:var(--text-secondary)]' : 'text-[color:var(--text-muted)]'}`}>{item.columns.length} fields</span></div>
            </button>
          ))}
          {catalog.length === 0 ? <Empty text="No datasets are available in this workspace yet." /> : null}
          {catalog.length > 0 && filteredCatalog.length === 0 ? <Empty text={`No datasets match "${datasetSearch.trim()}".`} /> : null}
        </div>
      </div>
    </Card>
  );
}

function SelectBox({ value, onChange, placeholder, options }: { value: string; onChange: (value: string) => void; placeholder: string; options: string[] }): JSX.Element {
  return <select value={value} onChange={(e) => onChange(e.target.value)} className="h-10 rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 text-sm text-[color:var(--text-primary)] shadow-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--accent)] focus-visible:ring-offset-2 focus-visible:ring-offset-[color:var(--app-bg)]"><option value="">{placeholder}</option>{options.map((o) => <option key={o} value={o}>{o}</option>)}</select>;
}

function FieldSection({ title, action, onAdd, children }: { title: string; action: string; onAdd: () => void; children: JSX.Element[] }): JSX.Element {
  return <div className="mt-5"><div className="mb-3 flex items-center justify-between"><h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[color:var(--text-muted)]">{title}</h3><Button variant="outline" size="sm" onClick={onAdd}>{action}</Button></div><div className="space-y-3">{children.length ? children : [<Empty key="empty" text={`No ${title.toLowerCase()} yet.`} />]}</div></div>;
}

function FieldRow({ name, type, expression, onName, onType, onExpression, onRemove }: { name: string; type: string; expression: string; onName: (value: string) => void; onType: (value: string) => void; onExpression: (value: string) => void; onRemove: () => void }): JSX.Element {
  return <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-3 shadow-sm"><div className="grid gap-3 md:grid-cols-[1.1fr_1fr_140px_auto]"><Input value={name} onChange={(e) => onName(e.target.value)} placeholder="field_name" /><Input value={expression} onChange={(e) => onExpression(e.target.value)} placeholder="expression" /><SelectBox value={type} onChange={onType} placeholder="Type" options={FIELD_TYPES} /><Button variant="ghost" onClick={onRemove}>Remove</Button></div></div>;
}

function fromCatalog(item: DatasetCatalogItem, keys: string[]): Ds {
  const dims: Dim[] = [];
  const meas: Mea[] = [];
  for (const col of item.columns || []) {
    const type = normalize(col.dataType);
    const pk = Boolean(col.primaryKey || isPk(col.name, item.name));
    const idish = col.name === 'id' || col.name.endsWith('_id');
    if (['integer', 'decimal', 'float'].includes(type) && !pk && !idish) meas.push({ id: createClientId(), name: col.name, expression: col.name, type, aggregation: 'sum' });
    else dims.push({ id: createClientId(), name: col.name, expression: col.name, type, primaryKey: pk });
  }
  return { id: createClientId(), datasetId: item.id, datasetName: item.name, key: uniqueKey(item.sqlAlias || item.name || 'dataset', keys), description: '', dimensions: dims, measures: meas };
}

function parseYaml(text: string, catalog: DatasetCatalogItem[]): { datasets: Ds[]; relationships: Rel[] } {
  const raw = yaml.load(text);
  if (!record(raw)) return { datasets: [], relationships: [] };
  const dsPayload = record(raw.datasets) ? raw.datasets : record(raw.tables) ? raw.tables : {};
  const datasets = Object.entries(dsPayload).map(([key, value]) => {
    const item = record(value) ? value : {};
    const datasetId = str(item.dataset_id) || str(item.datasetId) || '';
    const match = catalog.find((entry) => entry.id === datasetId);
    return { id: createClientId(), datasetId, datasetName: match?.name || str(item.relation_name) || key, key, description: str(item.description) || '', dimensions: parseDims(item.dimensions), measures: parseMeas(item.measures) };
  });
  const rels = Array.isArray(raw.relationships) ? raw.relationships.map(parseRel).filter(Boolean) as Rel[] : [];
  return { datasets, relationships: rels };
}

function parseDims(value: unknown): Dim[] { return Array.isArray(value) ? value.map((x) => record(x) ? { id: createClientId(), name: str(x.name) || '', expression: str(x.expression) || '', type: str(x.type) || 'string', primaryKey: Boolean(x.primary_key ?? x.primaryKey) } : null).filter(Boolean) as Dim[] : []; }
function parseMeas(value: unknown): Mea[] { return Array.isArray(value) ? value.map((x) => record(x) ? { id: createClientId(), name: str(x.name) || '', expression: str(x.expression) || '', type: str(x.type) || 'decimal', aggregation: str(x.aggregation) || 'sum' } : null).filter(Boolean) as Mea[] : []; }

function parseRel(value: unknown): Rel | null {
  if (!record(value)) return null;
  const join = str(value.join_on) || str(value.on) || '';
  let sourceDataset = str(value.source_dataset) || str(value.from_) || str(value.from) || '';
  let targetDataset = str(value.target_dataset) || str(value.to) || '';
  let sourceField = str(value.source_field) || '';
  let targetField = str(value.target_field) || '';
  if ((!sourceField || !targetField) && join.includes('=')) {
    const [left, right] = join.split('=').map((p) => p.trim());
    const [ld, lf] = left.split('.');
    const [rd, rf] = right.split('.');
    sourceDataset ||= ld || '';
    targetDataset ||= rd || '';
    sourceField ||= lf || '';
    targetField ||= rf || '';
  }
  return { id: createClientId(), name: str(value.name) || `${sourceDataset}_to_${targetDataset}`, sourceDataset, sourceField, targetDataset, targetField, type: str(value.type) || 'many_to_one' };
}

function toModel(name: string, description: string, datasets: Ds[], relationships: Rel[]): Record<string, unknown> {
  return {
    version: '1.0',
    name: name.trim() || undefined,
    description: description.trim() || undefined,
    datasets: Object.fromEntries(
      datasets.map((d) => [
        d.key,
        {
          dataset_id: d.datasetId,
          relation_name: d.key,
          description: d.description || undefined,
          dimensions: d.dimensions
            .filter((f) => f.name.trim())
            .map((f) => ({
              name: f.name.trim(),
              expression: f.expression?.trim() || f.name.trim(),
              type: f.type,
              primary_key: Boolean(f.primaryKey),
            })),
          measures: d.measures
            .filter((f) => f.name.trim())
            .map((f) => ({
              name: f.name.trim(),
              expression: f.expression?.trim() || f.name.trim(),
              type: f.type,
              aggregation: f.aggregation || 'sum',
            })),
        },
      ]),
    ),
    relationships: relationships
      .filter((r) => r.name.trim() && r.sourceDataset && r.sourceField && r.targetDataset && r.targetField)
      .map((r) => ({
        name: r.name.trim(),
        source_dataset: r.sourceDataset,
        source_field: r.sourceField,
        target_dataset: r.targetDataset,
        target_field: r.targetField,
        type: r.type,
      })),
  };
}

function splitPrompts(value: string): string[] { return value.split(/\r?\n/).map((entry) => entry.trim()).filter(Boolean); }
function isTerminalJobStatus(status: string | null | undefined): boolean { return status === 'succeeded' || status === 'failed' || status === 'cancelled'; }
async function sleep(ms: number): Promise<void> { await new Promise((resolve) => window.setTimeout(resolve, ms)); }
function uniqueKey(value: string, existing: string[]): string { const root = sanitize(value) || 'dataset'; let next = root; let i = 2; while (existing.includes(next)) { next = `${root}_${i}`; i += 1; } return next; }
function sanitize(value: string): string { return value.trim().toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, ''); }
function normalize(value: string): string { const raw = value.toLowerCase(); if (raw.includes('int') && !raw.includes('point')) return 'integer'; if (raw.includes('float') || raw.includes('double') || raw.includes('real')) return 'float'; if (raw.includes('decimal') || raw.includes('numeric') || raw.includes('number') || raw.includes('bigint')) return 'decimal'; if (raw.includes('bool')) return 'boolean'; if (raw.includes('date') || raw.includes('time')) return 'date'; return 'string'; }
function isPk(column: string, dataset: string): boolean { const d = dataset.toLowerCase().replace(/[^a-z0-9]/g, ''); const c = column.toLowerCase(); return c === 'id' || c === `${d}id` || c === `${d}_id`; }
function str(value: unknown): string | null { return typeof value === 'string' && value.trim() ? value.trim() : null; }
function record(value: unknown): value is Record<string, unknown> { return Boolean(value) && typeof value === 'object' && !Array.isArray(value); }
function msg(cause: unknown, fallback: string): string { if (cause instanceof ApiError) return cause.message || fallback; if (cause instanceof Error && cause.message) return cause.message; return fallback; }
