'use client';

import { FormEvent, JSX, useCallback, useEffect, useMemo, useState } from 'react';
import yaml from 'js-yaml';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { cn, createClientId, formatRelativeDate } from '@/lib/utils';
import { ApiError } from '@/orchestration/http';
import {
  createSemanticModel,
  listSemanticModels,
  updateSemanticModel,
} from '@/orchestration/semanticModels';
import type { SemanticModelRecord } from '@/orchestration/semanticModels/types';
import { runUnifiedSemanticQuery } from '@/orchestration/semanticQuery';
import type {
  UnifiedSemanticRelationshipPayload,
  UnifiedSemanticQueryResponse,
  UnifiedSemanticSourceModelPayload,
} from '@/orchestration/semanticQuery/types';

type UnifiedSemanticModelPageProps = {
  params: { organizationId: string };
};

type JoinType = 'inner' | 'left' | 'right' | 'full';
type JoinOperator = '=' | '!=' | '>' | '>=' | '<' | '<=';

interface FormState {
  name: string;
  description: string;
  version: string;
}

interface StructuredJoinDraft {
  id: string;
  name: string;
  type: JoinType;
  leftOptionId: string;
  leftColumn: string;
  operator: JoinOperator;
  rightOptionId: string;
  rightColumn: string;
}

interface UnifiedMetricDraft {
  id: string;
  name: string;
  expression: string;
  description: string;
}

interface TableOption {
  optionId: string;
  tableKey: string;
  modelId: string;
  modelName: string;
  modelAlias: string;
  columns: string[];
}

const DEFAULT_VERSION = '1.0';
const DEFAULT_PREVIEW_QUERY = '{\n  "measures": [],\n  "dimensions": [],\n  "limit": 25\n}';
const JOIN_TYPES: JoinType[] = ['inner', 'left', 'right', 'full'];
const JOIN_OPERATORS: JoinOperator[] = ['=', '!=', '>', '>=', '<', '<='];

type UnifiedModelPersistencePayload = {
  projectId?: string;
  connectorId?: string | null;
  name: string;
  description?: string;
  modelYaml: string;
  autoGenerate: false;
  sourceDatasetIds?: string[];
};

export default function UnifiedSemanticModelPage({
  params,
}: UnifiedSemanticModelPageProps): JSX.Element {
  const {
    selectedOrganizationId,
    selectedProjectId,
    organizations,
    loading: scopeLoading,
    setSelectedOrganizationId,
  } = useWorkspaceScope();
  const organizationId = params.organizationId;

  useEffect(() => {
    if (organizationId && organizationId !== selectedOrganizationId) {
      setSelectedOrganizationId(organizationId);
    }
  }, [organizationId, selectedOrganizationId, setSelectedOrganizationId]);

  const [sourceModels, setSourceModels] = useState<SemanticModelRecord[]>([]);
  const [unifiedModels, setUnifiedModels] = useState<SemanticModelRecord[]>([]);
  const [sourceLoading, setSourceLoading] = useState(false);
  const [unifiedLoading, setUnifiedLoading] = useState(false);

  const [selectedUnifiedModelId, setSelectedUnifiedModelId] = useState<string | null>(null);
  const [selectedModelIds, setSelectedModelIds] = useState<string[]>([]);
  const [joinDrafts, setJoinDrafts] = useState<StructuredJoinDraft[]>([]);
  const [sourceModelOverrides, setSourceModelOverrides] = useState<
    Record<string, UnifiedSemanticSourceModelPayload>
  >({});
  const [metrics, setMetrics] = useState<UnifiedMetricDraft[]>([]);
  const [formState, setFormState] = useState<FormState>({
    name: '',
    description: '',
    version: DEFAULT_VERSION,
  });

  const [previewQueryJson, setPreviewQueryJson] = useState(DEFAULT_PREVIEW_QUERY);
  const [previewResult, setPreviewResult] = useState<UnifiedSemanticQueryResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const [saveLoading, setSaveLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const hasOrganization = Boolean(organizationId);

  const selectedSourceModels = useMemo(
    () => sourceModels.filter((model) => selectedModelIds.includes(model.id)),
    [sourceModels, selectedModelIds],
  );

  const selectedUnifiedModel = useMemo(
    () => unifiedModels.find((model) => model.id === selectedUnifiedModelId) ?? null,
    [unifiedModels, selectedUnifiedModelId],
  );

  const sourceModelPayload = useMemo(
    () => buildUnifiedSourceModels(selectedSourceModels, sourceModelOverrides),
    [selectedSourceModels, sourceModelOverrides],
  );

  const organizationName = useMemo(() => {
    if (!organizationId) {
      return 'Select an organization';
    }
    return organizations.find((org) => org.id === organizationId)?.name ?? 'Unknown organization';
  }, [organizations, organizationId]);

  const tableOptions = useMemo<TableOption[]>(() => {
    return buildTableOptions(selectedSourceModels, sourceModelPayload);
  }, [selectedSourceModels, sourceModelPayload]);

  const tableOptionLookup = useMemo(() => {
    const lookup = new Map<string, TableOption>();
    tableOptions.forEach((option) => {
      lookup.set(option.optionId, option);
    });
    return lookup;
  }, [tableOptions]);
  const loadSourceModels = useCallback(async () => {
    if (!organizationId) {
      setSourceModels([]);
      return;
    }
    setSourceLoading(true);
    try {
      const models = await listSemanticModels(
        organizationId,
        selectedProjectId ?? undefined,
        'standard',
      );
      setSourceModels(models);
    } catch (loadError) {
      setError(resolveError(loadError));
    } finally {
      setSourceLoading(false);
    }
  }, [organizationId, selectedProjectId]);

  const loadUnifiedModels = useCallback(async () => {
    if (!organizationId) {
      setUnifiedModels([]);
      return;
    }
    setUnifiedLoading(true);
    try {
      const models = await listSemanticModels(
        organizationId,
        selectedProjectId ?? undefined,
        'unified',
      );
      setUnifiedModels(models);
    } catch (loadError) {
      setError(resolveError(loadError));
    } finally {
      setUnifiedLoading(false);
    }
  }, [organizationId, selectedProjectId]);

  useEffect(() => {
    if (!organizationId) {
      setSourceModels([]);
      setUnifiedModels([]);
      return;
    }
    void loadSourceModels();
    void loadUnifiedModels();
  }, [organizationId, loadSourceModels, loadUnifiedModels]);

  useEffect(() => {
    const sourceModelIds = new Set(sourceModels.map((model) => model.id));
    setSelectedModelIds((current) => current.filter((id) => sourceModelIds.has(id)));
  }, [sourceModels]);

  useEffect(() => {
    setPreviewResult(null);
    setPreviewError(null);
  }, [selectedModelIds, joinDrafts, metrics]);

  const unifiedYamlPreview = useMemo(() => {
    try {
      const payload = buildUnifiedPayload({
        formState,
        sourceModels: sourceModelPayload,
        joinDrafts,
        tableOptionLookup,
        metrics,
      });
      return {
        yaml: yaml.dump(payload, { noRefs: true, sortKeys: false }),
        error: null,
      };
    } catch (previewBuildError) {
      return {
        yaml: '',
        error:
          previewBuildError instanceof Error
            ? previewBuildError.message
            : 'Unable to build unified model YAML.',
      };
    }
  }, [formState, sourceModelPayload, joinDrafts, metrics, tableOptionLookup]);

  const resetBuilder = useCallback(() => {
    setSelectedUnifiedModelId(null);
    setSelectedModelIds([]);
    setJoinDrafts([]);
    setSourceModelOverrides({});
    setMetrics([]);
    setFormState({ name: '', description: '', version: DEFAULT_VERSION });
    setPreviewQueryJson(DEFAULT_PREVIEW_QUERY);
    setPreviewResult(null);
    setPreviewError(null);
    setNotice(null);
    setError(null);
  }, []);

  const handleToggleModel = (modelId: string) => {
    setSelectedModelIds((current) =>
      current.includes(modelId)
        ? current.filter((id) => id !== modelId)
        : [...current, modelId],
    );
  };

  const addJoinDraft = () => {
    const firstOptionId = tableOptions[0]?.optionId || '';
    const secondOptionId = tableOptions[1]?.optionId || firstOptionId;
    const firstLeftColumn = tableOptionLookup.get(firstOptionId)?.columns[0] || '';
    const firstRightColumn = tableOptionLookup.get(secondOptionId)?.columns[0] || '';
    setJoinDrafts((current) => [
      ...current,
      {
        id: createId('join'),
        name: '',
        type: 'inner',
        leftOptionId: firstOptionId,
        leftColumn: firstLeftColumn,
        operator: '=',
        rightOptionId: secondOptionId,
        rightColumn: firstRightColumn,
      },
    ]);
  };

  const updateJoinDraft = (
    joinId: string,
    updates: Partial<StructuredJoinDraft>,
  ) => {
    setJoinDrafts((current) =>
      current.map((draft) => {
        if (draft.id !== joinId) {
          return draft;
        }
        const next = { ...draft, ...updates };
        if (updates.leftOptionId !== undefined) {
          const leftColumns = tableOptionLookup.get(next.leftOptionId)?.columns || [];
          if (!leftColumns.includes(next.leftColumn)) {
            next.leftColumn = leftColumns[0] || '';
          }
        }
        if (updates.rightOptionId !== undefined) {
          const rightColumns = tableOptionLookup.get(next.rightOptionId)?.columns || [];
          if (!rightColumns.includes(next.rightColumn)) {
            next.rightColumn = rightColumns[0] || '';
          }
        }
        return next;
      }),
    );
  };
  const handleLoadUnifiedModel = (model: SemanticModelRecord) => {
    const payload = safeParseYaml(model.contentYaml || '');
    if (!payload) {
      setError('Selected unified model has invalid YAML content.');
      return;
    }

    const sourceModelIds = readSourceModelIds(payload);
    const availableIds = new Set(sourceModels.map((entry) => entry.id));
    const normalizedSourceIds = sourceModelIds.filter((id) => availableIds.has(id));
    const loadedSourceModels = parseUnifiedSourceModels(payload);
    const loadedSourceModelLookup = loadedSourceModels.reduce<
      Record<string, UnifiedSemanticSourceModelPayload>
    >((acc, entry) => {
      acc[entry.id] = entry;
      return acc;
    }, {});

    const relationshipPayload = Array.isArray(payload.relationships) ? payload.relationships : [];
    let parseFailures = 0;
    const loadedJoinDrafts: StructuredJoinDraft[] = relationshipPayload
      .filter((entry): entry is Record<string, unknown> => isRecord(entry))
      .map((entry) => {
        const sourceModelId =
          readString(entry.source_semantic_model_id) ?? readString(entry.sourceSemanticModelId) ?? '';
        const targetModelId =
          readString(entry.target_semantic_model_id) ?? readString(entry.targetSemanticModelId) ?? '';
        const parsedSourceField = parseFieldReference(
          readString(entry.source_field) ?? readString(entry.sourceField) ?? '',
        );
        const parsedTargetField = parseFieldReference(
          readString(entry.target_field) ?? readString(entry.targetField) ?? '',
        );
        if (!sourceModelId || !targetModelId || !parsedSourceField || !parsedTargetField) {
          parseFailures += 1;
        }
        return {
          id: createId('join'),
          name: readString(entry.name) || '',
          type:
            normalizeJoinType(
              readString(entry.relationship_type) ??
                readString(entry.relationshipType) ??
                readString(entry.type),
            ),
          leftOptionId: sourceModelId && parsedSourceField
            ? buildTableOptionId(sourceModelId, parsedSourceField.tableKey)
            : '',
          leftColumn: parsedSourceField?.fieldName || '',
          operator: normalizeJoinOperator(readString(entry.operator)),
          rightOptionId: targetModelId && parsedTargetField
            ? buildTableOptionId(targetModelId, parsedTargetField.tableKey)
            : '',
          rightColumn: parsedTargetField?.fieldName || '',
        };
      });

    const metricPayload = isRecord(payload.metrics) ? payload.metrics : {};
    const loadedMetrics = Object.entries(metricPayload)
      .map(([metricName, metricValue]) => {
        if (!isRecord(metricValue)) {
          return null;
        }
        const expression = readString(metricValue.expression);
        if (!expression) {
          return null;
        }
        return {
          id: createId('metric'),
          name: metricName,
          expression,
          description: readString(metricValue.description) || '',
        };
      })
      .filter((entry): entry is UnifiedMetricDraft => Boolean(entry));

    setSelectedUnifiedModelId(model.id);
    setFormState({
      name: readString(payload.name) || model.name,
      description: readString(payload.description) || model.description || '',
      version: readString(payload.version) || DEFAULT_VERSION,
    });
    setSelectedModelIds(normalizedSourceIds);
    setSourceModelOverrides(loadedSourceModelLookup);
    setJoinDrafts(loadedJoinDrafts);
    setMetrics(loadedMetrics);
    setPreviewResult(null);
    setPreviewError(null);

    if (parseFailures > 0) {
      setNotice(
        `${parseFailures} relationship${parseFailures > 1 ? 's could' : ' could'} not be fully resolved and need re-selection.`,
      );
    } else {
      setNotice('Unified model loaded into the builder.');
    }
  };

  const handleSaveUnifiedModel = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!organizationId) {
      setError('Select an organization before saving.');
      return;
    }
    if (selectedSourceModels.length === 0) {
      setError('Select at least one source semantic model.');
      return;
    }

    setSaveLoading(true);
    setError(null);
    setNotice(null);

    try {
      const payload = buildUnifiedPayload({
        formState,
        sourceModels: sourceModelPayload,
        joinDrafts,
        tableOptionLookup,
        metrics,
      });
      const modelYaml = yaml.dump(payload, { noRefs: true, sortKeys: false });
      const persistencePayload = buildUnifiedModelPersistencePayload({
        selectedUnifiedModel,
        selectedSourceModels,
        selectedProjectId,
        formState,
        modelYaml,
        payloadName: payload.name,
      });

      if (selectedUnifiedModelId) {
        const updated = await updateSemanticModel(
          selectedUnifiedModelId,
          organizationId,
          persistencePayload,
        );
        setSelectedUnifiedModelId(updated.id);
        setNotice('Unified model updated.');
      } else {
        const created = await createSemanticModel(organizationId, {
          organizationId,
          ...persistencePayload,
        });
        setSelectedUnifiedModelId(created.id);
        setNotice('Unified model saved.');
      }

      await loadUnifiedModels();
    } catch (saveError) {
      setError(resolveError(saveError));
    } finally {
      setSaveLoading(false);
    }
  };

  const handlePreviewQuery = async () => {
    if (!organizationId) {
      setPreviewError('Select an organization before running preview queries.');
      return;
    }
    if (selectedModelIds.length === 0) {
      setPreviewError('Select at least one source semantic model.');
      return;
    }

    let parsedQuery: Record<string, unknown>;
    try {
      const raw = JSON.parse(previewQueryJson);
      if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
        throw new Error('Query payload must be a JSON object.');
      }
      parsedQuery = raw as Record<string, unknown>;
    } catch (parseError) {
      setPreviewError(
        parseError instanceof Error ? parseError.message : 'Invalid JSON query payload.',
      );
      return;
    }

    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const response = await runUnifiedSemanticQuery(organizationId, {
        organizationId,
        projectId: selectedProjectId ?? undefined,
        semanticModelIds: selectedModelIds,
        sourceModels: sourceModelPayload,
        relationships: buildUnifiedRelationshipPayload(joinDrafts, tableOptionLookup),
        metrics: buildUnifiedMetricPayload(metrics),
        query: parsedQuery,
      });
      setPreviewResult(response);
    } catch (queryError) {
      setPreviewError(resolveError(queryError));
      setPreviewResult(null);
    } finally {
      setPreviewLoading(false);
    }
  };

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      <section className="relative overflow-hidden rounded-3xl border border-[color:var(--panel-border)] bg-gradient-to-br from-[color:var(--panel-bg)] via-[color:var(--panel-alt)] to-[color:var(--panel-bg)] p-6 shadow-soft">
        <div className="absolute -top-20 -right-12 h-48 w-48 rounded-full bg-[color:var(--accent)]/10 blur-3xl" />
        <div className="relative space-y-2">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-[color:var(--text-muted)]">
            Unified semantic models
          </p>
          <h1 className="text-2xl font-semibold text-[color:var(--text-primary)] md:text-3xl">
            Cross-source model composer
          </h1>
          <p className="max-w-3xl text-sm">
            Compose unified models from existing semantic models, define cross-model relationships with guided dropdowns,
            and preview cross-source semantic queries executed on the unified query runtime.
          </p>
          <p className="text-xs text-[color:var(--text-muted)]">
            Scope: <span className="font-medium text-[color:var(--text-primary)]">{organizationName}</span>
            {selectedProjectId ? ' - project scoped' : ' - organization scoped'}
          </p>
        </div>
      </section>
      {error ? (
        <div className="rounded-xl border border-rose-300 bg-rose-100/50 px-4 py-3 text-sm text-rose-700">
          {error}
        </div>
      ) : null}
      {notice ? (
        <div className="rounded-xl border border-emerald-300 bg-emerald-100/50 px-4 py-3 text-sm text-emerald-800">
          {notice}
        </div>
      ) : null}

      {!hasOrganization && !scopeLoading ? (
        <div className="rounded-2xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 text-center text-sm">
          Choose an organization from scope selector to use unified semantic modeling.
        </div>
      ) : (
        <div className="grid gap-6 xl:grid-cols-[1.6fr_1fr]">
          <section className="space-y-5 rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
            <form className="space-y-5" onSubmit={(event) => void handleSaveUnifiedModel(event)}>
              <div className="flex items-center justify-between gap-3">
                <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Builder</h2>
                <div className="flex items-center gap-2">
                  <Button type="button" size="sm" variant="outline" onClick={resetBuilder}>
                    New unified model
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      void loadSourceModels();
                      void loadUnifiedModels();
                    }}
                    isLoading={sourceLoading || unifiedLoading}
                  >
                    Refresh
                  </Button>
                </div>
              </div>

              <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-1">
                    <Label htmlFor="unified-name">Name</Label>
                    <Input
                      id="unified-name"
                      value={formState.name}
                      onChange={(event) =>
                        setFormState((current) => ({ ...current, name: event.target.value }))
                      }
                      placeholder="e.g. Revenue Operations Hub"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label htmlFor="unified-version">Version</Label>
                    <Input
                      id="unified-version"
                      value={formState.version}
                      onChange={(event) =>
                        setFormState((current) => ({
                          ...current,
                          version: event.target.value || DEFAULT_VERSION,
                        }))
                      }
                    />
                  </div>
                </div>
                <div className="mt-4 space-y-1">
                  <Label htmlFor="unified-description">Description</Label>
                  <Textarea
                    id="unified-description"
                    rows={3}
                    value={formState.description}
                    onChange={(event) =>
                      setFormState((current) => ({ ...current, description: event.target.value }))
                    }
                    placeholder="Explain the business domain this unified model serves"
                  />
                </div>
              </div>

              <div className="space-y-3 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Source models</h3>
                  <Badge variant="secondary">{selectedSourceModels.length}</Badge>
                </div>
                {sourceLoading ? (
                  <p className="text-sm">Loading semantic models...</p>
                ) : sourceModels.length === 0 ? (
                  <p className="text-sm text-[color:var(--text-muted)]">
                    No standard semantic models found in this scope.
                  </p>
                ) : (
                  <div className="grid gap-3 md:grid-cols-2">
                    {sourceModels.map((model) => {
                      const isSelected = selectedModelIds.includes(model.id);
                      return (
                        <button
                          key={model.id}
                          type="button"
                          onClick={() => handleToggleModel(model.id)}
                          className={cn(
                            'rounded-2xl border p-4 text-left transition',
                            isSelected
                              ? 'border-[color:var(--accent)] bg-[color:var(--panel-bg)] shadow-soft'
                              : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] hover:border-[color:var(--border-strong)]',
                          )}
                        >
                          <p className="text-sm font-semibold text-[color:var(--text-primary)]">{model.name}</p>
                          <p className="mt-1 text-xs text-[color:var(--text-muted)]">
                            Updated {formatRelativeDate(model.updatedAt)}
                          </p>
                          {model.description ? (
                            <p className="mt-2 text-xs text-[color:var(--text-secondary)]">{model.description}</p>
                          ) : null}
                        </button>
                      );
                    })}
                  </div>
                )}

                <div className="rounded-xl border border-sky-300 bg-sky-100/60 px-3 py-2 text-xs text-sky-900">
                  Relationships are scoped by source semantic model, so repeated dataset keys across domains are allowed.
                </div>
              </div>

              <div className="space-y-3 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Model relationships</h3>
                  <Button type="button" size="sm" variant="outline" onClick={addJoinDraft}>
                    Add relationship
                  </Button>
                </div>

                {joinDrafts.length === 0 ? (
                  <p className="text-sm text-[color:var(--text-muted)]">
                    Add relationships by selecting a source model field and a target model field. Manual SQL join expressions are not required.
                  </p>
                ) : (
                  <div className="space-y-3">
                    {joinDrafts.map((draft) => {
                      const leftOption = tableOptionLookup.get(draft.leftOptionId);
                      const rightOption = tableOptionLookup.get(draft.rightOptionId);
                      const leftColumns = leftOption?.columns || [];
                      const rightColumns = rightOption?.columns || [];
                      return (
                        <div
                          key={draft.id}
                          className="space-y-3 rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4"
                        >
                          <div className="grid gap-3 md:grid-cols-[2fr_1fr_auto]">
                            <Input
                              value={draft.name}
                              onChange={(event) =>
                                updateJoinDraft(draft.id, { name: event.target.value })
                              }
                              placeholder="Join name"
                            />
                            <Select
                              value={draft.type}
                              onChange={(event) =>
                                updateJoinDraft(draft.id, {
                                  type: event.target.value as JoinType,
                                })
                              }
                            >
                              {JOIN_TYPES.map((joinType) => (
                                <option key={joinType} value={joinType}>
                                  {joinType}
                                </option>
                              ))}
                            </Select>
                            <Button
                              type="button"
                              size="sm"
                              variant="ghost"
                              onClick={() =>
                                setJoinDrafts((current) =>
                                  current.filter((entry) => entry.id !== draft.id),
                                )
                              }
                            >
                              Remove
                            </Button>
                          </div>

                          <div className="grid gap-3 md:grid-cols-2">
                            <Select
                              value={draft.leftOptionId}
                              onChange={(event) =>
                                updateJoinDraft(draft.id, { leftOptionId: event.target.value })
                              }
                            >
                              <option value="">Source dataset</option>
                              {tableOptions.map((option) => (
                                <option key={`left-${option.optionId}`} value={option.optionId}>
                                  {option.modelAlias}.{option.tableKey} ({option.modelName})
                                </option>
                              ))}
                            </Select>
                            <Select
                              value={draft.rightOptionId}
                              onChange={(event) =>
                                updateJoinDraft(draft.id, { rightOptionId: event.target.value })
                              }
                            >
                              <option value="">Target dataset</option>
                              {tableOptions.map((option) => (
                                <option key={`right-${option.optionId}`} value={option.optionId}>
                                  {option.modelAlias}.{option.tableKey} ({option.modelName})
                                </option>
                              ))}
                            </Select>
                          </div>

                          <div className="grid gap-3 md:grid-cols-[2fr_1fr_2fr]">
                            <Select
                              value={draft.leftColumn}
                              onChange={(event) =>
                                updateJoinDraft(draft.id, { leftColumn: event.target.value })
                              }
                              disabled={!draft.leftOptionId}
                            >
                              <option value="">Source field</option>
                              {leftColumns.map((column) => (
                                <option key={`left-col-${draft.id}-${column}`} value={column}>
                                  {column}
                                </option>
                              ))}
                            </Select>
                            <Select
                              value={draft.operator}
                              onChange={(event) =>
                                updateJoinDraft(draft.id, {
                                  operator: event.target.value as JoinOperator,
                                })
                              }
                            >
                              {JOIN_OPERATORS.map((operator) => (
                                <option key={`operator-${draft.id}-${operator}`} value={operator}>
                                  {operator}
                                </option>
                              ))}
                            </Select>
                            <Select
                              value={draft.rightColumn}
                              onChange={(event) =>
                                updateJoinDraft(draft.id, { rightColumn: event.target.value })
                              }
                              disabled={!draft.rightOptionId}
                            >
                              <option value="">Target field</option>
                              {rightColumns.map((column) => (
                                <option key={`right-col-${draft.id}-${column}`} value={column}>
                                  {column}
                                </option>
                              ))}
                            </Select>
                          </div>
                          <p className="text-xs text-[color:var(--text-muted)]">
                            Relationship preview:{' '}
                            <span className="font-mono text-[color:var(--text-secondary)]">
                              {leftOption && draft.leftColumn && rightOption && draft.rightColumn
                                ? `${leftOption.modelAlias}.${leftOption.tableKey}.${draft.leftColumn} ${draft.operator} ${rightOption.modelAlias}.${rightOption.tableKey}.${draft.rightColumn}`
                                : 'Select both source and target fields'}
                            </span>
                          </p>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>

              <div className="space-y-3 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Unified metrics</h3>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() =>
                      setMetrics((current) => [
                        ...current,
                        { id: createId('metric'), name: '', expression: '', description: '' },
                      ])
                    }
                  >
                    Add metric
                  </Button>
                </div>
                {metrics.length === 0 ? (
                  <p className="text-sm text-[color:var(--text-muted)]">
                    Optional metrics can still use SQL expressions across selected source models.
                  </p>
                ) : (
                  <div className="space-y-3">
                    {metrics.map((metric) => (
                      <div
                        key={metric.id}
                        className="space-y-3 rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4"
                      >
                        <div className="grid gap-3 md:grid-cols-[1fr_1fr_auto]">
                          <Input
                            value={metric.name}
                            onChange={(event) =>
                              setMetrics((current) =>
                                current.map((entry) =>
                                  entry.id === metric.id
                                    ? { ...entry, name: event.target.value }
                                    : entry,
                                ),
                              )
                            }
                            placeholder="Metric name"
                          />
                          <Input
                            value={metric.description}
                            onChange={(event) =>
                              setMetrics((current) =>
                                current.map((entry) =>
                                  entry.id === metric.id
                                    ? { ...entry, description: event.target.value }
                                    : entry,
                                ),
                              )
                            }
                            placeholder="Description"
                          />
                          <Button
                            type="button"
                            size="sm"
                            variant="ghost"
                            onClick={() =>
                              setMetrics((current) =>
                                current.filter((entry) => entry.id !== metric.id),
                              )
                            }
                          >
                            Remove
                          </Button>
                        </div>
                        <Textarea
                          rows={3}
                          value={metric.expression}
                          onChange={(event) =>
                            setMetrics((current) =>
                              current.map((entry) =>
                                entry.id === metric.id
                                  ? { ...entry, expression: event.target.value }
                                  : entry,
                              ),
                            )
                          }
                          placeholder="SQL expression"
                        />
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="space-y-3 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Unified YAML</h3>
                  <Badge variant="secondary">Preview</Badge>
                </div>
                {unifiedYamlPreview.error ? (
                  <p className="text-xs text-rose-600">{unifiedYamlPreview.error}</p>
                ) : null}
                <Textarea
                  readOnly
                  rows={12}
                  className="font-mono text-xs"
                  value={unifiedYamlPreview.yaml}
                  placeholder="YAML appears once source models are selected."
                />
                <Button type="submit" className="w-full" isLoading={saveLoading} disabled={saveLoading}>
                  {selectedUnifiedModelId ? 'Update unified model' : 'Save unified model'}
                </Button>
              </div>

              <div className="space-y-3 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Unified query preview</h3>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => void handlePreviewQuery()}
                    isLoading={previewLoading}
                    disabled={previewLoading}
                  >
                    Run query
                  </Button>
                </div>
                <Textarea
                  rows={7}
                  className="font-mono text-xs"
                  value={previewQueryJson}
                  onChange={(event) => setPreviewQueryJson(event.target.value)}
                />
                {previewError ? (
                  <div className="rounded-xl border border-rose-300 bg-rose-100/50 px-3 py-2 text-xs text-rose-700">
                    {previewError}
                  </div>
                ) : null}
                {previewResult ? (
                  <div className="space-y-2">
                    <p className="text-xs text-[color:var(--text-muted)]">
                      Returned {previewResult.data.length} rows from unified query execution.
                    </p>
                    <Textarea
                      readOnly
                      rows={8}
                      className="font-mono text-xs"
                      value={JSON.stringify(previewResult.data.slice(0, 25), null, 2)}
                    />
                  </div>
                ) : null}
              </div>
            </form>
          </section>

          <aside className="space-y-5 rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Unified library</h2>
                <Badge variant="secondary">{unifiedModels.length}</Badge>
              </div>
              <p className="text-xs text-[color:var(--text-muted)]">
                Unified models are listed separately from standard semantic models.
              </p>
            </div>

            {unifiedLoading ? (
              <p className="text-sm">Loading unified models...</p>
            ) : unifiedModels.length === 0 ? (
              <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4 text-sm text-[color:var(--text-muted)]">
                No unified models yet. Build and save one from the panel.
              </div>
            ) : (
              <ul className="space-y-3">
                {unifiedModels.map((model) => {
                  const isActive = model.id === selectedUnifiedModelId;
                  return (
                    <li
                      key={model.id}
                      className={cn(
                        'rounded-2xl border p-4',
                        isActive
                          ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)]'
                          : 'border-[color:var(--panel-border)] bg-[color:var(--panel-alt)]',
                      )}
                    >
                      <p className="text-sm font-semibold text-[color:var(--text-primary)]">{model.name}</p>
                      <p className="mt-1 text-xs text-[color:var(--text-muted)]">
                        Updated {formatRelativeDate(model.updatedAt)}
                      </p>
                      {model.description ? (
                        <p className="mt-2 text-xs text-[color:var(--text-secondary)]">{model.description}</p>
                      ) : null}
                      <div className="mt-3 flex justify-end">
                        <Button
                          type="button"
                          size="sm"
                          variant={isActive ? 'default' : 'outline'}
                          onClick={() => handleLoadUnifiedModel(model)}
                        >
                          {isActive ? 'Loaded' : 'Load'}
                        </Button>
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}

            <div className="space-y-2 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-semibold text-[color:var(--text-primary)]">Current source set</h3>
                <Badge variant="secondary">{selectedSourceModels.length}</Badge>
              </div>
              {selectedSourceModels.length === 0 ? (
                <p className="text-xs text-[color:var(--text-muted)]">No source models selected.</p>
              ) : (
                <ul className="space-y-2 text-xs">
                  {selectedSourceModels.map((model) => (
                    <li
                      key={model.id}
                      className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-2"
                    >
                      <p className="font-medium text-[color:var(--text-primary)]">{model.name}</p>
                      <p className="text-[color:var(--text-muted)]">{model.id}</p>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </aside>
        </div>
      )}
    </div>
  );
}
function resolveError(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Something went wrong while processing your request.';
}

function readSourceModelIds(payload: Record<string, unknown>): string[] {
  return parseUnifiedSourceModels(payload).map((entry) => entry.id);
}

export function collectUnifiedSourceDatasetIds(models: SemanticModelRecord[]): string[] {
  const seen = new Set<string>();
  const sourceDatasetIds: string[] = [];
  models.forEach((model) => {
    (model.sourceDatasetIds || []).forEach((datasetId) => {
      if (!datasetId || seen.has(datasetId)) {
        return;
      }
      seen.add(datasetId);
      sourceDatasetIds.push(datasetId);
    });
  });
  return sourceDatasetIds;
}

export function buildUnifiedModelPersistencePayload(input: {
  selectedUnifiedModel: SemanticModelRecord | null;
  selectedSourceModels: SemanticModelRecord[];
  selectedProjectId?: string | null;
  formState: FormState;
  modelYaml: string;
  payloadName: string;
}): UnifiedModelPersistencePayload {
  const { selectedUnifiedModel, selectedSourceModels, selectedProjectId, formState, modelYaml, payloadName } = input;
  const sourceDatasetIds = collectUnifiedSourceDatasetIds(selectedSourceModels);

  return {
    projectId: selectedProjectId ?? undefined,
    connectorId: selectedUnifiedModel?.connectorId ?? undefined,
    name: formState.name || payloadName,
    description: formState.description || undefined,
    modelYaml,
    autoGenerate: false,
    sourceDatasetIds: sourceDatasetIds.length > 0 ? sourceDatasetIds : undefined,
  };
}

function buildUnifiedPayload(input: {
  formState: FormState;
  sourceModels: UnifiedSemanticSourceModelPayload[];
  joinDrafts: StructuredJoinDraft[];
  tableOptionLookup: Map<string, TableOption>;
  metrics: UnifiedMetricDraft[];
}) {
  const { formState, sourceModels, joinDrafts, tableOptionLookup, metrics } = input;
  if (sourceModels.length === 0) {
    throw new Error('Select at least one source model.');
  }

  const name = formState.name || sourceModels[0]?.name || sourceModels[0]?.alias || 'unified_model';
  const relationshipPayload = buildUnifiedYamlRelationshipPayload(joinDrafts, tableOptionLookup);
  const metricPayload = buildUnifiedMetricPayload(metrics);

  return {
    name,
    version: formState.version || DEFAULT_VERSION,
    description: formState.description || undefined,
    source_models: sourceModels.map((sourceModel) => ({
      id: sourceModel.id,
      alias: sourceModel.alias,
      name: sourceModel.name || undefined,
      description: sourceModel.description || undefined,
    })),
    relationships: relationshipPayload.length > 0 ? relationshipPayload : undefined,
    metrics: Object.keys(metricPayload).length > 0 ? metricPayload : undefined,
  };
}

function parseUnifiedSourceModels(payload: Record<string, unknown>): UnifiedSemanticSourceModelPayload[] {
  const sourceModelsValue = Array.isArray(payload.source_models)
    ? payload.source_models
    : Array.isArray(payload.sourceModels)
      ? payload.sourceModels
      : [];
  return sourceModelsValue.flatMap((entry) => {
    if (!isRecord(entry)) {
      return [];
    }
    const id = readString(entry.id);
    const alias = readString(entry.alias);
    if (!id || !alias) {
      return [];
    }
    return [
      {
        id,
        alias,
        name: readString(entry.name) ?? undefined,
        description: readString(entry.description) ?? undefined,
      },
    ];
  });
}

function buildUnifiedSourceModels(
  selectedModels: SemanticModelRecord[],
  overrides: Record<string, UnifiedSemanticSourceModelPayload>,
): UnifiedSemanticSourceModelPayload[] {
  const seenAliases = new Set<string>();
  return selectedModels.map((model, index) => {
    const override = overrides[model.id];
    const baseAlias = sanitizeSourceModelAlias(override?.alias || model.name || `model_${index + 1}`);
    let alias = baseAlias || `model_${index + 1}`;
    let suffix = 2;
    while (seenAliases.has(alias)) {
      alias = `${baseAlias || 'model'}_${suffix}`;
      suffix += 1;
    }
    seenAliases.add(alias);
    return {
      id: model.id,
      alias,
      name: override?.name ?? model.name,
      description: override?.description ?? model.description ?? undefined,
    };
  });
}

function buildTableOptions(
  selectedModels: SemanticModelRecord[],
  sourceModels: UnifiedSemanticSourceModelPayload[],
): TableOption[] {
  const aliasLookup = new Map(sourceModels.map((entry) => [entry.id, entry.alias]));
  const options: TableOption[] = [];

  selectedModels.forEach((model) => {
    const payload = safeParseYaml(model.contentYaml || '');
    if (!payload) {
      return;
    }
    const datasets = isRecord(payload.datasets)
      ? payload.datasets
      : isRecord(payload.tables)
        ? payload.tables
        : null;
    if (!datasets) {
      return;
    }

    Object.entries(datasets).forEach(([tableKey, value]) => {
      if (!isRecord(value)) {
        return;
      }
      const dimensions = Array.isArray(value.dimensions) ? value.dimensions : [];
      const measures = Array.isArray(value.measures) ? value.measures : [];
      const columns = [
        ...dimensions
          .map((dimension) => (isRecord(dimension) ? readString(dimension.name) : null))
          .filter((name): name is string => Boolean(name)),
        ...measures
          .map((measure) => (isRecord(measure) ? readString(measure.name) : null))
          .filter((name): name is string => Boolean(name)),
      ];

      options.push({
        optionId: buildTableOptionId(model.id, tableKey),
        tableKey,
        modelId: model.id,
        modelName: model.name,
        modelAlias: aliasLookup.get(model.id) || model.name,
        columns: Array.from(new Set(columns)).sort(),
      });
    });
  });

  return options;
}

function buildUnifiedRelationshipPayload(
  joinDrafts: StructuredJoinDraft[],
  tableOptionLookup: Map<string, TableOption>,
): UnifiedSemanticRelationshipPayload[] {
  return joinDrafts.flatMap((draft) => {
    const resolved = resolveRelationshipDraft(draft, tableOptionLookup);
    if (!resolved) {
      return [];
    }
    return [
      {
        name: resolved.name,
        sourceSemanticModelId: resolved.sourceSemanticModelId,
        sourceField: resolved.sourceField,
        targetSemanticModelId: resolved.targetSemanticModelId,
        targetField: resolved.targetField,
        relationshipType: resolved.relationshipType,
        operator: resolved.operator,
      },
    ];
  });
}

function buildUnifiedYamlRelationshipPayload(
  joinDrafts: StructuredJoinDraft[],
  tableOptionLookup: Map<string, TableOption>,
): Array<Record<string, string>> {
  return joinDrafts.flatMap((draft) => {
    const resolved = resolveRelationshipDraft(draft, tableOptionLookup);
    if (!resolved) {
      return [];
    }
    const output: Record<string, string> = {
      source_semantic_model_id: resolved.sourceSemanticModelId,
      source_field: resolved.sourceField,
      target_semantic_model_id: resolved.targetSemanticModelId,
      target_field: resolved.targetField,
      relationship_type: resolved.relationshipType,
      operator: resolved.operator,
    };
    if (resolved.name) {
      output.name = resolved.name;
    }
    return [output];
  });
}

function resolveRelationshipDraft(
  draft: StructuredJoinDraft,
  tableOptionLookup: Map<string, TableOption>,
):
  | {
      name?: string;
      sourceSemanticModelId: string;
      sourceField: string;
      targetSemanticModelId: string;
      targetField: string;
      relationshipType: JoinType;
      operator: JoinOperator;
    }
  | null {
  const leftOption = tableOptionLookup.get(draft.leftOptionId);
  const rightOption = tableOptionLookup.get(draft.rightOptionId);
  if (!leftOption || !rightOption || !draft.leftColumn || !draft.rightColumn) {
    return null;
  }
  return {
    name: draft.name || undefined,
    sourceSemanticModelId: leftOption.modelId,
    sourceField: buildFieldReference(leftOption.tableKey, draft.leftColumn),
    targetSemanticModelId: rightOption.modelId,
    targetField: buildFieldReference(rightOption.tableKey, draft.rightColumn),
    relationshipType: draft.type,
    operator: draft.operator,
  };
}

function buildUnifiedMetricPayload(metrics: UnifiedMetricDraft[]) {
  return metrics.reduce<Record<string, { expression: string; description?: string }>>(
    (acc, metric) => {
      if (!metric.name || !metric.expression) {
        return acc;
      }
      acc[metric.name] = {
        expression: metric.expression,
        description: metric.description || undefined,
      };
      return acc;
    },
    {},
  );
}

function buildTableOptionId(modelId: string, tableKey: string): string {
  return `${modelId}::${tableKey}`;
}

function buildFieldReference(tableKey: string, fieldName: string): string {
  return `${tableKey}.${fieldName}`;
}

function parseFieldReference(value: string): { tableKey: string; fieldName: string } | null {
  const match = value.match(/^\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*$/);
  if (!match) {
    return null;
  }
  return {
    tableKey: match[1],
    fieldName: match[2],
  };
}

function sanitizeSourceModelAlias(value: string): string {
  const normalized = value.replace(/[^0-9A-Za-z_]+/g, '_').replace(/^_+|_+$/g, '');
  return normalized || 'model';
}
function normalizeJoinType(value?: string | null): JoinType {
  if (value === 'left' || value === 'right' || value === 'full') {
    return value;
  }
  return 'inner';
}

function normalizeJoinOperator(value?: string | null): JoinOperator {
  if (value === '!=' || value === '>' || value === '>=' || value === '<' || value === '<=') {
    return value;
  }
  return '=';
}

function safeParseYaml(content: string): Record<string, unknown> | null {
  try {
    const parsed = yaml.load(content);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return null;
    }
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function readString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function createId(prefix: string): string {
  return createClientId(prefix);
}
