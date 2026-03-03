'use client';

import { useSearchParams } from 'next/navigation';
import { JSX, useCallback, useEffect, useMemo, useState } from 'react';
import yaml from 'js-yaml';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { fetchLLMConnections } from '@/orchestration/agents';
import { fetchConnectors } from '@/orchestration/connectors';
import type { ConnectorResponse } from '@/orchestration/connectors/types';
import {
  createSemanticModel,
  fetchSemanticModel,
  fetchSemanticModelCatalog,
  generateSemanticModelYaml,
  generateSemanticModelYamlFromSelection,
  startAgenticSemanticModelJob,
  updateSemanticModel,
} from '@/orchestration/semanticModels';
import { fetchAgentJobState } from '@/orchestration/jobs';
import type {
  SemanticDimension,
  SemanticMeasure,
  SemanticMetric,
  SemanticModelCatalogColumn,
  SemanticModelCatalogResponse,
  SemanticModelRecord,
  SemanticRelationship,
  SemanticTable,
} from '@/orchestration/semanticModels/types';
import { ApiError } from '@/orchestration/http';
import { cn } from '@/lib/utils';

interface FormState {
  name: string;
  description: string;
  filename: string;
}

interface BuilderDimension extends SemanticDimension {
  id: string;
}

interface BuilderMeasure extends SemanticMeasure {
  id: string;
}

type RelationshipType = 'one_to_many' | 'many_to_one' | 'one_to_one' | 'many_to_many';

interface BuilderTable extends Omit<SemanticTable, 'dimensions' | 'measures'> {
  id: string;
  entityName: string;
  dimensions: BuilderDimension[];
  measures: BuilderMeasure[];
}

interface BuilderRelationship extends Omit<SemanticRelationship, 'type'> {
  id: string;
  type: RelationshipType;
}

interface BuilderMetric extends SemanticMetric {
  id: string;
  name: string;
}

interface BuilderModel {
  version: string;
  description?: string;
  tables: BuilderTable[];
  relationships: BuilderRelationship[];
  metrics: BuilderMetric[];
}

type CreationMode = 'manual' | 'auto' | 'agentic';
type BuilderStage = 'modal' | 'wizard' | 'builder';
type SelectionTab = 'all' | 'selected';

interface CatalogTableNode {
  schemaName: string;
  tableName: string;
  tableRef: string;
  columns: SemanticModelCatalogColumn[];
}

const RELATIONSHIP_TYPES: RelationshipType[] = ['one_to_many', 'many_to_one', 'one_to_one', 'many_to_many'];
const COLUMN_TYPE_OPTIONS = [
  'string',
  'integer',
  'decimal',
  'float',
  'number',
  'boolean',
  'date',
  'datetime',
  'timestamp',
  'time',
] as const;
const DEFAULT_MODEL_VERSION = '1.0';
const SUGGESTED_QUESTION_PROMPTS = [
  'revenue by region',
  'top customers',
  'monthly trend',
  'gross margin by segment',
  'churn by cohort',
];

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

type SemanticModelPageProps = {
  params: { organizationId: string };
};

export default function SemanticModelPage({ params }: SemanticModelPageProps): JSX.Element {
  const {
    selectedOrganizationId,
    selectedProjectId,
    organizations,
    loading: scopeLoading,
    setSelectedOrganizationId,
  } = useWorkspaceScope();
  const searchParams = useSearchParams();
  const organizationId = params.organizationId;
  const editingModelId = searchParams.get('modelId') ?? '';
  const isEditMode = Boolean(editingModelId);
  const normalizedProjectId = selectedProjectId && selectedProjectId.length > 0 ? selectedProjectId : null;

  useEffect(() => {
    if (organizationId && organizationId !== selectedOrganizationId) {
      setSelectedOrganizationId(organizationId);
    }
  }, [organizationId, selectedOrganizationId, setSelectedOrganizationId]);

  const [formState, setFormState] = useState<FormState>({
    name: '',
    description: '',
    filename: 'semantic_model.yml',
  });
  const [stage, setStage] = useState<BuilderStage>(isEditMode ? 'builder' : 'modal');
  const [creationMode, setCreationMode] = useState<CreationMode>('manual');
  const [wizardStepIndex, setWizardStepIndex] = useState(0);
  const [connectors, setConnectors] = useState<ConnectorResponse[]>([]);
  const [connectorsLoading, setConnectorsLoading] = useState(false);
  const [selectedConnectorId, setSelectedConnectorId] = useState('');
  const [llmConnectionAvailable, setLlmConnectionAvailable] = useState(false);
  const [checkingLlmConnections, setCheckingLlmConnections] = useState(false);
  const [catalog, setCatalog] = useState<SemanticModelCatalogResponse | null>(null);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogConnectorId, setCatalogConnectorId] = useState<string | null>(null);
  const [tableTab, setTableTab] = useState<SelectionTab>('all');
  const [columnTab, setColumnTab] = useState<SelectionTab>('all');
  const [columnSearch, setColumnSearch] = useState('');
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<Record<string, string[]>>({});
  const [includeSampleValues, setIncludeSampleValues] = useState(false);
  const [questionPrompts, setQuestionPrompts] = useState<string[]>([]);
  const [promptInput, setPromptInput] = useState('');
  const [builder, setBuilder] = useState<BuilderModel>(() => createEmptyBuilderModel());
  const [yamlDraft, setYamlDraft] = useState('');
  const [yamlDirty, setYamlDirty] = useState(false);
  const [yamlError, setYamlError] = useState<string | null>(null);
  const [editingModel, setEditingModel] = useState<SemanticModelRecord | null>(null);
  const [workingModelId, setWorkingModelId] = useState<string | null>(null);
  const [loadingModel, setLoadingModel] = useState(false);
  const [autoGenerating, setAutoGenerating] = useState(false);
  const [wizardSubmitting, setWizardSubmitting] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const handleCreationModalOpenChange = useCallback(() => undefined, []);

  const organizationAvailable = Boolean(organizationId);
  const activeModelId = editingModelId || workingModelId || '';
  const isUpdateMode = Boolean(activeModelId);
  const headerTitle = isUpdateMode ? 'Edit semantic model' : 'Semantic model builder';
  const headerDescription = isUpdateMode
    ? 'Update the semantic layer for a connector and save changes to the existing model.'
    : 'Describe the semantic layer for a connector and LangBridge will persist the YAML definition for your agents.';
  const submitLabel = isUpdateMode ? 'Update semantic model' : 'Save semantic model';
  const wizardSteps = useMemo(
    () =>
      creationMode === 'agentic'
        ? ['Getting started', 'Select tables', 'Select columns', 'Questions', 'Review']
        : ['Getting started', 'Select tables', 'Select columns', 'Review'],
    [creationMode],
  );

  const currentOrganizationName = useMemo(() => {
    if (!organizationId) {
      return 'Select an organization';
    }
    return organizations.find((org) => org.id === organizationId)?.name ?? 'Unknown organization';
  }, [organizations, organizationId]);

  const selectedConnector = useMemo(
    () => connectors.find((connector) => connector.id === selectedConnectorId),
    [connectors, selectedConnectorId],
  );
  const selectedTableSet = useMemo(() => new Set(selectedTables), [selectedTables]);
  const catalogTables = useMemo<CatalogTableNode[]>(() => {
    if (!catalog) {
      return [];
    }
    const tables: CatalogTableNode[] = [];
    catalog.schemas.forEach((schemaEntry) => {
      schemaEntry.tables.forEach((table) => {
        tables.push({
          schemaName: schemaEntry.name,
          tableName: table.name,
          tableRef: table.fullyQualifiedName,
          columns: table.columns,
        });
      });
    });
    return tables;
  }, [catalog]);
  const selectedColumnCount = useMemo(
    () => Object.values(selectedColumns).reduce((total, columns) => total + columns.length, 0),
    [selectedColumns],
  );
  const filteredColumnTables = useMemo(() => {
    const search = columnSearch.trim().toLowerCase();
    return catalogTables
      .filter((table) => selectedTableSet.has(table.tableRef))
      .map((table) => {
        const selectedColumnSet = new Set(selectedColumns[table.tableRef] ?? []);
        const availableColumns =
          columnTab === 'selected'
            ? table.columns.filter((column) => selectedColumnSet.has(column.name))
            : table.columns;
        const visibleColumns =
          search.length === 0
            ? availableColumns
            : availableColumns.filter((column) => column.name.toLowerCase().includes(search));
        return { ...table, visibleColumns, selectedColumnSet };
      });
  }, [catalogTables, columnSearch, columnTab, selectedColumns, selectedTableSet]);
  const outline = useMemo(() => {
    const tables = builder.tables.filter(tableHasContent);
    const dimensions = tables.flatMap((table) => table.dimensions ?? []);
    const measures = tables.flatMap((table) => table.measures ?? []);
    const relationships = builder.relationships.filter(
      (relationship) => relationship.from && relationship.to && relationship.joinOn,
    );
    return {
      tables,
      dimensions,
      measures,
      relationships,
    };
  }, [builder]);

  const loadConnectors = useCallback(async () => {
    if (!organizationId) {
      return;
    }
    setConnectorsLoading(true);
    try {
      const data = await fetchConnectors(organizationId);
      setConnectors(data);
      setSelectedConnectorId((current) => {
        if (current) {
          return current;
        }
        const firstUsable = data.find((connector) => connector.id);
        return firstUsable?.id ?? '';
      });
    } catch (err) {
      setError(resolveError(err));
    } finally {
      setConnectorsLoading(false);
    }
  }, [organizationId]);

  const loadLlmConnections = useCallback(async () => {
    if (!organizationId) {
      setLlmConnectionAvailable(false);
      return;
    }
    setCheckingLlmConnections(true);
    try {
      const connections = await fetchLLMConnections(organizationId);
      setLlmConnectionAvailable(connections.some((connection) => connection.isActive));
    } catch {
      setLlmConnectionAvailable(false);
    } finally {
      setCheckingLlmConnections(false);
    }
  }, [organizationId]);

  const applyBuilderFromYaml = useCallback(
    (yamlText: string) => {
      const nextBuilder = parseYamlToBuilderModel(yamlText);
      validateBuilderModel(nextBuilder);
      setBuilder(nextBuilder);
      setYamlDraft(yamlText);
      setYamlDirty(false);
      setYamlError(null);
    },
    [],
  );

  const initializeCatalogSelection = useCallback((nextCatalog: SemanticModelCatalogResponse) => {
    const nextTables: string[] = [];
    const nextColumns: Record<string, string[]> = {};
    nextCatalog.schemas.forEach((schemaEntry) => {
      schemaEntry.tables.forEach((table) => {
        nextTables.push(table.fullyQualifiedName);
        nextColumns[table.fullyQualifiedName] = table.columns.map((column) => column.name);
      });
    });
    setSelectedTables(nextTables);
    setSelectedColumns(nextColumns);
  }, []);

  const loadCatalog = useCallback(async () => {
    if (!organizationId) {
      throw new Error('Select an organization before loading table metadata.');
    }
    if (!selectedConnectorId) {
      throw new Error('Select a connector before loading table metadata.');
    }
    setCatalogLoading(true);
    try {
      const response = await fetchSemanticModelCatalog(organizationId, selectedConnectorId);
      setCatalog(response);
      setCatalogConnectorId(selectedConnectorId);
      initializeCatalogSelection(response);
      return response;
    } finally {
      setCatalogLoading(false);
    }
  }, [initializeCatalogSelection, organizationId, selectedConnectorId]);

  const ensureCatalogLoaded = useCallback(async () => {
    if (catalog && catalogConnectorId === selectedConnectorId) {
      return catalog;
    }
    return await loadCatalog();
  }, [catalog, catalogConnectorId, loadCatalog, selectedConnectorId]);

  useEffect(() => {
    if (!organizationId) {
      setConnectors([]);
      setSelectedConnectorId('');
      setLlmConnectionAvailable(false);
      return;
    }
    setSelectedConnectorId('');
    void loadConnectors();
    void loadLlmConnections();
  }, [organizationId, loadConnectors, loadLlmConnections]);

  useEffect(() => {
    if (isEditMode) {
      setStage('builder');
      return;
    }
    setStage('modal');
  }, [isEditMode]);

  useEffect(() => {
    if (!isEditMode) {
      setEditingModel(null);
      return;
    }
    if (!organizationId || !editingModelId) {
      return;
    }
    let cancelled = false;
    const loadModel = async () => {
      setLoadingModel(true);
      setError(null);
      try {
        const model = await fetchSemanticModel(editingModelId, organizationId);
        if (cancelled) {
          return;
        }
        const parsedBuilder = parseYamlToBuilderModel(model.contentYaml);
        setEditingModel(model);
        setFormState({
          name: model.name ?? '',
          description: model.description ?? '',
          filename: `${(model.name ?? 'semantic_model').replace(/\s+/g, '_').toLowerCase()}.yml`,
        });
        setSelectedConnectorId(model.connectorId ?? '');
        setBuilder(parsedBuilder);
        setYamlDraft(model.contentYaml);
        setYamlDirty(false);
        setYamlError(null);
      } catch (err) {
        if (!cancelled) {
          setError(resolveError(err));
        }
      } finally {
        if (!cancelled) {
          setLoadingModel(false);
        }
      }
    };
    void loadModel();
    return () => {
      cancelled = true;
    };
  }, [editingModelId, isEditMode, organizationId]);

  useEffect(() => {
    if (!selectedConnectorId) {
      setBuilder(createEmptyBuilderModel());
      setYamlDraft('');
      setYamlDirty(false);
      setYamlError(null);
      return;
    }
    if (!isEditMode) {
      setBuilder(createEmptyBuilderModel());
      setYamlDraft('');
      setYamlDirty(false);
      setYamlError(null);
    }
  }, [isEditMode, selectedConnectorId]);

  useEffect(() => {
    if (yamlDirty) {
      return;
    }
    try {
      const nextYaml = serializeBuilderModel(builder, selectedConnector?.name);
      setYamlDraft(nextYaml);
      setYamlError(null);
    } catch (err) {
      setYamlError(resolveError(err));
    }
  }, [builder, selectedConnector?.name, yamlDirty]);

  async function handleSave(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!organizationId) {
      setError('Select an organization before saving a semantic model.');
      return;
    }
    if (!selectedConnectorId) {
      setError('Select a connector before saving a semantic model.');
      return;
    }
    if (!formState.name.trim()) {
      setError('Provide a name for the semantic model.');
      return;
    }

    let parsedBuilder: BuilderModel;
    try {
      parsedBuilder = parseYamlToBuilderModel(yamlDraft);
      validateBuilderModel(parsedBuilder);
      setBuilder(parsedBuilder);
      setYamlError(null);
      setYamlDirty(false);
    } catch (validationError) {
      setYamlError(resolveError(validationError));
      setError('Semantic YAML is invalid. Fix errors in YAML editor and apply before saving.');
      return;
    }

    const trimmedName = formState.name.trim();
    const trimmedDescription = formState.description.trim();

    setSubmitting(true);
    setError(null);
    try {
      const yamlPayload = yamlDraft;
      if (isUpdateMode) {
        if (!activeModelId) {
          setError('Choose a semantic model to update.');
          return;
        }
        const updated = await updateSemanticModel(activeModelId, organizationId, {
          projectId: normalizedProjectId,
          connectorId: selectedConnectorId,
          name: trimmedName,
          description: trimmedDescription,
          modelYaml: yamlPayload,
          autoGenerate: false,
        });
        setEditingModel(updated);
        setWorkingModelId(updated.id);
        setNotice('Semantic model updated.');
      } else {
        const created = await createSemanticModel(organizationId, {
          organizationId,
          projectId: normalizedProjectId,
          connectorId: selectedConnectorId,
          name: trimmedName,
          description: trimmedDescription || undefined,
          modelYaml: yamlPayload,
          autoGenerate: false,
        });
        setEditingModel(created);
        setWorkingModelId(created.id);
        setNotice('Semantic model saved.');
      }
    } catch (err) {
      setError(resolveError(err));
    } finally {
      setSubmitting(false);
    }
  }

  const handleAutoGenerate = useCallback(async () => {
    if (!organizationId) {
      setError('Select an organization before generating a semantic model.');
      return;
    }
    if (!selectedConnectorId) {
      setError('Select a connector before generating a semantic model.');
      return;
    }
    setAutoGenerating(true);
    setError(null);
    try {
      const yamlText = await generateSemanticModelYaml(organizationId, selectedConnectorId);
      const generatedModel = parseYamlToBuilderModel(yamlText);
      setBuilder(generatedModel);
      setYamlDraft(yamlText);
      setYamlDirty(false);
      setYamlError(null);
    } catch (err) {
      setError(resolveError(err));
    } finally {
      setAutoGenerating(false);
    }
  }, [organizationId, selectedConnectorId]);

  const handleApplyYaml = useCallback(() => {
    try {
      applyBuilderFromYaml(yamlDraft);
      setNotice('YAML applied to the visual builder.');
      setError(null);
    } catch (err) {
      setYamlError(resolveError(err));
      setError('YAML parsing failed. Fix errors and apply again.');
    }
  }, [applyBuilderFromYaml, yamlDraft]);

  const handleResetYamlFromBuilder = useCallback(() => {
    try {
      const nextYaml = serializeBuilderModel(builder, selectedConnector?.name);
      setYamlDraft(nextYaml);
      setYamlDirty(false);
      setYamlError(null);
      setNotice('YAML reset from visual builder.');
    } catch (err) {
      setYamlError(resolveError(err));
    }
  }, [builder, selectedConnector?.name]);

  const handleModalContinue = useCallback(async () => {
    if (!selectedConnectorId) {
      setError('Select a connector before continuing.');
      return;
    }
    if (!formState.name.trim()) {
      setError('Model name is required.');
      return;
    }
    setError(null);
    setNotice(null);
    if (creationMode === 'manual') {
      setStage('builder');
      return;
    }
    try {
      await ensureCatalogLoaded();
      setWizardStepIndex(0);
      setStage('wizard');
    } catch (err) {
      setError(resolveError(err));
    }
  }, [creationMode, ensureCatalogLoaded, formState.name, selectedConnectorId]);

  const handleSelectAllTables = useCallback(
    (checked: boolean) => {
      if (!checked) {
        setSelectedTables([]);
        setSelectedColumns({});
        return;
      }
      const allTables = catalogTables.map((table) => table.tableRef);
      const allColumns: Record<string, string[]> = {};
      catalogTables.forEach((table) => {
        allColumns[table.tableRef] = table.columns.map((column) => column.name);
      });
      setSelectedTables(allTables);
      setSelectedColumns(allColumns);
    },
    [catalogTables],
  );

  const handleToggleTable = useCallback(
    (tableRef: string, checked: boolean) => {
      setSelectedTables((current) => {
        const set = new Set(current);
        if (checked) {
          set.add(tableRef);
        } else {
          set.delete(tableRef);
        }
        return Array.from(set);
      });
      if (!checked) {
        setSelectedColumns((current) => {
          const next = { ...current };
          delete next[tableRef];
          return next;
        });
        return;
      }
      const table = catalogTables.find((entry) => entry.tableRef === tableRef);
      if (!table) {
        return;
      }
      setSelectedColumns((current) => ({
        ...current,
        [tableRef]: current[tableRef] ?? table.columns.map((column) => column.name),
      }));
    },
    [catalogTables],
  );

  const handleToggleColumn = useCallback(
    (tableRef: string, columnName: string, checked: boolean) => {
      if (!selectedTableSet.has(tableRef)) {
        return;
      }
      setSelectedColumns((current) => {
        const columnSet = new Set(current[tableRef] ?? []);
        if (checked) {
          columnSet.add(columnName);
        } else {
          columnSet.delete(columnName);
        }
        return {
          ...current,
          [tableRef]: Array.from(columnSet),
        };
      });
    },
    [selectedTableSet],
  );

  const handleSelectAllColumns = useCallback(
    (checked: boolean) => {
      if (!checked) {
        setSelectedColumns(() => {
          const next: Record<string, string[]> = {};
          selectedTables.forEach((tableRef) => {
            next[tableRef] = [];
          });
          return next;
        });
        return;
      }
      const next: Record<string, string[]> = {};
      selectedTables.forEach((tableRef) => {
        const table = catalogTables.find((entry) => entry.tableRef === tableRef);
        next[tableRef] = table ? table.columns.map((column) => column.name) : [];
      });
      setSelectedColumns(next);
    },
    [catalogTables, selectedTables],
  );

  const handleSelectAllColumnsForTable = useCallback(
    (tableRef: string, checked: boolean) => {
      const table = catalogTables.find((entry) => entry.tableRef === tableRef);
      if (!table) {
        return;
      }
      setSelectedColumns((current) => ({
        ...current,
        [tableRef]: checked ? table.columns.map((column) => column.name) : [],
      }));
    },
    [catalogTables],
  );

  const handlePromptAdd = useCallback((value: string) => {
    const prompt = value.trim();
    if (!prompt) {
      return;
    }
    setQuestionPrompts((current) => {
      if (current.includes(prompt) || current.length >= 10) {
        return current;
      }
      return [...current, prompt];
    });
  }, []);

  const handlePromptParseInput = useCallback(() => {
    const prompts = promptInput
      .split('\n')
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
    if (prompts.length === 0) {
      return;
    }
    setQuestionPrompts((current) => {
      const next = [...current];
      prompts.forEach((entry) => {
        if (!next.includes(entry) && next.length < 10) {
          next.push(entry);
        }
      });
      return next;
    });
    setPromptInput('');
  }, [promptInput]);

  const runWizardGeneration = useCallback(async () => {
    if (!organizationId) {
      throw new Error('Select an organization before generating a semantic model.');
    }
    if (!selectedConnectorId) {
      throw new Error('Select a connector before generating a semantic model.');
    }
    const selectedColumnsPayload: Record<string, string[]> = {};
    selectedTables.forEach((tableRef) => {
      selectedColumnsPayload[tableRef] = selectedColumns[tableRef] ?? [];
    });
    if (creationMode === 'auto') {
      const response = await generateSemanticModelYamlFromSelection(organizationId, {
        connectorId: selectedConnectorId,
        selectedTables,
        selectedColumns: selectedColumnsPayload,
        includeSampleValues,
        description: formState.description.trim() || undefined,
      });
      applyBuilderFromYaml(response.yamlText);
      setStage('builder');
      setNotice(
        response.warnings.length > 0
          ? `Auto-generation completed with ${response.warnings.length} warning(s).`
          : 'Auto-generated YAML loaded into the editor.',
      );
      return;
    }

    const job = await startAgenticSemanticModelJob(organizationId, {
      connectorId: selectedConnectorId,
      projectId: normalizedProjectId,
      name: formState.name.trim(),
      description: formState.description.trim() || undefined,
      filename: formState.filename.trim() || undefined,
      selectedTables,
      selectedColumns: selectedColumnsPayload,
      questionPrompts,
      includeSampleValues,
    });
    setWorkingModelId(job.semanticModelId);

    let terminalState: Awaited<ReturnType<typeof fetchAgentJobState>> | null = null;
    for (let attempt = 0; attempt < 120; attempt += 1) {
      const state = await fetchAgentJobState(organizationId, job.jobId);
      if (state.status === 'succeeded' || state.status === 'failed' || state.status === 'cancelled') {
        terminalState = state;
        break;
      }
      await sleep(2500);
    }
    if (!terminalState) {
      throw new Error('Agentic generation timed out.');
    }
    if (terminalState.status !== 'succeeded') {
      throw new Error('Agentic generation did not complete successfully.');
    }

    const terminalResult =
      terminalState.finalResponse && typeof terminalState.finalResponse.result === 'object'
        ? (terminalState.finalResponse.result as Record<string, unknown>)
        : null;
    const yamlFromJob =
      typeof terminalResult?.yaml_text === 'string'
        ? terminalResult.yaml_text
        : typeof terminalResult?.yamlText === 'string'
          ? terminalResult.yamlText
          : null;

    const model = await fetchSemanticModel(job.semanticModelId, organizationId);
    setEditingModel(model);
    applyBuilderFromYaml(yamlFromJob ?? model.contentYaml);
    setStage('builder');
    setNotice('Agentic draft generated and loaded as a draft model.');
  }, [
    applyBuilderFromYaml,
    creationMode,
    formState.description,
    formState.filename,
    formState.name,
    includeSampleValues,
    organizationId,
    questionPrompts,
    selectedColumns,
    selectedConnectorId,
    normalizedProjectId,
    selectedTables,
  ]);

  const handleWizardNext = useCallback(async () => {
    const currentStep = wizardSteps[wizardStepIndex];
    if (currentStep === 'Getting started') {
      if (!selectedConnectorId) {
        setError('Select a connector before continuing.');
        return;
      }
      if (!formState.name.trim()) {
        setError('Model name is required.');
        return;
      }
      try {
        await ensureCatalogLoaded();
      } catch (err) {
        setError(resolveError(err));
        return;
      }
      setError(null);
      setWizardStepIndex((current) => current + 1);
      return;
    }
    if (currentStep === 'Select tables') {
      if (selectedTables.length === 0) {
        setError('Select at least one table.');
        return;
      }
      setError(null);
      setWizardStepIndex((current) => current + 1);
      return;
    }
    if (currentStep === 'Select columns') {
      const invalidTable = selectedTables.find((tableRef) => (selectedColumns[tableRef] ?? []).length === 0);
      if (invalidTable) {
        setError(`Select at least one column for ${invalidTable}.`);
        return;
      }
      setError(null);
      setWizardStepIndex((current) => current + 1);
      return;
    }
    if (currentStep === 'Questions') {
      if (questionPrompts.length < 3 || questionPrompts.length > 10) {
        setError('Provide 3 to 10 question prompts for agentic generation.');
        return;
      }
      setError(null);
      setWizardStepIndex((current) => current + 1);
      return;
    }

    setWizardSubmitting(true);
    setError(null);
    try {
      await runWizardGeneration();
    } catch (err) {
      setError(resolveError(err));
    } finally {
      setWizardSubmitting(false);
    }
  }, [
    ensureCatalogLoaded,
    formState.name,
    questionPrompts.length,
    runWizardGeneration,
    selectedColumns,
    selectedConnectorId,
    selectedTables,
    wizardStepIndex,
    wizardSteps,
  ]);

  const handleWizardBack = useCallback(() => {
    if (wizardStepIndex === 0) {
      setStage('modal');
      return;
    }
    setWizardStepIndex((current) => Math.max(0, current - 1));
  }, [wizardStepIndex]);

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold text-[color:var(--text-primary)]">{headerTitle}</h1>
        <p className="max-w-3xl text-sm">
          {headerDescription}
          {isUpdateMode
            ? ' Review the YAML output before saving.'
            : ' Choose a connector, tune dimensions and measures, then review the YAML output before saving.'}
        </p>
        {isUpdateMode && editingModel ? (
          <div className="text-xs text-[color:var(--text-muted)]">
            Editing:{' '}
            <span className="font-medium text-[color:var(--text-primary)]">
              {formState.name.trim() || editingModel.name}
            </span>
          </div>
        ) : null}
        <div className="text-xs text-[color:var(--text-muted)]">
          Scope: <span className="font-medium text-[color:var(--text-primary)]">{currentOrganizationName}</span>
          {selectedProjectId ? ' - project scoped' : ' - organization scoped'}
        </div>
      </header>

      {error ? (
        <div className="rounded-lg border border-rose-300 bg-rose-100/40 px-4 py-3 text-sm text-rose-700">{error}</div>
      ) : null}
      {notice ? (
        <div className="rounded-lg border border-emerald-300 bg-emerald-100/40 px-4 py-3 text-sm text-emerald-800">
          {notice}
        </div>
      ) : null}
      {isEditMode && loadingModel ? (
        <div className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-4 py-3 text-sm">
          Loading semantic model details...
        </div>
      ) : null}

      <Dialog open={stage === 'modal'} onOpenChange={handleCreationModalOpenChange}>
        <DialogContent className="max-w-3xl">
          <DialogHeader>
            <DialogTitle>Create Semantic Model</DialogTitle>
            <DialogDescription>
              Choose a build mode, then provide connector, name, and description before continuing.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-5">
            <div className="grid gap-3 md:grid-cols-3">
              {(
                [
                  {
                    mode: 'manual' as CreationMode,
                    title: 'Manual',
                    description: 'Start directly in the builder and define semantic entities manually.',
                    disabled: false,
                    disabledReason: '',
                  },
                  {
                    mode: 'auto' as CreationMode,
                    title: 'Auto-generate',
                    description: 'Select tables and columns, then auto-generate YAML.',
                    disabled: false,
                    disabledReason: '',
                  },
                  {
                    mode: 'agentic' as CreationMode,
                    title: 'Agentic',
                    description: 'Generate a draft from selected data and question themes.',
                    disabled: !llmConnectionAvailable,
                    disabledReason: checkingLlmConnections
                      ? 'Checking LLM availability...'
                      : 'Enable an active LLM connection to use agentic generation.',
                  },
                ]
              ).map((option) => {
                const selected = creationMode === option.mode;
                return (
                  <button
                    key={option.mode}
                    type="button"
                    className={cn(
                      'rounded-2xl border p-4 text-left transition',
                      selected
                        ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)]'
                        : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] hover:border-[color:var(--border-strong)]',
                      option.disabled ? 'cursor-not-allowed opacity-60' : '',
                    )}
                    onClick={() => {
                      if (!option.disabled) {
                        setCreationMode(option.mode);
                      }
                    }}
                    disabled={option.disabled}
                    title={option.disabled ? option.disabledReason : undefined}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-sm font-semibold text-[color:var(--text-primary)]">{option.title}</p>
                      {selected ? <Badge variant="secondary">Selected</Badge> : null}
                    </div>
                    <p className="mt-2 text-xs text-[color:var(--text-muted)]">{option.description}</p>
                    {option.disabled && option.disabledReason ? (
                      <p className="mt-2 text-xs text-amber-700">{option.disabledReason}</p>
                    ) : null}
                  </button>
                );
              })}
            </div>
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-1">
                <Label htmlFor="create-modal-connector">Connection</Label>
                <Select
                  id="create-modal-connector"
                  value={selectedConnectorId}
                  onChange={(event) => setSelectedConnectorId(event.target.value)}
                  disabled={connectorsLoading}
                >
                  <option value="">Select connection</option>
                  {connectors.map((connector) => (
                    <option key={connector.id ?? connector.name} value={connector.id}>
                      {connector.name}
                    </option>
                  ))}
                </Select>
              </div>
              <div className="space-y-1">
                <Label htmlFor="create-modal-name">Model name</Label>
                <Input
                  id="create-modal-name"
                  value={formState.name}
                  onChange={(event) =>
                    setFormState((current) => ({ ...current, name: event.target.value }))
                  }
                  placeholder="e.g. Revenue semantic layer"
                />
              </div>
            </div>
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-1">
                <Label htmlFor="create-modal-description">Description</Label>
                <Input
                  id="create-modal-description"
                  value={formState.description}
                  onChange={(event) =>
                    setFormState((current) => ({ ...current, description: event.target.value }))
                  }
                  placeholder="Optional description"
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="create-modal-filename">Filename</Label>
                <Input
                  id="create-modal-filename"
                  value={formState.filename}
                  onChange={(event) =>
                    setFormState((current) => ({ ...current, filename: event.target.value }))
                  }
                  placeholder="semantic_model.yml"
                />
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button type="button" onClick={() => void handleModalContinue()}>
              Continue
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {stage === 'wizard' ? (
        <section className="grid gap-5 rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft lg:grid-cols-[250px_1fr]">
          <aside className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
            <p className="mb-4 text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
              Create model
            </p>
            <ol className="space-y-3">
              {wizardSteps.map((step, index) => {
                const active = index === wizardStepIndex;
                const complete = index < wizardStepIndex;
                return (
                  <li key={step} className="flex items-center gap-3">
                    <span
                      className={cn(
                        'inline-flex h-7 w-7 items-center justify-center rounded-full border text-xs font-semibold',
                        active
                          ? 'border-[color:var(--accent)] bg-[color:var(--chip-bg)] text-[color:var(--text-primary)]'
                          : complete
                            ? 'border-emerald-600 bg-emerald-100 text-emerald-700'
                            : 'border-[color:var(--panel-border)] text-[color:var(--text-muted)]',
                      )}
                    >
                      {index + 1}
                    </span>
                    <span className={cn('text-sm', active ? 'font-semibold text-[color:var(--text-primary)]' : 'text-[color:var(--text-muted)]')}>
                      {step}
                    </span>
                  </li>
                );
              })}
            </ol>
          </aside>

          <div className="space-y-5">
            <div>
              <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">{wizardSteps[wizardStepIndex]}</h2>
              <p className="text-sm text-[color:var(--text-muted)]">
                {wizardSteps[wizardStepIndex] === 'Getting started'
                  ? 'Confirm location, connection, and model metadata.'
                  : wizardSteps[wizardStepIndex] === 'Select tables'
                    ? 'Choose the tables to include.'
                    : wizardSteps[wizardStepIndex] === 'Select columns'
                      ? 'Choose the columns to include from selected tables.'
                      : wizardSteps[wizardStepIndex] === 'Questions'
                        ? 'Provide question themes for the agentic generator.'
                        : 'Review your selection and generate the semantic YAML.'}
              </p>
            </div>

            {wizardSteps[wizardStepIndex] === 'Getting started' ? (
              <div className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-1">
                    <Label htmlFor="wizard-connector">Connection</Label>
                    <Select
                      id="wizard-connector"
                      value={selectedConnectorId}
                      onChange={(event) => setSelectedConnectorId(event.target.value)}
                    >
                      <option value="">Select connection</option>
                      {connectors.map((connector) => (
                        <option key={connector.id ?? connector.name} value={connector.id}>
                          {connector.name}
                        </option>
                      ))}
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <Label htmlFor="wizard-file">Filename</Label>
                    <Input
                      id="wizard-file"
                      value={formState.filename}
                      onChange={(event) =>
                        setFormState((current) => ({ ...current, filename: event.target.value }))
                      }
                    />
                  </div>
                </div>
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-1">
                    <Label htmlFor="wizard-name">Model name</Label>
                    <Input
                      id="wizard-name"
                      value={formState.name}
                      onChange={(event) =>
                        setFormState((current) => ({ ...current, name: event.target.value }))
                      }
                    />
                  </div>
                  <div className="space-y-1">
                    <Label htmlFor="wizard-description">Description</Label>
                    <Input
                      id="wizard-description"
                      value={formState.description}
                      onChange={(event) =>
                        setFormState((current) => ({ ...current, description: event.target.value }))
                      }
                    />
                  </div>
                </div>
              </div>
            ) : null}

            {wizardSteps[wizardStepIndex] === 'Select tables' ? (
              <div className="space-y-4">
                <div className="flex flex-wrap items-center justify-between gap-2 text-sm text-[color:var(--text-muted)]">
                  <span>{selectedTables.length} tables selected</span>
                  <div className="flex gap-2">
                    <Button type="button" size="sm" variant="outline" onClick={() => handleSelectAllTables(true)}>
                      Select all
                    </Button>
                    <Button type="button" size="sm" variant="outline" onClick={() => handleSelectAllTables(false)}>
                      Clear
                    </Button>
                  </div>
                </div>
                <div className="inline-flex rounded-full border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-1">
                  <button
                    type="button"
                    className={cn(
                      'rounded-full px-3 py-1 text-sm',
                      tableTab === 'all'
                        ? 'bg-[color:var(--panel-alt)] text-[color:var(--text-primary)]'
                        : 'text-[color:var(--text-muted)]',
                    )}
                    onClick={() => setTableTab('all')}
                  >
                    All
                  </button>
                  <button
                    type="button"
                    className={cn(
                      'rounded-full px-3 py-1 text-sm',
                      tableTab === 'selected'
                        ? 'bg-[color:var(--panel-alt)] text-[color:var(--text-primary)]'
                        : 'text-[color:var(--text-muted)]',
                    )}
                    onClick={() => setTableTab('selected')}
                  >
                    Selected
                  </button>
                </div>
                <div className="max-h-[420px] space-y-3 overflow-y-auto rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                  {catalogLoading ? (
                    <p className="text-sm">Loading table metadata...</p>
                  ) : (
                    catalogTables
                      .filter((table) => (tableTab === 'selected' ? selectedTableSet.has(table.tableRef) : true))
                      .map((table) => {
                        const checked = selectedTableSet.has(table.tableRef);
                        return (
                          <label
                            key={table.tableRef}
                            className={cn(
                              'flex cursor-pointer items-start gap-3 rounded-lg border px-3 py-2',
                              checked
                                ? 'border-[color:var(--accent)] bg-[color:var(--panel-bg)]'
                                : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)]',
                            )}
                          >
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={(event) => handleToggleTable(table.tableRef, event.target.checked)}
                              className="mt-1 h-4 w-4"
                            />
                            <div>
                              <p className="text-sm font-medium text-[color:var(--text-primary)]">{table.tableName}</p>
                              <p className="text-xs text-[color:var(--text-muted)]">{table.tableRef}</p>
                            </div>
                          </label>
                        );
                      })
                  )}
                </div>
              </div>
            ) : null}

            {wizardSteps[wizardStepIndex] === 'Select columns' ? (
              <div className="space-y-4">
                <div className="grid gap-3 md:grid-cols-[1fr_auto]">
                  <Input
                    value={columnSearch}
                    onChange={(event) => setColumnSearch(event.target.value)}
                    placeholder="Search columns"
                  />
                  <div className="flex gap-2">
                    <Button type="button" size="sm" variant="outline" onClick={() => handleSelectAllColumns(true)}>
                      Select all columns
                    </Button>
                    <Button type="button" size="sm" variant="outline" onClick={() => handleSelectAllColumns(false)}>
                      Clear
                    </Button>
                  </div>
                </div>
                <div className="flex items-center justify-between text-sm text-[color:var(--text-muted)]">
                  <span>{selectedColumnCount} columns selected</span>
                  <label className="inline-flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={includeSampleValues}
                      onChange={(event) => setIncludeSampleValues(event.target.checked)}
                      className="h-4 w-4"
                    />
                    Include example data
                  </label>
                </div>
                <div className="inline-flex rounded-full border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-1">
                  <button
                    type="button"
                    className={cn(
                      'rounded-full px-3 py-1 text-sm',
                      columnTab === 'all'
                        ? 'bg-[color:var(--panel-alt)] text-[color:var(--text-primary)]'
                        : 'text-[color:var(--text-muted)]',
                    )}
                    onClick={() => setColumnTab('all')}
                  >
                    All
                  </button>
                  <button
                    type="button"
                    className={cn(
                      'rounded-full px-3 py-1 text-sm',
                      columnTab === 'selected'
                        ? 'bg-[color:var(--panel-alt)] text-[color:var(--text-primary)]'
                        : 'text-[color:var(--text-muted)]',
                    )}
                    onClick={() => setColumnTab('selected')}
                  >
                    Selected
                  </button>
                </div>
                <div className="max-h-[420px] space-y-3 overflow-y-auto rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                  {filteredColumnTables.map((table) => {
                    const tableSelectedColumns = new Set(selectedColumns[table.tableRef] ?? []);
                    const allChecked =
                      table.columns.length > 0 &&
                      table.columns.every((column) => tableSelectedColumns.has(column.name));
                    return (
                      <div key={table.tableRef} className="space-y-2 rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-3">
                        <div className="flex items-center justify-between gap-2">
                          <div>
                            <p className="text-sm font-semibold text-[color:var(--text-primary)]">{table.tableName}</p>
                            <p className="text-xs text-[color:var(--text-muted)]">{table.tableRef}</p>
                          </div>
                          <label className="inline-flex items-center gap-2 text-xs">
                            <input
                              type="checkbox"
                              checked={allChecked}
                              onChange={(event) =>
                                handleSelectAllColumnsForTable(table.tableRef, event.target.checked)
                              }
                              className="h-4 w-4"
                            />
                            Select all for table
                          </label>
                        </div>
                        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                          {table.visibleColumns.map((column) => {
                            const checked = tableSelectedColumns.has(column.name);
                            return (
                              <label
                                key={`${table.tableRef}.${column.name}`}
                                className={cn(
                                  'flex cursor-pointer items-center gap-2 rounded-md border px-2 py-1 text-xs',
                                  checked
                                    ? 'border-[color:var(--accent)] bg-[color:var(--chip-bg)]'
                                    : 'border-[color:var(--panel-border)]',
                                )}
                              >
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  onChange={(event) =>
                                    handleToggleColumn(table.tableRef, column.name, event.target.checked)
                                  }
                                  className="h-3.5 w-3.5"
                                />
                                <span className="truncate">{column.name}</span>
                              </label>
                            );
                          })}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : null}

            {wizardSteps[wizardStepIndex] === 'Questions' ? (
              <div className="space-y-4">
                <div className="space-y-1">
                  <Label htmlFor="wizard-prompts">Questions this model should answer</Label>
                  <Textarea
                    id="wizard-prompts"
                    rows={4}
                    value={promptInput}
                    onChange={(event) => setPromptInput(event.target.value)}
                    placeholder={'One prompt per line, for example:\nrevenue by region\ntop customers'}
                  />
                </div>
                <div className="flex items-center gap-2">
                  <Button type="button" size="sm" variant="outline" onClick={handlePromptParseInput}>
                    Add prompts
                  </Button>
                  <span className="text-xs text-[color:var(--text-muted)]">{questionPrompts.length} / 10</span>
                </div>
                <div className="space-y-2">
                  <p className="text-xs font-semibold uppercase tracking-wide text-[color:var(--text-muted)]">Suggestions</p>
                  <div className="flex flex-wrap gap-2">
                    {SUGGESTED_QUESTION_PROMPTS.map((prompt) => (
                      <button
                        key={prompt}
                        type="button"
                        className="rounded-full border border-[color:var(--panel-border)] px-3 py-1 text-xs"
                        onClick={() => handlePromptAdd(prompt)}
                      >
                        {prompt}
                      </button>
                    ))}
                  </div>
                </div>
                <div className="space-y-2">
                  {questionPrompts.map((prompt) => (
                    <div key={prompt} className="flex items-center justify-between rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-2 text-sm">
                      <span>{prompt}</span>
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        onClick={() =>
                          setQuestionPrompts((current) => current.filter((entry) => entry !== prompt))
                        }
                      >
                        Remove
                      </Button>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {wizardSteps[wizardStepIndex] === 'Review' ? (
              <div className="space-y-3">
                <div className="grid gap-3 md:grid-cols-3">
                  <div className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
                    <p className="text-xs uppercase tracking-wide text-[color:var(--text-muted)]">Tables</p>
                    <p className="mt-1 text-xl font-semibold text-[color:var(--text-primary)]">{selectedTables.length}</p>
                  </div>
                  <div className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
                    <p className="text-xs uppercase tracking-wide text-[color:var(--text-muted)]">Columns</p>
                    <p className="mt-1 text-xl font-semibold text-[color:var(--text-primary)]">{selectedColumnCount}</p>
                  </div>
                  <div className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
                    <p className="text-xs uppercase tracking-wide text-[color:var(--text-muted)]">Mode</p>
                    <p className="mt-1 text-xl font-semibold text-[color:var(--text-primary)]">
                      {creationMode === 'auto' ? 'Auto' : 'Agentic'}
                    </p>
                  </div>
                </div>
                {creationMode === 'agentic' ? (
                  <div className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
                    <p className="text-xs uppercase tracking-wide text-[color:var(--text-muted)]">Prompts</p>
                    <ul className="mt-2 space-y-1 text-sm">
                      {questionPrompts.map((prompt) => (
                        <li key={prompt}>{prompt}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </div>
            ) : null}

            <div className="flex items-center justify-between">
              <Button type="button" variant="outline" onClick={() => handleWizardBack()} disabled={wizardSubmitting}>
                {wizardStepIndex === 0 ? 'Back to modal' : 'Back'}
              </Button>
              <Button type="button" onClick={() => void handleWizardNext()} isLoading={wizardSubmitting}>
                {wizardStepIndex >= wizardSteps.length - 1
                  ? creationMode === 'agentic'
                    ? 'Generate with agent'
                    : 'Generate draft'
                  : 'Next'}
              </Button>
            </div>
          </div>
        </section>
      ) : null}

      {!organizationAvailable && !scopeLoading ? (
        <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 text-center text-sm">
          Choose an organization from the scope selector to begin modeling.
        </div>
      ) : stage === 'builder' ? (
        <div className="grid gap-6 xl:grid-cols-[1.6fr_1fr]">
          <section className="space-y-6 rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
            <form className="space-y-6" onSubmit={(event) => void handleSave(event)}>
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">1. Select a connector</h2>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => void loadConnectors()}
                    isLoading={connectorsLoading}
                  >
                    Refresh list
                  </Button>
                </div>
                <p className="text-sm">
                  Each semantic model is tied to one connector. Pick a connector to unlock the builder and optional
                  auto-generation.
                </p>
                {connectors.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-6 text-sm">
                    No connectors available in this scope. Create a connector first, then return to build a semantic
                    model.
                  </div>
                ) : (
                  <div className="grid gap-3 md:grid-cols-2">
                    {connectors.map((connector) => {
                      const connectorId = connector.id ?? '';
                      const isSelected = connectorId !== '' && connectorId === selectedConnectorId;
                      return (
                        <button
                          key={connector.id ?? connector.name}
                          type="button"
                          className={cn(
                            'rounded-2xl border bg-[color:var(--panel-alt)] p-4 text-left transition hover:border-[color:var(--border-strong)]',
                            isSelected ? 'border-[color:var(--accent)] shadow-soft' : 'border-[color:var(--panel-border)]',
                          )}
                          onClick={() => setSelectedConnectorId(connectorId)}
                          disabled={!connectorId}
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <p className="text-sm font-semibold text-[color:var(--text-primary)]">
                                {connector.name}
                              </p>
                              <p className="text-xs text-[color:var(--text-muted)]">
                                {connector.description ?? 'No description provided.'}
                              </p>
                            </div>
                            {isSelected ? <Badge variant="secondary">Selected</Badge> : null}
                          </div>
                          <div className="mt-3 text-xs text-[color:var(--text-muted)]">
                            Type: <span className="text-[color:var(--text-primary)]">{connector.connectorType ?? 'Custom'}</span>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
              {!selectedConnectorId ? (
                <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-6 text-sm">
                  Select a connector to start configuring the semantic model.
                </div>
              ) : (
                <>
                  <div className="space-y-4 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div>
                        <h3 className="text-base font-semibold text-[color:var(--text-primary)]">2. Model metadata</h3>
                        <p className="text-xs text-[color:var(--text-muted)]">
                          Name and describe the saved record plus the semantic layer version.
                        </p>
                      </div>
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        onClick={() => void handleAutoGenerate()}
                        isLoading={autoGenerating}
                      >
                        Auto-generate YAML
                      </Button>
                    </div>
                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-1">
                        <Label htmlFor="model-name">Model name</Label>
                        <Input
                          id="model-name"
                          value={formState.name}
                          onChange={(event) =>
                            setFormState((current) => ({ ...current, name: event.target.value }))
                          }
                          placeholder="e.g. Sales semantic layer"
                        />
                      </div>
                      <div className="space-y-1">
                        <Label htmlFor="model-description">Record description</Label>
                        <Input
                          id="model-description"
                          value={formState.description}
                          onChange={(event) =>
                            setFormState((current) => ({ ...current, description: event.target.value }))
                          }
                          placeholder="This text appears when browsing saved models."
                        />
                      </div>
                    </div>
                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-1">
                        <Label htmlFor="semantic-version">Semantic version</Label>
                        <Input
                          id="semantic-version"
                          value={builder.version}
                          onChange={(event) =>
                            setBuilder((current) => ({ ...current, version: event.target.value || DEFAULT_MODEL_VERSION }))
                          }
                        />
                      </div>
                      <div className="space-y-1">
                        <Label htmlFor="semantic-description">Semantic description</Label>
                        <Textarea
                          id="semantic-description"
                          rows={3}
                          value={builder.description ?? ''}
                          onChange={(event) =>
                            setBuilder((current) => ({ ...current, description: event.target.value }))
                          }
                          placeholder="Optional context for how agents should interpret this model."
                        />
                      </div>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <h3 className="text-base font-semibold text-[color:var(--text-primary)]">3. Tables</h3>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() =>
                          setBuilder((current) => ({
                            ...current,
                            tables: [...current.tables, createEmptyTable(current.tables.length + 1)],
                          }))
                        }
                      >
                        Add table
                      </Button>
                    </div>
                    {builder.tables.length === 0 ? (
                      <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-6 text-sm">
                        No tables yet. Add an entity to start defining dimensions and measures.
                      </div>
                    ) : (
                      builder.tables.map((table, index) => (
                        <article
                          key={table.id}
                          className="space-y-4 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-5 shadow-soft"
                        >
                          <header className="flex flex-wrap items-center justify-between gap-3">
                            <div>
                              <p className="text-sm font-semibold text-[color:var(--text-primary)]">
                                {table.entityName || `Table ${index + 1}`}
                              </p>
                              <p className="text-xs text-[color:var(--text-muted)]">
                                {table.schema && table.name ? `${table.schema}.${table.name}` : 'Define schema and table name'}
                              </p>
                            </div>
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() =>
                                setBuilder((current) => ({
                                  ...current,
                                  tables: current.tables.filter((entry) => entry.id !== table.id),
                                }))
                              }
                            >
                              Remove
                            </Button>
                          </header>

                          <div className="grid gap-4 md:grid-cols-2">
                            <div className="space-y-1">
                              <Label htmlFor={`entity-${table.id}`}>Entity name</Label>
                              <Input
                                id={`entity-${table.id}`}
                                value={table.entityName}
                                onChange={(event) =>
                                  setBuilder((current) => ({
                                    ...current,
                                    tables: current.tables.map((entry) =>
                                      entry.id === table.id ? { ...entry, entityName: event.target.value } : entry,
                                    ),
                                  }))
                                }
                                placeholder="Alias used inside YAML"
                              />
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor={`schema-${table.id}`}>Schema</Label>
                              <Input
                                id={`schema-${table.id}`}
                                value={table.schema}
                                onChange={(event) =>
                                  setBuilder((current) => ({
                                    ...current,
                                    tables: current.tables.map((entry) =>
                                      entry.id === table.id ? { ...entry, schema: event.target.value } : entry,
                                    ),
                                  }))
                                }
                                placeholder="e.g. analytics"
                              />
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor={`table-${table.id}`}>Table name</Label>
                              <Input
                                id={`table-${table.id}`}
                                value={table.name}
                                onChange={(event) =>
                                  setBuilder((current) => ({
                                    ...current,
                                    tables: current.tables.map((entry) =>
                                      entry.id === table.id ? { ...entry, name: event.target.value } : entry,
                                    ),
                                  }))
                                }
                                placeholder="e.g. orders"
                              />
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor={`table-description-${table.id}`}>Description</Label>
                              <Textarea
                                id={`table-description-${table.id}`}
                                rows={3}
                                value={table.description ?? ''}
                                onChange={(event) =>
                                  setBuilder((current) => ({
                                    ...current,
                                    tables: current.tables.map((entry) =>
                                      entry.id === table.id ? { ...entry, description: event.target.value } : entry,
                                    ),
                                  }))
                                }
                              />
                            </div>
                          </div>

                          <div className="grid gap-4 lg:grid-cols-2">
                            <div className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--surface-muted)] p-4">
                              <div className="mb-3 flex items-center justify-between">
                                <h4 className="text-sm font-semibold text-[color:var(--text-primary)]">
                                  Dimensions ({table.dimensions.length})
                                </h4>
                                <Button
                                  type="button"
                                  variant="outline"
                                  size="sm"
                                  onClick={() =>
                                    setBuilder((current) => ({
                                      ...current,
                                      tables: current.tables.map((entry) =>
                                        entry.id === table.id
                                          ? {
                                              ...entry,
                                              dimensions: [
                                                ...entry.dimensions,
                                                {
                                                  id: createId('dimension'),
                                                  name: '',
                                                  type: '',
                                                  primaryKey: false,
                                                  vectorized: false,
                                                },
                                              ],
                                            }
                                          : entry,
                                      ),
                                    }))
                                  }
                                >
                                  Add dimension
                                </Button>
                              </div>
                              {table.dimensions.length === 0 ? (
                                <p className="text-xs text-[color:var(--text-muted)]">No dimensions yet.</p>
                              ) : (
                                <div className="space-y-3">
                                  {table.dimensions.map((dimension) => (
                                    <div
                                      key={dimension.id}
                                      className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-3"
                                    >
                                      <div className="flex items-center justify-between gap-3">
                                        <Label className="text-xs font-semibold uppercase tracking-wide">
                                          Dimension
                                        </Label>
                                        <Button
                                          type="button"
                                          variant="ghost"
                                          size="sm"
                                          onClick={() =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) =>
                                                entry.id === table.id
                                                  ? {
                                                      ...entry,
                                                      dimensions: entry.dimensions.filter(
                                                        (item) => item.id !== dimension.id,
                                                      ),
                                                    }
                                                  : entry,
                                              ),
                                            }))
                                          }
                                        >
                                          Remove
                                        </Button>
                                      </div>
                                      <div className="mt-3 grid gap-3 md:grid-cols-2">
                                        <Input
                                          value={dimension.name}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  dimensions: entry.dimensions.map((item) =>
                                                    item.id === dimension.id ? { ...item, name: event.target.value } : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                          placeholder="Name"
                                        />
                                        <Input
                                          value={dimension.expression ?? ''}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  dimensions: entry.dimensions.map((item) =>
                                                    item.id === dimension.id ? { ...item, expression: event.target.value } : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                          placeholder="Optional SQL expression if different from column name"
                                        />
                                        <Select
                                          value={dimension.type}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  dimensions: entry.dimensions.map((item) =>
                                                    item.id === dimension.id ? { ...item, type: event.target.value } : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                          placeholder="Select type"
                                        >
                                          {COLUMN_TYPE_OPTIONS.map((typeOption) => (
                                            <option key={typeOption} value={typeOption}>
                                              {typeOption}
                                            </option>
                                          ))}
                                        </Select>
                                      </div>
                                      <Textarea
                                        className="mt-3"
                                        rows={2}
                                        value={dimension.description ?? ''}
                                        onChange={(event) =>
                                          setBuilder((current) => ({
                                            ...current,
                                            tables: current.tables.map((entry) => {
                                              if (entry.id !== table.id) {
                                                return entry;
                                              }
                                              return {
                                                ...entry,
                                                dimensions: entry.dimensions.map((item) =>
                                                  item.id === dimension.id
                                                    ? { ...item, description: event.target.value }
                                                    : item,
                                                ),
                                              };
                                            }),
                                          }))
                                        }
                                        placeholder="Description"
                                      />
                                      <label className="mt-3 flex items-center gap-2 text-xs font-medium text-[color:var(--text-primary)]">
                                        <input
                                          type="checkbox"
                                          checked={Boolean(dimension.primaryKey)}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  dimensions: entry.dimensions.map((item) =>
                                                    item.id === dimension.id
                                                      ? { ...item, primaryKey: event.target.checked }
                                                      : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                        />
                                        Primary key
                                      </label>
                                      <label className="mt-2 flex items-center gap-2 text-xs font-medium text-[color:var(--text-primary)]">
                                        <input
                                          type="checkbox"
                                          checked={Boolean(dimension.vectorized)}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  dimensions: entry.dimensions.map((item) =>
                                                    item.id === dimension.id
                                                      ? { ...item, vectorized: event.target.checked }
                                                      : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                        />
                                        Vectorize values for semantic search
                                      </label>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                            <div className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--surface-muted)] p-4">
                              <div className="mb-3 flex items-center justify-between">
                                <h4 className="text-sm font-semibold text-[color:var(--text-primary)]">
                                  Measures ({table.measures.length})
                                </h4>
                                <Button
                                  type="button"
                                  variant="outline"
                                  size="sm"
                                  onClick={() =>
                                    setBuilder((current) => ({
                                      ...current,
                                      tables: current.tables.map((entry) =>
                                        entry.id === table.id
                                          ? {
                                              ...entry,
                                              measures: [
                                                ...entry.measures,
                                                {
                                                  id: createId('measure'),
                                                  expression: '',
                                                  name: '',
                                                  type: '',
                                                  aggregation: '',
                                                },
                                              ],
                                            }
                                          : entry,
                                      ),
                                    }))
                                  }
                                >
                                  Add measure
                                </Button>
                              </div>
                              {table.measures.length === 0 ? (
                                <p className="text-xs text-[color:var(--text-muted)]">No measures yet.</p>
                              ) : (
                                <div className="space-y-3">
                                  {table.measures.map((measure) => (
                                    <div
                                      key={measure.id}
                                      className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-3"
                                    >
                                      <div className="flex items-center justify-between gap-3">
                                        <Label className="text-xs font-semibold uppercase tracking-wide">Measure</Label>
                                        <Button
                                          type="button"
                                          variant="ghost"
                                          size="sm"
                                          onClick={() =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) =>
                                                entry.id === table.id
                                                  ? {
                                                      ...entry,
                                                      measures: entry.measures.filter((item) => item.id !== measure.id),
                                                    }
                                                  : entry,
                                              ),
                                            }))
                                          }
                                        >
                                          Remove
                                        </Button>
                                      </div>
                                      <div className="mt-3 grid gap-3 md:grid-cols-3">
                                        <Input
                                          value={measure.name}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  measures: entry.measures.map((item) =>
                                                    item.id === measure.id ? { ...item, name: event.target.value } : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                          placeholder="Name"
                                        />
                                        
                                        <Input
                                          value={measure.expression ?? ''}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  measures: entry.measures.map((item) =>
                                                    item.id === measure.id ? { ...item, expression: event.target.value } : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                          placeholder="Name"
                                        />
                                        <Select
                                          value={measure.type}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  measures: entry.measures.map((item) =>
                                                    item.id === measure.id ? { ...item, type: event.target.value } : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                          placeholder="Select type"
                                        >
                                          {COLUMN_TYPE_OPTIONS.map((typeOption) => (
                                            <option key={typeOption} value={typeOption}>
                                              {typeOption}
                                            </option>
                                          ))}
                                        </Select>
                                        <Input
                                          value={measure.aggregation ?? ''}
                                          onChange={(event) =>
                                            setBuilder((current) => ({
                                              ...current,
                                              tables: current.tables.map((entry) => {
                                                if (entry.id !== table.id) {
                                                  return entry;
                                                }
                                                return {
                                                  ...entry,
                                                  measures: entry.measures.map((item) =>
                                                    item.id === measure.id
                                                      ? { ...item, aggregation: event.target.value }
                                                      : item,
                                                  ),
                                                };
                                              }),
                                            }))
                                          }
                                          placeholder="Aggregation e.g. sum"
                                        />
                                      </div>
                                      <Textarea
                                        className="mt-3"
                                        rows={2}
                                        value={measure.description ?? ''}
                                        onChange={(event) =>
                                          setBuilder((current) => ({
                                            ...current,
                                            tables: current.tables.map((entry) => {
                                              if (entry.id !== table.id) {
                                                return entry;
                                              }
                                              return {
                                                ...entry,
                                                measures: entry.measures.map((item) =>
                                                  item.id === measure.id
                                                    ? { ...item, description: event.target.value }
                                                    : item,
                                                ),
                                              };
                                            }),
                                          }))
                                        }
                                        placeholder="Description"
                                      />
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          </div>
                        </article>
                      ))
                    )}
                  </div>
                  <div className="space-y-4 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div>
                        <h3 className="text-base font-semibold text-[color:var(--text-primary)]">4. Joins</h3>
                        <p className="text-xs text-[color:var(--text-muted)]">
                          Map how these entities relate so downstream tools can combine tables safely.
                        </p>
                      </div>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={builder.tables.length === 0}
                        onClick={() =>
                          setBuilder((current) => ({
                            ...current,
                            relationships: [
                              ...current.relationships,
                              createEmptyRelationship(current.relationships.length + 1),
                            ],
                          }))
                        }
                      >
                        Add join
                      </Button>
                    </div>
                    {builder.relationships.length === 0 ? (
                      <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--surface-muted)] p-4 text-sm">
                        No joins configured yet. Add at least one join to describe how tables connect.
                      </div>
                    ) : (
                      <div className="space-y-4">
                        {builder.relationships.map((relationship, index) => (
                          <div
                            key={relationship.id}
                            className="space-y-3 rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4"
                          >
                            <div className="flex flex-wrap items-center justify-between gap-3">
                              <div>
                                <p className="text-sm font-semibold text-[color:var(--text-primary)]">
                                  {relationship.name || `Join ${index + 1}`}
                                </p>
                                <p className="text-xs text-[color:var(--text-muted)]">
                                  {relationship.from && relationship.to
                                    ? `${relationship.from} ? ${relationship.to}`
                                    : 'Define the source and target entities'}
                                </p>
                              </div>
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={() =>
                                  setBuilder((current) => ({
                                    ...current,
                                    relationships: current.relationships.filter((entry) => entry.id !== relationship.id),
                                  }))
                                }
                              >
                                Remove
                              </Button>
                            </div>
                            <div className="grid gap-3 md:grid-cols-2">
                              <div className="space-y-1">
                                <Label htmlFor={`join-name-${relationship.id}`}>Join name</Label>
                                <Input
                                  id={`join-name-${relationship.id}`}
                                  value={relationship.name}
                                  onChange={(event) =>
                                    setBuilder((current) => ({
                                      ...current,
                                      relationships: current.relationships.map((entry) =>
                                        entry.id === relationship.id ? { ...entry, name: event.target.value } : entry,
                                      ),
                                    }))
                                  }
                                  placeholder="sales_to_customers"
                                />
                              </div>
                              <div className="space-y-1">
                                <Label htmlFor={`join-type-${relationship.id}`}>Cardinality</Label>
                                <Select
                                  id={`join-type-${relationship.id}`}
                                  placeholder="Select cardinality"
                                  value={relationship.type}
                                  onChange={(event) =>
                                    setBuilder((current) => ({
                                      ...current,
                                      relationships: current.relationships.map((entry) =>
                                        entry.id === relationship.id
                                          ? { ...entry, type: event.target.value as RelationshipType }
                                          : entry,
                                      ),
                                    }))
                                  }
                                >
                                  {RELATIONSHIP_TYPES.map((type) => (
                                    <option key={type} value={type}>
                                      {type.replace(/_/g, ' ')}
                                    </option>
                                  ))}
                                </Select>
                              </div>
                            </div>
                            <div className="grid gap-3 md:grid-cols-2">
                              <div className="space-y-1">
                                <Label htmlFor={`join-from-${relationship.id}`}>From entity</Label>
                                <Input
                                  id={`join-from-${relationship.id}`}
                                  value={relationship.from}
                                  onChange={(event) =>
                                    setBuilder((current) => ({
                                      ...current,
                                      relationships: current.relationships.map((entry) =>
                                        entry.id === relationship.id ? { ...entry, from: event.target.value } : entry,
                                      ),
                                    }))
                                  }
                                  placeholder="_main_sales"
                                />
                              </div>
                              <div className="space-y-1">
                                <Label htmlFor={`join-to-${relationship.id}`}>To entity</Label>
                                <Input
                                  id={`join-to-${relationship.id}`}
                                  value={relationship.to}
                                  onChange={(event) =>
                                    setBuilder((current) => ({
                                      ...current,
                                      relationships: current.relationships.map((entry) =>
                                        entry.id === relationship.id ? { ...entry, to: event.target.value } : entry,
                                      ),
                                    }))
                                  }
                                  placeholder="_main_customers"
                                />
                              </div>
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor={`join-condition-${relationship.id}`}>Join condition</Label>
                              <Textarea
                                id={`join-condition-${relationship.id}`}
                                rows={2}
                                value={relationship.joinOn}
                                onChange={(event) =>
                                  setBuilder((current) => ({
                                    ...current,
                                    relationships: current.relationships.map((entry) =>
                                      entry.id === relationship.id ? { ...entry, joinOn: event.target.value } : entry,
                                    ),
                                  }))
                                }
                                placeholder="_main_sales.customer_id = _main_customers.customer_id"
                              />
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="space-y-4 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                    <div className="flex items-center justify-between">
                      <h3 className="text-base font-semibold text-[color:var(--text-primary)]">5. Metrics</h3>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() =>
                          setBuilder((current) => ({
                            ...current,
                            metrics: [
                              ...current.metrics,
                              {
                                id: createId('metric'),
                                name: '',
                                expression: '',
                              },
                            ],
                          }))
                        }
                      >
                        Add metric
                      </Button>
                    </div>
                    {builder.metrics.length === 0 ? (
                      <p className="text-sm text-[color:var(--text-muted)]">No derived metrics defined.</p>
                    ) : (
                      <div className="space-y-3">
                        {builder.metrics.map((metric) => (
                          <div
                            key={metric.id}
                            className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4"
                          >
                            <div className="flex items-center justify-between">
                              <Label className="text-xs font-semibold uppercase tracking-wide">Metric</Label>
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={() =>
                                  setBuilder((current) => ({
                                    ...current,
                                    metrics: current.metrics.filter((item) => item.id !== metric.id),
                                  }))
                                }
                              >
                                Remove
                              </Button>
                            </div>
                            <div className="mt-3 grid gap-3 md:grid-cols-2">
                              <Input
                                value={metric.name}
                                onChange={(event) =>
                                  setBuilder((current) => ({
                                    ...current,
                                    metrics: current.metrics.map((item) =>
                                      item.id === metric.id ? { ...item, name: event.target.value } : item,
                                    ),
                                  }))
                                }
                                placeholder="Metric name"
                              />
                              <Input
                                value={metric.description ?? ''}
                                onChange={(event) =>
                                  setBuilder((current) => ({
                                    ...current,
                                    metrics: current.metrics.map((item) =>
                                      item.id === metric.id ? { ...item, description: event.target.value } : item,
                                    ),
                                  }))
                                }
                                placeholder="Description"
                              />
                            </div>
                            <Textarea
                              className="mt-3"
                              rows={3}
                              value={metric.expression}
                              onChange={(event) =>
                                setBuilder((current) => ({
                                  ...current,
                                  metrics: current.metrics.map((item) =>
                                    item.id === metric.id ? { ...item, expression: event.target.value } : item,
                                  ),
                                }))
                              }
                              placeholder="SQL expression referencing table columns"
                            />
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="space-y-3 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                    <div className="flex items-center justify-between">
                      <h3 className="text-base font-semibold text-[color:var(--text-primary)]">6. YAML editor</h3>
                      <span className="text-xs text-[color:var(--text-muted)]">YAML is source of truth for save</span>
                    </div>
                    <div className="flex items-center gap-2">
                      {yamlDirty ? <Badge variant="secondary">Unsynced changes</Badge> : null}
                      <Button type="button" variant="outline" size="sm" onClick={() => handleApplyYaml()}>
                        Apply YAML
                      </Button>
                      <Button type="button" variant="outline" size="sm" onClick={() => handleResetYamlFromBuilder()}>
                        Reset
                      </Button>
                    </div>
                    {yamlError ? (
                      <p className="text-xs text-rose-600">{yamlError}</p>
                    ) : null}
                    <Textarea
                      rows={12}
                      value={yamlDraft}
                      onChange={(event) => {
                        setYamlDraft(event.target.value);
                        setYamlDirty(true);
                      }}
                      className="font-mono text-xs"
                    />
                    <Button
                      type="submit"
                      className="w-full"
                      isLoading={submitting}
                      disabled={!selectedConnectorId || loadingModel}
                    >
                      {submitLabel}
                    </Button>
                  </div>
                </>
              )}
            </form>
          </section>
          <aside className="space-y-4 rounded-3xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Outline</h2>
                <Badge variant="secondary">{outline.tables.length} tables</Badge>
              </div>
              <p className="text-xs text-[color:var(--text-muted)]">
                Logical tables, dimensions, facts, and relationships in the current model.
              </p>
            </div>
            <div className="grid gap-2 rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4 text-sm">
              <div className="flex items-center justify-between">
                <span>Dimensions</span>
                <Badge variant="secondary">{outline.dimensions.length}</Badge>
              </div>
              <div className="flex items-center justify-between">
                <span>Facts</span>
                <Badge variant="secondary">{outline.measures.length}</Badge>
              </div>
              <div className="flex items-center justify-between">
                <span>Relationships</span>
                <Badge variant="secondary">{outline.relationships.length}</Badge>
              </div>
            </div>
            {outline.tables.length === 0 ? (
              <p className="text-sm text-[color:var(--text-muted)]">No logical tables defined yet.</p>
            ) : (
              <ul className="space-y-2">
                {outline.tables.map((table) => (
                  <li
                    key={table.id}
                    className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-3 py-2 text-sm"
                  >
                    <p className="font-medium text-[color:var(--text-primary)]">{table.entityName || table.name}</p>
                    <p className="text-xs text-[color:var(--text-muted)]">
                      {table.schema ? `${table.schema}.` : ''}
                      {table.name || 'missing table name'}
                    </p>
                    <p className="text-xs text-[color:var(--text-muted)]">
                      {table.dimensions.length} dims / {table.measures.length} facts
                    </p>
                  </li>
                ))}
              </ul>
            )}
            {outline.relationships.length > 0 ? (
              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-wide text-[color:var(--text-muted)]">
                  Relationships
                </p>
                <ul className="space-y-1 text-xs text-[color:var(--text-secondary)]">
                  {outline.relationships.map((relationship) => (
                    <li
                      key={relationship.id}
                      className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-2 py-1.5"
                    >
                      {relationship.name || `${relationship.from} -> ${relationship.to}`}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </aside>
        </div>
      ) : null}
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

function createId(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 9)}`;
}

function createEmptyBuilderModel(): BuilderModel {
  return {
    version: DEFAULT_MODEL_VERSION,
    description: '',
    tables: [],
    relationships: [],
    metrics: [],
  };
}

function createEmptyRelationship(position: number): BuilderRelationship {
  return {
    id: createId('relationship'),
    name: `join_${position}`,
    from: '',
    to: '',
    type: 'many_to_one',
    joinOn: '',
  };
}

function createEmptyTable(position: number): BuilderTable {
  return {
    id: createId('table'),
    entityName: `entity_${position}`,
    schema: '',
    name: '',
    description: '',
    synonyms: null,
    filters: null,
    dimensions: [],
    measures: [],
  };
}

function tableHasContent(table: BuilderTable): boolean {
  return Boolean(
    table.entityName.trim() ||
      table.schema.trim() ||
      table.name.trim() ||
      table.dimensions.length > 0 ||
      table.measures.length > 0,
  );
}

function validateBuilderModel(builder: BuilderModel): void {
  const populatedTables = builder.tables.filter(tableHasContent);
  if (populatedTables.length === 0) {
    throw new Error('Add at least one table with dimensions or measures.');
  }
  populatedTables.forEach((table) => {
    if (!table.entityName.trim() || !table.schema.trim() || !table.name.trim()) {
      throw new Error('Each table must include an entity name, schema, and table name.');
    }
    if (table.dimensions.length === 0 && table.measures.length === 0) {
      throw new Error(`Table "${table.entityName}" must include at least one dimension or measure.`);
    }
  });
}

function serializeBuilderModel(builder: BuilderModel, connectorName?: string): string {
  const payload = buildSemanticModelPayload(builder, connectorName);
  return yaml.dump(payload, { noRefs: true, sortKeys: false });
}

function buildSemanticModelPayload(builder: BuilderModel, connectorName?: string) {
  const tables = builder.tables
    .filter(tableHasContent)
    .reduce<Record<string, unknown>>((acc, table) => {
      if (!table.entityName.trim() || !table.schema.trim() || !table.name.trim()) {
        return acc;
      }
      acc[table.entityName] = {
        schema: table.schema,
        name: table.name,
        description: table.description || undefined,
        dimensions:
          table.dimensions.length > 0
            ? table.dimensions
                .filter((dimension) => dimension.name && dimension.type)
                .map((dimension) => ({
                  name: dimension.name,
                  expression: dimension.expression || undefined,
                  type: dimension.type,
                  description: dimension.description || undefined,
                  primary_key: dimension.primaryKey || undefined,
                  synonyms: dimension.synonyms && dimension.synonyms.length > 0 ? dimension.synonyms : undefined,
                  vectorized: dimension.vectorized ? true : undefined,
                }))
            : undefined,
        measures:
          table.measures.length > 0
            ? table.measures
                .filter((measure) => measure.name && measure.type)
                .map((measure) => ({
                  name: measure.name,
                  expression: measure.expression || undefined,
                  type: measure.type,
                  aggregation: measure.aggregation || undefined,
                  description: measure.description || undefined,
                  synonyms: measure.synonyms && measure.synonyms.length > 0 ? measure.synonyms : undefined,
                }))
            : undefined,
      };
      return acc;
    }, {});

  const relationships = builder.relationships
    .filter((relationship) => relationship.name && relationship.from && relationship.to && relationship.joinOn)
    .map((relationship) => ({
      name: relationship.name,
      from_: relationship.from,
      to: relationship.to,
      type: relationship.type,
      join_on: relationship.joinOn,
    }));

  const metrics = builder.metrics.reduce<Record<string, { expression: string; description?: string }>>(
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

  return {
    version: builder.version || DEFAULT_MODEL_VERSION,
    connector: connectorName,
    description: builder.description || undefined,
    tables,
    relationships: relationships.length > 0 ? relationships : undefined,
    metrics: Object.keys(metrics).length > 0 ? metrics : undefined,
  };
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' ? (value as Record<string, unknown>) : {};
}

function toArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function toStringValue(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback;
}

function parseYamlToBuilderModel(yamlText: string): BuilderModel {
  const parsed = yaml.load(yamlText);
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('Generated YAML was empty.');
  }

  const candidate = toRecord(parsed);
  const tablesEntries = Object.entries(toRecord(candidate.tables));
  const tables: BuilderTable[] = tablesEntries.map(([entityName, rawTable]) => {
    const table = toRecord(rawTable);
    const dimensions = toArray(table.dimensions);
    const measures = toArray(table.measures);
    return {
      id: createId('table'),
      entityName,
      schema: toStringValue(table.schema),
      name: toStringValue(table.name),
      description: toStringValue(table.description),
      synonyms: (table.synonyms as SemanticTable['synonyms']) ?? null,
      filters: (table.filters as SemanticTable['filters']) ?? null,
      dimensions: dimensions.map((dimension) => {
        const mappedDimension = toRecord(dimension);
        return {
          id: createId('dimension'),
          expression: toStringValue(mappedDimension.expression),
          name: toStringValue(mappedDimension.name),
          type: toStringValue(mappedDimension.type),
          description: toStringValue(mappedDimension.description),
          primaryKey: Boolean(mappedDimension.primary_key ?? mappedDimension.primaryKey),
          vectorized: Boolean(mappedDimension.vectorized),
        };
      }),
      measures: measures.map((measure) => {
        const mappedMeasure = toRecord(measure);
        return {
          id: createId('measure'),
          name: toStringValue(mappedMeasure.name),
          expression: toStringValue(mappedMeasure.expression),
          type: toStringValue(mappedMeasure.type),
          description: toStringValue(mappedMeasure.description),
          aggregation: toStringValue(mappedMeasure.aggregation),
        };
      }),
    };
  });

  const relationshipSource = candidate.relationships ?? candidate.joins;
  const relationships: BuilderRelationship[] = toArray(relationshipSource).map((relationship) => {
    const mappedRelationship = toRecord(relationship);
    const rawType = mappedRelationship.type ?? mappedRelationship.cardinality;
    const candidateType = typeof rawType === 'string' ? (rawType as RelationshipType) : undefined;
    const resolvedType = candidateType && RELATIONSHIP_TYPES.includes(candidateType) ? candidateType : 'many_to_one';
    return {
      id: createId('relationship'),
      name: toStringValue(mappedRelationship.name),
      from: toStringValue(mappedRelationship.from_ ?? mappedRelationship.from ?? mappedRelationship.left),
      to: toStringValue(mappedRelationship.to ?? mappedRelationship.right),
      type: resolvedType,
      joinOn: toStringValue(mappedRelationship.join_on ?? mappedRelationship.joinOn ?? mappedRelationship.on),
    };
  });

  const metricsEntries = Object.entries(toRecord(candidate.metrics));
  const metrics: BuilderMetric[] = metricsEntries.map(([metricName, metric]) => {
    const mappedMetric = toRecord(metric);
    return {
      id: createId('metric'),
      name: metricName,
      expression: toStringValue(mappedMetric.expression),
      description: toStringValue(mappedMetric.description),
    };
  });

  return {
    version: typeof candidate.version === 'string' ? candidate.version : DEFAULT_MODEL_VERSION,
    description: toStringValue(candidate.description),
    tables,
    relationships,
    metrics,
  };
}
