'use client';

import { JSX, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import type { LucideIcon } from 'lucide-react';
import {
  Atom,
  Box,
  Braces,
  Cloud,
  Database,
  Plug,
  Search,
  Server,
  Snowflake as SnowflakeIcon,
} from 'lucide-react';

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
import { Spinner } from '@/components/ui/spinner';
import { Textarea } from '@/components/ui/textarea';
import { cn } from '@/lib/utils';
import { ApiError } from '@/orchestration/http';
import { fetchAgentJobState } from '@/orchestration/jobs';
import {
  bulkCreateDatasets,
  type DatasetSelectionColumnPayload,
} from '@/orchestration/datasets';
import {
  createConnector,
  fetchConnectorCatalog,
  fetchConnectorSchema,
  fetchConnectorTypes,
  type ConnectorCatalogSchema,
  type ConnectorConfigEntry,
  type ConnectorConfigSchema,
  type ConnectorResponse,
  type CreateConnectorPayload,
} from '@/orchestration/connectors';
import {
  fetchOrganizationEnvironmentSettings,
  setOrganizationEnvironmentSetting,
} from '@/orchestration/organizations';
import type { Project } from '@/orchestration/organizations';
import { useWorkspaceScope } from '@/context/workspaceScope';

type FeedbackTone = 'positive' | 'negative';

interface FormFields {
  name: string;
  description: string;
  version: string;
  label: string;
  icon: string;
  organizationId: string;
  projectId: string;
}

interface FeedbackState {
  message: string;
  tone: FeedbackTone;
}

type DatasetGenerationMode = 'guided' | 'all' | 'skip';

type WizardStep = 0 | 1 | 2 | 3;

type CatalogTableNode = {
  schemaName: string;
  tableName: string;
  tableRef: string;
  columns: DatasetSelectionColumnPayload[];
};

interface ConnectorTypeCardProps {
  type: string;
  schema?: ConnectorConfigSchema | null;
  isSelected: boolean;
  isLoading: boolean;
  onSelect: (type: string) => void;
}

const CONNECTOR_ICON_MAP: Record<string, LucideIcon> = {
  SNOWFLAKE: SnowflakeIcon,
  POSTGRES: Database,
  MYSQL: Database,
  MARIADB: Database,
  MONGODB: Database,
  REDSHIFT: Database,
  BIGQUERY: Database,
  SQLSERVER: Server,
  ORACLE: Database,
  ELASTICSEARCH: Search,
  RESTAPI: Braces,
  GRAPHQL: Atom,
  SALESFORCE: Cloud,
  ZAPIER: Plug,
  GENERIC: Box,
};

function resolveErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Something went wrong. Please try again.';
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, milliseconds);
  });
}

function parseBooleanSetting(value: string | undefined): boolean {
  return String(value || '').trim().toLowerCase() === 'true';
}

type DataConnectionsPageProps = {
  params: { organizationId: string };
};

export default function DataConnectionsPage({ params }: DataConnectionsPageProps): JSX.Element {
  const router = useRouter();
  const [connectorTypes, setConnectorTypes] = useState<string[]>([]);
  const [schemasByType, setSchemasByType] = useState<Record<string, ConnectorConfigSchema>>({});
  const [selectedType, setSelectedType] = useState('');
  const [typesLoading, setTypesLoading] = useState(true);
  const [configValues, setConfigValues] = useState<Record<string, string>>({});
  const [formFields, setFormFields] = useState<FormFields>({
    name: '',
    description: '',
    version: '',
    label: '',
    icon: '',
    organizationId: '',
    projectId: '',
  });
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState<FeedbackState | null>(null);
  const [createdConnector, setCreatedConnector] = useState<ConnectorResponse | null>(null);
  const [datasetPromptOpen, setDatasetPromptOpen] = useState(false);
  const [datasetMode, setDatasetMode] = useState<DatasetGenerationMode>('guided');
  const [rememberDatasetChoice, setRememberDatasetChoice] = useState(false);
  const [datasetWizardOpen, setDatasetWizardOpen] = useState(false);
  const [wizardStep, setWizardStep] = useState<WizardStep>(0);
  const [datasetCatalogSchemas, setDatasetCatalogSchemas] = useState<ConnectorCatalogSchema[]>([]);
  const [datasetCatalogLoading, setDatasetCatalogLoading] = useState(false);
  const [datasetSchemaSearch, setDatasetSchemaSearch] = useState('');
  const [datasetIncludeSystemSchemas, setDatasetIncludeSystemSchemas] = useState(false);
  const [selectedDatasetSchemas, setSelectedDatasetSchemas] = useState<string[]>([]);
  const [tableSearch, setTableSearch] = useState('');
  const [tableTab, setTableTab] = useState<'all' | 'selected'>('all');
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<Record<string, string[]>>({});
  const [searchTerm, setSearchTerm] = useState('');
  const [columnSearch, setColumnSearch] = useState('');
  const [columnTab, setColumnTab] = useState<'all' | 'selected'>('all');
  const [includeExampleData, setIncludeExampleData] = useState(false);
  const [namingTemplate, setNamingTemplate] = useState('{schema}.{table}');
  const [policyMaxPreviewRows, setPolicyMaxPreviewRows] = useState('1000');
  const [policyMaxExportRows, setPolicyMaxExportRows] = useState('100000');
  const [policyAllowDml, setPolicyAllowDml] = useState(false);
  const [datasetJobId, setDatasetJobId] = useState<string | null>(null);
  const [datasetJobProgress, setDatasetJobProgress] = useState(0);
  const [datasetJobStatus, setDatasetJobStatus] = useState<string | null>(null);
  const [datasetWizardSubmitting, setDatasetWizardSubmitting] = useState(false);
  const [datasetWizardError, setDatasetWizardError] = useState<string | null>(null);
  const [environmentSettings, setEnvironmentSettings] = useState<Record<string, string>>({});
  const feedbackTimeout = useRef<number | undefined>(undefined);
  const lastInitializedType = useRef<string | null>(null);
  const pendingSchemaRequests = useRef<Map<string, Promise<ConnectorConfigSchema>>>(new Map());

  const showFeedback = useCallback((message: string, tone: FeedbackTone = 'positive') => {
    setFeedback({ message, tone });
    if (feedbackTimeout.current) {
      window.clearTimeout(feedbackTimeout.current);
    }
    feedbackTimeout.current = window.setTimeout(() => setFeedback(null), 5000);
  }, []);

  useEffect(() => {
    return () => {
      if (feedbackTimeout.current) {
        window.clearTimeout(feedbackTimeout.current);
      }
    };
  }, []);

  const routeOrganizationId = params.organizationId;

  const loadSchemaForType = useCallback(
    async (type: string, options: { silent?: boolean } = {}): Promise<ConnectorConfigSchema | null> => {
      if (!routeOrganizationId) {
        return null;
      }
      const normalized = type.trim();
      if (!normalized) {
        return null;
      }
      if (schemasByType[normalized]) {
        return schemasByType[normalized];
      }
      const pending = pendingSchemaRequests.current.get(normalized);
      if (pending) {
        return pending;
      }
      const request = fetchConnectorSchema(routeOrganizationId, normalized)
        .then((schema) => {
          setSchemasByType((current) => {
            if (current[normalized]) {
              return current;
            }
            return { ...current, [normalized]: schema };
          });
          return schema;
        })
        .catch((error) => {
          if (!options.silent) {
            showFeedback(resolveErrorMessage(error), 'negative');
          }
          throw error;
        });
      pendingSchemaRequests.current.set(normalized, request);
      request.finally(() => {
        pendingSchemaRequests.current.delete(normalized);
      });
      return request;
    },
    [routeOrganizationId, schemasByType, showFeedback],
  );

  useEffect(() => {
    async function loadConnectorTypes(): Promise<void> {
      if (!routeOrganizationId) {
        return;
      }
      setTypesLoading(true);
      try {
        const types = await fetchConnectorTypes(routeOrganizationId);
        setConnectorTypes(types);
      } catch (error) {
        showFeedback(resolveErrorMessage(error), 'negative');
      } finally {
        setTypesLoading(false);
      }
    }

    void loadConnectorTypes();
  }, [routeOrganizationId, showFeedback]);

  useEffect(() => {
    async function loadConnectorSchemas(): Promise<void> {
      if (connectorTypes.length === 0) {
        return;
      }
      await Promise.all(
        connectorTypes.map((type) =>
          loadSchemaForType(type, { silent: true }).catch(() => undefined),
        ),
      );
    }

    void loadConnectorSchemas();
  }, [connectorTypes, loadSchemaForType]);

  const {
    organizations,
    loading: organizationsLoading,
    selectedOrganizationId: activeOrganizationId,
    selectedProjectId: activeProjectId,
    setSelectedOrganizationId,
  } = useWorkspaceScope();

  useEffect(() => {
    if (routeOrganizationId && routeOrganizationId !== activeOrganizationId) {
      setSelectedOrganizationId(routeOrganizationId);
    }
  }, [activeOrganizationId, routeOrganizationId, setSelectedOrganizationId]);

  useEffect(() => {
    setFormFields((current) => {
      const nextOrganization = routeOrganizationId || activeOrganizationId || '';
      const nextProject = activeProjectId ?? '';
      if (current.organizationId === nextOrganization && current.projectId === nextProject) {
        return current;
      }
      return { ...current, organizationId: nextOrganization, projectId: nextProject };
    });
  }, [activeOrganizationId, activeProjectId, routeOrganizationId]);

  useEffect(() => {
    let cancelled = false;
    async function loadEnvironmentSettings(): Promise<void> {
      if (!routeOrganizationId) {
        setEnvironmentSettings({});
        return;
      }
      try {
        const rows = await fetchOrganizationEnvironmentSettings(routeOrganizationId);
        if (cancelled) {
          return;
        }
        const settingsByKey: Record<string, string> = {};
        rows.forEach((row) => {
          settingsByKey[row.settingKey] = row.settingValue;
        });
        setEnvironmentSettings(settingsByKey);

        const configuredMode = String(settingsByKey['datasets.auto_generate_mode'] || '')
          .trim()
          .toLowerCase();
        if (configuredMode === 'guided' || configuredMode === 'all' || configuredMode === 'skip') {
          setDatasetMode(configuredMode);
        } else {
          setDatasetMode('guided');
        }
        if (settingsByKey['datasets.default_max_preview_rows']) {
          setPolicyMaxPreviewRows(settingsByKey['datasets.default_max_preview_rows']);
        }
        if (settingsByKey['datasets.default_max_export_rows']) {
          setPolicyMaxExportRows(settingsByKey['datasets.default_max_export_rows']);
        }
      } catch {
        if (!cancelled) {
          setEnvironmentSettings({});
        }
      }
    }

    void loadEnvironmentSettings();
    return () => {
      cancelled = true;
    };
  }, [routeOrganizationId]);

  const selectedSchema = selectedType ? schemasByType[selectedType] ?? null : null;
  const isSchemaLoading = Boolean(selectedType && !selectedSchema);

  useEffect(() => {
    if (!selectedType) {
      lastInitializedType.current = null;
      return;
    }
    if (!selectedSchema) {
      return;
    }
    if (lastInitializedType.current === selectedType) {
      return;
    }

    setFormFields((current) => ({
      ...current,
      name: selectedSchema.name ?? '',
      description: selectedSchema.description ?? '',
      version: selectedSchema.version ?? '',
      label: selectedSchema.label ?? '',
      icon: selectedSchema.icon ?? '',
    }));

    const defaults: Record<string, string> = {};
    selectedSchema.config.forEach((entry) => {
      const fallback = entry.default ?? entry.value ?? '';
      defaults[entry.field] = fallback === null || fallback === undefined ? '' : String(fallback);
    });
    setConfigValues(defaults);
    lastInitializedType.current = selectedType;
  }, [selectedSchema, selectedType]);

  useEffect(() => {
    if (!selectedType) {
      return;
    }
    void loadSchemaForType(selectedType).catch(() => undefined);
  }, [selectedType, loadSchemaForType]);

  const sortedConnectorTypes = useMemo(() => {
    return [...connectorTypes].sort((a, b) => a.localeCompare(b));
  }, [connectorTypes]);

  const configEntries = useMemo(
    () => selectedSchema?.config ?? [],
    [selectedSchema],
  );

  const catalogTables = useMemo<CatalogTableNode[]>(() => {
    const rows: CatalogTableNode[] = [];
    datasetCatalogSchemas.forEach((schemaNode) => {
      schemaNode.tables.forEach((table) => {
        rows.push({
          schemaName: schemaNode.name,
          tableName: table.name,
          tableRef: `${schemaNode.name}.${table.name}`,
          columns: table.columns.map((column) => ({
            name: column.name,
            dataType: column.type,
            nullable: column.nullable ?? true,
          })),
        });
      });
    });
    return rows;
  }, [datasetCatalogSchemas]);

  const selectedTableSet = useMemo(() => new Set(selectedTables), [selectedTables]);
  const selectedColumnCount = useMemo(
    () => Object.values(selectedColumns).reduce((acc, values) => acc + values.length, 0),
    [selectedColumns],
  );
  const filteredSchemaNodes = useMemo(() => {
    const token = datasetSchemaSearch.trim().toLowerCase();
    if (!token) {
      return datasetCatalogSchemas;
    }
    return datasetCatalogSchemas.filter((schemaNode) =>
      schemaNode.name.toLowerCase().includes(token)
      || schemaNode.tables.some((table) => table.name.toLowerCase().includes(token)),
    );
  }, [datasetCatalogSchemas, datasetSchemaSearch]);
  const visibleTables = useMemo(() => {
    const token = tableSearch.trim().toLowerCase();
    return catalogTables.filter((table) => {
      if (tableTab === 'selected' && !selectedTableSet.has(table.tableRef)) {
        return false;
      }
      if (!token) {
        return true;
      }
      return table.tableRef.toLowerCase().includes(token) || table.tableName.toLowerCase().includes(token);
    });
  }, [catalogTables, selectedTableSet, tableSearch, tableTab]);
  const visibleColumnTables = useMemo(() => {
    const token = columnSearch.trim().toLowerCase();
    return catalogTables
      .filter((table) => selectedTableSet.has(table.tableRef))
      .map((table) => {
        const selectedNames = new Set(selectedColumns[table.tableRef] || []);
        const scopedColumns =
          columnTab === 'selected'
            ? table.columns.filter((column) => selectedNames.has(column.name))
            : table.columns;
        const filteredColumns = token
          ? scopedColumns.filter((column) => column.name.toLowerCase().includes(token))
          : scopedColumns;
        return {
          ...table,
          selectedNames,
          filteredColumns,
        };
      });
  }, [catalogTables, columnSearch, columnTab, selectedColumns, selectedTableSet]);

  const organizationId = routeOrganizationId || formFields.organizationId;
  const projectId = formFields.projectId;

  const projectOptions = useMemo(() => {
    if (organizations.length === 0) {
      return [] as Array<Project & { organizationName: string }>;
    }
    if (organizationId) {
      const org = organizations.find((item) => item.id === organizationId);
      if (!org) {
        return [];
      }
      return org.projects.map((project) => ({ ...project, organizationName: org.name }));
    }
    return organizations.flatMap((org) =>
      org.projects.map((project) => ({ ...project, organizationName: org.name })),
    );
  }, [organizations, organizationId]);

  const handleTypeSelect = useCallback(
    (type: string) => {
      if (type === selectedType) {
        return;
      }
      lastInitializedType.current = null;
      setSelectedType(type);
      setCreatedConnector(null);
    },
    [selectedType],
  );

  function handleConfigChange(field: string, value: string): void {
    setConfigValues((current) => ({
      ...current,
      [field]: value,
    }));
  }

  function handleFieldChange<K extends keyof FormFields>(field: K, value: FormFields[K]): void {
    setFormFields((current) => ({
      ...current,
      [field]: value,
    }));
  }

  function handleOrganizationSelect(value: string): void {
    setFormFields((current) => {
      const updated: FormFields = {
        ...current,
        organizationId: value,
      };
      if (value) {
        const organization = organizations.find((item) => item.id === value);
        const hasSelectedProject =
          organization?.projects.some((project) => project.id === current.projectId) ?? false;
        if (!hasSelectedProject) {
          updated.projectId = '';
        }
      } else {
        updated.organizationId = '';
      }
      return updated;
    });
  }

  function handleProjectSelect(value: string): void {
    setFormFields((current) => {
      if (!value) {
        return {
          ...current,
          projectId: '',
        };
      }
      const owningOrganization = organizations.find((org) =>
        org.projects.some((project) => project.id === value),
      );
      const updated: FormFields = {
        ...current,
        projectId: value,
      };
      if (owningOrganization && owningOrganization.id !== current.organizationId) {
        updated.organizationId = owningOrganization.id;
      }
      return updated;
    });
  }

  function validateBeforeSubmit(): string | null {
    if (!selectedType) {
      return 'Choose a connector type before continuing.';
    }
    if (!selectedSchema) {
      return 'Hold on while we finish loading the connector schema.';
    }
    const trimmedName = formFields.name.trim();
    if (!trimmedName) {
      return 'Provide a name for the connector.';
    }
    if (!formFields.organizationId.trim() && !formFields.projectId.trim()) {
      return 'Add an organization or project so we know where to attach this connector.';
    }
    const missingRequired = configEntries
      .filter((entry) => entry.required)
      .filter((entry) => !configValues[entry.field]?.trim());
    if (missingRequired.length > 0) {
      const labels = missingRequired.map((entry) => entry.label ?? entry.field);
      return `Fill in the required field${labels.length > 1 ? 's' : ''}: ${labels.join(', ')}.`;
    }
    return null;
  }

  const initializeDatasetSelectionFromCatalog = useCallback(
    (schemas: ConnectorCatalogSchema[], mode: DatasetGenerationMode) => {
      const schemaNames = schemas.map((schema) => schema.name);
      setSelectedDatasetSchemas(schemaNames);
      const nextTables: string[] = [];
      const nextColumns: Record<string, string[]> = {};
      schemas.forEach((schemaNode) => {
        schemaNode.tables.forEach((table) => {
          const tableRef = `${schemaNode.name}.${table.name}`;
          nextTables.push(tableRef);
          nextColumns[tableRef] = table.columns.map((column) => column.name);
        });
      });
      if (mode === 'all') {
        setSelectedTables(nextTables);
        setSelectedColumns(nextColumns);
        return;
      }
      setSelectedTables(nextTables);
      setSelectedColumns(nextColumns);
    },
    [],
  );

  const loadConnectorCatalogForWizard = useCallback(
    async (connectorId: string, mode: DatasetGenerationMode) => {
      if (!routeOrganizationId || !connectorId) {
        return;
      }
      setDatasetCatalogLoading(true);
      setDatasetWizardError(null);
      try {
        const response = await fetchConnectorCatalog(routeOrganizationId, connectorId, {
          includeSystemSchemas: datasetIncludeSystemSchemas,
          includeColumns: true,
          limit: 1000,
          offset: 0,
        });
        setDatasetCatalogSchemas(response.schemas || []);
        initializeDatasetSelectionFromCatalog(response.schemas || [], mode);
      } catch (error) {
        setDatasetWizardError(resolveErrorMessage(error));
      } finally {
        setDatasetCatalogLoading(false);
      }
    },
    [datasetIncludeSystemSchemas, initializeDatasetSelectionFromCatalog, routeOrganizationId],
  );

  useEffect(() => {
    if (!datasetWizardOpen || !createdConnector?.id) {
      return;
    }
    void loadConnectorCatalogForWizard(createdConnector.id, datasetMode);
  }, [createdConnector?.id, datasetMode, datasetWizardOpen, loadConnectorCatalogForWizard]);

  const persistDatasetChoice = useCallback(
    async (mode: DatasetGenerationMode) => {
      if (!rememberDatasetChoice || !routeOrganizationId) {
        return;
      }
      const shouldEnable = mode !== 'skip';
      await setOrganizationEnvironmentSetting(
        routeOrganizationId,
        'datasets.auto_generate_on_connection_add',
        shouldEnable ? 'true' : 'false',
      );
      await setOrganizationEnvironmentSetting(
        routeOrganizationId,
        'datasets.auto_generate_mode',
        mode,
      );
    },
    [rememberDatasetChoice, routeOrganizationId],
  );

  const toggleTableSelection = useCallback((tableRef: string, checked: boolean) => {
    setSelectedTables((current) => {
      const next = new Set(current);
      if (checked) {
        next.add(tableRef);
      } else {
        next.delete(tableRef);
      }
      return Array.from(next);
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
  }, [catalogTables]);

  const toggleColumnSelection = useCallback((tableRef: string, columnName: string, checked: boolean) => {
    setSelectedColumns((current) => {
      const set = new Set(current[tableRef] || []);
      if (checked) {
        set.add(columnName);
      } else {
        set.delete(columnName);
      }
      return {
        ...current,
        [tableRef]: Array.from(set),
      };
    });
  }, []);

  const toggleSchemaSelection = useCallback((schemaName: string, checked: boolean) => {
    setSelectedDatasetSchemas((current) => {
      const next = new Set(current);
      if (checked) {
        next.add(schemaName);
      } else {
        next.delete(schemaName);
      }
      return Array.from(next);
    });

    const schemaTables = catalogTables.filter((table) => table.schemaName === schemaName);
    schemaTables.forEach((table) => {
      toggleTableSelection(table.tableRef, checked);
    });
  }, [catalogTables, toggleTableSelection]);

  const selectAllTables = useCallback((checked: boolean) => {
    if (!checked) {
      setSelectedTables([]);
      setSelectedColumns({});
      return;
    }
    const nextTables = catalogTables.map((table) => table.tableRef);
    const nextColumns: Record<string, string[]> = {};
    catalogTables.forEach((table) => {
      nextColumns[table.tableRef] = table.columns.map((column) => column.name);
    });
    setSelectedTables(nextTables);
    setSelectedColumns(nextColumns);
  }, [catalogTables]);

  const selectAllColumns = useCallback((checked: boolean) => {
    if (!checked) {
      setSelectedColumns((current) => {
        const next: Record<string, string[]> = {};
        Object.keys(current).forEach((tableRef) => {
          next[tableRef] = [];
        });
        return next;
      });
      return;
    }
    const next: Record<string, string[]> = {};
    selectedTables.forEach((tableRef) => {
      const table = catalogTables.find((entry) => entry.tableRef === tableRef);
      next[tableRef] = (table?.columns || []).map((column) => column.name);
    });
    setSelectedColumns(next);
  }, [catalogTables, selectedTables]);

  const selectAllColumnsForTable = useCallback((tableRef: string, checked: boolean) => {
    const table = catalogTables.find((entry) => entry.tableRef === tableRef);
    if (!table) {
      return;
    }
    setSelectedColumns((current) => ({
      ...current,
      [tableRef]: checked ? table.columns.map((column) => column.name) : [],
    }));
  }, [catalogTables]);

  const handleDatasetPromptContinue = useCallback(async () => {
    if (!createdConnector?.id) {
      return;
    }
    try {
      await persistDatasetChoice(datasetMode);
    } catch {
      // Keep flow non-blocking if saving the preference fails.
    }

    if (datasetMode === 'skip') {
      setDatasetPromptOpen(false);
      return;
    }

    setDatasetPromptOpen(false);
    setDatasetWizardOpen(true);
    setWizardStep(0);
    await loadConnectorCatalogForWizard(createdConnector.id, datasetMode);
  }, [createdConnector?.id, datasetMode, loadConnectorCatalogForWizard, persistDatasetChoice]);

  const goBackWizardStep = useCallback(() => {
    setDatasetWizardError(null);
    setWizardStep((current) => (current <= 0 ? 0 : ((current - 1) as WizardStep)));
  }, []);

  const goNextWizardStep = useCallback(() => {
    if (wizardStep === 0 && selectedDatasetSchemas.length === 0) {
      setDatasetWizardError('Select at least one schema.');
      return;
    }
    if (wizardStep === 1 && selectedTables.length === 0) {
      setDatasetWizardError('Select at least one table.');
      return;
    }
    if (wizardStep === 2) {
      const invalid = selectedTables.find((tableRef) => (selectedColumns[tableRef] || []).length === 0);
      if (invalid) {
        setDatasetWizardError(`Select at least one column for ${invalid}.`);
        return;
      }
    }
    setDatasetWizardError(null);
    setWizardStep((current) => (current >= 3 ? 3 : ((current + 1) as WizardStep)));
  }, [selectedColumns, selectedDatasetSchemas.length, selectedTables, wizardStep]);

  const startBulkDatasetGeneration = useCallback(async () => {
    if (!routeOrganizationId || !createdConnector?.id) {
      return;
    }
    if (selectedTables.length === 0) {
      setDatasetWizardError('Select at least one table to continue.');
      return;
    }
    const selections = selectedTables.map((tableRef) => {
      const table = catalogTables.find((item) => item.tableRef === tableRef);
      const selectedNames = new Set(selectedColumns[tableRef] || []);
      const [schema, ...tableParts] = tableRef.split('.');
      const tableName = tableParts.join('.');
      return {
        schema,
        table: tableName,
        columns: (table?.columns || [])
          .filter((column) => selectedNames.has(column.name))
          .map((column) => ({
            name: column.name,
            dataType: column.dataType ?? null,
            nullable: column.nullable ?? true,
          })),
      };
    });
    if (selections.some((selection) => selection.columns.length === 0)) {
      setDatasetWizardError('Each selected table must include at least one column.');
      return;
    }

    setDatasetWizardSubmitting(true);
    setDatasetWizardError(null);
    try {
      const start = await bulkCreateDatasets({
        workspaceId: routeOrganizationId,
        projectId: formFields.projectId || undefined,
        connectionId: createdConnector.id,
        selections,
        namingTemplate: namingTemplate || '{schema}.{table}',
        policyDefaults: {
          maxPreviewRows: Math.max(1, Number(policyMaxPreviewRows) || 1000),
          maxExportRows: Math.max(1, Number(policyMaxExportRows) || 100000),
          allowDml: policyAllowDml,
          redactionRules: {},
        },
        tags: ['auto-generated', 'connection-onboarding'],
        profileAfterCreate: includeExampleData,
      });
      setDatasetJobId(start.jobId);
      setDatasetJobStatus(start.jobStatus);
      let terminalStatus: string | null = null;
      for (let attempt = 0; attempt < 180; attempt += 1) {
        const state = await fetchAgentJobState(routeOrganizationId, start.jobId);
        setDatasetJobProgress(state.progress || 0);
        setDatasetJobStatus(state.status || null);
        if (state.status === 'succeeded' || state.status === 'failed' || state.status === 'cancelled') {
          terminalStatus = state.status;
          break;
        }
        await sleep(2000);
      }
      if (terminalStatus !== 'succeeded') {
        throw new Error('Bulk dataset generation did not finish successfully.');
      }
      setDatasetWizardOpen(false);
      showFeedback('Datasets generated successfully.', 'positive');
      router.push(`/datasets/${routeOrganizationId}?auto_generated=1`);
    } catch (error) {
      setDatasetWizardError(resolveErrorMessage(error));
    } finally {
      setDatasetWizardSubmitting(false);
    }
  }, [
    catalogTables,
    createdConnector?.id,
    formFields.projectId,
    includeExampleData,
    namingTemplate,
    policyAllowDml,
    policyMaxExportRows,
    policyMaxPreviewRows,
    routeOrganizationId,
    router,
    selectedColumns,
    selectedTables,
    showFeedback,
  ]);

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    const validationError = validateBeforeSubmit();
    if (validationError) {
      showFeedback(validationError, 'negative');
      return;
    }

    if (!organizationId) {
      showFeedback('Select an organization before creating a connector.', 'negative');
      return;
    }

    const cleanedConfig: Record<string, string> = {};
    configEntries.forEach((entry) => {
      const rawValue = configValues[entry.field] ?? '';
      const trimmedValue = rawValue.trim();
      if (trimmedValue) {
        cleanedConfig[entry.field] = trimmedValue;
      }
    });

    const payload: CreateConnectorPayload = {
      name: formFields.name.trim(),
      connectorType: selectedType,
      config: { config: cleanedConfig },
    };

    const description = formFields.description.trim();
    if (description) {
      payload.description = description;
    }
    const version = formFields.version.trim();
    if (version) {
      payload.version = version;
    }
    const label = formFields.label.trim();
    if (label) {
      payload.label = label;
    }
    const icon = formFields.icon.trim();
    if (icon) {
      payload.icon = icon;
    }
    const organizationIdValue = formFields.organizationId.trim();
    if (organizationIdValue) {
      payload.organizationId = organizationIdValue;
    }
    const projectIdValue = formFields.projectId.trim();
    if (projectIdValue) {
      payload.projectId = projectIdValue;
    }

    setSubmitting(true);
    setCreatedConnector(null);

    try {
      const connector = await createConnector(organizationId, payload);
      setCreatedConnector(connector);
      showFeedback(`Connector "${connector.name}" created successfully.`, 'positive');
      const autoGenerateEnabled = parseBooleanSetting(
        environmentSettings['datasets.auto_generate_on_connection_add'],
      );
      const configuredMode = String(environmentSettings['datasets.auto_generate_mode'] || '')
        .trim()
        .toLowerCase();
      const preferredMode: DatasetGenerationMode =
        configuredMode === 'guided' || configuredMode === 'all' || configuredMode === 'skip'
          ? configuredMode
          : datasetMode;
      setDatasetMode(autoGenerateEnabled ? preferredMode : 'guided');
      setRememberDatasetChoice(false);
      setDatasetPromptOpen(true);
    } catch (error) {
      showFeedback(resolveErrorMessage(error), 'negative');
    } finally {
      setSubmitting(false);
    }
  }

  function renderConfigInput(entry: ConnectorConfigEntry): JSX.Element {
    const value = configValues[entry.field] ?? '';

    if (entry.valueList && entry.valueList.length > 0) {
      return (
        <Select
          id={`config-${entry.field}`}
          placeholder={`Select ${entry.label ?? entry.field}`}
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        >
          {entry.valueList.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </Select>
      );
    }

    if (entry.type === 'password') {
      return (
        <Input
          id={`config-${entry.field}`}
          type="password"
          autoComplete="off"
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        />
      );
    }

    if (entry.type === 'number') {
      return (
        <Input
          id={`config-${entry.field}`}
          type="number"
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        />
      );
    }

    if (entry.type === 'boolean') {
      return (
        <Select
          id={`config-${entry.field}`}
          placeholder={`Select ${entry.label ?? entry.field}`}
          value={value || ''}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        >
          <option value="true">True</option>
          <option value="false">False</option>
        </Select>
      );
    }

    if (entry.type === 'textarea') {
      return (
        <Textarea
          id={`config-${entry.field}`}
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        />
      );
    }

    return (
      <Input
        id={`config-${entry.field}`}
        value={value}
        onChange={(event) => handleConfigChange(entry.field, event.target.value)}
      />
    );
  }

  function renderSchemaDetails(currentSchema: ConnectorConfigSchema): JSX.Element | null {
    if (!currentSchema) {
      return null;
    }

    return (
      <div className="flex flex-wrap items-center gap-3 text-sm text-[color:var(--text-secondary)]">
        <Badge variant="secondary" className="uppercase tracking-wide">
          {selectedType}
        </Badge>
        <Badge variant="warning" className="capitalize">
          {currentSchema.connectorType}
        </Badge>
        <span>
          Version{' '}
          <strong className="font-semibold text-[color:var(--text-primary)]">
            {currentSchema.version}
          </strong>
        </span>
      </div>
    );
  }

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      {feedback ? (
        <div
          role="status"
          className={cn(
            'rounded-lg border px-4 py-3 text-sm shadow-soft',
            feedback.tone === 'positive'
              ? 'border-emerald-400/60 bg-emerald-500/10 text-emerald-700'
              : 'border-rose-400/60 bg-rose-500/10 text-rose-800',
          )}
        >
          {feedback.message}
        </div>
      ) : null}

      <section className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
        <header className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-base font-semibold text-[color:var(--text-primary)]">Choose a connector type</h2>
            <p>Click a card to load its schema and generate a tailored form automatically.</p>
          </div>
          {typesLoading ? <Spinner className="h-5 w-5 text-[color:var(--text-secondary)]" /> : null}
        </header>

        <Input
          className="mt-5"
          placeholder="Search connector types"
          value={searchTerm}
          onChange={(event) => setSearchTerm(event.target.value)}
        />

        {sortedConnectorTypes.length > 0 ? (
          <div className="mt-5 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {sortedConnectorTypes.map((type) => (
              <ConnectorTypeCard
                key={type}
                type={type}
                schema={schemasByType[type]}
                isSelected={selectedType === type}
                isLoading={selectedType === type && isSchemaLoading}
                onSelect={handleTypeSelect}
              />
            ))}
          </div>
        ) : (
          <div className="mt-5 flex items-center gap-2 rounded-lg border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-6 text-sm">
            <Spinner className="h-4 w-4 text-[color:var(--text-secondary)]" />
            Fetching connector types...
          </div>
        )}

        {selectedSchema ? <div className="mt-6">{renderSchemaDetails(selectedSchema)}</div> : null}
      </section>

      {selectedType ? (
        isSchemaLoading ? (
          <section className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 text-center text-sm shadow-soft">
            Loading the schema for {selectedType}...
          </section>
        ) : selectedSchema ? (
          <section className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
            <form className="space-y-8" onSubmit={handleSubmit}>
              <div className="space-y-4">
                <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                  <div>
                    <h2 className="text-base font-semibold text-[color:var(--text-primary)]">Connector details</h2>
                    <p>
                      Give the connector a friendly name and optional metadata that appears in dashboards.
                    </p>
                  </div>
                  {selectedSchema.name ? (
                    <Badge variant="secondary" className="text-xs">
                      Default name: {selectedSchema.name}
                    </Badge>
                  ) : null}
                </div>

                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="connector-name" className="text-[color:var(--text-secondary)]">
                      Name
                    </Label>
                    <Input
                      id="connector-name"
                      value={formFields.name}
                      onChange={(event) => handleFieldChange('name', event.target.value)}
                      placeholder={selectedSchema.name}
                      required
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="connector-description" className="text-[color:var(--text-secondary)]">
                    Description
                  </Label>
                  <Textarea
                    id="connector-description"
                    value={formFields.description}
                    onChange={(event) => handleFieldChange('description', event.target.value)}
                    placeholder={selectedSchema.description}
                  />
                </div>
              </div>

              <div className="space-y-4">
                <div>
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Connection scope</h3>
                  <p className="text-sm">
                    Add the organization or project that should own this connector. Provide at least one ID.
                  </p>
                </div>
                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="organization-id" className="text-[color:var(--text-secondary)]">
                      Organization
                    </Label>
                    <Select
                      id="organization-id"
                      value={organizationId}
                      placeholder={organizationsLoading ? 'Loading organizations...' : 'Select an organization'}
                      disabled={organizationsLoading || organizations.length === 0 || Boolean(routeOrganizationId)}
                      onChange={(event) => handleOrganizationSelect(event.target.value)}
                    >
                      {organizations.map((organization) => (
                        <option key={organization.id} value={organization.id}>
                          {organization.name}
                        </option>
                      ))}
                    </Select>
                    {organizations.length === 0 && !organizationsLoading ? (
                      <p className="text-xs text-[color:var(--text-muted)]">
                        You do not have any organizations yet. Create one to continue.
                      </p>
                    ) : null}
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="project-id" className="text-[color:var(--text-secondary)]">
                      Project
                    </Label>
                    <Select
                      id="project-id"
                      value={projectId}
                      placeholder={
                        organizationId
                          ? projectOptions.length > 0
                            ? 'Select a project'
                            : 'No projects found for this organization'
                          : 'Select a project'
                      }
                      disabled={projectOptions.length === 0}
                      onChange={(event) => handleProjectSelect(event.target.value)}
                    >
                      <option value="">-- No project (workspace) --</option>
                      {projectOptions.map((project) => (
                        <option key={project.id} value={project.id}>
                          {organizationId ? project.name : `${project.organizationName} - ${project.name}`}
                        </option>
                      ))}
                    </Select>
                  </div>
                </div>
              </div>

              <div className="space-y-4">
                <div>
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Connector configuration</h3>
                  <p className="text-sm">Fill in the required connection fields. We will validate the schema before saving.</p>
                </div>

                <div className="grid gap-4 sm:grid-cols-2">
                  {configEntries.map((entry) => (
                    <div key={entry.field} className="space-y-2">
                      <Label
                        htmlFor={`config-${entry.field}`}
                        className="flex items-center gap-2 text-[color:var(--text-secondary)]"
                      >
                        <span>{entry.label ?? entry.field}</span>
                        {entry.required ? (
                          <span className="rounded-full bg-rose-100 px-2 py-0.5 text-xs font-medium text-rose-700">
                            Required
                          </span>
                        ) : null}
                      </Label>
                      {renderConfigInput(entry)}
                      {entry.description ? (
                        <p className="text-xs text-[color:var(--text-muted)]">{entry.description}</p>
                      ) : null}
                    </div>
                  ))}
                </div>
              </div>

              <div className="flex items-center justify-between gap-3 border-t border-[color:var(--panel-border)] pt-4 text-xs">
                <div className="text-[color:var(--text-muted)]">
                  We will validate the connection before saving it to your workspace.
                </div>
                <Button
                  type="submit"
                  isLoading={submitting}
                  loadingText="Creating..."
                  disabled={submitting || isSchemaLoading || !selectedSchema}
                >
                  Create connector
                </Button>
              </div>
            </form>
          </section>
        ) : null
      ) : (
        <section className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 text-center text-sm shadow-soft">
          Select a connector card above to generate its form.
        </section>
      )}

      {createdConnector ? (
        <section className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
          <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Connector ready to use</h3>
          <p className="mt-1 text-sm">
            We saved {createdConnector.name}. Keep the identifiers handy in case you need to update this connector later.
          </p>
          <dl className="mt-4 grid gap-3 text-sm sm:grid-cols-2">
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Connector ID</dt>
              <dd className="mt-1 font-mono text-xs">{createdConnector.id ?? 'Not returned'}</dd>
            </div>
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Connector type</dt>
              <dd className="mt-1 font-mono text-xs uppercase">
                {createdConnector.connectorType ?? selectedType}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Organization</dt>
              <dd className="mt-1 font-mono text-xs">{createdConnector.organizationId ?? 'Not provided'}</dd>
            </div>
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Project</dt>
              <dd className="mt-1 font-mono text-xs">{createdConnector.projectId ?? 'Not provided'}</dd>
            </div>
            {createdConnector.catalogSummary ? (
              <div className="sm:col-span-2">
                <dt className="font-medium text-[color:var(--text-primary)]">Catalog summary</dt>
                <dd className="mt-1 text-xs">
                  {createdConnector.catalogSummary.schemaCount} schemas /{' '}
                  {createdConnector.catalogSummary.tableCount} tables /{' '}
                  {createdConnector.catalogSummary.columnCount} columns
                </dd>
              </div>
            ) : null}
          </dl>
        </section>
      ) : null}

      <Dialog open={datasetPromptOpen} onOpenChange={setDatasetPromptOpen}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Connection added</DialogTitle>
            <DialogDescription>
              Would you like to create datasets from this connection?
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            {([
              {
                mode: 'guided' as DatasetGenerationMode,
                title: 'Guided dataset generation',
                description: 'Recommended: review schemas, tables, columns, and policies before creation.',
              },
              {
                mode: 'all' as DatasetGenerationMode,
                title: 'Auto-generate all datasets',
                description: 'Advanced: start with all schemas, tables, and columns selected.',
              },
              {
                mode: 'skip' as DatasetGenerationMode,
                title: 'Skip for now',
                description: 'You can generate datasets later from the Datasets page.',
              },
            ]).map((option) => (
              <button
                key={option.mode}
                type="button"
                onClick={() => setDatasetMode(option.mode)}
                className={cn(
                  'w-full rounded-xl border p-3 text-left',
                  datasetMode === option.mode
                    ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)]'
                    : 'border-[color:var(--panel-border)]',
                )}
              >
                <p className="text-sm font-semibold text-[color:var(--text-primary)]">{option.title}</p>
                <p className="text-xs text-[color:var(--text-muted)]">{option.description}</p>
              </button>
            ))}
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={rememberDatasetChoice}
                onChange={(event) => setRememberDatasetChoice(event.target.checked)}
              />
              Remember my choice for this workspace
            </label>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDatasetPromptOpen(false)}>
              Close
            </Button>
            <Button onClick={() => void handleDatasetPromptContinue()}>
              Continue
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={datasetWizardOpen} onOpenChange={setDatasetWizardOpen}>
        <DialogContent className="max-h-[92vh] overflow-y-auto sm:max-w-5xl">
          <DialogHeader>
            <DialogTitle>Create datasets from connection</DialogTitle>
            <DialogDescription>
              Guided wizard for scoped, governed dataset generation.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div className="grid gap-2 sm:grid-cols-4">
              {['Scope', 'Select tables', 'Select columns', 'Policies & naming'].map((label, index) => (
                <div
                  key={label}
                  className={cn(
                    'rounded-lg border px-3 py-2 text-xs',
                    wizardStep === index
                      ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)] text-[color:var(--text-primary)]'
                      : 'border-[color:var(--panel-border)] text-[color:var(--text-muted)]',
                  )}
                >
                  {index + 1}. {label}
                </div>
              ))}
            </div>

            {datasetWizardSubmitting ? (
              <div className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                <p className="text-sm font-semibold text-[color:var(--text-primary)]">Generating datasets...</p>
                <p className="text-xs text-[color:var(--text-muted)]">
                  Job {datasetJobId ?? 'queued'} | {datasetJobStatus ?? 'running'} | {datasetJobProgress}%
                </p>
              </div>
            ) : null}

            {wizardStep === 0 ? (
              <div className="space-y-3">
                <div className="flex items-center gap-3">
                  <Input
                    value={datasetSchemaSearch}
                    onChange={(event) => setDatasetSchemaSearch(event.target.value)}
                    placeholder="Search schemas"
                  />
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      checked={datasetIncludeSystemSchemas}
                      onChange={(event) => setDatasetIncludeSystemSchemas(event.target.checked)}
                    />
                    Include system schemas
                  </label>
                </div>
                <div className="max-h-[360px] space-y-2 overflow-y-auto rounded-lg border border-[color:var(--panel-border)] p-3">
                  {datasetCatalogLoading ? <p className="text-xs">Loading catalog...</p> : null}
                  {filteredSchemaNodes.map((schemaNode) => {
                    const checked = selectedDatasetSchemas.includes(schemaNode.name);
                    return (
                      <label key={schemaNode.name} className="flex items-center justify-between gap-2 rounded px-2 py-1 text-sm">
                        <span>
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={(event) => toggleSchemaSelection(schemaNode.name, event.target.checked)}
                            className="mr-2"
                          />
                          {schemaNode.name}
                        </span>
                        <Badge variant="secondary">{schemaNode.tables.length} tables</Badge>
                      </label>
                    );
                  })}
                </div>
              </div>
            ) : null}

            {wizardStep === 1 ? (
              <div className="space-y-3">
                <div className="flex flex-wrap items-center gap-2">
                  <Input
                    value={tableSearch}
                    onChange={(event) => setTableSearch(event.target.value)}
                    placeholder="Search tables"
                  />
                  <Button type="button" size="sm" variant="outline" onClick={() => setTableTab('all')}>
                    All
                  </Button>
                  <Button type="button" size="sm" variant="outline" onClick={() => setTableTab('selected')}>
                    Selected
                  </Button>
                  <Button type="button" size="sm" variant="outline" onClick={() => selectAllTables(true)}>
                    Select all tables
                  </Button>
                </div>
                <div className="max-h-[380px] space-y-3 overflow-y-auto rounded-lg border border-[color:var(--panel-border)] p-3">
                  {filteredSchemaNodes.map((schemaNode) => {
                    const schemaTables = visibleTables.filter((table) => table.schemaName === schemaNode.name);
                    if (schemaTables.length === 0) {
                      return null;
                    }
                    return (
                      <div key={schemaNode.name} className="rounded-lg border border-[color:var(--panel-border)] p-2">
                        <div className="mb-2 flex items-center justify-between">
                          <p className="text-sm font-semibold text-[color:var(--text-primary)]">{schemaNode.name}</p>
                          <Button
                            type="button"
                            size="sm"
                            variant="outline"
                            onClick={() => {
                              const everySelected = schemaTables.every((table) => selectedTableSet.has(table.tableRef));
                              schemaTables.forEach((table) => toggleTableSelection(table.tableRef, !everySelected));
                            }}
                          >
                            Select all for schema
                          </Button>
                        </div>
                        <div className="space-y-1">
                          {schemaTables.map((table) => (
                            <label key={table.tableRef} className="flex items-center gap-2 text-xs">
                              <input
                                type="checkbox"
                                checked={selectedTableSet.has(table.tableRef)}
                                onChange={(event) => toggleTableSelection(table.tableRef, event.target.checked)}
                              />
                              {table.tableName}
                            </label>
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : null}

            {wizardStep === 2 ? (
              <div className="space-y-3">
                <div className="flex flex-wrap items-center gap-2">
                  <Input
                    value={columnSearch}
                    onChange={(event) => setColumnSearch(event.target.value)}
                    placeholder="Search columns"
                  />
                  <Button type="button" size="sm" variant="outline" onClick={() => setColumnTab('all')}>
                    All
                  </Button>
                  <Button type="button" size="sm" variant="outline" onClick={() => setColumnTab('selected')}>
                    Selected
                  </Button>
                  <Button type="button" size="sm" variant="outline" onClick={() => selectAllColumns(true)}>
                    Select all columns
                  </Button>
                </div>
                <div className="max-h-[380px] space-y-3 overflow-y-auto rounded-lg border border-[color:var(--panel-border)] p-3">
                  {visibleColumnTables.map((table) => {
                    const allChecked =
                      table.columns.length > 0 && table.columns.every((column) => table.selectedNames.has(column.name));
                    return (
                      <div key={table.tableRef} className="rounded-lg border border-[color:var(--panel-border)] p-2">
                        <div className="mb-2 flex items-center justify-between">
                          <p className="text-sm font-semibold text-[color:var(--text-primary)]">{table.tableRef}</p>
                          <Button
                            type="button"
                            size="sm"
                            variant="outline"
                            onClick={() => selectAllColumnsForTable(table.tableRef, !allChecked)}
                          >
                            Select all for table
                          </Button>
                        </div>
                        <div className="grid gap-1 sm:grid-cols-2 lg:grid-cols-3">
                          {table.filteredColumns.map((column) => (
                            <label key={`${table.tableRef}.${column.name}`} className="flex items-center gap-2 text-xs">
                              <input
                                type="checkbox"
                                checked={table.selectedNames.has(column.name)}
                                onChange={(event) => toggleColumnSelection(table.tableRef, column.name, event.target.checked)}
                              />
                              {column.name}
                            </label>
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
                <label className="flex items-center gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={includeExampleData}
                    onChange={(event) => setIncludeExampleData(event.target.checked)}
                  />
                  Include example data in profiling
                </label>
              </div>
            ) : null}

            {wizardStep === 3 ? (
              <div className="space-y-3">
                <div className="grid gap-3 md:grid-cols-2">
                  <div className="space-y-1">
                    <Label>Naming template</Label>
                    <Input
                      value={namingTemplate}
                      onChange={(event) => setNamingTemplate(event.target.value)}
                      placeholder="{schema}.{table}"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label>Max preview rows</Label>
                    <Input
                      value={policyMaxPreviewRows}
                      onChange={(event) => setPolicyMaxPreviewRows(event.target.value)}
                    />
                  </div>
                  <div className="space-y-1">
                    <Label>Max export rows</Label>
                    <Input
                      value={policyMaxExportRows}
                      onChange={(event) => setPolicyMaxExportRows(event.target.value)}
                    />
                  </div>
                  <label className="flex items-center gap-2 text-sm">
                    <input
                      type="checkbox"
                      checked={policyAllowDml}
                      onChange={(event) => setPolicyAllowDml(event.target.checked)}
                    />
                    Allow DML
                  </label>
                </div>
                <div className="rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3 text-sm">
                  <p>Datasets to create: {selectedTables.length}</p>
                  <p>Columns selected: {selectedColumnCount}</p>
                </div>
              </div>
            ) : null}

            {datasetWizardError ? (
              <p className="text-sm text-rose-600">{datasetWizardError}</p>
            ) : null}
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                if (wizardStep === 0) {
                  setDatasetWizardOpen(false);
                  return;
                }
                goBackWizardStep();
              }}
              disabled={datasetWizardSubmitting}
            >
              {wizardStep === 0 ? 'Cancel' : 'Back'}
            </Button>
            {wizardStep < 3 ? (
              <Button onClick={goNextWizardStep} disabled={datasetWizardSubmitting}>
                Next
              </Button>
            ) : (
              <Button onClick={() => void startBulkDatasetGeneration()} isLoading={datasetWizardSubmitting}>
                Generate datasets
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function ConnectorTypeCard({ type, schema, isSelected, isLoading, onSelect }: ConnectorTypeCardProps) {
  const IconComponent = CONNECTOR_ICON_MAP[type] ?? Plug;
  const title = schema?.label ?? type.toLowerCase().replace(/_/g, ' ');
  const description =
    schema?.description ??
    `Configure and launch a ${type.toLowerCase().replace(/_/g, ' ')} connector in minutes.`;

  return (
    <button
      type="button"
      onClick={() => onSelect(type)}
      className={cn(
        'group flex h-full flex-col justify-between gap-4 rounded-xl border p-4 text-left transition hover:-translate-y-1 hover:border-[color:var(--accent)] hover:shadow-lg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent)]',
        isSelected
          ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)] shadow-lg'
          : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] shadow-soft',
      )}
    >
      <div className="flex items-start gap-4">
        <span className={cn(
          'flex h-12 w-12 items-center justify-center rounded-full bg-[color:var(--panel-alt)] text-[color:var(--accent)] transition group-hover:scale-105',
          isSelected ? 'bg-[color:var(--accent-soft)] text-[color:var(--accent-strong)]' : '',
        )}>
          <IconComponent className="h-6 w-6" aria-hidden />
        </span>
        <div className="flex-1">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-semibold capitalize text-[color:var(--text-primary)]">
              {title}
            </h3>
            {schema?.version ? (
              <Badge variant="secondary" className="text-xs">
                v{schema.version}
              </Badge>
            ) : null}
          </div>
          <p className="mt-1 line-clamp-3 text-xs text-[color:var(--text-secondary)]">{description}</p>
        </div>
      </div>
      <div className="flex items-center justify-between text-xs text-[color:var(--text-secondary)]">
        <span className="uppercase tracking-wide">Select</span>
        {isLoading ? <Spinner className="h-4 w-4 text-[color:var(--text-secondary)]" /> : <span>{'>'}</span>}
      </div>
    </button>
  );
}
