'use client';

import dynamic from 'next/dynamic';
import type { OnMount } from '@monaco-editor/react';
import { useRouter, useSearchParams } from 'next/navigation';
import { use, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  AlertTriangle,
  Bot,
  Database,
  Download,
  Eraser,
  History,
  Plus,
  Play,
  Save,
  Search,
  Share2,
  Square,
  Table2,
  Trash2,
  Wand2,
} from 'lucide-react';

import { ErrorPanel } from '@/components/ErrorPanel';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useToast } from '@/components/ui/toast';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { toDisplayError, type DisplayError } from '@/lib/errors';
import { fetchConnectors } from '@/orchestration/connectors';
import type { ConnectorResponse } from '@/orchestration/connectors/types';
import { ensureDataset, fetchDatasetCatalog } from '@/orchestration/datasets';
import type { DatasetCatalogItem } from '@/orchestration/datasets/types';
import {
  assistSql,
  cancelSqlJob,
  createSavedSqlQuery,
  deleteSavedSqlQuery,
  downloadSqlJobResults,
  executeSql,
  fetchConnectorColumns,
  fetchConnectorSchemas,
  fetchConnectorTables,
  fetchSqlHistory,
  fetchSqlJob,
  fetchSqlJobResults,
  fetchSqlWorkspacePolicy,
  listSavedSqlQueries,
  updateSavedSqlQuery,
  updateSqlWorkspacePolicy,
} from '@/orchestration/sql';
import {
  getDatasetWorkbenchCatalogItems,
  getDirectSqlConnectors,
  groupDatasetWorkbenchItems,
  inferSavedQueryWorkbenchMode,
  isDirectSqlConnector,
  resolveSavedQuerySelectedDatasets,
} from '@/orchestration/sql/workbench';
import type {
  SqlAssistMode,
  SqlDialect,
  SqlHistoryPayload,
  SqlJobRecord,
  SqlJobResultsPayload,
  SqlSavedQueryRecord,
  SqlWorkbenchMode,
  SqlWorkspacePolicyRecord,
} from '@/orchestration/sql/types';

type SqlWorkbenchPageProps = {
  params: Promise<{ organizationId: string }>;
};

type MonacoModule = typeof import('@monaco-editor/react');

type SchemaNode = {
  schema: string;
  tables: string[];
  loading: boolean;
  error: string | null;
};

type TableColumns = Record<string, Array<{ name: string; type: string }>>;

type LogEntry = {
  timestamp: string;
  level: 'info' | 'warn' | 'error';
  message: string;
};

type SortState = {
  column: string;
  direction: 'asc' | 'desc';
} | null;

type FederatedSource = {
  id: string;
  datasetId?: string;
};

const MonacoEditor = dynamic(
  async () => {
    const mod = (await import('@monaco-editor/react')) as MonacoModule;
    return mod.default;
  },
  { ssr: false },
);

const SQL_KEYWORDS = [
  'SELECT',
  'FROM',
  'WHERE',
  'JOIN',
  'LEFT JOIN',
  'RIGHT JOIN',
  'INNER JOIN',
  'GROUP BY',
  'ORDER BY',
  'HAVING',
  'TOP',
  'WITH',
  'CASE',
  'WHEN',
  'THEN',
  'ELSE',
  'END',
  'AND',
  'OR',
  'NOT',
  'IN',
  'IS NULL',
  'COUNT',
  'SUM',
  'AVG',
  'MIN',
  'MAX',
  'CAST',
  'DATEADD',
  'DATEDIFF',
];

const TERMINAL_JOB_STATES = new Set(['succeeded', 'failed', 'cancelled']);
const CANCELABLE_JOB_STATES = new Set(['queued', 'running', 'awaiting_approval']);
const DIALECT_OPTIONS: Array<{ value: SqlDialect; label: string }> = [
  { value: 'tsql', label: 'T-SQL' },
  { value: 'postgres', label: 'PostgreSQL' },
  { value: 'mysql', label: 'MySQL' },
  { value: 'snowflake', label: 'Snowflake' },
  { value: 'redshift', label: 'Redshift' },
  { value: 'bigquery', label: 'BigQuery' },
  { value: 'oracle', label: 'Oracle' },
  { value: 'sqlite', label: 'SQLite' },
];
const CONNECTOR_DIALECT_MAP: Record<string, SqlDialect> = {
  SQLSERVER: 'tsql',
  POSTGRES: 'postgres',
  MYSQL: 'mysql',
  MARIADB: 'mysql',
  SNOWFLAKE: 'snowflake',
  REDSHIFT: 'redshift',
  BIGQUERY: 'bigquery',
  ORACLE: 'oracle',
  SQLITE: 'sqlite',
};
const QUERY_SYNC_DELAY_MS = 120;
const MAX_COMPLETION_SUGGESTIONS = 150;
const DEFAULT_QUERY = ``;

function parseParameterKeys(sql: string): string[] {
  const keys = new Set<string>();
  const templatePattern = /\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}/g;
  const colonPattern = /(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)\b/g;

  for (const match of sql.matchAll(templatePattern)) {
    keys.add(match[1]);
  }
  for (const match of sql.matchAll(colonPattern)) {
    keys.add(match[1]);
  }
  return Array.from(keys);
}

function detectRiskHints(sql: string): { warnings: string[]; dangerous: string[] } {
  const lowered = sql.toLowerCase();
  const warnings: string[] = [];
  const dangerous: string[] = [];

  for (const keyword of ['drop', 'truncate', 'delete', 'update', 'insert', 'alter', 'merge']) {
    if (new RegExp(`\\b${keyword}\\b`).test(lowered)) {
      dangerous.push(keyword.toUpperCase());
    }
  }

  const joinCount = (lowered.match(/\bjoin\b/g) || []).length;
  if (joinCount >= 3) {
    warnings.push('Query joins 3 or more tables.');
  }
  if (!/\bwhere\b/.test(lowered)) {
    warnings.push('Query has no WHERE clause.');
  }
  if (!/\btop\b/.test(lowered) && !/\blimit\b/.test(lowered)) {
    warnings.push('Query has no explicit row cap.');
  }
  if (/select\s+\*/i.test(sql)) {
    warnings.push('SELECT * can increase payload and scan costs.');
  }

  return { warnings, dangerous };
}

function parseNumeric(value: string, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function csvFromRows(rows: Array<Record<string, unknown>>, columns: string[]): string {
  const header = columns.join(',');
  const lines = rows.map((row) =>
    columns
      .map((column) => {
        const value = row[column];
        const text = value == null ? '' : String(value).replaceAll('"', '""');
        return `"${text}"`;
      })
      .join(','),
  );
  return [header, ...lines].join('\n');
}

function compareUnknown(left: unknown, right: unknown): number {
  if (left == null && right == null) {
    return 0;
  }
  if (left == null) {
    return 1;
  }
  if (right == null) {
    return -1;
  }

  const leftText = String(left);
  const rightText = String(right);
  const leftNum = Number(leftText);
  const rightNum = Number(rightText);
  const leftIsNumber = Number.isFinite(leftNum) && leftText.trim() !== '';
  const rightIsNumber = Number.isFinite(rightNum) && rightText.trim() !== '';

  if (leftIsNumber && rightIsNumber) {
    return leftNum - rightNum;
  }
  return leftText.localeCompare(rightText);
}

function formatDate(value: string | null | undefined): string {
  if (!value) {
    return 'n/a';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
}

function inferDialectFromConnector(connectorType?: string | null): SqlDialect | null {
  if (!connectorType) {
    return null;
  }
  return CONNECTOR_DIALECT_MAP[connectorType.toUpperCase()] || null;
}

function nextFederatedSourceId(): string {
  return `src-${Math.random().toString(36).slice(2, 10)}`;
}

function resolveDatasetSqlAlias(dataset: DatasetCatalogItem | null | undefined, fallback = 'dataset'): string {
  return String(dataset?.sqlAlias || fallback).trim();
}

function datasetReferencePreview(dataset: DatasetCatalogItem): string {
  return resolveDatasetSqlAlias(dataset);
}

export default function SqlWorkbenchPage({ params }: SqlWorkbenchPageProps) {
  const { organizationId } = use(params);
  const router = useRouter();
  const searchParams = useSearchParams();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const {
    selectedOrganizationId,
    selectedProjectId,
    setSelectedOrganizationId,
  } = useWorkspaceScope();

  const [queryText, setQueryText] = useState(DEFAULT_QUERY);
  const [selectedConnectionId, setSelectedConnectionId] = useState('');
  const [requestedLimit, setRequestedLimit] = useState('1000');
  const [requestedTimeoutSeconds, setRequestedTimeoutSeconds] = useState('30');
  const [queryDialect, setQueryDialect] = useState<SqlDialect>('tsql');
  const [dialectTouched, setDialectTouched] = useState(false);
  const [federatedMode, setFederatedMode] = useState(true);
  const [modeTouched, setModeTouched] = useState(false);
  const [federatedSources, setFederatedSources] = useState<FederatedSource[]>([]);
  const [explainMode, setExplainMode] = useState(false);
  const [activeTab, setActiveTab] = useState('results');
  const [jobId, setJobId] = useState<string | null>(null);
  const [jobState, setJobState] = useState<SqlJobRecord | null>(null);
  const [jobResults, setJobResults] = useState<SqlJobResultsPayload | null>(null);
  const [resultCursor, setResultCursor] = useState<string | null>(null);
  const [parameterValues, setParameterValues] = useState<Record<string, string>>({});
  const [schemaMap, setSchemaMap] = useState<Record<string, SchemaNode>>({});
  const [columnsMap, setColumnsMap] = useState<TableColumns>({});
  const [selectedSchema, setSelectedSchema] = useState('');
  const [selectedTable, setSelectedTable] = useState('');
  const [datasetSearch, setDatasetSearch] = useState('');
  const [lastEnsuredDatasetId, setLastEnsuredDatasetId] = useState<string | null>(null);
  const [sqlLogs, setSqlLogs] = useState<LogEntry[]>([]);
  const [historyScope, setHistoryScope] = useState<'user' | 'workspace'>('user');
  const [selectedSavedQueryId, setSelectedSavedQueryId] = useState('');
  const [savedQueryName, setSavedQueryName] = useState('');
  const [savedQueryTags, setSavedQueryTags] = useState('');
  const [shareEnabled, setShareEnabled] = useState(false);
  const [policyEditor, setPolicyEditor] = useState<SqlWorkspacePolicyRecord | null>(null);
  const [assistantPrompt, setAssistantPrompt] = useState('');
  const [assistantMode, setAssistantMode] = useState<SqlAssistMode>('generate');
  const [assistantSuggestion, setAssistantSuggestion] = useState('');
  const [cancellingJobId, setCancellingJobId] = useState<string | null>(null);
  const [sortState, setSortState] = useState<SortState>(null);
  const [executionError, setExecutionError] = useState<DisplayError | null>(null);
  const [schemaBrowserError, setSchemaBrowserError] = useState<DisplayError | null>(null);
  const completionItemsRef = useRef<string[]>([]);
  const editorRef = useRef<{ getValue: () => string; setValue: (value: string) => void; focus: () => void } | null>(null);
  const queryTextRef = useRef(DEFAULT_QUERY);
  const querySyncTimerRef = useRef<number | null>(null);
  const lastJobStatusRef = useRef<string | null>(null);
  const loadedSavedQueryIdRef = useRef<string | null>(null);
  const loadedHistoryJobIdRef = useRef<string | null>(null);
  const workbenchMode: SqlWorkbenchMode = federatedMode ? 'dataset' : 'direct_sql';

  const syncQueryText = useCallback((nextValue: string, delayMs: number) => {
    if (querySyncTimerRef.current != null) {
      window.clearTimeout(querySyncTimerRef.current);
    }
    if (delayMs <= 0) {
      setQueryText(nextValue);
      return;
    }
    querySyncTimerRef.current = window.setTimeout(() => {
      setQueryText(nextValue);
      querySyncTimerRef.current = null;
    }, delayMs);
  }, []);

  const applyQueryText = useCallback((nextValue: string, options?: { immediateState?: boolean }) => {
    queryTextRef.current = nextValue;
    if (editorRef.current && editorRef.current.getValue() !== nextValue) {
      editorRef.current.setValue(nextValue);
    }
    syncQueryText(nextValue, options?.immediateState ? 0 : QUERY_SYNC_DELAY_MS);
  }, [syncQueryText]);

  useEffect(() => {
    return () => {
      if (querySyncTimerRef.current != null) {
        window.clearTimeout(querySyncTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (organizationId && organizationId !== selectedOrganizationId) {
      setSelectedOrganizationId(organizationId);
    }
  }, [organizationId, selectedOrganizationId, setSelectedOrganizationId]);

  const connectorsQuery = useQuery<ConnectorResponse[]>({
    queryKey: ['sql-connectors', organizationId],
    queryFn: () => fetchConnectors(organizationId),
    enabled: Boolean(organizationId),
  });

  const policyQuery = useQuery<SqlWorkspacePolicyRecord>({
    queryKey: ['sql-policy', organizationId],
    queryFn: () => fetchSqlWorkspacePolicy(organizationId),
    enabled: Boolean(organizationId),
  });

  const datasetCatalogQuery = useQuery({
    queryKey: ['sql-dataset-catalog', organizationId, selectedProjectId],
    queryFn: () => fetchDatasetCatalog(organizationId, selectedProjectId || undefined),
    enabled: Boolean(organizationId),
  });

  const historyQuery = useQuery<SqlHistoryPayload>({
    queryKey: ['sql-history', organizationId, historyScope],
    queryFn: () => fetchSqlHistory(organizationId, historyScope, 100),
    enabled: Boolean(organizationId),
    refetchInterval: jobId ? 3000 : false,
  });

  const savedQueriesQuery = useQuery({
    queryKey: ['sql-saved', organizationId],
    queryFn: () => listSavedSqlQueries(organizationId),
    enabled: Boolean(organizationId),
  });

  useEffect(() => {
    if (!policyQuery.data) {
      return;
    }
    setPolicyEditor(policyQuery.data);
    setRequestedLimit(String(policyQuery.data.maxPreviewRows));
    setRequestedTimeoutSeconds(String(policyQuery.data.maxRuntimeSeconds));
    if (!federatedMode && policyQuery.data.defaultDatasource && !selectedConnectionId) {
      setSelectedConnectionId(policyQuery.data.defaultDatasource);
    }
  }, [policyQuery.data, selectedConnectionId, federatedMode]);

  const availableFederatedDatasets = useMemo(
    () => getDatasetWorkbenchCatalogItems(datasetCatalogQuery.data?.items || []),
    [datasetCatalogQuery.data?.items],
  );

  const visibleFederatedDatasets = useMemo(() => {
    const search = datasetSearch.trim().toLowerCase();
    if (!search) {
      return availableFederatedDatasets;
    }
    return availableFederatedDatasets.filter((dataset) => {
      const haystack = [
        dataset.name,
        dataset.connectorKind,
        dataset.sourceKind,
        dataset.storageKind,
        ...dataset.tags,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      return haystack.includes(search);
    });
  }, [availableFederatedDatasets, datasetSearch]);

  const datasetGroups = useMemo(
    () => groupDatasetWorkbenchItems(visibleFederatedDatasets),
    [visibleFederatedDatasets],
  );

  useEffect(() => {
    if (!connectorsQuery.data || connectorsQuery.data.length === 0 || selectedConnectionId || federatedMode) {
      return;
    }
    setSelectedConnectionId(connectorsQuery.data.filter((connector) => isDirectSqlConnector(connector))[0]?.id ?? '');
  }, [connectorsQuery.data, selectedConnectionId, federatedMode]);

  const parameterKeys = useMemo(() => parseParameterKeys(queryText), [queryText]);

  useEffect(() => {
    setParameterValues((current) => {
      const next: Record<string, string> = {};
      parameterKeys.forEach((key) => {
        next[key] = current[key] ?? '';
      });
      return next;
    });
  }, [parameterKeys]);

  

  const availableConnectors = useMemo(
    () => getDirectSqlConnectors(connectorsQuery.data || []),
    [connectorsQuery.data],
  );

  const selectedConnector = useMemo(
    () => availableConnectors.find((connector) => connector.id === selectedConnectionId) || null,
    [availableConnectors, selectedConnectionId],
  );

  const datasetById = useMemo(
    () => Object.fromEntries(availableFederatedDatasets.map((dataset) => [dataset.id, dataset])),
    [availableFederatedDatasets],
  );
  const selectedDatasetIds = useMemo(
    () => new Set(federatedSources.map((source) => source.datasetId).filter(Boolean) as string[]),
    [federatedSources],
  );

  useEffect(() => {
    if (modeTouched) {
      return;
    }
    setFederatedMode(Boolean(policyQuery.data?.allowFederation ?? true));
  }, [modeTouched, policyQuery.data?.allowFederation]);

  useEffect(() => {
    if (dialectTouched) {
      return;
    }
    if (federatedMode) {
      setQueryDialect('tsql');
      return;
    }
    const inferredDialect = inferDialectFromConnector(selectedConnector?.connectorType || null);
    setQueryDialect(inferredDialect || 'tsql');
  }, [dialectTouched, federatedMode, selectedConnector?.connectorType]);

  const riskHints = useMemo(() => detectRiskHints(queryText), [queryText]);

  const completionCandidates = useMemo(() => {
    const datasetItems = federatedSources.flatMap((source) => {
      if (!source.datasetId) {
        return [];
      }
      const dataset = datasetById[source.datasetId];
      if (!dataset) {
        return [];
      }
      const reference = datasetReferencePreview(dataset);
      const columnItems = (dataset.columns || []).map((column) => `${reference}.${column.name}`);
      return [reference, ...columnItems];
    });
    const schemaItems = Object.values(schemaMap)
      .flatMap((schema) => schema.tables.map((table) => `${schema.schema}.${table}`));
    const columnItems = Object.entries(columnsMap).flatMap(([tableKey, columns]) =>
      columns.map((column) => `${tableKey}.${column.name}`),
    );
    return [...SQL_KEYWORDS, ...datasetItems, ...schemaItems, ...columnItems];
  }, [columnsMap, datasetById, federatedSources, schemaMap]);

  useEffect(() => {
    completionItemsRef.current = completionCandidates;
  }, [completionCandidates]);

  const appendLog = useCallback((level: LogEntry['level'], message: string) => {
    setSqlLogs((current) => [
      { timestamp: new Date().toISOString(), level, message },
      ...current,
    ].slice(0, 200));
  }, []);

  const executeMutation = useMutation({
    mutationFn: executeSql,
    onSuccess: (payload) => {
      setExecutionError(null);
      setJobId(payload.sqlJobId);
      setJobState(null);
      setJobResults(null);
      setResultCursor(null);
      setSortState(null);
      lastJobStatusRef.current = null;
      setActiveTab('results');
      appendLog('info', `SQL job ${payload.sqlJobId} queued.`);
      if (payload.warnings.length > 0) {
        appendLog('warn', payload.warnings.join(' '));
      }
      toast({
        title: 'SQL job queued',
        description: `Job ${payload.sqlJobId} has been submitted to the worker plane.`,
      });
    },
    onError: (error: Error) => {
      appendLog('error', error.message);
      setExecutionError(toDisplayError(error, 'sql.execution'));
      setActiveTab('results');
    },
  });

  const cancelMutation = useMutation({
    mutationFn: cancelSqlJob,
    onSuccess: (payload, variables) => {
      setExecutionError(null);
      appendLog('warn', `SQL job ${variables.sqlJobId} cancelled (${payload.status}).`);
      setJobState((current) =>
        current && current.id === variables.sqlJobId
          ? {
              ...current,
              status: payload.status,
              error: { message: 'Query cancelled by user.' },
            }
          : current,
      );
      setJobResults((current) =>
        current && current.sqlJobId === variables.sqlJobId
          ? {
              ...current,
              status: payload.status,
            }
          : current,
      );
      setJobId((current) => (current === variables.sqlJobId ? null : current));
      void queryClient.invalidateQueries({ queryKey: ['sql-history', organizationId] });
      toast({ title: 'SQL job cancelled', description: 'Cancellation was acknowledged.' });
    },
    onError: (error: Error) => {
      appendLog('error', error.message);
      toast({ title: 'Unable to cancel SQL job', description: error.message, variant: 'destructive' });
    },
    onSettled: () => {
      setCancellingJobId(null);
    },
  });

  const saveQueryMutation = useMutation({
    mutationFn: createSavedSqlQuery,
    onSuccess: (saved) => {
      setSelectedSavedQueryId(saved.id);
      setSavedQueryName(saved.name);
      setSavedQueryTags(saved.tags.join(','));
      setShareEnabled(saved.isShared);
      appendLog('info', `Saved query '${saved.name}' created.`);
      void queryClient.invalidateQueries({ queryKey: ['sql-saved', organizationId] });
      toast({ title: 'Saved query created', description: saved.name });
    },
    onError: (error: Error) => {
      toast({ title: 'Unable to save query', description: error.message, variant: 'destructive' });
    },
  });

  const updateQueryMutation = useMutation({
    mutationFn: ({ savedQueryId, payload }: { savedQueryId: string; payload: Parameters<typeof updateSavedSqlQuery>[1] }) =>
      updateSavedSqlQuery(savedQueryId, payload),
    onSuccess: (saved) => {
      setSavedQueryName(saved.name);
      setSavedQueryTags(saved.tags.join(','));
      setShareEnabled(saved.isShared);
      appendLog('info', `Saved query '${saved.name}' updated.`);
      void queryClient.invalidateQueries({ queryKey: ['sql-saved', organizationId] });
      toast({ title: 'Saved query updated', description: saved.name });
    },
    onError: (error: Error) => {
      toast({ title: 'Unable to update query', description: error.message, variant: 'destructive' });
    },
  });

  const deleteQueryMutation = useMutation({
    mutationFn: ({ workspaceId, savedQueryId }: { workspaceId: string; savedQueryId: string }) =>
      deleteSavedSqlQuery(workspaceId, savedQueryId),
    onSuccess: () => {
      const removedId = selectedSavedQueryId;
      setSelectedSavedQueryId('');
      appendLog('info', `Saved query ${removedId} deleted.`);
      void queryClient.invalidateQueries({ queryKey: ['sql-saved', organizationId] });
      toast({ title: 'Saved query deleted', description: 'The query was removed.' });
    },
    onError: (error: Error) => {
      toast({ title: 'Unable to delete query', description: error.message, variant: 'destructive' });
    },
  });

  const updatePolicyMutation = useMutation({
    mutationFn: updateSqlWorkspacePolicy,
    onSuccess: (policy) => {
      setPolicyEditor(policy);
      appendLog('info', 'SQL workspace policy updated.');
      void queryClient.invalidateQueries({ queryKey: ['sql-policy', organizationId] });
      toast({ title: 'SQL policy updated', description: 'Workspace bounds were updated.' });
    },
    onError: (error: Error) => {
      toast({ title: 'Unable to update SQL policy', description: error.message, variant: 'destructive' });
    },
  });

  const assistantMutation = useMutation({
    mutationFn: assistSql,
    onSuccess: (payload) => {
      setAssistantSuggestion(payload.suggestion);
      if (payload.warnings.length > 0) {
        appendLog('warn', payload.warnings.join(' '));
      }
      setActiveTab('assistant');
    },
    onError: (error: Error) => {
      toast({ title: 'SQL assistant failed', description: error.message, variant: 'destructive' });
    },
  });

  const fetchSchemas = useCallback(async () => {
    if (!organizationId || !selectedConnectionId || federatedMode) {
      setSchemaMap({});
      setColumnsMap({});
      setSchemaBrowserError(null);
      return;
    }
    try {
      const payload = await fetchConnectorSchemas(organizationId, selectedConnectionId);
      const nextMap: Record<string, SchemaNode> = {};
      payload.schemas.forEach((schema) => {
        nextMap[schema] = {
          schema,
          tables: [],
          loading: false,
          error: null,
        };
      });
      setSchemaMap(nextMap);
      setSchemaBrowserError(null);
      appendLog('info', `Loaded ${payload.schemas.length} schemas for ${selectedConnector?.name || selectedConnectionId}.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to load schemas.';
      appendLog('error', message);
      setSchemaBrowserError(toDisplayError(error, 'schema.browser'));
    }
  }, [organizationId, selectedConnectionId, federatedMode, selectedConnector?.name, appendLog]);

  useEffect(() => {
    void fetchSchemas();
  }, [fetchSchemas]);

  const loadSchemaTables = useCallback(
    async (schema: string) => {
      if (!organizationId || !selectedConnectionId || federatedMode) {
        return;
      }
      setSchemaMap((current) => ({
        ...current,
        [schema]: {
          ...(current[schema] || { schema, tables: [], error: null }),
          loading: true,
          error: null,
        },
      }));
      try {
        const payload = await fetchConnectorTables(organizationId, selectedConnectionId, schema);
        setSchemaMap((current) => ({
          ...current,
          [schema]: {
            schema,
            tables: payload.tables,
            loading: false,
            error: null,
          },
        }));
        setSchemaBrowserError(null);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unable to load tables.';
        setSchemaMap((current) => ({
          ...current,
          [schema]: {
            ...(current[schema] || { schema, tables: [] }),
            loading: false,
            error: message,
          },
        }));
        setSchemaBrowserError(toDisplayError(error, 'schema.browser'));
      }
    },
    [organizationId, selectedConnectionId, federatedMode],
  );

  const loadTableColumns = useCallback(
    async (schema: string, table: string) => {
      if (!organizationId || !selectedConnectionId || federatedMode) {
        return;
      }
      const key = `${schema}.${table}`;
      if (columnsMap[key]) {
        return;
      }
      try {
        const payload = await fetchConnectorColumns(organizationId, selectedConnectionId, schema, table);
        const columns = Object.values(payload.columns || {}).map((column) => ({
          name: column.name,
          type: column.type,
        }));
        setColumnsMap((current) => ({ ...current, [key]: columns }));
        setSchemaBrowserError(null);
      } catch (error) {
        setSchemaBrowserError(toDisplayError(error, 'schema.browser'));
      }
    },
    [columnsMap, organizationId, selectedConnectionId, federatedMode],
  );

  const ensureDatasetForPhysicalTable = useCallback(
    async (schema: string, table: string): Promise<string | null> => {
      if (!organizationId || !selectedConnectionId) {
        return null;
      }
      const key = `${schema}.${table}`;
      let tableColumns = columnsMap[key];
      if (!tableColumns || tableColumns.length === 0) {
        const response = await fetchConnectorColumns(organizationId, selectedConnectionId, schema, table);
        tableColumns = Object.values(response.columns || {}).map((column) => ({
          name: column.name,
          type: column.type,
        }));
        setColumnsMap((current) => ({ ...current, [key]: tableColumns || [] }));
      }
      const ensured = await ensureDataset({
        workspaceId: organizationId,
        projectId: selectedProjectId || undefined,
        connectionId: selectedConnectionId,
        schema,
        table,
        columns: (tableColumns || []).map((column) => ({
          name: column.name,
          dataType: column.type,
          nullable: true,
        })),
        namingTemplate: '{schema}.{table}',
        tags: ['auto-generated', 'sql-workbench'],
      });
      setLastEnsuredDatasetId(ensured.datasetId);
      appendLog(
        'info',
        ensured.created
          ? `Created governed dataset ${ensured.name} (${ensured.datasetId}).`
          : `Reused governed dataset ${ensured.name} (${ensured.datasetId}).`,
      );
      return ensured.datasetId;
    },
    [appendLog, columnsMap, organizationId, selectedConnectionId, selectedProjectId],
  );

  const federatedSourceValidation = useMemo(() => {
    const issues: string[] = [];
    const seenDatasetIds = new Set<string>();
    federatedSources.forEach((source, index) => {
      if (!source.datasetId) {
        issues.push(`Dataset ${index + 1} is missing a selection.`);
        return;
      }
      if (seenDatasetIds.has(source.datasetId)) {
        issues.push(`Dataset ${index + 1} is selected more than once.`);
      }
      seenDatasetIds.add(source.datasetId);
    });
    if (federatedSources.length < 1) {
      issues.push('Dataset mode requires at least 1 selected dataset.');
    }
    return issues;
  }, [federatedSources]);

  const runSql = useCallback(
    async (overrideQuery?: string, options?: { explain?: boolean }) => {
      if (!organizationId) {
        return;
      }
      const sql = (overrideQuery || queryTextRef.current).trim();
      if (!sql) {
        toast({ title: 'Query is empty', description: 'Write a SQL query before running.', variant: 'destructive' });
        return;
      }
      if (!federatedMode && !selectedConnectionId) {
        toast({ title: 'Connector required', description: 'Select a SQL database before running.', variant: 'destructive' });
        return;
      }
      if (federatedMode && federatedSourceValidation.length > 0) {
        toast({
          title: 'Datasets not ready',
          description: federatedSourceValidation[0],
          variant: 'destructive',
        });
        return;
      }

      const hints = detectRiskHints(sql);
      if (hints.dangerous.length > 0) {
        const shouldContinue = window.confirm(
          `Potentially dangerous statements detected (${hints.dangerous.join(', ')}). Continue?`,
        );
        if (!shouldContinue) {
          return;
        }
      }

      const paramsPayload = Object.fromEntries(
        Object.entries(parameterValues)
          .filter(([, value]) => value !== '')
          .map(([key, value]) => [key, value]),
      );


      executeMutation.mutate({
        workspaceId: organizationId,
        projectId: selectedProjectId || null,
        workbenchMode,
        connectionId: federatedMode ? null : selectedConnectionId,
        query: sql,
        queryDialect,
        params: paramsPayload,
        requestedLimit: parseNumeric(requestedLimit, policyQuery.data?.maxPreviewRows || 1000),
        requestedTimeoutSeconds: parseNumeric(
          requestedTimeoutSeconds,
          policyQuery.data?.maxRuntimeSeconds || 30,
        ),
        explain: options?.explain ?? explainMode,
        selectedDatasets: federatedSources
          .filter((source) => source.datasetId)
          .map((source) => {
            const dataset = source.datasetId ? datasetById[source.datasetId] : null;
            const sqlAlias = resolveDatasetSqlAlias(dataset);
            return {
              alias: sqlAlias,
              sqlAlias,
              datasetId: source.datasetId as string,
            };
          }),
      });
    },
    [
      organizationId,
      toast,
      federatedMode,
      selectedConnectionId,
      workbenchMode,
      queryDialect,
      parameterValues,
      federatedSources,
      federatedSourceValidation,
      executeMutation,
      selectedProjectId,
      requestedLimit,
      requestedTimeoutSeconds,
      policyQuery.data?.maxPreviewRows,
      policyQuery.data?.maxRuntimeSeconds,
      explainMode,
      datasetById,
    ],
  );

  const requestCancelJob = useCallback((targetJobId: string) => {
    if (!targetJobId || !organizationId) {
      return;
    }
    setCancellingJobId(targetJobId);
    cancelMutation.mutate({ sqlJobId: targetJobId, workspaceId: organizationId });
  }, [organizationId, cancelMutation]);

  const cancelRunningJob = useCallback(() => {
    if (!jobId) {
      return;
    }
    requestCancelJob(jobId);
  }, [jobId, requestCancelJob]);

  const cancelHistoryJob = useCallback((targetJobId: string) => {
    const confirmed = window.confirm('Cancel this SQL job?');
    if (!confirmed) {
      return;
    }
    requestCancelJob(targetJobId);
  }, [requestCancelJob]);

  const pollSqlJob = useCallback(async () => {
    if (!jobId || !organizationId) {
      return;
    }
    try {
      const [job, results] = await Promise.all([
        fetchSqlJob(organizationId, jobId),
        fetchSqlJobResults(organizationId, jobId, null, 250),
      ]);
      setJobState(job);
      if (job.status === 'failed' && job.error?.message) {
        setExecutionError(toDisplayError(new Error(String(job.error.message)), 'sql.execution'));
      } else if (job.status !== 'failed') {
        setExecutionError(null);
      }
      setJobResults(results);
      setResultCursor(results.nextCursor || null);

      if (lastJobStatusRef.current !== job.status) {
        if (job.status === 'succeeded') {
          appendLog('info', `Job ${job.id} succeeded in ${job.durationMs ?? 0}ms.`);
        } else if (job.status === 'failed') {
          appendLog('error', String(job.error?.message || 'SQL job failed.'));
        } else if (job.status === 'cancelled') {
          appendLog('warn', `Job ${job.id} was cancelled.`);
        } else {
          appendLog('info', `Job ${job.id} is ${job.status}.`);
        }
        lastJobStatusRef.current = job.status;
      }

      if (TERMINAL_JOB_STATES.has(job.status)) {
        setJobId(null);
        void queryClient.invalidateQueries({ queryKey: ['sql-history', organizationId] });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to poll SQL job status.';
      appendLog('error', message);
      setExecutionError(toDisplayError(error, 'sql.execution'));
    }
  }, [jobId, organizationId, appendLog, queryClient]);

  useEffect(() => {
    if (!jobId) {
      return;
    }
    const timer = window.setInterval(() => {
      void pollSqlJob();
    }, 1500);
    void pollSqlJob();
    return () => {
      window.clearInterval(timer);
    };
  }, [jobId, pollSqlJob]);

  const onEditorMount = useCallback<OnMount>((editor, monaco) => {
    editorRef.current = {
      getValue: () => editor.getValue(),
      setValue: (value: string) => editor.setValue(value),
      focus: () => editor.focus(),
    };
    if (editor.getValue() !== queryTextRef.current) {
      editor.setValue(queryTextRef.current);
    }
    monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems(
        model: {
          getWordUntilPosition: (position: { lineNumber: number; column: number }) => {
            startColumn: number;
            endColumn: number;
            word?: string;
          };
        },
        position: { lineNumber: number; column: number },
      ) {
        const word = model.getWordUntilPosition(position);
        const lookup = (word.word || '').toLowerCase();
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };
        const suggestions = completionItemsRef.current
          .filter((item) => (lookup ? item.toLowerCase().includes(lookup) : true))
          .slice(0, MAX_COMPLETION_SUGGESTIONS);
        return {
          suggestions: suggestions.map((item) => ({
            label: item,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: item,
            range,
          })),
        };
      },
    });
    editor.focus();
  }, []);

  const currentRows = useMemo(() => jobResults?.rows || [], [jobResults]);
  const currentColumns = useMemo(
    () => (jobResults?.columns || []).map((column) => column.name),
    [jobResults?.columns],
  );

  const sortedRows = useMemo(() => {
    if (!sortState) {
      return currentRows;
    }
    const next = [...currentRows];
    next.sort((left, right) => {
      const comparison = compareUnknown(left[sortState.column], right[sortState.column]);
      return sortState.direction === 'asc' ? comparison : comparison * -1;
    });
    return next;
  }, [currentRows, sortState]);

  const toggleSort = useCallback((column: string) => {
    setSortState((current) => {
      if (!current || current.column !== column) {
        return { column, direction: 'asc' };
      }
      if (current.direction === 'asc') {
        return { column, direction: 'desc' };
      }
      return null;
    });
  }, []);

  const copyCell = useCallback((value: unknown) => {
    void navigator.clipboard.writeText(value == null ? '' : String(value));
    toast({ title: 'Copied', description: 'Cell copied to clipboard.' });
  }, [toast]);

  const copyRow = useCallback((row: Record<string, unknown>) => {
    void navigator.clipboard.writeText(JSON.stringify(row, null, 2));
    toast({ title: 'Copied', description: 'Row copied as JSON.' });
  }, [toast]);

  const copyAsCsv = useCallback(() => {
    if (!currentRows.length || !currentColumns.length) {
      return;
    }
    const csv = csvFromRows(currentRows, currentColumns);
    void navigator.clipboard.writeText(csv);
    toast({ title: 'Copied', description: 'Result copied as CSV.' });
  }, [currentRows, currentColumns, toast]);

  const loadNextResultsPage = useCallback(async () => {
    if (!jobState || !resultCursor || !organizationId) {
      return;
    }
    try {
      const payload = await fetchSqlJobResults(organizationId, jobState.id, resultCursor, 250);
      setJobResults((current) => {
        if (!current) {
          return payload;
        }
        return {
          ...payload,
          rows: [...current.rows, ...payload.rows],
          nextCursor: payload.nextCursor,
        };
      });
      setResultCursor(payload.nextCursor || null);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to load additional rows.';
      toast({ title: 'Pagination failed', description: message, variant: 'destructive' });
    }
  }, [jobState, resultCursor, organizationId, toast]);

  const downloadResults = useCallback(async (format: 'csv' | 'parquet') => {
    if (!jobState || !organizationId) {
      return;
    }
    try {
      const blob = await downloadSqlJobResults(organizationId, jobState.id, format);
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `sql_job_${jobState.id}.${format}`;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      const message = error instanceof Error ? error.message : `Unable to download ${format}.`;
      toast({ title: 'Download failed', description: message, variant: 'destructive' });
    }
  }, [jobState, organizationId, toast]);

  const selectedSavedQuery = useMemo(() => {
    const items = savedQueriesQuery.data?.items || [];
    return items.find((item) => item.id === selectedSavedQueryId) || null;
  }, [savedQueriesQuery.data?.items, selectedSavedQueryId]);

  const loadSavedQuery = useCallback((saved: SqlSavedQueryRecord) => {
    const savedMode = inferSavedQueryWorkbenchMode(saved);
    const savedDatasets = resolveSavedQuerySelectedDatasets(saved);
    setSelectedSavedQueryId(saved.id);
    setSavedQueryName(saved.name);
    setSavedQueryTags(saved.tags.join(','));
    setShareEnabled(saved.isShared);
    applyQueryText(saved.query, { immediateState: true });
    if (savedMode === 'dataset') {
      setFederatedSources(
        savedDatasets.map((binding) => ({
          id: nextFederatedSourceId(),
          datasetId: binding.datasetId,
        })),
      );
      setFederatedMode(true);
      setModeTouched(true);
      setSelectedConnectionId('');
    } else {
      setFederatedSources([]);
      setFederatedMode(false);
      setModeTouched(true);
      setSelectedConnectionId(saved.connectionId || '');
    }
    setParameterValues(
      Object.fromEntries(Object.entries(saved.defaultParams || {}).map(([key, value]) => [key, String(value)])),
    );
    appendLog('info', `Loaded saved query '${saved.name}'.`);
  }, [appendLog, applyQueryText]);

  const sharedSavedQueryId = searchParams.get('savedQueryId');
  const sharedHistoryJobId = searchParams.get('jobId');

  useEffect(() => {
    if (!sharedSavedQueryId) {
      loadedSavedQueryIdRef.current = null;
      return;
    }
    if (loadedSavedQueryIdRef.current === sharedSavedQueryId) {
      return;
    }
    const item = (savedQueriesQuery.data?.items || []).find((saved) => saved.id === sharedSavedQueryId);
    if (!item) {
      return;
    }
    loadSavedQuery(item);
    setActiveTab('saved');
    loadedSavedQueryIdRef.current = sharedSavedQueryId;
  }, [sharedSavedQueryId, savedQueriesQuery.data?.items, loadSavedQuery]);

  const loadHistoryJobById = useCallback(async (historyJobId: string) => {
    if (!organizationId) {
      return;
    }
    try {
      const [job, results] = await Promise.all([
        fetchSqlJob(organizationId, historyJobId),
        fetchSqlJobResults(organizationId, historyJobId, null, 250),
      ]);
      applyQueryText(job.query, { immediateState: true });
      setFederatedMode(job.workbenchMode === 'dataset');
      setModeTouched(true);
      if (job.workbenchMode === 'dataset') {
        setFederatedSources(
          job.selectedDatasets.map((dataset) => ({
            id: nextFederatedSourceId(),
            datasetId: dataset.datasetId,
          })),
        );
        setSelectedConnectionId('');
      } else {
        setFederatedSources([]);
        setSelectedConnectionId(job.connectionId || '');
      }
      setJobId(job.id);
      setJobState(job);
      setJobResults(results);
      setResultCursor(results.nextCursor || null);
      setSortState(null);
      setActiveTab('results');
      appendLog('info', `Loaded history job ${historyJobId}.`);
      setExecutionError(null);
    } catch (error) {
      setExecutionError(toDisplayError(error, 'sql.execution'));
      setActiveTab('results');
    }
  }, [organizationId, appendLog, applyQueryText]);

  useEffect(() => {
    if (!sharedHistoryJobId) {
      loadedHistoryJobIdRef.current = null;
      return;
    }
    if (loadedHistoryJobIdRef.current === sharedHistoryJobId) {
      return;
    }
    loadedHistoryJobIdRef.current = sharedHistoryJobId;
    void loadHistoryJobById(sharedHistoryJobId);
  }, [loadHistoryJobById, sharedHistoryJobId]);

  const viewHistoryJob = useCallback(async (historyItem: SqlJobRecord) => {
    await loadHistoryJobById(historyItem.id);
  }, [loadHistoryJobById]);

  const saveCurrentQuery = useCallback(() => {
    if (!organizationId) {
      return;
    }
    if (!savedQueryName.trim()) {
      toast({ title: 'Name required', description: 'Provide a name for the saved query.', variant: 'destructive' });
      return;
    }

    const basePayload = {
      workspaceId: organizationId,
      projectId: selectedProjectId || null,
      workbenchMode,
      connectionId: federatedMode ? null : (selectedConnectionId || null),
      selectedDatasets: federatedMode
        ? federatedSources
            .filter((source) => source.datasetId)
            .map((source) => {
              const dataset = source.datasetId ? datasetById[source.datasetId] : null;
              const sqlAlias = resolveDatasetSqlAlias(dataset);
              return {
                alias: sqlAlias,
                sqlAlias,
                datasetId: source.datasetId as string,
              };
            })
        : [],
      name: savedQueryName.trim(),
      description: null,
      query: queryTextRef.current,
      tags: savedQueryTags.split(',').map((tag) => tag.trim()).filter(Boolean),
      defaultParams: parameterValues,
      isShared: shareEnabled,
      lastSqlJobId: jobState?.id || null,
    };

    if (!selectedSavedQueryId) {
      saveQueryMutation.mutate(basePayload);
      return;
    }

    updateQueryMutation.mutate({
      savedQueryId: selectedSavedQueryId,
      payload: basePayload,
    });
  }, [
    organizationId,
    savedQueryName,
    toast,
    selectedProjectId,
    selectedConnectionId,
    federatedMode,
    workbenchMode,
    federatedSources,
    savedQueryTags,
    parameterValues,
    shareEnabled,
    jobState?.id,
    selectedSavedQueryId,
    datasetById,
    saveQueryMutation,
    updateQueryMutation,
  ]);

  const deleteSelectedQuery = useCallback(() => {
    if (!organizationId || !selectedSavedQueryId) {
      return;
    }
    const confirmed = window.confirm('Delete this saved query?');
    if (!confirmed) {
      return;
    }
    deleteQueryMutation.mutate({ workspaceId: organizationId, savedQueryId: selectedSavedQueryId });
  }, [organizationId, selectedSavedQueryId, deleteQueryMutation]);

  const runAssistant = useCallback(() => {
    if (!organizationId || !assistantPrompt.trim()) {
      return;
    }
    assistantMutation.mutate({
      workspaceId: organizationId,
      connectionId: federatedMode ? null : (selectedConnectionId || null),
      mode: assistantMode,
      prompt: assistantPrompt,
      query: queryTextRef.current,
    });
  }, [organizationId, assistantPrompt, assistantMutation, federatedMode, selectedConnectionId, assistantMode]);

  const applyAssistantSuggestion = useCallback(() => {
    if (!assistantSuggestion.trim()) {
      return;
    }
    applyQueryText(assistantSuggestion, { immediateState: true });
    setActiveTab('results');
    appendLog('info', 'Applied SQL assistant suggestion to the editor.');
  }, [assistantSuggestion, appendLog, applyQueryText]);

  const savePolicy = useCallback(() => {
    if (!policyEditor) {
      return;
    }
    updatePolicyMutation.mutate({
      workspaceId: policyEditor.workspaceId,
      maxPreviewRows: policyEditor.maxPreviewRows,
      maxExportRows: policyEditor.maxExportRows,
      maxRuntimeSeconds: policyEditor.maxRuntimeSeconds,
      maxConcurrency: policyEditor.maxConcurrency,
      allowDml: policyEditor.allowDml,
      allowFederation: policyEditor.allowFederation,
      allowedSchemas: policyEditor.allowedSchemas,
      allowedTables: policyEditor.allowedTables,
      defaultDatasource: policyEditor.defaultDatasource || null,
      budgetLimitBytes: policyEditor.budgetLimitBytes || undefined,
    });
  }, [policyEditor, updatePolicyMutation]);

  const usedBytes = useMemo(
    () => (historyQuery.data?.items || []).reduce((acc, item) => acc + Number(item.bytesScanned || 0), 0),
    [historyQuery.data?.items],
  );

  const budgetWarning = useMemo(() => {
    if (!policyQuery.data?.budgetLimitBytes) {
      return null;
    }
    if (usedBytes <= policyQuery.data.budgetLimitBytes) {
      return null;
    }
    return `Workspace SQL bytes scanned (${usedBytes}) exceed budget (${policyQuery.data.budgetLimitBytes}).`;
  }, [policyQuery.data?.budgetLimitBytes, usedBytes]);

  const addFederatedSource = useCallback(() => {
    const usedIds = new Set(federatedSources.map((source) => source.datasetId));
    const fallbackDataset = availableFederatedDatasets.find((dataset) => !usedIds.has(dataset.id))
      || availableFederatedDatasets[0];
    if (!fallbackDataset?.id) {
      toast({
        title: 'No datasets',
        description: 'Create a structured dataset before querying in dataset mode.',
        variant: 'destructive',
      });
      return;
    }
    setFederatedSources((current) => [
      ...current,
      {
        id: nextFederatedSourceId(),
        datasetId: fallbackDataset.id,
      },
    ]);
  }, [federatedSources, availableFederatedDatasets, toast]);

  const removeFederatedSource = useCallback((sourceId: string) => {
    setFederatedSources((current) => current.filter((source) => source.id !== sourceId));
  }, []);

  const updateFederatedSource = useCallback(
    (sourceId: string, patch: Partial<FederatedSource>) => {
      setFederatedSources((current) =>
        current.map((source) =>
          source.id === sourceId
            ? {
                ...source,
                ...patch,
              }
            : source,
        ),
      );
    },
    [],
  );

  const insertFederatedTemplate = useCallback(() => {
    const validSources = federatedSources
      .map((source) => ({
        datasetId: source.datasetId,
        dataset: source.datasetId ? datasetById[source.datasetId] : null,
      }))
      .filter((source) => source.datasetId && source.dataset);
    if (validSources.length < 2) {
      toast({
        title: 'Add more datasets',
        description: 'Pick at least 2 datasets before generating a join template.',
        variant: 'destructive',
      });
      return;
    }
    const leftReference = datasetReferencePreview(validSources[0].dataset as DatasetCatalogItem);
    const rightReference = datasetReferencePreview(validSources[1].dataset as DatasetCatalogItem);
    const template = `-- Query datasets through their dataset SQL names:
--   <dataset_sql_alias>
SELECT TOP 100
  l.id,
  r.id
FROM ${leftReference} AS l
JOIN ${rightReference} AS r
  ON l.id = r.id
ORDER BY l.id DESC;`;
    applyQueryText(template, { immediateState: true });
    appendLog('info', 'Inserted dataset SQL template.');
  }, [federatedSources, toast, applyQueryText, appendLog, datasetById]);

  const runProfilingQuery = useCallback((column: string, mode: 'top' | 'nulls' | 'distribution') => {
    const trimmedQuery = queryTextRef.current.trim().replace(/;$/, '');
    if (!trimmedQuery) {
      return;
    }
    let profilingSql = trimmedQuery;
    if (mode === 'top') {
      profilingSql = `SELECT TOP 20 [${column}], COUNT(1) AS value_count
FROM (${trimmedQuery}) AS src
GROUP BY [${column}]
ORDER BY value_count DESC;`;
    }
    if (mode === 'nulls') {
      profilingSql = `SELECT
  SUM(CASE WHEN [${column}] IS NULL THEN 1 ELSE 0 END) AS null_count,
  COUNT(1) AS total_count,
  CAST(SUM(CASE WHEN [${column}] IS NULL THEN 1 ELSE 0 END) AS FLOAT) / NULLIF(COUNT(1), 0) AS null_rate
FROM (${trimmedQuery}) AS src;`;
    }
    if (mode === 'distribution') {
      profilingSql = `SELECT TOP 200 [${column}]
FROM (${trimmedQuery}) AS src
WHERE [${column}] IS NOT NULL
ORDER BY [${column}] ASC;`;
    }
    applyQueryText(profilingSql, { immediateState: true });
    void runSql(profilingSql);
  }, [applyQueryText, runSql]);

  const shareLink = useMemo(() => {
    if (!selectedSavedQuery || !selectedSavedQuery.isShared) {
      return null;
    }
    return `/sql/${organizationId}?savedQueryId=${selectedSavedQuery.id}`;
  }, [selectedSavedQuery, organizationId]);

  const copyShareLink = useCallback(() => {
    if (!shareLink) {
      return;
    }
    const absoluteUrl = `${window.location.origin}${shareLink}`;
    void navigator.clipboard.writeText(absoluteUrl);
    toast({ title: 'Share link copied', description: absoluteUrl });
  }, [shareLink, toast]);

  const promoteToDataset = useCallback(() => {
    if (!organizationId) {
      return;
    }
    if (!selectedSavedQueryId) {
      toast({
        title: 'Save query first',
        description: 'Save the SQL query before promoting it into BI Studio.',
        variant: 'destructive',
      });
      return;
    }
    router.push(`/bi/${organizationId}?source=sql&savedQueryId=${selectedSavedQueryId}`);
  }, [organizationId, selectedSavedQueryId, router, toast]);

  return (
    <div className="grid gap-4 lg:grid-cols-[280px_1fr]">
      <aside className="surface-panel rounded-2xl p-4 shadow-soft">
        <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-[color:var(--text-primary)]">
          <Database className="h-4 w-4" />
          Query Context
        </div>
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-2 rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-1">
            <Button
              type="button"
              size="sm"
              variant={federatedMode ? 'default' : 'ghost'}
              onClick={() => {
                setModeTouched(true);
                setFederatedMode(true);
              }}
              disabled={!policyQuery.data?.allowFederation}
            >
              Datasets
            </Button>
            <Button
              type="button"
              size="sm"
              variant={!federatedMode ? 'default' : 'ghost'}
              onClick={() => {
                setModeTouched(true);
                setFederatedMode(false);
              }}
            >
              Direct SQL
            </Button>
          </div>

          {federatedMode ? (
            <div className="space-y-3">
              <div className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
                <p className="text-sm font-semibold text-[color:var(--text-primary)]">Datasets</p>
                <p className="mt-1 text-xs text-[color:var(--text-secondary)]">
                  Query governed datasets by their SQL name. Langbridge resolves them to the underlying sources through the federated layer.
                </p>
              </div>
              <Input
                value={datasetSearch}
                onChange={(event) => setDatasetSearch(event.target.value)}
                placeholder="Search datasets"
              />
              <div className="space-y-2 rounded-xl border border-[color:var(--panel-border)] p-3">
                <div className="flex items-center justify-between gap-2">
                  <p className="text-sm font-semibold text-[color:var(--text-primary)]">Selected datasets</p>
                  <div className="flex gap-2">
                    <Button type="button" variant="outline" size="sm" onClick={addFederatedSource}>
                      <Plus className="mr-1 h-4 w-4" />
                      Add
                    </Button>
                    <Button type="button" variant="outline" size="sm" onClick={insertFederatedTemplate}>
                      <Wand2 className="mr-1 h-4 w-4" />
                      Template
                    </Button>
                  </div>
                </div>
                {federatedSources.length === 0 ? (
                  <p className="text-xs text-[color:var(--text-muted)]">Choose one or more datasets to create a query context.</p>
                ) : null}
                <div className="space-y-2">
                  {federatedSources.map((source) => (
                    <div key={source.id} className="space-y-2 rounded-lg border border-[color:var(--panel-border)] p-2">
                      <div className="grid gap-2 sm:grid-cols-[1fr_auto]">
                        <Select
                          value={source.datasetId || ''}
                          onChange={(event) =>
                            updateFederatedSource(source.id, { datasetId: event.target.value })
                          }
                        >
                          <option value="">Select dataset</option>
                          {availableFederatedDatasets.map((dataset) => (
                            <option key={`selected-${source.id}-${dataset.id}`} value={dataset.id}>
                              {dataset.name}
                            </option>
                          ))}
                        </Select>
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon"
                          onClick={() => removeFederatedSource(source.id)}
                          disabled={federatedSources.length <= 1}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                      <p className="text-[11px] text-[color:var(--text-muted)]">
                        {(source.datasetId && datasetById[source.datasetId]?.name) || 'No dataset selected'}
                        {' '}
                        <span className="font-mono">
                          {source.datasetId && datasetById[source.datasetId]
                            ? datasetReferencePreview(datasetById[source.datasetId])
                            : 'dataset_name'}
                        </span>
                      </p>
                    </div>
                  ))}
                </div>
                {federatedSourceValidation.length > 0 ? (
                  <div className="space-y-1 rounded-lg border border-amber-400/40 bg-amber-400/10 p-2 text-xs text-amber-200">
                    {federatedSourceValidation.slice(0, 3).map((issue) => (
                      <p key={issue}>{issue}</p>
                    ))}
                  </div>
                ) : (
                  <p className="text-xs text-[color:var(--text-muted)]">
                    SQL uses dataset names like <span className="font-mono">dataset_1</span>.
                  </p>
                )}
              </div>
              <div className="max-h-[46vh] space-y-3 overflow-auto rounded-xl border border-[color:var(--panel-border)] p-3">
                {datasetGroups.length === 0 ? (
                  <p className="text-xs text-[color:var(--text-muted)]">
                    {datasetSearch.trim()
                      ? 'No datasets matched your search.'
                      : 'No SQL-ready datasets found for this workspace scope.'}
                  </p>
                ) : null}
                {datasetGroups.map((group) => (
                  <div key={group.key} className="space-y-2">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[color:var(--text-muted)]">
                      {group.label}
                    </p>
                    {group.items.map((dataset) => {
                      const selected = selectedDatasetIds.has(dataset.id);
                      return (
                        <button
                          type="button"
                          key={dataset.id}
                          className={`w-full rounded-xl border p-3 text-left ${
                            selected
                              ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)]'
                              : 'border-[color:var(--panel-border)] hover:bg-[color:var(--panel-alt)]'
                          }`}
                          onClick={() => {
                            const existing = federatedSources.find((source) => source.datasetId === dataset.id);
                            if (existing) {
                              return;
                            }
                            setFederatedSources((current) => [
                              ...current,
                              {
                                id: nextFederatedSourceId(),
                                datasetId: dataset.id,
                              },
                            ]);
                          }}
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <p className="font-semibold text-[color:var(--text-primary)]">{dataset.name}</p>
                              <p className="mt-1 text-xs text-[color:var(--text-secondary)]">
                                {dataset.connectorKind || dataset.sourceKind} · {dataset.storageKind}
                              </p>
                            </div>
                            <span className="rounded-full bg-[color:var(--chip-bg)] px-2 py-1 text-[10px] text-[color:var(--text-secondary)]">
                              {selected ? 'Selected' : 'Add'}
                            </span>
                          </div>
                          <p className="mt-2 text-[11px] text-[color:var(--text-muted)]">
                            SQL name: <span className="font-mono">{dataset.sqlAlias}</span>
                          </p>
                        </button>
                      );
                    })}
                  </div>
                ))}
              </div>
              {!policyQuery.data?.allowFederation ? (
                <p className="text-xs text-[color:var(--text-muted)]">
                  Dataset mode is disabled by workspace policy.
                </p>
              ) : null}
            </div>
          ) : (
            <div className="space-y-3">
              <div className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
                <p className="text-sm font-semibold text-[color:var(--text-primary)]">Direct SQL</p>
                <p className="mt-1 text-xs text-[color:var(--text-secondary)]">
                  Query SQL databases directly. API connectors, file uploads, and non-database sources are excluded here.
                </p>
              </div>
              <Select
                value={selectedConnectionId}
                onChange={(event) => setSelectedConnectionId(event.target.value)}
              >
                <option value="">Select SQL database</option>
                {availableConnectors.map((connector) => (
                  <option key={connector.id} value={connector.id}>
                    {connector.name}
                  </option>
                ))}
              </Select>
              {lastEnsuredDatasetId ? (
                <p className="text-[11px] text-[color:var(--text-muted)]">
                  Last ensured dataset: <span className="font-mono">{lastEnsuredDatasetId}</span>
                </p>
              ) : null}
              <div className="max-h-[55vh] space-y-2 overflow-auto pr-1 text-sm">
                {schemaBrowserError ? <ErrorPanel {...schemaBrowserError} /> : null}
                {Object.values(schemaMap).length === 0 ? (
                  <p className="text-xs text-[color:var(--text-muted)]">No schemas loaded.</p>
                ) : null}
                {Object.values(schemaMap).map((schemaNode) => (
                  <div key={schemaNode.schema} className="rounded-xl border border-[color:var(--panel-border)] p-2">
                    <button
                      type="button"
                      className="w-full text-left font-medium text-[color:var(--text-primary)]"
                      onClick={() => {
                        setSelectedSchema(schemaNode.schema);
                        void loadSchemaTables(schemaNode.schema);
                      }}
                    >
                      {schemaNode.schema}
                    </button>
                    {selectedSchema === schemaNode.schema ? (
                      <div className="mt-2 space-y-1 text-xs text-[color:var(--text-secondary)]">
                        {schemaNode.loading ? <p>Loading tables...</p> : null}
                        {schemaNode.tables.map((table) => {
                          const key = `${schemaNode.schema}.${table}`;
                          return (
                            <div key={key} className="rounded-lg bg-[color:var(--panel-alt)] p-2">
                              <div className="flex items-center justify-between gap-2">
                                <button
                                  type="button"
                                  className="font-medium"
                                  onClick={() => {
                                    setSelectedTable(key);
                                    void loadTableColumns(schemaNode.schema, table);
                                  }}
                                >
                                  {table}
                                </button>
                                <Button
                                  type="button"
                                  size="sm"
                                  variant="ghost"
                                  onClick={() => {
                                    void ensureDatasetForPhysicalTable(schemaNode.schema, table).catch((error) => {
                                      setSchemaBrowserError(toDisplayError(error, 'dataset.form'));
                                    });
                                  }}
                                >
                                  Publish dataset
                                </Button>
                              </div>
                              {selectedTable === key ? (
                                <div className="mt-1 space-y-1">
                                  {(columnsMap[key] || []).map((column) => (
                                    <button
                                      type="button"
                                      key={`${key}.${column.name}`}
                                      className="block w-full rounded px-1 py-0.5 text-left hover:bg-[color:var(--chip-bg)]"
                                      onClick={() =>
                                        applyQueryText(
                                          `${queryTextRef.current}\n${schemaNode.schema}.${table}.${column.name}`,
                                          { immediateState: true },
                                        )
                                      }
                                    >
                                      {column.name} <span className="text-[color:var(--text-muted)]">{column.type}</span>
                                    </button>
                                  ))}
                                </div>
                              ) : null}
                            </div>
                          );
                        })}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </aside>

      <section className="space-y-4">
        <div className="surface-panel rounded-2xl p-4 shadow-soft">
          <div className="mb-3 flex flex-wrap items-center gap-2">
            <Button size="sm" onClick={() => void runSql()} isLoading={executeMutation.isPending}>
              <Play className="h-4 w-4" /> Run
            </Button>
            <Button size="sm" variant="outline" onClick={() => void runSql(undefined, { explain: true })}>
              <Search className="h-4 w-4" /> Explain
            </Button>
            <Button size="sm" variant="outline" onClick={cancelRunningJob} disabled={!jobId}>
              <Square className="h-4 w-4" /> Cancel
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => downloadResults('csv')}
              disabled={!jobState || jobState.status !== 'succeeded'}
            >
              <Download className="h-4 w-4" /> CSV
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => downloadResults('parquet')}
              disabled={!jobState || jobState.status !== 'succeeded'}
            >
              <Download className="h-4 w-4" /> Parquet
            </Button>
            <Button size="sm" variant="outline" onClick={saveCurrentQuery}>
              <Save className="h-4 w-4" /> Save Query
            </Button>
            <label className="ml-auto flex items-center gap-2 text-xs text-[color:var(--text-secondary)]">
              <input type="checkbox" checked={explainMode} onChange={(event) => setExplainMode(event.target.checked)} />
              Explain by default
            </label>
          </div>
          <div className="mb-3 grid gap-3 md:grid-cols-5">
            <Input value={requestedLimit} onChange={(event) => setRequestedLimit(event.target.value)} placeholder="Preview row limit" />
            <Input value={requestedTimeoutSeconds} onChange={(event) => setRequestedTimeoutSeconds(event.target.value)} placeholder="Timeout seconds" />
            <Select
              value={queryDialect}
              onChange={(event) => {
                setQueryDialect((event.target.value as SqlDialect) || 'tsql');
                setDialectTouched(true);
              }}
            >
              {DIALECT_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </Select>
            <Input value={savedQueryName} onChange={(event) => setSavedQueryName(event.target.value)} placeholder="Saved query name" />
            <Input value={savedQueryTags} onChange={(event) => setSavedQueryTags(event.target.value)} placeholder="Tags (comma separated)" />
          </div>
          <MonacoEditor
            height="360px"
            language="sql"
            defaultValue={DEFAULT_QUERY}
            onChange={(value) => {
              const nextValue = value || '';
              queryTextRef.current = nextValue;
              syncQueryText(nextValue, QUERY_SYNC_DELAY_MS);
            }}
            onMount={onEditorMount}
            options={{
              minimap: { enabled: false },
              fontSize: 13,
              wordWrap: 'on',
              automaticLayout: true,
              suggestOnTriggerCharacters: true,
            }}
          />
          <div className="mt-3 flex flex-wrap gap-2 text-xs text-[color:var(--text-secondary)]">
            <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Mode: {federatedMode ? 'Datasets' : 'Direct SQL'}</span>
            {federatedMode ? (
              <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Datasets: {federatedSources.filter((source) => source.datasetId).length}</span>
            ) : selectedConnector ? (
              <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Connector: {selectedConnector.name}</span>
            ) : null}
            <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Dialect: {queryDialect}</span>
            {jobState ? <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Status: {jobState.status}</span> : null}
            {jobState?.durationMs != null ? <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Duration: {jobState.durationMs}ms</span> : null}
            {jobState?.bytesScanned != null ? <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Bytes scanned: {jobState.bytesScanned}</span> : null}
            {jobState?.correlationId ? <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Correlation: {jobState.correlationId}</span> : null}
          </div>
        </div>

        {parameterKeys.length > 0 ? (
          <div className="surface-panel rounded-2xl p-4 shadow-soft">
            <div className="mb-2 text-sm font-semibold text-[color:var(--text-primary)]">Parameters</div>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {parameterKeys.map((key) => (
                <Input
                  key={key}
                  value={parameterValues[key] || ''}
                  onChange={(event) =>
                    setParameterValues((current) => ({
                      ...current,
                      [key]: event.target.value,
                    }))
                  }
                  placeholder={key}
                />
              ))}
            </div>
          </div>
        ) : null}

        {riskHints.warnings.length > 0 || riskHints.dangerous.length > 0 || budgetWarning ? (
          <div className="surface-panel rounded-2xl border border-amber-400/50 bg-amber-100/40 p-4 text-sm text-amber-900 dark:bg-amber-900/20 dark:text-amber-100">
            <div className="mb-2 flex items-center gap-2 font-semibold">
              <AlertTriangle className="h-4 w-4" /> Guardrails
            </div>
            {riskHints.warnings.map((warning) => (
              <p key={warning}>- {warning}</p>
            ))}
            {riskHints.dangerous.map((danger) => (
              <p key={danger}>- Dangerous token detected: {danger}</p>
            ))}
            {budgetWarning ? <p>- {budgetWarning}</p> : null}
          </div>
        ) : null}

        <div className="surface-panel rounded-2xl p-4 shadow-soft">
          <Tabs value={activeTab} onValueChange={setActiveTab} defaultValue="results">
            <TabsList className="w-full justify-start overflow-x-auto">
              <TabsTrigger value="results">
                <Table2 className="h-4 w-4" /> Results
              </TabsTrigger>
              <TabsTrigger value="logs">
                <Eraser className="h-4 w-4" /> Logs
              </TabsTrigger>
              <TabsTrigger value="history">
                <History className="h-4 w-4" /> History
              </TabsTrigger>
              <TabsTrigger value="saved">
                <Save className="h-4 w-4" /> Saved
              </TabsTrigger>
              <TabsTrigger value="policy">
                <Database className="h-4 w-4" /> Policy
              </TabsTrigger>
              <TabsTrigger value="assistant">
                <Bot className="h-4 w-4" /> AI Helper
              </TabsTrigger>
            </TabsList>

            <TabsContent value="results" className="space-y-3">
              {executionError ? <ErrorPanel {...executionError} /> : null}
              {!executionError && jobState?.status !== 'cancelled' && jobState?.error?.message ? (
                <ErrorPanel
                  {...toDisplayError(new Error(String(jobState.error.message)), 'sql.execution')}
                  technicalDetails={
                    [
                      `Job: ${jobState.id}`,
                      jobState.correlationId ? `Correlation: ${jobState.correlationId}` : null,
                      String(jobState.error.message),
                    ]
                      .filter(Boolean)
                      .join('\n')
                  }
                />
              ) : null}
              <div className="flex flex-wrap items-center gap-2 text-xs text-[color:var(--text-secondary)]">
                <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Rows: {jobResults?.rowCountPreview ?? 0}</span>
                <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Total estimate: {jobResults?.totalRowsEstimate ?? 'n/a'}</span>
                <span className="rounded-full bg-[color:var(--chip-bg)] px-3 py-1">Columns: {currentColumns.length}</span>
                <Button size="sm" variant="outline" onClick={copyAsCsv} disabled={currentRows.length === 0}>
                  Copy CSV
                </Button>
                <Button size="sm" variant="outline" onClick={loadNextResultsPage} disabled={!resultCursor}>
                  Load next page
                </Button>
              </div>
              {currentRows.length === 0 ? (
                <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] p-6 text-sm text-[color:var(--text-muted)]">
                  No result rows yet. Run a SQL query to preview rows.
                </div>
              ) : (
                <div className="overflow-x-auto rounded-xl border border-[color:var(--panel-border)]">
                  <table className="min-w-full divide-y divide-[color:var(--panel-border)] text-sm">
                    <thead className="bg-[color:var(--panel-alt)] text-[color:var(--text-secondary)]">
                      <tr>
                        {currentColumns.map((columnName) => (
                          <th key={columnName} className="min-w-[170px] px-3 py-2 text-left align-top">
                            <button
                              type="button"
                              className="font-semibold text-[color:var(--text-primary)]"
                              onClick={() => toggleSort(columnName)}
                            >
                              {columnName}
                              {sortState?.column === columnName ? (sortState.direction === 'asc' ? ' ↑' : ' ↓') : ''}
                            </button>
                            <div className="mt-1 flex flex-wrap gap-1 text-[10px]">
                              <button
                                type="button"
                                className="rounded border border-[color:var(--panel-border)] px-1.5 py-0.5 hover:bg-[color:var(--chip-bg)]"
                                onClick={() => runProfilingQuery(columnName, 'top')}
                              >
                                Top
                              </button>
                              <button
                                type="button"
                                className="rounded border border-[color:var(--panel-border)] px-1.5 py-0.5 hover:bg-[color:var(--chip-bg)]"
                                onClick={() => runProfilingQuery(columnName, 'nulls')}
                              >
                                Null rate
                              </button>
                              <button
                                type="button"
                                className="rounded border border-[color:var(--panel-border)] px-1.5 py-0.5 hover:bg-[color:var(--chip-bg)]"
                                onClick={() => runProfilingQuery(columnName, 'distribution')}
                              >
                                Distribution
                              </button>
                            </div>
                          </th>
                        ))}
                        <th className="px-3 py-2 text-left">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[color:var(--panel-border)]">
                      {sortedRows.map((row, rowIndex) => (
                        <tr key={`row-${rowIndex}`} className="hover:bg-[color:var(--panel-alt)]/60">
                          {currentColumns.map((columnName) => {
                            const rawValue = row[columnName];
                            const rendered = rawValue == null ? 'NULL' : String(rawValue);
                            return (
                              <td key={`${rowIndex}-${columnName}`} className="max-w-[320px] px-3 py-2 align-top">
                                <button
                                  type="button"
                                  className="w-full truncate text-left"
                                  title={rendered}
                                  onClick={() => copyCell(rawValue)}
                                >
                                  {rendered}
                                </button>
                              </td>
                            );
                          })}
                          <td className="px-3 py-2 align-top">
                            <Button size="sm" variant="ghost" onClick={() => copyRow(row)}>
                              Copy row
                            </Button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </TabsContent>

            <TabsContent value="logs" className="space-y-3">
              <div className="flex justify-end">
                <Button size="sm" variant="outline" onClick={() => setSqlLogs([])}>
                  <Eraser className="h-4 w-4" /> Clear logs
                </Button>
              </div>
              <div className="max-h-[420px] space-y-2 overflow-auto rounded-xl border border-[color:var(--panel-border)] p-3">
                {sqlLogs.length === 0 ? (
                  <p className="text-sm text-[color:var(--text-muted)]">No logs yet.</p>
                ) : (
                  sqlLogs.map((entry, index) => (
                    <div key={`${entry.timestamp}-${index}`} className="text-xs">
                      <span className="mr-2 text-[color:var(--text-muted)]">{formatDate(entry.timestamp)}</span>
                      <span
                        className={
                          entry.level === 'error'
                            ? 'text-rose-500'
                            : entry.level === 'warn'
                              ? 'text-amber-600 dark:text-amber-300'
                              : 'text-[color:var(--text-secondary)]'
                        }
                      >
                        [{entry.level.toUpperCase()}]
                      </span>{' '}
                      <span className="text-[color:var(--text-primary)]">{entry.message}</span>
                    </div>
                  ))
                )}
              </div>
            </TabsContent>

            <TabsContent value="history" className="space-y-3">
              <div className="flex flex-wrap items-center gap-2">
                <Select
                  value={historyScope}
                  onChange={(event) => setHistoryScope((event.target.value as 'user' | 'workspace') || 'user')}
                  className="max-w-[240px]"
                >
                  <option value="user">My history</option>
                  <option value="workspace">Workspace history</option>
                </Select>
              </div>
              <div className="max-h-[420px] space-y-2 overflow-auto">
                {historyQuery.isLoading ? <p className="text-sm text-[color:var(--text-muted)]">Loading SQL history...</p> : null}
                {historyQuery.isError ? (
                  <p className="text-sm text-rose-500">Unable to load history for this scope.</p>
                ) : null}
                {(historyQuery.data?.items || []).map((item) => (
                  <div
                    key={item.id}
                    className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-3 text-sm"
                  >
                    <div className="flex flex-wrap items-center gap-2 text-xs text-[color:var(--text-secondary)]">
                      <span className="rounded-full bg-[color:var(--chip-bg)] px-2 py-1">Status: {item.status}</span>
                      <span className="rounded-full bg-[color:var(--chip-bg)] px-2 py-1">Mode: {item.workbenchMode === 'dataset' ? 'Datasets' : 'Direct SQL'}</span>
                      {item.workbenchMode === 'dataset' ? (
                        <span className="rounded-full bg-[color:var(--chip-bg)] px-2 py-1">Datasets: {item.selectedDatasets.length}</span>
                      ) : item.connectionId ? (
                        <span className="rounded-full bg-[color:var(--chip-bg)] px-2 py-1">Connector: {item.connectionId}</span>
                      ) : null}
                    </div>
                    <div className="mt-2 grid gap-1 text-xs text-[color:var(--text-secondary)] md:grid-cols-2">
                      <p>Job: {item.id}</p>
                      <p>Query hash: {item.queryHash}</p>
                      <p>Rows: {item.rowCountPreview}</p>
                      <p>Created: {formatDate(item.createdAt)}</p>
                    </div>
                    <div className="mt-3 flex gap-2">
                      <Button size="sm" variant="outline" onClick={() => void viewHistoryJob(item)}>
                        View results
                      </Button>
                      {CANCELABLE_JOB_STATES.has(item.status) ? (
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => cancelHistoryJob(item.id)}
                          isLoading={cancellingJobId === item.id}
                        >
                          Cancel job
                        </Button>
                      ) : null}
                    </div>
                  </div>
                ))}
                {!historyQuery.isLoading && (historyQuery.data?.items || []).length === 0 ? (
                  <p className="text-sm text-[color:var(--text-muted)]">No history entries yet.</p>
                ) : null}
              </div>
            </TabsContent>

            <TabsContent value="saved" className="space-y-3">
              <div className="grid gap-3 lg:grid-cols-[300px_1fr]">
                <div className="space-y-2">
                  <div className="flex gap-2">
                    <Button size="sm" onClick={saveCurrentQuery} isLoading={saveQueryMutation.isPending || updateQueryMutation.isPending}>
                      <Save className="h-4 w-4" /> {selectedSavedQueryId ? 'Update' : 'Save'}
                    </Button>
                    <Button size="sm" variant="outline" onClick={deleteSelectedQuery} disabled={!selectedSavedQueryId || deleteQueryMutation.isPending}>
                      Delete
                    </Button>
                  </div>
                  <label className="flex items-center gap-2 text-xs text-[color:var(--text-secondary)]">
                    <input
                      type="checkbox"
                      checked={shareEnabled}
                      onChange={(event) => setShareEnabled(event.target.checked)}
                    />
                    Shared
                  </label>
                  <div className="max-h-[340px] space-y-2 overflow-auto rounded-xl border border-[color:var(--panel-border)] p-2">
                    {(savedQueriesQuery.data?.items || []).map((saved) => (
                      <button
                        key={saved.id}
                        type="button"
                        onClick={() => loadSavedQuery(saved)}
                        className={`w-full rounded-lg border px-3 py-2 text-left text-sm ${
                          saved.id === selectedSavedQueryId
                            ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)]'
                            : 'border-[color:var(--panel-border)] hover:bg-[color:var(--panel-alt)]'
                        }`}
                      >
                        <p className="font-semibold text-[color:var(--text-primary)]">{saved.name}</p>
                        <p className="text-xs text-[color:var(--text-secondary)]">
                          {saved.workbenchMode === 'dataset' ? 'Datasets' : 'Direct SQL'}
                          {' · '}
                          {saved.tags.join(', ') || 'No tags'}
                        </p>
                        <p className="mt-1 text-[10px] text-[color:var(--text-muted)]">
                          Updated: {formatDate(saved.updatedAt)}
                        </p>
                      </button>
                    ))}
                    {(savedQueriesQuery.data?.items || []).length === 0 ? (
                      <p className="px-2 text-xs text-[color:var(--text-muted)]">No saved queries yet.</p>
                    ) : null}
                  </div>
                </div>
                <div className="space-y-3">
                  <Input value={savedQueryName} onChange={(event) => setSavedQueryName(event.target.value)} placeholder="Query name" />
                  <Input value={savedQueryTags} onChange={(event) => setSavedQueryTags(event.target.value)} placeholder="Tags (comma separated)" />
                  {shareLink ? (
                    <div className="rounded-xl border border-[color:var(--panel-border)] p-3 text-xs">
                      <p className="font-semibold text-[color:var(--text-primary)]">Shareable link</p>
                      <p className="mt-1 break-all text-[color:var(--text-secondary)]">{shareLink}</p>
                      <div className="mt-2 flex gap-2">
                        <Button size="sm" variant="outline" onClick={copyShareLink}>
                          <Share2 className="h-4 w-4" /> Copy link
                        </Button>
                        <Button size="sm" variant="outline" onClick={promoteToDataset}>
                          <Wand2 className="h-4 w-4" /> Promote to BI
                        </Button>
                      </div>
                    </div>
                  ) : (
                    <div className="rounded-xl border border-dashed border-[color:var(--panel-border)] p-3 text-xs text-[color:var(--text-muted)]">
                      Enable <strong>Shared</strong> and save to generate a permissioned link.
                    </div>
                  )}
                  <div className="rounded-xl border border-[color:var(--panel-border)] p-3 text-xs text-[color:var(--text-secondary)]">
                    <p className="font-semibold text-[color:var(--text-primary)]">Promotion flow</p>
                    <p className="mt-1">
                      Promoted queries are opened in BI Studio and can be used as dataset inputs for charts and dashboards.
                    </p>
                  </div>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="policy" className="space-y-3">
              {!policyEditor ? (
                <p className="text-sm text-[color:var(--text-muted)]">Loading workspace SQL policy...</p>
              ) : (
                <>
                  <div className="grid gap-3 md:grid-cols-2">
                    <Input
                      value={String(policyEditor.maxPreviewRows)}
                      onChange={(event) =>
                        setPolicyEditor((current) =>
                          current
                            ? {
                                ...current,
                                maxPreviewRows: parseNumeric(event.target.value, current.maxPreviewRows),
                              }
                            : current,
                        )
                      }
                      placeholder="max preview rows"
                    />
                    <Input
                      value={String(policyEditor.maxExportRows)}
                      onChange={(event) =>
                        setPolicyEditor((current) =>
                          current
                            ? {
                                ...current,
                                maxExportRows: parseNumeric(event.target.value, current.maxExportRows),
                              }
                            : current,
                        )
                      }
                      placeholder="max export rows"
                    />
                    <Input
                      value={String(policyEditor.maxRuntimeSeconds)}
                      onChange={(event) =>
                        setPolicyEditor((current) =>
                          current
                            ? {
                                ...current,
                                maxRuntimeSeconds: parseNumeric(event.target.value, current.maxRuntimeSeconds),
                              }
                            : current,
                        )
                      }
                      placeholder="max runtime seconds"
                    />
                    <Input
                      value={String(policyEditor.maxConcurrency)}
                      onChange={(event) =>
                        setPolicyEditor((current) =>
                          current
                            ? {
                                ...current,
                                maxConcurrency: parseNumeric(event.target.value, current.maxConcurrency),
                              }
                            : current,
                        )
                      }
                      placeholder="max concurrency"
                    />
                    <Input
                      value={policyEditor.budgetLimitBytes ? String(policyEditor.budgetLimitBytes) : ''}
                      onChange={(event) =>
                        setPolicyEditor((current) =>
                          current
                            ? {
                                ...current,
                                budgetLimitBytes: event.target.value ? parseNumeric(event.target.value, current.budgetLimitBytes || 1) : null,
                              }
                            : current,
                        )
                      }
                      placeholder="budget limit bytes (optional)"
                    />
                    <Select
                      value={policyEditor.defaultDatasource || ''}
                      onChange={(event) =>
                        setPolicyEditor((current) =>
                          current
                            ? {
                                ...current,
                                defaultDatasource: event.target.value || null,
                              }
                            : current,
                        )
                      }
                    >
                      <option value="">No default datasource</option>
                      {availableConnectors.map((connector) => (
                        <option key={connector.id} value={connector.id}>
                          {connector.name}
                        </option>
                      ))}
                    </Select>
                  </div>
                  <Input
                    value={policyEditor.allowedSchemas.join(',')}
                    onChange={(event) =>
                      setPolicyEditor((current) =>
                        current
                          ? {
                              ...current,
                              allowedSchemas: event.target.value.split(',').map((value) => value.trim()).filter(Boolean),
                            }
                          : current,
                      )
                    }
                    placeholder="allowed schemas (comma separated)"
                  />
                  <Input
                    value={policyEditor.allowedTables.join(',')}
                    onChange={(event) =>
                      setPolicyEditor((current) =>
                        current
                          ? {
                              ...current,
                              allowedTables: event.target.value.split(',').map((value) => value.trim()).filter(Boolean),
                            }
                          : current,
                      )
                    }
                    placeholder="allowed tables (schema.table, comma separated)"
                  />
                  <div className="flex flex-wrap gap-4 text-sm">
                    <label className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        checked={policyEditor.allowDml}
                        onChange={(event) =>
                          setPolicyEditor((current) =>
                            current
                              ? {
                                  ...current,
                                  allowDml: event.target.checked,
                                }
                              : current,
                          )
                        }
                      />
                      Allow DML
                    </label>
                    <label className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        checked={policyEditor.allowFederation}
                        onChange={(event) =>
                          setPolicyEditor((current) =>
                            current
                              ? {
                                  ...current,
                                  allowFederation: event.target.checked,
                                }
                              : current,
                          )
                        }
                      />
                      Allow federation
                    </label>
                  </div>
                  <div className="text-xs text-[color:var(--text-secondary)]">
                    Bounds: preview up to {policyEditor.bounds.maxPreviewRowsUpperBound}, export up to {policyEditor.bounds.maxExportRowsUpperBound}, runtime up to {policyEditor.bounds.maxRuntimeSecondsUpperBound}s, concurrency up to {policyEditor.bounds.maxConcurrencyUpperBound}.
                  </div>
                  <Button size="sm" onClick={savePolicy} isLoading={updatePolicyMutation.isPending}>
                    Save policy
                  </Button>
                </>
              )}
            </TabsContent>

            <TabsContent value="assistant" className="space-y-3">
              <div className="grid gap-3 md:grid-cols-[180px_1fr_auto]">
                <Select value={assistantMode} onChange={(event) => setAssistantMode((event.target.value as SqlAssistMode) || 'generate')}>
                  <option value="generate">Generate SQL</option>
                  <option value="fix">Fix SQL</option>
                  <option value="explain">Explain SQL</option>
                  <option value="lint">Lint SQL</option>
                </Select>
                <Input
                  value={assistantPrompt}
                  onChange={(event) => setAssistantPrompt(event.target.value)}
                  placeholder="Ask SQL helper to generate/fix/explain your query"
                />
                <Button size="sm" onClick={runAssistant} isLoading={assistantMutation.isPending}>
                  <Bot className="h-4 w-4" /> Run
                </Button>
              </div>
              <div className="rounded-xl border border-[color:var(--panel-border)] p-3">
                <p className="text-xs font-semibold text-[color:var(--text-primary)]">Assistant output</p>
                <pre className="mt-2 max-h-[240px] overflow-auto whitespace-pre-wrap rounded-lg bg-[color:var(--panel-alt)] p-3 text-xs text-[color:var(--text-secondary)]">
                  {assistantSuggestion || 'No suggestion yet.'}
                </pre>
                <div className="mt-3 flex gap-2">
                  <Button size="sm" variant="outline" onClick={applyAssistantSuggestion} disabled={!assistantSuggestion.trim()}>
                    Apply to editor
                  </Button>
                </div>
              </div>
            </TabsContent>
          </Tabs>
        </div>
      </section>
    </div>
  );
}
