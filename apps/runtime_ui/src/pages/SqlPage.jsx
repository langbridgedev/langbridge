import { useEffect, useMemo, useRef, useState } from "react";
import { Copy, Download, Save } from "lucide-react";

import { FederationDiagnosticsPanel } from "../components/FederationDiagnosticsPanel";
import { ResultTable } from "../components/ResultTable";
import { PageEmpty, Panel, SectionTabs } from "../components/PagePrimitives";
import { useAsyncData } from "../hooks/useAsyncData";
import { usePersistentState } from "../hooks/usePersistentState";
import {
  fetchConnectors,
  fetchDataset,
  fetchDatasets,
  fetchSemanticModel,
  fetchSemanticModels,
  querySql,
} from "../lib/runtimeApi";
import {
  formatDateTime,
  formatValue,
  getErrorMessage,
  splitCsv,
  toSqlAlias,
} from "../lib/format";
import {
  DEFAULT_SQL_QUERY,
  SQL_HISTORY_STORAGE_KEY,
  SQL_SAVED_STORAGE_KEY,
  copyTextToClipboard,
  createLocalId,
  detectSqlWarnings,
  downloadTextFile,
  extractSemanticDatasets,
  normalizeTabularResult,
  toCsvText,
} from "../lib/runtimeUi";

const CLOSED_DATASET_AUTOCOMPLETE = {
  open: false,
  items: [],
  selectedIndex: 0,
  start: 0,
  end: 0,
  partial: "",
  kind: "",
  label: "",
};

const SQL_SCOPE_OPTIONS = [
  { value: "semantic", label: "Semantic query" },
  { value: "dataset", label: "Dataset SQL" },
  { value: "source", label: "Source SQL" },
];

const QUERY_MODE_CARDS = [
  {
    value: "semantic",
    label: "Semantic query",
    description: "Default governed path through the runtime semantic layer.",
  },
  {
    value: "dataset",
    label: "Dataset SQL",
    description: "Query runtime datasets directly when you need explicit dataset control.",
  },
  {
    value: "source",
    label: "Source SQL",
    description: "Power-user mode for direct connector execution against the underlying source.",
  },
];

const SQL_RESOURCE_AUTOCOMPLETE_PATTERN =
  /(?:^|[\s,(])(?:from|join|update|into|table)\s+([a-z0-9_]*)$/i;
const SQL_QUALIFIED_COLUMN_AUTOCOMPLETE_PATTERN = /([a-z0-9_]+)\.([a-z0-9_]*)$/i;
const SQL_BARE_IDENTIFIER_AUTOCOMPLETE_PATTERN = /(?:^|[\s,(])([a-z0-9_]*)$/i;
const SQL_RELATION_ALIAS_PATTERN =
  /\b(?:from|join)\s+([a-z0-9_]+)(?:\s+(?:as\s+)?([a-z0-9_]+))?(?=\s|,|$)/gi;
const SQL_RESERVED_ALIAS_WORDS = new Set([
  "on",
  "where",
  "group",
  "order",
  "limit",
  "join",
  "left",
  "right",
  "inner",
  "outer",
  "full",
  "cross",
  "union",
  "having",
  "offset",
  "select",
]);

function buildDatasetAutocompleteItems(datasets) {
  const seen = new Set();
  return (Array.isArray(datasets) ? datasets : [])
    .map((item) => {
      const rawName = String(item?.name || "").trim();
      if (!rawName) {
        return null;
      }
      const alias = toSqlAlias(rawName);
      if (!alias || seen.has(alias)) {
        return null;
      }
      seen.add(alias);
      return {
        label: alias,
        value: alias,
        resourceKind: "dataset alias",
        detail: [
          alias !== rawName ? rawName : null,
          item?.connector ? `connector ${item.connector}` : null,
          item?.semantic_model ? `model ${item.semantic_model}` : null,
          item?.materialization_mode || null,
        ]
          .filter(Boolean)
          .join(" | "),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.label.localeCompare(right.label));
}

function buildSemanticModelAutocompleteItems(models) {
  const seen = new Set();
  return (Array.isArray(models) ? models : [])
    .map((item) => {
      const name = String(item?.name || "").trim();
      if (!name || seen.has(name)) {
        return null;
      }
      seen.add(name);
      return {
        label: name,
        value: name,
        resourceKind: "semantic model",
        detail: [
          item?.default ? "default" : null,
          Number(item?.dataset_count || 0) > 0 ? `${item.dataset_count} datasets` : null,
          item?.description || null,
        ]
          .filter(Boolean)
          .join(" | "),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.label.localeCompare(right.label));
}

function buildAutocompleteState({ query, cursorIndex, items, force = false }) {
  const safeQuery = String(query || "");
  const safeCursor = Math.max(0, Math.min(cursorIndex, safeQuery.length));
  const safeItems = Array.isArray(items) ? items : [];

  if (safeItems.length === 0) {
    return CLOSED_DATASET_AUTOCOMPLETE;
  }

  if (force) {
    return {
      open: true,
      items: safeItems.slice(0, 8),
      selectedIndex: 0,
      start: safeCursor,
      end: safeCursor,
      partial: "",
      kind: "resource",
      label: "",
    };
  }

  const beforeCursor = safeQuery.slice(0, safeCursor);
  const match = beforeCursor.match(SQL_RESOURCE_AUTOCOMPLETE_PATTERN);
  if (!match) {
    return CLOSED_DATASET_AUTOCOMPLETE;
  }

  const partial = String(match[1] || "");
  const normalizedPartial = partial.toLowerCase();
  const start = safeCursor - partial.length;
  const filteredItems = safeItems.filter((item) => {
    if (!normalizedPartial) {
      return true;
    }
    return (
      item.label.toLowerCase().startsWith(normalizedPartial) ||
      item.detail.toLowerCase().includes(normalizedPartial)
    );
  });

  if (filteredItems.length === 0) {
    return CLOSED_DATASET_AUTOCOMPLETE;
  }

  return {
    open: true,
    items: filteredItems.slice(0, 8),
    selectedIndex: 0,
    start,
    end: safeCursor,
    partial,
    kind: "resource",
    label: "",
  };
}

function buildFilteredAutocompleteState({
  cursorIndex,
  partial = "",
  items,
  kind,
  label,
}) {
  const safeItems = Array.isArray(items) ? items : [];
  if (safeItems.length === 0) {
    return CLOSED_DATASET_AUTOCOMPLETE;
  }

  const normalizedPartial = String(partial || "").trim().toLowerCase();
  const filteredItems = safeItems.filter((item) => {
    if (!normalizedPartial) {
      return true;
    }
    return (
      item.label.toLowerCase().startsWith(normalizedPartial) ||
      item.detail.toLowerCase().includes(normalizedPartial)
    );
  });

  if (filteredItems.length === 0) {
    return CLOSED_DATASET_AUTOCOMPLETE;
  }

  return {
    open: true,
    items: filteredItems.slice(0, 8),
    selectedIndex: 0,
    start: cursorIndex - normalizedPartial.length,
    end: cursorIndex,
    partial: normalizedPartial,
    kind,
    label,
  };
}

function extractQualifiedColumnContext(query, cursorIndex) {
  const safeQuery = String(query || "");
  const safeCursor = Math.max(0, Math.min(cursorIndex, safeQuery.length));
  const beforeCursor = safeQuery.slice(0, safeCursor);
  const match = beforeCursor.match(SQL_QUALIFIED_COLUMN_AUTOCOMPLETE_PATTERN);
  if (!match) {
    return null;
  }
  const qualifier = String(match[1] || "").trim();
  const partial = String(match[2] || "").trim();
  if (!qualifier) {
    return null;
  }
  return {
    qualifier,
    partial,
    cursorIndex: safeCursor,
  };
}

function extractBareIdentifierContext(query, cursorIndex) {
  const safeQuery = String(query || "");
  const safeCursor = Math.max(0, Math.min(cursorIndex, safeQuery.length));
  const beforeCursor = safeQuery.slice(0, safeCursor);
  if (SQL_RESOURCE_AUTOCOMPLETE_PATTERN.test(beforeCursor)) {
    return null;
  }
  const match = beforeCursor.match(SQL_BARE_IDENTIFIER_AUTOCOMPLETE_PATTERN);
  if (!match) {
    return null;
  }
  return {
    partial: String(match[1] || "").trim(),
    cursorIndex: safeCursor,
  };
}

function extractSqlRelationAliases(query) {
  const aliases = new Map();
  const sql = String(query || "");
  let match = SQL_RELATION_ALIAS_PATTERN.exec(sql);
  while (match) {
    const relationName = String(match[1] || "").trim();
    const aliasName = String(match[2] || "").trim();
    if (relationName) {
      aliases.set(relationName.toLowerCase(), relationName);
    }
    if (aliasName && !SQL_RESERVED_ALIAS_WORDS.has(aliasName.toLowerCase())) {
      aliases.set(aliasName.toLowerCase(), relationName || aliasName);
    }
    match = SQL_RELATION_ALIAS_PATTERN.exec(sql);
  }
  SQL_RELATION_ALIAS_PATTERN.lastIndex = 0;
  return aliases;
}

function extractPrimarySqlRelationName(query) {
  const aliases = extractSqlRelationAliases(query);
  const iterator = aliases.values();
  const firstValue = iterator.next();
  return firstValue.done ? "" : String(firstValue.value || "");
}

function buildDatasetColumnAutocompleteItems(alias, detail) {
  const columns = Array.isArray(detail?.columns) ? detail.columns : [];
  return columns
    .map((column, index) => {
      const name = String(column?.name || "").trim();
      if (!name) {
        return null;
      }
      const typeLabel =
        column?.type ||
        column?.logical_type ||
        column?.data_type ||
        column?.dtype ||
        null;
      const nullable =
        column?.nullable === true || column?.is_nullable === true
          ? "nullable"
          : column?.nullable === false || column?.is_nullable === false
            ? "required"
            : null;
      return {
        key: `${alias}.${name}.${index}`,
        label: name,
        value: name,
        resourceKind: "column",
        detail: [typeLabel, nullable, alias].filter(Boolean).join(" | "),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.label.localeCompare(right.label));
}

function buildSemanticMemberAutocompleteItems(detail) {
  const datasets = extractSemanticDatasets(detail);
  const members = new Map();

  datasets.forEach((dataset) => {
    const datasetName = String(dataset?.name || "").trim() || "semantic dataset";
    const dimensions = Array.isArray(dataset?.dimensions) ? dataset.dimensions : [];
    const measures = Array.isArray(dataset?.measures) ? dataset.measures : [];

    dimensions.forEach((item) => {
      const name = String(item?.name || "").trim();
      if (!name) {
        return;
      }
      const entry = members.get(name) || {
        datasets: new Set(),
        kinds: new Set(),
        aggregations: new Set(),
      };
      entry.datasets.add(datasetName);
      entry.kinds.add("dimension");
      members.set(name, entry);
    });

    measures.forEach((item) => {
      const name = String(item?.name || "").trim();
      if (!name) {
        return;
      }
      const entry = members.get(name) || {
        datasets: new Set(),
        kinds: new Set(),
        aggregations: new Set(),
      };
      entry.datasets.add(datasetName);
      entry.kinds.add("measure");
      if (item?.aggregation) {
        entry.aggregations.add(String(item.aggregation));
      }
      members.set(name, entry);
    });
  });

  return Array.from(members.entries())
    .map(([name, value]) => ({
      key: `semantic-member-${name}`,
      label: name,
      value: name,
      resourceKind: "semantic member",
      detail: [
        Array.from(value.kinds).join("/"),
        Array.from(value.datasets).join(", "),
        Array.from(value.aggregations).join(", "),
      ]
        .filter(Boolean)
        .join(" | "),
    }))
    .sort((left, right) => left.label.localeCompare(right.label));
}

function insertAutocompleteValue(query, autocomplete, suggestion) {
  const prefix = String(query || "").slice(0, autocomplete.start);
  const suffix = String(query || "").slice(autocomplete.end);
  const needsTrailingSpace = !/^[\s,)\n]/.test(suffix);
  const insertedValue = `${suggestion.value}${needsTrailingSpace ? " " : ""}`;
  return {
    query: `${prefix}${insertedValue}${suffix}`,
    cursorIndex: prefix.length + insertedValue.length,
  };
}

function resolveQueryScope(value, connectionName = "") {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "semantic" || normalized === "dataset" || normalized === "source") {
    return normalized;
  }
  return String(connectionName || "").trim() ? "source" : "dataset";
}

function buildScopeLabel(queryScope) {
  if (queryScope === "semantic") {
    return "Semantic query";
  }
  if (queryScope === "source") {
    return "Source SQL";
  }
  return "Dataset SQL";
}

function buildQueryCardLabel(queryScope, connectionName = "") {
  if (queryScope === "source") {
    return connectionName ? `Source SQL - ${connectionName}` : "Source SQL";
  }
  return buildScopeLabel(queryScope);
}

function buildSemanticStarterQuery(modelName = "<semantic_model>") {
  return `SELECT *
FROM ${modelName}
LIMIT 20`;
}

function buildSourceStarterQuery() {
  return `SELECT *
FROM source_table
LIMIT 25`;
}

export function SqlPage() {
  const queryInputRef = useRef(null);
  const autocompleteItemRefs = useRef([]);
  const datasetDetailRequestsRef = useRef(new Set());
  const semanticModelDetailRequestsRef = useRef(new Set());
  const seededStarterRef = useRef(false);
  const connectorsState = useAsyncData(fetchConnectors);
  const datasetsState = useAsyncData(fetchDatasets);
  const semanticModelsState = useAsyncData(fetchSemanticModels);
  const [activeTab, setActiveTab] = useState("results");
  const [resultView, setResultView] = useState("rows");
  const [autocomplete, setAutocomplete] = useState(CLOSED_DATASET_AUTOCOMPLETE);
  const [form, setForm] = useState({
    queryScope: "semantic",
    query: "",
    connectionName: "",
    requestedLimit: "200",
  });
  const [result, setResult] = useState(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [workspaceNotice, setWorkspaceNotice] = useState("");
  const [selectedSavedId, setSelectedSavedId] = useState("");
  const [savedName, setSavedName] = useState("");
  const [savedTags, setSavedTags] = useState("");
  const [savedQueries, setSavedQueries] = usePersistentState(SQL_SAVED_STORAGE_KEY, []);
  const [historyItems, setHistoryItems] = usePersistentState(SQL_HISTORY_STORAGE_KEY, []);
  const [datasetDetailsByAlias, setDatasetDetailsByAlias] = useState({});
  const [semanticModelDetailsByName, setSemanticModelDetailsByName] = useState({});

  const connectors = Array.isArray(connectorsState.data?.items)
    ? connectorsState.data.items
    : [];
  const datasets = Array.isArray(datasetsState.data?.items) ? datasetsState.data.items : [];
  const semanticModels = Array.isArray(semanticModelsState.data?.items)
    ? semanticModelsState.data.items
    : [];
  const warnings = useMemo(() => detectSqlWarnings(form.query), [form.query]);
  const normalizedResult = result ? normalizeTabularResult(result) : null;
  const datasetAutocompleteItems = useMemo(
    () => buildDatasetAutocompleteItems(datasets),
    [datasets],
  );
  const semanticModelAutocompleteItems = useMemo(
    () => buildSemanticModelAutocompleteItems(semanticModels),
    [semanticModels],
  );
  const defaultSemanticModelName =
    semanticModels.find((item) => item.default)?.name || semanticModels[0]?.name || "";
  const defaultDatasetAlias = datasetAutocompleteItems[0]?.label || "";
  const defaultSourceConnection = connectors[0]?.name || "";
  const starterTemplates = useMemo(
    () => [
      {
        id: "semantic",
        label: "Semantic starter",
        description: "Load a governed semantic query skeleton and refine it from the semantic layer.",
        queryScope: "semantic",
        connectionName: "",
        query: buildSemanticStarterQuery(defaultSemanticModelName || "<semantic_model>"),
      },
      {
        id: "dataset",
        label: "Dataset starter",
        description: "Drop into dataset SQL when you need explicit dataset-level control.",
        queryScope: "dataset",
        connectionName: "",
        query: defaultDatasetAlias
          ? `SELECT *
FROM ${defaultDatasetAlias}
LIMIT 25`
          : DEFAULT_SQL_QUERY,
      },
      {
        id: "source",
        label: "Source starter",
        description: "Power-user path for direct connector SQL against the underlying source.",
        queryScope: "source",
        connectionName: defaultSourceConnection,
        query: buildSourceStarterQuery(),
      },
    ],
    [defaultDatasetAlias, defaultSemanticModelName, defaultSourceConnection],
  );
  const activeAutocompleteItems =
    form.queryScope === "semantic"
      ? semanticModelAutocompleteItems
      : form.queryScope === "dataset"
        ? datasetAutocompleteItems
        : [];
  const datasetColumnItemsByAlias = useMemo(
    () =>
      Object.fromEntries(
        Object.entries(datasetDetailsByAlias).map(([alias, detail]) => [
          alias,
          buildDatasetColumnAutocompleteItems(alias, detail),
        ]),
      ),
    [datasetDetailsByAlias],
  );
  const semanticMemberItemsByModel = useMemo(
    () =>
      Object.fromEntries(
        Object.entries(semanticModelDetailsByName).map(([name, detail]) => [
          name,
          buildSemanticMemberAutocompleteItems(detail),
        ]),
      ),
    [semanticModelDetailsByName],
  );
  const activeSemanticModelName = useMemo(
    () => extractPrimarySqlRelationName(form.query).toLowerCase(),
    [form.query],
  );
  const activeSuggestion =
    autocomplete.open && autocomplete.items.length > 0
      ? autocomplete.items[autocomplete.selectedIndex] || autocomplete.items[0]
      : null;
  const semanticModelChipItems = semanticModels.slice(0, 6);
  const queryModeLabel = buildScopeLabel(form.queryScope);
  const queryScopeNote =
    form.queryScope === "semantic"
      ? semanticModels.length > 0
        ? "Semantic querying is the default analytical path. Start with a semantic model, ask a governed question, and use Ctrl+Space after FROM to insert a model name."
        : "Semantic querying is the default analytical path, but this runtime does not currently expose semantic models in the workspace."
      : form.queryScope === "source"
        ? form.connectionName
          ? `Queries now run directly against ${form.connectionName}. Source SQL is the most explicit and least-governed execution path in the runtime.`
          : "Select a connector to run source SQL directly against the underlying system. Source SQL is the most explicit and least-governed execution path in the runtime."
        : "Dataset SQL stays available when you need direct control over runtime dataset relations. Dataset completion is available directly in the editor.";
  const autocompleteHeaderLabel =
    autocomplete.label ||
    (form.queryScope === "semantic" ? "Semantic models" : "Dataset aliases");
  const autocompleteHintLabel =
    autocomplete.kind === "column"
      ? "Tab to move, Enter to insert column name"
      : autocomplete.kind === "semantic-member"
        ? "Tab to move, Enter to insert member name"
        : form.queryScope === "semantic"
          ? "Tab to move, Enter to insert model name"
          : "Tab to move, Enter to insert";
  const autocompleteStatusLabel =
    form.queryScope === "source"
      ? "Autocomplete is available for semantic and dataset SQL only."
      : form.queryScope === "semantic"
        ? "Semantic model completion opens after FROM. Member completion opens on semantic members or with Ctrl+Space."
        : "Dataset completion opens after FROM or JOIN. Column completion opens after table aliases such as shopify_orders. or s.";
  const autocompleteAvailabilityLabel =
    form.queryScope === "source"
      ? form.connectionName
        ? `Using connector ${form.connectionName}`
        : "Select a connector to run source SQL."
      : activeSuggestion
        ? `Ready to insert ${activeSuggestion.label}`
        : form.queryScope === "semantic"
          ? activeSemanticModelName && semanticMemberItemsByModel[activeSemanticModelName]?.length
            ? `${formatValue(semanticMemberItemsByModel[activeSemanticModelName].length)} semantic members available`
            : `${formatValue(activeAutocompleteItems.length)} semantic models available`
          : `${formatValue(activeAutocompleteItems.length)} dataset aliases available`;
  const canRunQuery =
    !running &&
    Boolean(String(form.query || "").trim()) &&
    (form.queryScope !== "source" || Boolean(String(form.connectionName || "").trim()));
  const editorLineCount = Math.max(1, String(form.query || "").split(/\r?\n/).length);
  const resultPanelTitle =
    activeTab === "history"
      ? "Run history"
      : activeTab === "saved"
        ? "Saved workspace"
        : "Execution output";
  const resultViewTabs = [
    { value: "rows", label: "Rows" },
    ...(result?.generated_sql ? [{ value: "sql", label: "Generated SQL" }] : []),
    ...(result?.federation_diagnostics
      ? [{ value: "diagnostics", label: "Diagnostics" }]
      : []),
  ];
  const activeResultView = resultViewTabs.some((tab) => tab.value === resultView)
    ? resultView
    : "rows";

  useEffect(() => {
    if (seededStarterRef.current || !starterTemplates[0]?.query) {
      return;
    }
    setForm((current) => ({
      ...current,
      queryScope: starterTemplates[0].queryScope,
      query: starterTemplates[0].query,
      connectionName: starterTemplates[0].connectionName || "",
    }));
    seededStarterRef.current = true;
  }, [starterTemplates]);

  useEffect(() => {
    let cancelled = false;
    datasets.forEach((item) => {
      const ref = String(item?.id || item?.name || "").trim();
      if (!ref || datasetDetailRequestsRef.current.has(ref)) {
        return;
      }
      datasetDetailRequestsRef.current.add(ref);
      void fetchDataset(ref)
        .then((payload) => {
          if (cancelled || !payload) {
            return;
          }
          const alias = toSqlAlias(payload?.name || item?.name || "");
          if (!alias) {
            return;
          }
          setDatasetDetailsByAlias((current) => ({
            ...current,
            [alias.toLowerCase()]: payload,
          }));
        })
        .catch(() => {});
    });
    return () => {
      cancelled = true;
    };
  }, [datasets]);

  useEffect(() => {
    let cancelled = false;
    semanticModels.forEach((item) => {
      const ref = String(item?.id || item?.name || "").trim();
      if (!ref || semanticModelDetailRequestsRef.current.has(ref)) {
        return;
      }
      semanticModelDetailRequestsRef.current.add(ref);
      void fetchSemanticModel(ref)
        .then((payload) => {
          if (cancelled || !payload) {
            return;
          }
          const name = String(payload?.name || item?.name || "").trim().toLowerCase();
          if (!name) {
            return;
          }
          setSemanticModelDetailsByName((current) => ({
            ...current,
            [name]: payload,
          }));
        })
        .catch(() => {});
    });
    return () => {
      cancelled = true;
    };
  }, [semanticModels]);

  useEffect(() => {
    autocompleteItemRefs.current = autocomplete.items.map(
      (_, index) => autocompleteItemRefs.current[index] || null,
    );
  }, [autocomplete.items]);

  useEffect(() => {
    if (!autocomplete.open) {
      return;
    }
    const activeItem = autocompleteItemRefs.current[autocomplete.selectedIndex];
    if (!activeItem || typeof activeItem.scrollIntoView !== "function") {
      return;
    }
    activeItem.scrollIntoView({ block: "nearest" });
  }, [autocomplete.open, autocomplete.selectedIndex]);

  function closeAutocomplete() {
    setAutocomplete(CLOSED_DATASET_AUTOCOMPLETE);
  }

  function moveAutocompleteSelection(offset) {
    setAutocomplete((current) => {
      if (!current.open || current.items.length === 0) {
        return current;
      }
      const itemCount = current.items.length;
      return {
        ...current,
        selectedIndex: (current.selectedIndex + offset + itemCount) % itemCount,
      };
    });
  }

  function focusQueryInput(cursorIndex) {
    requestAnimationFrame(() => {
      const textarea = queryInputRef.current;
      if (!textarea) {
        return;
      }
      textarea.focus();
      textarea.setSelectionRange(cursorIndex, cursorIndex);
    });
  }

  function refreshAutocomplete(nextQuery, cursorIndex, { force = false } = {}) {
    if (form.queryScope === "source") {
      closeAutocomplete();
      return;
    }

    if (form.queryScope === "dataset") {
      const qualifiedColumnContext = extractQualifiedColumnContext(nextQuery, cursorIndex);
      if (qualifiedColumnContext) {
        const relationAliases = extractSqlRelationAliases(nextQuery);
        const relationName =
          relationAliases.get(qualifiedColumnContext.qualifier.toLowerCase()) ||
          qualifiedColumnContext.qualifier;
        const columnItems =
          datasetColumnItemsByAlias[String(relationName || "").trim().toLowerCase()] || [];
        setAutocomplete(
          buildFilteredAutocompleteState({
            cursorIndex: qualifiedColumnContext.cursorIndex,
            partial: qualifiedColumnContext.partial,
            items: columnItems,
            kind: "column",
            label: relationName ? `Columns for ${relationName}` : "Columns",
          }),
        );
        return;
      }
    }

    if (form.queryScope === "semantic") {
      const semanticContext = extractBareIdentifierContext(nextQuery, cursorIndex);
      const memberItems = semanticMemberItemsByModel[extractPrimarySqlRelationName(nextQuery).toLowerCase()] || [];
      if ((semanticContext || force) && memberItems.length > 0) {
        setAutocomplete(
          buildFilteredAutocompleteState({
            cursorIndex: semanticContext?.cursorIndex ?? cursorIndex,
            partial: semanticContext?.partial ?? "",
            items: memberItems,
            kind: "semantic-member",
            label: "Semantic members",
          }),
        );
        return;
      }
    }

    if (form.queryScope === "dataset") {
      const datasetContext = extractBareIdentifierContext(nextQuery, cursorIndex);
      const relationAliases = extractSqlRelationAliases(nextQuery);
      const uniqueRelationNames = Array.from(new Set(relationAliases.values())).filter(Boolean);
      if ((datasetContext || force) && uniqueRelationNames.length === 1) {
        const relationName = String(uniqueRelationNames[0] || "").trim().toLowerCase();
        const columnItems = datasetColumnItemsByAlias[relationName] || [];
        if (columnItems.length > 0) {
          setAutocomplete(
            buildFilteredAutocompleteState({
              cursorIndex: datasetContext?.cursorIndex ?? cursorIndex,
              partial: datasetContext?.partial ?? "",
              items: columnItems,
              kind: "column",
              label: `Columns for ${uniqueRelationNames[0]}`,
            }),
          );
          return;
        }
      }
    }

    const resourceState = buildAutocompleteState({
      query: nextQuery,
      cursorIndex,
      items: activeAutocompleteItems,
      force,
    });
    if (resourceState.open) {
      setAutocomplete({
        ...resourceState,
        kind: "resource",
        label: form.queryScope === "semantic" ? "Semantic models" : "Dataset aliases",
      });
      return;
    }

    closeAutocomplete();
  }

  function acceptAutocomplete(item) {
    if (!item || !autocomplete.open) {
      return;
    }
    const nextValue = insertAutocompleteValue(form.query, autocomplete, item);
    setForm((current) => ({
      ...current,
      query: nextValue.query,
    }));
    setWorkspaceNotice(`Inserted ${item.resourceKind || "value"} '${item.label}' into the query.`);
    closeAutocomplete();
    focusQueryInput(nextValue.cursorIndex);
  }

  function insertTextAtCursor(value, resourceKind = "value") {
    const textarea = queryInputRef.current;
    const currentQuery = String(form.query || "");
    const start = textarea?.selectionStart ?? currentQuery.length;
    const end = textarea?.selectionEnd ?? start;
    const prefix = currentQuery.slice(0, start);
    const suffix = currentQuery.slice(end);
    const needsTrailingSpace = !/^[\s,)\n]/.test(suffix);
    const insertedValue = `${value}${needsTrailingSpace ? " " : ""}`;
    const nextQuery = `${prefix}${insertedValue}${suffix}`;
    setForm((current) => ({
      ...current,
      query: nextQuery,
    }));
    setWorkspaceNotice(`Inserted ${resourceKind} '${value}' into the query.`);
    closeAutocomplete();
    focusQueryInput(prefix.length + insertedValue.length);
  }

  function applyStarterTemplate(template) {
    if (!template) {
      return;
    }
    closeAutocomplete();
    setForm((current) => ({
      ...current,
      queryScope: template.queryScope,
      query: template.query,
      connectionName: template.connectionName || "",
    }));
    setWorkspaceNotice(`Loaded ${template.label.toLowerCase()} into Query Workspace.`);
    setActiveTab("results");
    setResultView("rows");
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setRunning(true);
    setError("");
    setWorkspaceNotice("");
    closeAutocomplete();
    try {
      const payload = {
        query_scope: form.queryScope,
        query: form.query,
        requested_limit:
          Number(form.requestedLimit) > 0 ? Number(form.requestedLimit) : undefined,
      };
      if (form.queryScope === "source") {
        payload.connection_name = form.connectionName;
      }
      const response = await querySql(payload);
      setResult(response);
      setActiveTab("results");
      setResultView("rows");
      setHistoryItems((current) =>
        [
          {
            id: createLocalId("sql-run"),
            createdAt: new Date().toISOString(),
            queryScope: form.queryScope,
            connectionName: form.connectionName,
            requestedLimit: form.requestedLimit,
            query: form.query,
            rowCount: response?.rowCount || response?.row_count_preview || 0,
            durationMs: response?.duration_ms || null,
            status: response?.status || "succeeded",
            response,
            errorMessage: "",
          },
          ...current,
        ].slice(0, 20),
      );
    } catch (caughtError) {
      setResult(null);
      const errorMessage = getErrorMessage(caughtError);
      setError(errorMessage);
      setHistoryItems((current) =>
        [
          {
            id: createLocalId("sql-run"),
            createdAt: new Date().toISOString(),
            queryScope: form.queryScope,
            connectionName: form.connectionName,
            requestedLimit: form.requestedLimit,
            query: form.query,
            rowCount: 0,
            durationMs: null,
            status: "failed",
            response: null,
            errorMessage,
          },
          ...current,
        ].slice(0, 20),
      );
    } finally {
      setRunning(false);
    }
  }

  function resetWorkbench() {
    const semanticStarter = starterTemplates[0];
    setForm({
      queryScope: semanticStarter?.queryScope || "semantic",
      query: semanticStarter?.query || "",
      connectionName: semanticStarter?.connectionName || "",
      requestedLimit: "200",
    });
    setWorkspaceNotice("Query Workspace reset to the semantic-first default path.");
    setSelectedSavedId("");
    setSavedName("");
    setSavedTags("");
    closeAutocomplete();
  }

  function saveCurrentQuery() {
    const nextEntry = {
      id: selectedSavedId || createLocalId("sql"),
      name: String(savedName || "").trim() || `Saved query ${savedQueries.length + 1}`,
      tags: splitCsv(savedTags),
      queryScope: form.queryScope,
      query: form.query,
      connectionName: form.connectionName,
      requestedLimit: form.requestedLimit,
      updatedAt: new Date().toISOString(),
    };
    setSavedQueries((current) => {
      const next = [nextEntry, ...current.filter((item) => item.id !== nextEntry.id)];
      next.sort((left, right) =>
        String(right.updatedAt || "").localeCompare(String(left.updatedAt || "")),
      );
      return next;
    });
    setSelectedSavedId(nextEntry.id);
    setSavedName(nextEntry.name);
    setSavedTags(nextEntry.tags.join(", "));
    setWorkspaceNotice(`Saved "${nextEntry.name}" to local workspace storage.`);
    setActiveTab("saved");
  }

  function loadSavedQuery(entry) {
    const queryScope = resolveQueryScope(entry?.queryScope, entry?.connectionName);
    setSelectedSavedId(entry.id);
    setSavedName(entry.name || "");
    setSavedTags(Array.isArray(entry.tags) ? entry.tags.join(", ") : "");
    setForm({
      queryScope,
      query: entry.query || DEFAULT_SQL_QUERY,
      connectionName: entry.connectionName || "",
      requestedLimit: entry.requestedLimit || "200",
    });
    setWorkspaceNotice(`Loaded "${entry.name}" into Query Workspace.`);
    closeAutocomplete();
    setActiveTab("results");
  }

  function deleteSavedQueryById(id) {
    setSavedQueries((current) => current.filter((item) => item.id !== id));
    if (selectedSavedId === id) {
      setSelectedSavedId("");
      setSavedName("");
      setSavedTags("");
    }
    setWorkspaceNotice("Removed saved query from local workspace storage.");
  }

  async function handleCopySql() {
    try {
      await copyTextToClipboard(form.query);
      setWorkspaceNotice("SQL copied to clipboard.");
    } catch (caughtError) {
      setWorkspaceNotice(getErrorMessage(caughtError));
    }
  }

  async function handleCopyGeneratedSql() {
    if (!result?.generated_sql) {
      return;
    }
    try {
      await copyTextToClipboard(result.generated_sql);
      setWorkspaceNotice("Generated SQL copied to clipboard.");
    } catch (caughtError) {
      setWorkspaceNotice(getErrorMessage(caughtError));
    }
  }

  function handleEditorChange(event) {
    const nextQuery = event.target.value;
    const cursorIndex = event.target.selectionStart ?? nextQuery.length;
    setForm((current) => ({
      ...current,
      query: nextQuery,
    }));
    refreshAutocomplete(nextQuery, cursorIndex);
  }

  function handleEditorSelectionChange(event) {
    const cursorIndex = event.target.selectionStart ?? 0;
    refreshAutocomplete(event.target.value, cursorIndex);
  }

  function handleEditorKeyUp(event) {
    if (
      [
        "ArrowDown",
        "ArrowUp",
        "Enter",
        "Escape",
        "Tab",
        "PageDown",
        "PageUp",
      ].includes(event.key)
    ) {
      return;
    }
    handleEditorSelectionChange(event);
  }

  function handleEditorKeyDown(event) {
    if (event.ctrlKey && event.key === " ") {
      event.preventDefault();
      refreshAutocomplete(
        form.query,
        event.currentTarget.selectionStart ?? form.query.length,
        { force: true },
      );
      return;
    }

    if (!autocomplete.open) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      moveAutocompleteSelection(1);
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      moveAutocompleteSelection(-1);
      return;
    }

    if (event.key === "Tab") {
      event.preventDefault();
      moveAutocompleteSelection(event.shiftKey ? -1 : 1);
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      acceptAutocomplete(activeSuggestion);
      return;
    }

    if (event.key === "Escape") {
      event.preventDefault();
      closeAutocomplete();
    }
  }

  return (
    <div className="page-stack query-workspace-shell">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Query Workspace</p>
            <h2>Semantic-first query execution</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{queryModeLabel}</span>
              <span className="chip">{formatValue(connectors.length)} connectors</span>
              <span className="chip">{formatValue(datasets.length)} datasets</span>
              <span className="chip">{formatValue(semanticModels.length)} models</span>
              <span className="chip">{formatValue(historyItems.length)} local runs</span>
            </div>
          </div>
        </div>
      </section>

      <section className="sql-workbench-stack">
        <Panel
          title="Query workspace"
          eyebrow="Query"
          className="compact-panel sql-workbench-panel"
        >
          {/* <p className="sql-workbench-copy">{queryScopeNote}</p>
          <div className="product-panel-meta sql-workbench-meta">
            <span>{queryModeLabel}</span>
            <span>{formatValue(connectors.length)} connectors</span>
            <span>{formatValue(datasets.length)} datasets</span>
            <span>{formatValue(semanticModels.length)} models</span>
            <span>{formatValue(form.requestedLimit)} row limit</span>
          </div>
          {workspaceNotice ? <div className="sql-inline-note">{workspaceNotice}</div> : null} */}

          <form className="sql-workbench-form" onSubmit={handleSubmit}>
            {/* <div className="query-mode-grid">
              {QUERY_MODE_CARDS.map((item) => (
                <button
                  key={item.value}
                  className={`query-mode-card ${form.queryScope === item.value ? "active" : ""}`.trim()}
                  type="button"
                  onClick={() => {
                    closeAutocomplete();
                    setForm((current) => ({
                      ...current,
                      queryScope: resolveQueryScope(item.value, current.connectionName),
                    }));
                  }}
                  disabled={running}
                >
                  <strong>{item.label}</strong>
                  <span>{item.description}</span>
                </button>
              ))}
            </div> */}

            {/* <div className="query-starter-grid">
              {starterTemplates.map((template) => (
                <button
                  key={template.id}
                  className="query-starter-card"
                  type="button"
                  onClick={() => applyStarterTemplate(template)}
                  disabled={running}
                >
                  <strong>{template.label}</strong>
                  <span>{template.description}</span>
                </button>
              ))}
            </div> */}

            <div className="sql-workbench-toolbar">
              <label className="field sql-toolbar-field">
                <span>Execution mode</span>
                <select
                  className="select-input"
                  value={form.queryScope}
                  onChange={(event) => {
                    closeAutocomplete();
                    setForm((current) => ({
                      ...current,
                      queryScope: resolveQueryScope(event.target.value),
                    }));
                  }}
                  disabled={running}
                >
                  {SQL_SCOPE_OPTIONS.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>

              {form.queryScope === "source" ? (
                <label className="field sql-toolbar-field">
                  <span>Connector</span>
                  <select
                    className="select-input"
                    value={form.connectionName}
                    onChange={(event) => {
                      closeAutocomplete();
                      setForm((current) => ({
                        ...current,
                        connectionName: event.target.value,
                      }));
                    }}
                    disabled={running}
                  >
                    <option value="">
                      {connectors.length > 0 ? "Select connector" : "No connectors available"}
                    </option>
                    {connectors.map((item) => (
                      <option key={item.id || item.name} value={item.name}>
                        {item.name}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}

              <label className="field sql-toolbar-field sql-toolbar-field--limit">
                <span>Row limit</span>
                <input
                  className="text-input"
                  type="number"
                  min="1"
                  value={form.requestedLimit}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      requestedLimit: event.target.value,
                    }))
                  }
                  disabled={running}
                />
              </label>

              <div className="sql-workbench-toolbar-actions">
                <button className="primary-button" type="submit" disabled={!canRunQuery}>
                  {running ? "Running query..." : "Run query"}
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={resetWorkbench}
                  disabled={running}
                >
                  Reset
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={saveCurrentQuery}
                  disabled={!form.query.trim()}
                >
                  Save locally
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => void handleCopySql()}
                  disabled={!form.query.trim()}
                >
                  Copy SQL
                </button>
              </div>
            </div>

            {form.queryScope === "semantic" && semanticModelChipItems.length > 0 ? (
              <div className="field-pill-list">
                {semanticModelChipItems.map((item) => (
                  <button
                    key={item.id || item.name}
                    className="field-pill"
                    type="button"
                    onClick={() => insertTextAtCursor(String(item.name), "semantic model")}
                    disabled={running}
                  >
                    {item.name}
                  </button>
                ))}
                {semanticModels.length > semanticModelChipItems.length ? (
                  <span className="field-pill static">
                    +{semanticModels.length - semanticModelChipItems.length} more
                  </span>
                ) : null}
              </div>
            ) : null}

            <div className="sql-editor-surface">
              <div className="sql-editor-surface-bar">
                <div className="sql-editor-surface-bar-left">
                  <span className="sql-editor-dot sql-editor-dot--red" />
                  <span className="sql-editor-dot sql-editor-dot--amber" />
                  <span className="sql-editor-dot sql-editor-dot--green" />
                  <span className="sql-editor-filename">query.sql</span>
                </div>
                <div className="sql-editor-surface-bar-right">
                  <span>{queryModeLabel}</span>
                  <span>{formatValue(editorLineCount)} lines</span>
                  <span>{formatValue(form.query.length)} chars</span>
                </div>
              </div>

              <div className="sql-editor-frame">
                <textarea
                  ref={queryInputRef}
                  className="sql-editor-input"
                  value={form.query}
                  onChange={handleEditorChange}
                  onClick={handleEditorSelectionChange}
                  onKeyDown={handleEditorKeyDown}
                  onKeyUp={handleEditorKeyUp}
                  onSelect={handleEditorSelectionChange}
                  disabled={running}
                  rows={14}
                  spellCheck={false}
                  aria-label="SQL query editor"
                  placeholder="Start with a semantic model and ask a governed question, then move to dataset or source SQL only when you need the more explicit runtime surface."
                />

                {autocomplete.open ? (
                  <div className="sql-autocomplete" role="listbox" aria-label={`${autocompleteHeaderLabel} autocomplete`}>
                    <div className="sql-autocomplete-header">
                      <span>{autocompleteHeaderLabel}</span>
                      <small>{autocompleteHintLabel}</small>
                    </div>
                    <div
                      className="sql-autocomplete-list"
                      onWheel={(event) => event.stopPropagation()}
                    >
                {autocomplete.items.map((item, index) => (
                  <button
                    key={item.key || item.value}
                    ref={(element) => {
                      autocompleteItemRefs.current[index] = element;
                    }}
                    type="button"
                    className={`sql-autocomplete-item ${index === autocomplete.selectedIndex ? "active" : ""}`.trim()}
                    role="option"
                          aria-selected={index === autocomplete.selectedIndex}
                          tabIndex={-1}
                          onMouseEnter={() =>
                            setAutocomplete((current) => ({
                              ...current,
                              selectedIndex: index,
                            }))
                          }
                          onMouseDown={(event) => {
                            event.preventDefault();
                            acceptAutocomplete(item);
                          }}
                        >
                          <strong>{item.label}</strong>
                          {item.detail ? <span>{item.detail}</span> : null}
                        </button>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>

              <div className="sql-editor-status-bar">
                <span>{autocompleteStatusLabel}</span>
                <span>{autocompleteAvailabilityLabel}</span>
              </div>
            </div>
            {warnings.length > 0 ? (
              <div className="sql-warning-strip" aria-live="polite">
                <span className="sql-warning-label">Query notes</span>
                {warnings.map((warning) => (
                  <span key={warning} className="sql-warning-pill">
                    {warning}
                  </span>
                ))}
              </div>
            ) : null}

            {error ? <div className="error-banner">{error}</div> : null}
          </form>
        </Panel>

        <Panel title={resultPanelTitle} eyebrow="Execution" className="compact-panel sql-results-workbench">
          <SectionTabs
            tabs={[
              { value: "results", label: "Results" },
              { value: "history", label: "Run history" },
              { value: "saved", label: "Saved workspace" },
            ]}
            value={activeTab}
            onChange={setActiveTab}
          />

          {activeTab === "results" ? (
            normalizedResult ? (
              <div className="sql-result-viewer">
                <div className="inline-notes sql-results-metrics">
                  <span>Rows: {formatValue(result.rowCount || result.row_count_preview)}</span>
                  <span>Duration: {formatValue(result.duration_ms)}</span>
                  <span>Redaction: {formatValue(result.redaction_applied)}</span>
                </div>

                <div className="panel-actions-inline">
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() =>
                      downloadTextFile(
                        "runtime-sql-results.csv",
                        toCsvText(normalizedResult),
                        "text/csv;charset=utf-8",
                      )
                    }
                  >
                    <Download className="button-icon" aria-hidden="true" />
                    Download CSV
                  </button>
                  {result.generated_sql ? (
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => void handleCopyGeneratedSql()}
                    >
                      <Copy className="button-icon" aria-hidden="true" />
                      Copy generated SQL
                    </button>
                  ) : null}
                </div>

                {resultViewTabs.length > 1 ? (
                  <SectionTabs
                    tabs={resultViewTabs}
                    value={activeResultView}
                    onChange={setResultView}
                  />
                ) : null}

                {activeResultView === "rows" ? (
                  <ResultTable result={normalizedResult} maxPreviewRows={32} />
                ) : null}

                {activeResultView === "sql" && result.generated_sql ? (
                  <pre className="code-block">{result.generated_sql}</pre>
                ) : null}

                {activeResultView === "diagnostics" && result.federation_diagnostics ? (
                  <FederationDiagnosticsPanel diagnostics={result.federation_diagnostics} />
                ) : null}
              </div>
            ) : (
              <PageEmpty
                title="No query result yet"
                message="Run semantic, dataset, or source SQL to inspect runtime execution output."
              />
            )
          ) : null}

          {activeTab === "history" ? (
            historyItems.length > 0 ? (
              <div className="stack-list">
                {historyItems.map((item) => {
                  const queryScope = resolveQueryScope(item?.queryScope, item?.connectionName);
                  return (
                    <div key={item.id} className="list-card static">
                      <strong>{buildQueryCardLabel(queryScope, item.connectionName)}</strong>
                      <span>
                        {[formatDateTime(item.createdAt), item.status, `${item.rowCount || 0} rows`]
                          .filter(Boolean)
                          .join(" | ")}
                      </span>
                      <small>{item.query}</small>
                      <div className="panel-actions-inline">
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => {
                            closeAutocomplete();
                            setForm({
                              queryScope,
                              query: item.query || DEFAULT_SQL_QUERY,
                              connectionName: item.connectionName || "",
                              requestedLimit: item.requestedLimit || "200",
                            });
                            setActiveTab("results");
                          }}
                        >
                          Load
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <PageEmpty
                title="No local history"
                message="Executed Query Workspace runs will appear here for this browser."
              />
            )
          ) : null}

          {activeTab === "saved" ? (
            <div className="sql-result-viewer">
              <div className="page-stack">
                <label className="field">
                  <span>Saved query name</span>
                  <input
                    className="text-input"
                    type="text"
                    value={savedName}
                    onChange={(event) => setSavedName(event.target.value)}
                    placeholder="Revenue by region"
                  />
                </label>
                <label className="field">
                  <span>Tags</span>
                  <input
                    className="text-input"
                    type="text"
                    value={savedTags}
                    onChange={(event) => setSavedTags(event.target.value)}
                    placeholder="finance, weekly"
                  />
                </label>
                <div className="page-actions">
                  <button className="primary-button" type="button" onClick={saveCurrentQuery}>
                    <Save className="button-icon" aria-hidden="true" />
                    {selectedSavedId ? "Update saved query" : "Save query"}
                  </button>
                </div>
              </div>

              {savedQueries.length > 0 ? (
                <div className="stack-list">
                  {savedQueries.map((item) => (
                    <div
                      key={item.id}
                      className={`list-card static ${selectedSavedId === item.id ? "active" : ""}`.trim()}
                    >
                      <strong>{item.name}</strong>
                      <span>
                        {[
                          formatDateTime(item.updatedAt),
                          buildQueryCardLabel(
                            resolveQueryScope(item?.queryScope, item?.connectionName),
                            item.connectionName,
                          ),
                          ...(Array.isArray(item.tags) ? item.tags : []),
                        ]
                          .filter(Boolean)
                          .join(" | ")}
                      </span>
                      <small>{item.query}</small>
                      <div className="panel-actions-inline">
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => loadSavedQuery(item)}
                        >
                          Load
                        </button>
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => deleteSavedQueryById(item.id)}
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <PageEmpty
                  title="No saved queries"
                  message="Save the current query to keep a local Query Workspace library."
                />
              )}
            </div>
          ) : null}
        </Panel>
      </section>
    </div>
  );
}
