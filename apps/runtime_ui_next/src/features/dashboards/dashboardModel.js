export const DASHBOARD_SCHEMA_VERSION = 1;
export const DASHBOARD_BUILDER_STORAGE_KEY = "langbridge.runtime_ui_next.dashboard_builder";
export const LEGACY_DASHBOARD_BUILDER_STORAGE_KEY = "langbridge.runtime_ui.dashboard_builder";

export const DASHBOARD_WIDGET_TYPES = [
  { value: "bar", label: "Bar" },
  { value: "line", label: "Line" },
  { value: "pie", label: "Pie" },
  { value: "donut", label: "Donut" },
  { value: "stat", label: "KPI" },
  { value: "table", label: "Table" },
  { value: "note", label: "Note" },
];

export const DASHBOARD_TIME_GRAINS = [
  { value: "", label: "None" },
  { value: "day", label: "Day" },
  { value: "week", label: "Week" },
  { value: "month", label: "Month" },
  { value: "quarter", label: "Quarter" },
  { value: "year", label: "Year" },
];

export const DASHBOARD_TIME_PRESETS = [
  { value: "", label: "No filter" },
  { value: "today", label: "Today" },
  { value: "yesterday", label: "Yesterday" },
  { value: "last_7_days", label: "Last 7 days" },
  { value: "last_30_days", label: "Last 30 days" },
  { value: "month_to_date", label: "Month to date" },
  { value: "year_to_date", label: "Year to date" },
  { value: "custom_between", label: "Custom: Between" },
  { value: "custom_before", label: "Custom: Before date" },
  { value: "custom_after", label: "Custom: After date" },
  { value: "custom_on", label: "Custom: On date" },
];

export const DASHBOARD_PALETTES = [
  { id: "amber", label: "Amber", colors: ["#d97706", "#dc2626", "#7c3aed", "#0891b2"] },
  { id: "ocean", label: "Ocean", colors: ["#0369a1", "#0ea5e9", "#14b8a6", "#7dd3fc"] },
  { id: "forest", label: "Forest", colors: ["#166534", "#16a34a", "#65a30d", "#a3e635"] },
  { id: "slate", label: "Slate", colors: ["#334155", "#475569", "#64748b", "#94a3b8"] },
];

const DATE_LIKE_TYPES = new Set(["date", "datetime", "timestamp", "time"]);
const VALUELESS_FILTER_OPERATORS = new Set(["set", "notset"]);
const FILTER_LOGICS = new Set(["and", "or"]);

function timestamp(value) {
  const normalized = String(value || "").trim();
  return normalized || new Date().toISOString();
}

function normalizeFilterLogic(value) {
  const normalized = String(value || "and").trim().toLowerCase();
  return FILTER_LOGICS.has(normalized) ? normalized : "and";
}

function createId(prefix) {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeList(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function normalizeSelectedMembers(...sources) {
  const values = [];
  sources.forEach((source) => {
    if (Array.isArray(source)) {
      source.forEach((item) => {
        const value = String(item || "").trim();
        if (value) {
          values.push(value);
        }
      });
      return;
    }
    const value = String(source || "").trim();
    if (value) {
      values.push(value);
    }
  });
  return [...new Set(values)];
}

function normalizeWidgetType(value) {
  const normalized = String(value || "bar").trim().toLowerCase();
  return DASHBOARD_WIDGET_TYPES.some((item) => item.value === normalized) ? normalized : "bar";
}

function normalizeChartBinding(value, selectedMembers) {
  const normalized = String(value || "").trim();
  if (normalized && selectedMembers.includes(normalized)) {
    return normalized;
  }
  return selectedMembers[0] || "";
}

export function createDashboardFilter(seed = {}) {
  return {
    id: seed.id || createId("filter"),
    member: String(seed.member || "").trim(),
    operator: String(seed.operator || "equals").trim().toLowerCase(),
    values: String(seed.values || "").trim(),
  };
}

export function createDashboardOrder(seed = {}) {
  return {
    id: seed.id || createId("order"),
    member: String(seed.member || "").trim(),
    direction: String(seed.direction || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc",
  };
}

export function createDashboardWidget(seed = {}) {
  const createdAt = timestamp(seed.createdAt || seed.created_at);
  const dimensions = normalizeSelectedMembers(seed.dimensions, seed.dimension);
  const measures = normalizeSelectedMembers(seed.measures, seed.measure);
  const type = normalizeWidgetType(seed.type || seed.chartType);
  const timeDimension = String(seed.timeDimension || seed.time_dimension || "").trim();
  const chartDimensionMembers = normalizeSelectedMembers(dimensions, timeDimension);
  return {
    id: seed.id || createId("widget"),
    title: String(seed.title || (type === "note" ? "Note" : "Untitled widget")).trim(),
    type,
    description: String(seed.description || "").trim(),
    noteMarkdown: String(seed.noteMarkdown || seed.note_markdown || "").trim(),
    dimensions,
    measures,
    filters: normalizeList(seed.filters).map((item) => createDashboardFilter(item)),
    filterLogic: normalizeFilterLogic(seed.filterLogic || seed.filter_logic),
    orderBys: normalizeList(seed.orderBys || seed.order_bys).map((item) => createDashboardOrder(item)),
    limit: String(seed.limit || "12"),
    timeDimension,
    timeGrain: String(seed.timeGrain || seed.time_grain || "").trim(),
    timeRangePreset: String(seed.timeRangePreset || seed.time_range_preset || "").trim(),
    timeRangeFrom: String(seed.timeRangeFrom || seed.time_range_from || "").trim(),
    timeRangeTo: String(seed.timeRangeTo || seed.time_range_to || "").trim(),
    chartX: normalizeChartBinding(seed.chartX || seed.chart_x, chartDimensionMembers),
    chartY: normalizeChartBinding(seed.chartY || seed.chart_y, measures),
    paletteId: String(seed.paletteId || seed.palette_id || DASHBOARD_PALETTES[0].id).trim(),
    createdAt,
    updatedAt: timestamp(seed.updatedAt || seed.updated_at || createdAt),
    result: seed.result || null,
    error: String(seed.error || "").trim(),
    running: Boolean(seed.running),
    lastRunAt: seed.lastRunAt || seed.last_run_at || null,
  };
}

export function createDashboardBoard(seed = {}) {
  const createdAt = timestamp(seed.createdAt || seed.created_at);
  const widgets =
    Array.isArray(seed.widgets) && seed.widgets.length > 0
      ? seed.widgets.map((item) => createDashboardWidget(item))
      : [createDashboardWidget({ title: "Revenue by channel" })];
  return {
    id: seed.id || createId("dashboard"),
    name: String(seed.name || seed.title || "Runtime dashboard").trim(),
    description: String(seed.description || "").trim(),
    selectedModel: String(seed.selectedModel || seed.selected_model || "").trim(),
    globalFilters: normalizeList(seed.globalFilters || seed.global_filters).map((item) => createDashboardFilter(item)),
    globalFilterLogic: normalizeFilterLogic(seed.globalFilterLogic || seed.global_filter_logic),
    widgets,
    createdAt,
    updatedAt: timestamp(seed.updatedAt || seed.updated_at || createdAt),
    lastRefreshedAt: seed.lastRefreshedAt || seed.last_refreshed_at || null,
  };
}

export function normalizeDashboardState(stored) {
  const source =
    stored?.dashboard && typeof stored.dashboard === "object"
      ? { activeBoardId: stored.dashboard.id, boards: [stored.dashboard] }
      : stored;
  const boards = Array.isArray(source?.boards)
    ? source.boards.map((item) => createDashboardBoard(item))
    : source && typeof source === "object" && (source.widgets || source.name || source.title)
      ? [createDashboardBoard(source)]
      : [];
  const normalizedBoards = boards.length > 0 ? boards : [createDashboardBoard()];
  const preferred = String(source?.activeBoardId || source?.active_board_id || "").trim();
  return {
    schemaVersion: DASHBOARD_SCHEMA_VERSION,
    activeBoardId: normalizedBoards.some((board) => board.id === preferred)
      ? preferred
      : normalizedBoards[0].id,
    boards: normalizedBoards,
  };
}

export function serializeDashboardState(state) {
  const normalized = normalizeDashboardState(state);
  return {
    schemaVersion: DASHBOARD_SCHEMA_VERSION,
    activeBoardId: normalized.activeBoardId,
    boards: normalized.boards.map((board) => serializeDashboardBoard(board)),
  };
}

export function serializeDashboardBoard(board, { includeExecutionMetadata = true } = {}) {
  const normalized = createDashboardBoard(board);
  const serialized = {
    ...normalized,
    widgets: normalized.widgets.map(({ running, error, result, ...widget }) => {
      if (includeExecutionMetadata) {
        return widget;
      }
      const { lastRunAt, ...portableWidget } = widget;
      return portableWidget;
    }),
  };
  if (!includeExecutionMetadata) {
    delete serialized.lastRefreshedAt;
  }
  return serialized;
}

export function serializeDashboardExport(board) {
  return {
    schemaVersion: DASHBOARD_SCHEMA_VERSION,
    exportedAt: new Date().toISOString(),
    dashboard: serializeDashboardBoard(board, { includeExecutionMetadata: false }),
  };
}

export function normalizeDashboardImportPayload(payload) {
  const source = payload?.dashboard && typeof payload.dashboard === "object" ? payload.dashboard : payload;
  const hasImportableDashboard =
    source &&
    typeof source === "object" &&
    (Array.isArray(source.boards) || Array.isArray(source.widgets) || source.name || source.title);
  if (!hasImportableDashboard) {
    throw new Error("The selected file does not contain a dashboard definition.");
  }
  const normalized = normalizeDashboardState(payload);
  return {
    ...normalized,
    activeBoardId: normalized.boards[0]?.id || normalized.activeBoardId,
  };
}

export function duplicateDashboardBoard(board, seed = {}) {
  const source = createDashboardBoard(board);
  return createDashboardBoard({
    ...source,
    ...seed,
    id: seed.id || undefined,
    name: seed.name || `${source.name} copy`,
    createdAt: seed.createdAt,
    updatedAt: seed.updatedAt,
    widgets: source.widgets.map((widget) => ({
      ...widget,
      id: undefined,
      createdAt: undefined,
      updatedAt: undefined,
      result: null,
      running: false,
      error: "",
      lastRunAt: null,
    })),
    lastRefreshedAt: null,
  });
}

export function removeDashboardBoard(state, boardId) {
  const normalized = normalizeDashboardState(state);
  const remaining = normalized.boards.filter((board) => board.id !== boardId);
  const boards = remaining.length > 0 ? remaining : [createDashboardBoard()];
  return {
    schemaVersion: DASHBOARD_SCHEMA_VERSION,
    activeBoardId: boards[0].id,
    boards,
  };
}

export function touchDashboardBoard(board, updates = {}) {
  return {
    ...board,
    ...updates,
    updatedAt: new Date().toISOString(),
  };
}

export function touchDashboardWidget(widget, updates = {}) {
  return {
    ...widget,
    ...updates,
    updatedAt: new Date().toISOString(),
  };
}

function parseObjectLike(value) {
  if (!value) {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  if (typeof value !== "string") {
    return null;
  }
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function fieldName(field) {
  if (typeof field === "string") {
    return field.trim();
  }
  if (!field || typeof field !== "object") {
    return "";
  }
  return String(field.name || field.label || field.key || field.field || "").trim();
}

function namedEntries(value) {
  if (Array.isArray(value)) {
    return value.map((item) => ({ name: fieldName(item), value: item })).filter((item) => item.name);
  }
  if (value && typeof value === "object") {
    return Object.entries(value).map(([name, item]) => ({
      name,
      value: item && typeof item === "object" ? { name, ...item } : { name },
    }));
  }
  return [];
}

function semanticPayload(detail) {
  return parseObjectLike(detail?.content_json) || parseObjectLike(detail?.model) || parseObjectLike(detail) || {};
}

function semanticDatasetEntries(payload) {
  const datasets = payload?.datasets || payload?.tables || {};
  if (Array.isArray(datasets)) {
    return datasets
      .map((dataset) => {
        const name = fieldName(dataset);
        return name ? [name, dataset] : null;
      })
      .filter(Boolean);
  }
  if (datasets && typeof datasets === "object") {
    return Object.entries(datasets);
  }
  return [];
}

export function extractDashboardSemanticResources(detail) {
  const payload = semanticPayload(detail);
  const fields = [];
  const datasets = semanticDatasetEntries(payload).map(([datasetName, dataset]) => {
    const datasetValue = dataset && typeof dataset === "object" ? dataset : {};
    const dimensions = namedEntries(datasetValue.dimensions).map(({ name, value }) => ({
      id: `${datasetName}.${name}`,
      value: `${datasetName}.${name}`,
      label: name,
      qualifiedLabel: `${datasetName}.${name}`,
      kind: "dimension",
      dataset: datasetName,
      type: value?.type || "dimension",
    }));
    const measures = namedEntries(datasetValue.measures).map(({ name, value }) => ({
      id: `${datasetName}.${name}`,
      value: `${datasetName}.${name}`,
      label: name,
      qualifiedLabel: `${datasetName}.${name}`,
      kind: "measure",
      dataset: datasetName,
      type: value?.type || "measure",
      aggregation: value?.aggregation || null,
    }));
    const metrics = namedEntries(datasetValue.metrics).map(({ name, value }) => ({
      id: `${datasetName}.${name}`,
      value: `${datasetName}.${name}`,
      label: name,
      qualifiedLabel: `${datasetName}.${name}`,
      kind: "metric",
      dataset: datasetName,
      type: "metric",
      expression: value?.expression || "",
    }));
    fields.push(...dimensions, ...measures, ...metrics);
    return {
      name: datasetName,
      relationName: datasetValue.relation_name || datasetValue.relationName || "",
      dimensions,
      measures,
      metrics,
    };
  });

  const modelMetrics = namedEntries(payload.metrics).map(({ name, value }) => ({
    id: name,
    value: name,
    label: name,
    qualifiedLabel: name,
    kind: "metric",
    dataset: "model",
    type: "metric",
    expression: value?.expression || "",
  }));
  fields.push(...modelMetrics);

  return {
    datasets,
    modelMetrics,
    fields,
    dimensions: fields.filter((field) => field.kind === "dimension"),
    measures: fields.filter((field) => field.kind === "measure"),
    metrics: fields.filter((field) => field.kind === "metric"),
  };
}

export function isDateLikeDashboardField(field) {
  return DATE_LIKE_TYPES.has(String(field?.type || "").trim().toLowerCase());
}

function filterValues(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function toSemanticFilter(filter) {
  const member = String(filter?.member || "").trim();
  if (!member) {
    return null;
  }
  const operator = String(filter?.operator || "equals").trim().toLowerCase();
  if (VALUELESS_FILTER_OPERATORS.has(operator)) {
    return { member, operator };
  }
  const values = filterValues(filter?.values);
  return values.length > 0 ? { member, operator, values } : null;
}

function buildFilterExpression(filters, logic = "and") {
  const normalizedFilters = normalizeList(filters).map(toSemanticFilter).filter(Boolean);
  if (normalizedFilters.length === 0) {
    return [];
  }
  if (normalizedFilters.length === 1 || normalizeFilterLogic(logic) === "and") {
    return normalizedFilters;
  }
  return [{ or: normalizedFilters }];
}

function resolveWidgetTimeRange(widget) {
  const preset = String(widget?.timeRangePreset || "").trim();
  if (!preset) {
    return undefined;
  }
  const from = String(widget?.timeRangeFrom || "").trim();
  const to = String(widget?.timeRangeTo || "").trim();
  if (preset === "custom_between") {
    return from && to ? [from, to] : undefined;
  }
  if (preset === "custom_before") {
    return from ? `before:${from}` : undefined;
  }
  if (preset === "custom_after") {
    return from ? `after:${from}` : undefined;
  }
  if (preset === "custom_on") {
    return from ? `on:${from}` : undefined;
  }
  return preset;
}

export function buildDashboardWidgetQueryPayload(board, widget) {
  const measures = normalizeSelectedMembers(widget?.measures);
  const dimensions = normalizeSelectedMembers(widget?.dimensions);
  const filters = [
    ...buildFilterExpression(board?.globalFilters, board?.globalFilterLogic),
    ...buildFilterExpression(widget?.filters, widget?.filterLogic),
  ];
  const order = normalizeList(widget?.orderBys)
    .filter((item) => item.member)
    .map((item) => ({ [item.member]: item.direction === "asc" ? "asc" : "desc" }));
  const timeRange = resolveWidgetTimeRange(widget);
  const timeDimensions =
    widget?.timeDimension
      ? [
          {
            dimension: widget.timeDimension,
            granularity: widget.timeGrain || undefined,
            dateRange: timeRange,
          },
        ]
      : [];

  return {
    semantic_models: [board?.selectedModel].filter(Boolean),
    measures,
    dimensions,
    filters,
    time_dimensions: timeDimensions,
    order: order.length > 0 ? order : measures[0] ? [{ [measures[0]]: "desc" }] : [],
    limit: Number(widget?.limit) > 0 ? Number(widget.limit) : 12,
  };
}

export function normalizeDashboardQueryResult(response) {
  const resultPayload = response?.result && typeof response.result === "object" ? response.result : {};
  const rows = Array.isArray(response?.data)
    ? response.data
    : Array.isArray(response?.rows)
      ? response.rows
      : Array.isArray(resultPayload?.data)
        ? resultPayload.data
        : Array.isArray(resultPayload?.rows)
          ? resultPayload.rows
          : [];
  const columns =
    Array.isArray(response?.columns) && response.columns.length > 0
      ? response.columns.map((column, index) => fieldName(column) || `Column ${index + 1}`)
      : Array.isArray(resultPayload?.columns) && resultPayload.columns.length > 0
        ? resultPayload.columns.map((column, index) => fieldName(column) || `Column ${index + 1}`)
      : rows[0] && typeof rows[0] === "object" && !Array.isArray(rows[0])
        ? Object.keys(rows[0])
        : [];
  const formatting =
    response?.formatting && typeof response.formatting === "object"
      ? response.formatting
      : resultPayload?.formatting && typeof resultPayload.formatting === "object"
        ? resultPayload.formatting
        : {};
  const rowCountValue = response?.rowcount ?? response?.row_count ?? resultPayload?.rowcount;
  const rowCount = Number.isFinite(Number(rowCountValue)) ? Number(rowCountValue) : rows.length;
  return {
    columns,
    rows,
    rowCount,
    metadata: Array.isArray(response?.metadata) ? response.metadata : [],
    formatting,
    generated_sql: response?.generated_sql || response?.generatedSql || resultPayload?.source_sql || "",
    duration_ms: response?.duration_ms ?? response?.elapsed_ms ?? resultPayload?.elapsed_ms ?? null,
    federation_diagnostics: response?.federation_diagnostics || null,
    raw: response,
  };
}

export function canRunDashboardWidget(widget, board) {
  if (!board?.selectedModel || widget?.type === "note") {
    return false;
  }
  return normalizeSelectedMembers(widget?.measures).length > 0;
}

export function reorderDashboardWidgets(widgets, sourceId, targetId) {
  const sourceIndex = widgets.findIndex((widget) => widget.id === sourceId);
  const targetIndex = widgets.findIndex((widget) => widget.id === targetId);
  if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) {
    return widgets;
  }
  const next = [...widgets];
  const [moved] = next.splice(sourceIndex, 1);
  next.splice(targetIndex, 0, moved);
  return next;
}

export function dashboardRecentsFromState(state) {
  return normalizeDashboardState(state).boards
    .map((board) => ({
      id: board.id,
      title: board.name,
      path: `/dashboards/${board.id}`,
      createdAt: board.lastRefreshedAt || "",
    }))
    .slice(0, 12);
}

export function dashboardProjectsFromState(state) {
  const boards = normalizeDashboardState(state).boards;
  return [
    {
      id: "local-dashboards",
      name: "Local dashboards",
      meta: `${boards.length} dashboard${boards.length === 1 ? "" : "s"}`,
      path: "/dashboards",
    },
  ];
}

export function fieldLabel(value) {
  const parts = String(value || "").split(".").filter(Boolean);
  return parts[parts.length - 1] || String(value || "");
}
