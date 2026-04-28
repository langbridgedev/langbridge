import {
  DASHBOARD_BUILDER_STORAGE_KEY,
  buildColumnsFromRows,
  createLocalId,
  readAgentJson,
} from "./runtimeUi";

export const DASHBOARD_BUILDER_PALETTES = [
  { id: "emerald", label: "Emerald", colors: ["#10a37f", "#0f8f6c", "#0ea5e9", "#f59e0b"] },
  { id: "ocean", label: "Ocean", colors: ["#0369a1", "#0ea5e9", "#14b8a6", "#7dd3fc"] },
  { id: "sunset", label: "Sunset", colors: ["#f97316", "#fb7185", "#f43f5e", "#f59e0b"] },
  { id: "slate", label: "Slate", colors: ["#334155", "#475569", "#64748b", "#94a3b8"] },
  { id: "orchard", label: "Orchard", colors: ["#65a30d", "#84cc16", "#22c55e", "#166534"] },
  { id: "canyon", label: "Canyon", colors: ["#c2410c", "#ea580c", "#fb923c", "#7c2d12"] },
  { id: "mulberry", label: "Mulberry", colors: ["#7c3aed", "#a855f7", "#ec4899", "#4c1d95"] },
  { id: "ember", label: "Ember", colors: ["#dc2626", "#f97316", "#facc15", "#7f1d1d"] },
  { id: "citrus", label: "Citrus", colors: ["#ca8a04", "#f59e0b", "#84cc16", "#365314"] },
  { id: "aurora", label: "Aurora", colors: ["#0f766e", "#06b6d4", "#8b5cf6", "#ec4899"] },
  { id: "rosewood", label: "Rosewood", colors: ["#9f1239", "#e11d48", "#fb7185", "#881337"] },
  { id: "charcoal", label: "Charcoal", colors: ["#111827", "#374151", "#6b7280", "#9ca3af"] },
  { id: "lavender", label: "Lavender", colors: ["#8b5cf6", "#c084fc", "#f0abfc", "#6d28d9"] },
  { id: "desert", label: "Desert", colors: ["#b45309", "#d97706", "#fbbf24", "#92400e"] },
  { id: "festival", label: "Festival", colors: ["#2563eb", "#7c3aed", "#db2777", "#ea580c"] },
];

export const DASHBOARD_BUILDER_FILTER_OPERATORS = [
  { value: "equals", label: "Equals" },
  { value: "notequals", label: "Not equals" },
  { value: "contains", label: "Contains" },
  { value: "gt", label: "Greater than" },
  { value: "gte", label: "Greater or equal" },
  { value: "lt", label: "Less than" },
  { value: "lte", label: "Less or equal" },
  { value: "in", label: "In list" },
  { value: "notin", label: "Not in list" },
  { value: "set", label: "Is set" },
  { value: "notset", label: "Is not set" },
];

export const DASHBOARD_BUILDER_DATE_FILTER_OPERATORS = [
  { value: "indaterange", label: "In date range" },
  { value: "notindaterange", label: "Not in date range" },
  { value: "set", label: "Is set" },
  { value: "notset", label: "Is not set" },
];

export const DASHBOARD_BUILDER_TIME_GRAINS = [
  { value: "", label: "No grain" },
  { value: "day", label: "Day" },
  { value: "week", label: "Week" },
  { value: "month", label: "Month" },
  { value: "quarter", label: "Quarter" },
  { value: "year", label: "Year" },
];

export const DASHBOARD_BUILDER_TIME_PRESETS = [
  { value: "no_filter", label: "No filter" },
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

const DATE_LIKE_TYPES = new Set(["date", "datetime", "timestamp", "time"]);
const VALUELESS_OPERATORS = new Set(["set", "notset"]);
const YEAR_PATTERN = /^\d{4}$/;
const YEAR_MONTH_PATTERN = /^\d{4}-(0[1-9]|1[0-2])$/;
const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const DATE_RANGE_OPERATORS = new Set(["indaterange", "notindaterange"]);

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

  return Array.from(new Set(values));
}

export function getDashboardBuilderPalette(paletteId) {
  return DASHBOARD_BUILDER_PALETTES.find((item) => item.id === paletteId) || DASHBOARD_BUILDER_PALETTES[0];
}

export function createFilterDraft(seed = {}) {
  return {
    id: seed.id || createLocalId("filter"),
    member: seed.member || "",
    operator: seed.operator || "equals",
    values: seed.values || "",
  };
}

export function createOrderDraft(seed = {}) {
  return {
    id: seed.id || createLocalId("order"),
    member: seed.member || "",
    direction: seed.direction === "asc" ? "asc" : "desc",
  };
}

export function createDashboardBuilderWidget(seed = {}) {
  const dimensions = normalizeSelectedMembers(seed.dimensions, seed.dimension);
  const measures = normalizeSelectedMembers(seed.measures, seed.measure);

  return {
    id: seed.id || createLocalId("widget"),
    title: seed.title || "Untitled widget",
    description: seed.description || "",
    chartType: normalizeChartType(seed.chartType),
    size: normalizeWidgetSize(seed.size),
    dimensions,
    measures,
    filters: Array.isArray(seed.filters)
      ? seed.filters.map((item) => createFilterDraft(item))
      : [],
    orderBys: Array.isArray(seed.orderBys)
      ? seed.orderBys.map((item) => createOrderDraft(item))
      : [],
    limit: String(seed.limit || "12"),
    timeDimension: seed.timeDimension || "",
    timeGrain: seed.timeGrain || "",
    timeRangePreset: seed.timeRangePreset || "",
    timeRangeFrom: seed.timeRangeFrom || "",
    timeRangeTo: seed.timeRangeTo || "",
    chartX: seed.chartX || dimensions[0] || "",
    chartY: seed.chartY || measures[0] || "",
    visualConfig: {
      paletteId: seed.visualConfig?.paletteId || DASHBOARD_BUILDER_PALETTES[0].id,
      showDataLabels: Boolean(seed.visualConfig?.showDataLabels),
      showGrid: seed.visualConfig?.showGrid ?? true,
      showLegend: Boolean(seed.visualConfig?.showLegend),
      lineStrokeWidth: Number(seed.visualConfig?.lineStrokeWidth || 2.25),
      barRadius: Number(seed.visualConfig?.barRadius || 10),
      pieInnerRadius: Number(seed.visualConfig?.pieInnerRadius || 42),
      lineCurve: seed.visualConfig?.lineCurve || "smooth",
      pieLabelMode: seed.visualConfig?.pieLabelMode || "none",
    },
    result: seed.result || null,
    running: Boolean(seed.running),
    error: seed.error || "",
    lastRunAt: seed.lastRunAt || null,
  };
}

export function createDashboardBuilderBoard(seed = {}) {
  const widgets =
    Array.isArray(seed.widgets) && seed.widgets.length > 0
      ? seed.widgets.map((item) => createDashboardBuilderWidget(item))
      : [createDashboardBuilderWidget()];
  const globalFilters =
    Array.isArray(seed.globalFilters) && seed.globalFilters.length > 0
      ? seed.globalFilters.map((item) => createFilterDraft(item))
      : [];

  return {
    id: seed.id || createLocalId("board"),
    name: seed.name || "Runtime dashboard",
    description:
      seed.description ||
      "Local runtime dashboard state derived from semantic models without control-plane storage.",
    selectedModel: seed.selectedModel || "",
    globalFilters,
    copilotSummary: seed.copilotSummary || "",
    lastRefreshedAt: seed.lastRefreshedAt || null,
    widgets,
  };
}

export function loadDashboardBuilderState(readStoredJson) {
  const stored = readStoredJson(DASHBOARD_BUILDER_STORAGE_KEY, null);
  const boards = Array.isArray(stored?.boards)
    ? stored.boards.map((item) => createDashboardBuilderBoard(item))
    : [];
  const normalizedBoards = boards.length > 0 ? boards : [createDashboardBuilderBoard()];
  const preferredActiveBoardId = String(stored?.activeBoardId || "");
  return {
    boards: normalizedBoards,
    activeBoardId: normalizedBoards.some((board) => board.id === preferredActiveBoardId)
      ? preferredActiveBoardId
      : normalizedBoards[0].id,
  };
}

export function isDateLikeField(field, fieldTypesByValue) {
  const type =
    field?.type ||
    fieldTypesByValue?.[field?.value || field?.member || ""] ||
    fieldTypesByValue?.[field?.id || ""] ||
    "";
  return DATE_LIKE_TYPES.has(String(type).trim().toLowerCase());
}

export function getFilterOperators(field, fieldTypesByValue) {
  return isDateLikeField(field, fieldTypesByValue)
    ? DASHBOARD_BUILDER_DATE_FILTER_OPERATORS
    : DASHBOARD_BUILDER_FILTER_OPERATORS;
}

export function getDefaultFilterOperator(field, fieldTypesByValue) {
  return getFilterOperators(field, fieldTypesByValue)[0]?.value || "equals";
}

export function isValuelessFilter(operator) {
  return VALUELESS_OPERATORS.has(String(operator || "").trim().toLowerCase());
}

export function ensureOperatorForField(operator, field, fieldTypesByValue) {
  const normalized = String(operator || "").trim().toLowerCase();
  const operators = getFilterOperators(field, fieldTypesByValue);
  if (operators.some((item) => item.value === normalized)) {
    return normalized;
  }
  return getDefaultFilterOperator(field, fieldTypesByValue);
}

export function usesDateRangePresetInput(field, fieldTypesByValue, operator) {
  return (
    isDateLikeField(field, fieldTypesByValue) &&
    DATE_RANGE_OPERATORS.has(String(operator || "").trim().toLowerCase())
  );
}

export function getFilterValuePlaceholder(field, fieldTypesByValue, operator) {
  if (!isDateLikeField(field, fieldTypesByValue)) {
    return "Value";
  }
  const normalizedOperator = String(operator || "").trim().toLowerCase();
  if (normalizedOperator === "indaterange" || normalizedOperator === "notindaterange") {
    return "YYYY-MM-DD,YYYY-MM-DD or last_30_days";
  }
  if (normalizedOperator === "beforedate" || normalizedOperator === "afterdate") {
    return "YYYY-MM-DD";
  }
  return "YYYY-MM-DD or YYYY";
}

export function parseDateRangeFilterValues(rawValues) {
  const trimmed = String(rawValues || "").trim();
  if (!trimmed) {
    return { preset: "no_filter", from: "", to: "" };
  }
  if (DASHBOARD_BUILDER_TIME_PRESETS.some((preset) => preset.value === trimmed)) {
    return { preset: trimmed, from: "", to: "" };
  }
  if (trimmed.startsWith("before:")) {
    return { preset: "custom_before", from: trimmed.slice("before:".length), to: "" };
  }
  if (trimmed.startsWith("after:")) {
    return { preset: "custom_after", from: trimmed.slice("after:".length), to: "" };
  }
  if (trimmed.startsWith("on:")) {
    return { preset: "custom_on", from: trimmed.slice("on:".length), to: "" };
  }

  const parts = trimmed
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length === 2 && ISO_DATE_PATTERN.test(parts[0]) && ISO_DATE_PATTERN.test(parts[1])) {
    return { preset: "custom_between", from: parts[0], to: parts[1] };
  }
  if (parts.length === 1 && ISO_DATE_PATTERN.test(parts[0])) {
    return { preset: "custom_on", from: parts[0], to: "" };
  }

  const range = normalizeYearOrMonthRange(trimmed);
  if (range) {
    return { preset: "custom_between", from: range[0], to: range[1] };
  }

  return { preset: "no_filter", from: "", to: "" };
}

export function serializeDateRangeFilterValues(state) {
  const preset = String(state?.preset || "").trim();
  if (!preset || preset === "no_filter") {
    return "";
  }
  if (
    DASHBOARD_BUILDER_TIME_PRESETS.some(
      (item) =>
        item.value === preset &&
        !["custom_between", "custom_before", "custom_after", "custom_on"].includes(item.value),
    )
  ) {
    return preset;
  }
  if (preset === "custom_between") {
    return state?.from && state?.to ? `${state.from},${state.to}` : "";
  }
  if (preset === "custom_before") {
    return state?.from ? `before:${state.from}` : "";
  }
  if (preset === "custom_after") {
    return state?.from ? `after:${state.from}` : "";
  }
  if (preset === "custom_on") {
    return state?.from ? `on:${state.from}` : "";
  }
  return "";
}

export function normalizeFilterForField(filter, field, fieldTypesByValue) {
  const operators = getFilterOperators(field, fieldTypesByValue);
  const normalized = String(filter?.operator || "").trim().toLowerCase();
  const valid = operators.find((item) => item.value === normalized);
  return valid?.value || getDefaultFilterOperator(field, fieldTypesByValue);
}

export function toSemanticFilter(filter) {
  const member = String(filter?.member || "").trim();
  if (!member) {
    return null;
  }

  const operator = String(filter?.operator || "equals").trim().toLowerCase();
  if (isValuelessFilter(operator)) {
    return { member, operator };
  }

  const values = splitFilterValues(filter?.values);
  if (values.length === 0) {
    return null;
  }
  return { member, operator, values };
}

export function resolveWidgetTimeRange(widget) {
  const preset = String(widget?.timeRangePreset || "").trim();
  if (!preset) {
    return undefined;
  }
  if (
    DASHBOARD_BUILDER_TIME_PRESETS.some(
      (item) =>
        item.value === preset &&
        !["no_filter", "custom_between", "custom_before", "custom_after", "custom_on"].includes(
          item.value,
        ),
    )
  ) {
    return preset;
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
  return undefined;
}

export function buildDashboardBuilderQueryPayload(board, widget) {
  const measures = normalizeSelectedMembers(widget?.measures, widget?.measure);
  const dimensions = normalizeSelectedMembers(widget?.dimensions, widget?.dimension);
  const filters = [
    ...(Array.isArray(board?.globalFilters) ? board.globalFilters : []),
    ...(Array.isArray(widget?.filters) ? widget.filters : []),
  ]
    .map((item) => toSemanticFilter(item))
    .filter(Boolean);

  const orderPayload =
    Array.isArray(widget?.orderBys) && widget.orderBys.length > 0
      ? widget.orderBys
          .filter((item) => item.member)
          .map((item) => ({ [item.member]: item.direction === "asc" ? "asc" : "desc" }))
      : measures[0]
        ? [{ [measures[0]]: "desc" }]
        : [];

  const timeRange = resolveWidgetTimeRange(widget);
  const timeDimensions =
    widget?.timeDimension && timeRange
      ? [
          {
            dimension: widget.timeDimension,
            granularity: widget.timeGrain || undefined,
            dateRange: timeRange,
          },
        ]
      : widget?.timeDimension
        ? [
            {
              dimension: widget.timeDimension,
              granularity: widget.timeGrain || undefined,
            },
          ]
        : [];

  return {
    semantic_models: [board.selectedModel],
    measures,
    dimensions,
    filters,
    time_dimensions: timeDimensions,
    order: orderPayload,
    limit: Number(widget.limit) > 0 ? Number(widget.limit) : 12,
  };
}

export function enrichDashboardBuilderResult(response) {
  const rows = Array.isArray(response?.data) ? response.data : [];
  return {
    columns: buildColumnsFromRows(rows),
    rows,
    rowCount: rows.length,
    metadata: Array.isArray(response?.metadata) ? response.metadata : [],
    generated_sql: response?.generated_sql || "",
    federation_diagnostics:
      response?.federation_diagnostics && typeof response.federation_diagnostics === "object"
        ? response.federation_diagnostics
        : null,
  };
}

export function applyCopilotSuggestion({
  summary,
  prompt,
  board,
  fields,
}) {
  const parsed = readAgentJson(summary);
  if (!parsed || typeof parsed !== "object") {
    return null;
  }

  const fieldLookup = new Map(fields.map((item) => [item.value, item]));
  const fallbackDimension = fields.find((item) => item.kind === "dimension")?.value || "";
  const fallbackMeasure = fields.find((item) => item.kind === "measure")?.value || "";
  const widgets = Array.isArray(parsed.widgets) ? parsed.widgets : [];

  return {
    name: typeof parsed.name === "string" && parsed.name.trim() ? parsed.name : board.name,
    description:
      typeof parsed.description === "string" && parsed.description.trim()
        ? parsed.description
        : board.description,
    copilotSummary:
      typeof parsed.summary === "string" && parsed.summary.trim()
        ? parsed.summary
        : `Copilot prompt: ${prompt}`,
    widgets:
      widgets.length > 0
        ? widgets.slice(0, 6).map((item, index) => {
            const dimensions = normalizeSelectedMembers(item.dimensions, item.dimension).filter((value) =>
              fieldLookup.has(value),
            );
            const measures = normalizeSelectedMembers(item.measures, item.measure).filter((value) =>
              fieldLookup.has(value),
            );

            return createDashboardBuilderWidget({
              title: item.title || `Widget ${index + 1}`,
              description: item.description || "",
              chartType: item.chart_type || item.chartType || "bar",
              dimensions: dimensions.length > 0 ? dimensions : fallbackDimension ? [fallbackDimension] : [],
              measures: measures.length > 0 ? measures : fallbackMeasure ? [fallbackMeasure] : [],
              size: item.size || "small",
            });
          })
        : board.widgets,
  };
}

function splitFilterValues(rawValues) {
  return String(rawValues || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

function normalizeYearOrMonthRange(value) {
  if (YEAR_PATTERN.test(value)) {
    return [`${value}-01-01`, `${value}-12-31`];
  }
  if (YEAR_MONTH_PATTERN.test(value)) {
    const [yearString, monthString] = value.split("-");
    const year = Number(yearString);
    const month = Number(monthString);
    const lastDay = new Date(Date.UTC(year, month, 0)).getUTCDate();
    return [`${value}-01`, `${value}-${String(lastDay).padStart(2, "0")}`];
  }
  return null;
}

function normalizeChartType(value) {
  const normalized = String(value || "bar").trim().toLowerCase();
  return ["bar", "line", "pie", "table"].includes(normalized) ? normalized : "bar";
}

function normalizeWidgetSize(value) {
  const normalized = String(value || "small").trim().toLowerCase();
  return ["small", "wide", "tall", "large"].includes(normalized)
    ? normalized
    : "small";
}
