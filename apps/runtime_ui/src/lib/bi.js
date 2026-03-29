import {
  BI_STUDIO_STORAGE_KEY,
  buildColumnsFromRows,
  createLocalId,
  readAgentJson,
} from "./runtimeUi";

export const BI_PALETTES = [
  { id: "emerald", label: "Emerald", colors: ["#10a37f", "#0f8f6c", "#0ea5e9", "#f59e0b"] },
  { id: "ocean", label: "Ocean", colors: ["#0369a1", "#0ea5e9", "#14b8a6", "#7dd3fc"] },
  { id: "sunset", label: "Sunset", colors: ["#f97316", "#fb7185", "#f43f5e", "#f59e0b"] },
  { id: "slate", label: "Slate", colors: ["#334155", "#475569", "#64748b", "#94a3b8"] },
  { id: "orchard", label: "Orchard", colors: ["#65a30d", "#84cc16", "#22c55e", "#166534"] },
];

export const BI_FILTER_OPERATORS = [
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

export const BI_DATE_FILTER_OPERATORS = [
  { value: "indaterange", label: "In date range" },
  { value: "notindaterange", label: "Not in date range" },
  { value: "set", label: "Is set" },
  { value: "notset", label: "Is not set" },
];

export const BI_TIME_GRAINS = [
  { value: "", label: "No grain" },
  { value: "day", label: "Day" },
  { value: "week", label: "Week" },
  { value: "month", label: "Month" },
  { value: "quarter", label: "Quarter" },
  { value: "year", label: "Year" },
];

export const BI_TIME_PRESETS = [
  { value: "today", label: "Today" },
  { value: "yesterday", label: "Yesterday" },
  { value: "last_7_days", label: "Last 7 days" },
  { value: "last_30_days", label: "Last 30 days" },
  { value: "month_to_date", label: "Month to date" },
  { value: "year_to_date", label: "Year to date" },
];

const DATE_LIKE_TYPES = new Set(["date", "datetime", "timestamp", "time"]);
const VALUELESS_OPERATORS = new Set(["set", "notset"]);

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

export function getBiPalette(paletteId) {
  return BI_PALETTES.find((item) => item.id === paletteId) || BI_PALETTES[0];
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

export function createBiWidget(seed = {}) {
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
      paletteId: seed.visualConfig?.paletteId || BI_PALETTES[0].id,
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

export function createBiBoard(seed = {}) {
  const widgets =
    Array.isArray(seed.widgets) && seed.widgets.length > 0
      ? seed.widgets.map((item) => createBiWidget(item))
      : [createBiWidget()];
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

export function loadBiStudioState(readStoredJson) {
  const stored = readStoredJson(BI_STUDIO_STORAGE_KEY, null);
  const boards = Array.isArray(stored?.boards)
    ? stored.boards.map((item) => createBiBoard(item))
    : [];
  const normalizedBoards = boards.length > 0 ? boards : [createBiBoard()];
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
    ? BI_DATE_FILTER_OPERATORS
    : BI_FILTER_OPERATORS;
}

export function getDefaultFilterOperator(field, fieldTypesByValue) {
  return getFilterOperators(field, fieldTypesByValue)[0]?.value || "equals";
}

export function isValuelessFilter(operator) {
  return VALUELESS_OPERATORS.has(String(operator || "").trim().toLowerCase());
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
  if (BI_TIME_PRESETS.some((item) => item.value === preset)) {
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

export function buildBiQueryPayload(board, widget) {
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

export function enrichBiResult(response) {
  const rows = Array.isArray(response?.data) ? response.data : [];
  return {
    columns: buildColumnsFromRows(rows),
    rows,
    rowCount: rows.length,
    metadata: Array.isArray(response?.metadata) ? response.metadata : [],
    generated_sql: response?.generated_sql || "",
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

            return createBiWidget({
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
