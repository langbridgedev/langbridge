import type { SemanticQueryResponse } from '@/orchestration/semanticQuery/types';

export type FieldOption = {
  id: string;
  label: string;
  tableKey?: string;
  type?: string;
  description?: string | null;
  aggregation?: string | null;
  kind: 'dimension' | 'measure' | 'metric' | 'segment';
};

export type TableGroup = {
  tableKey: string;
  schema: string;
  name: string;
  description?: string | null;
  dimensions: FieldOption[];
  measures: FieldOption[];
  segments: FieldOption[];
};

export type ChartType = 'table' | 'bar' | 'line' | 'pie';
export type WidgetSize = 'small' | 'wide' | 'tall' | 'large';
export type WidgetLayout = {
  x: number;
  y: number;
  w: number;
  h: number;
  minW?: number;
  minH?: number;
};

export type FilterDraft = {
  id: string;
  member: string;
  operator: string;
  values: string;
};

export type FilterOperatorOption = {
  value: string;
  label: string;
};

export type OrderByDraft = {
  id: string;
  member: string;
  direction: 'asc' | 'desc';
};

export type PaletteOption = {
  id: string;
  label: string;
  colors: string[];
};

export type PieLabelMode = 'none' | 'name' | 'value' | 'percent';
export type LineCurveMode = 'smooth' | 'linear' | 'step';

export type WidgetVisualConfig = {
  paletteId: string;
  showGrid: boolean;
  showLegend: boolean;
  showDataLabels: boolean;
  lineCurve: LineCurveMode;
  lineStrokeWidth: number;
  barRadius: number;
  pieInnerRadius: number;
  pieLabelMode: PieLabelMode;
};

export type BiWidget = {
  id: string;
  title: string;
  type: ChartType;
  size: WidgetSize;
  layout: WidgetLayout;
  // Data State
  measures: string[];
  dimensions: string[];
  filters: FilterDraft[];
  orderBys: OrderByDraft[];
  limit: number;
  timeDimension: string;
  timeGrain: string;
  timeRangePreset: string;
  timeRangeFrom: string;
  timeRangeTo: string;
  // Visual State
  chartX: string;
  chartY: string;
  visualConfig: WidgetVisualConfig;
  // Execution State
  queryResult: SemanticQueryResponse | null;
  isLoading: boolean;
  jobId?: string | null;
  jobStatus?: string | null;
  progress?: number;
  statusMessage?: string | null;
  error?: string | null;
};

export type PersistedBiWidget = Omit<
  BiWidget,
  'queryResult' | 'isLoading' | 'jobId' | 'jobStatus' | 'progress' | 'statusMessage' | 'error'
>;

export type DashboardBuilderState = {
  name: string;
  description: string;
  refreshMode: 'manual' | 'live';
  semanticModelId: string;
  globalFilters: FilterDraft[];
  widgets: PersistedBiWidget[];
};

export const FILTER_OPERATORS: FilterOperatorOption[] = [
  { value: 'equals', label: 'Equals' },
  { value: 'notequals', label: 'Not equals' },
  { value: 'contains', label: 'Contains' },
  { value: 'gt', label: 'Greater than' },
  { value: 'gte', label: 'Greater or equal' },
  { value: 'lt', label: 'Less than' },
  { value: 'lte', label: 'Less or equal' },
  { value: 'in', label: 'In list' },
  { value: 'notin', label: 'Not in list' },
  { value: 'set', label: 'Is set' },
  { value: 'notset', label: 'Is not set' },
];

export const DATE_FILTER_OPERATORS: FilterOperatorOption[] = [
  { value: 'indaterange', label: 'In date range' },
  { value: 'notindaterange', label: 'Not in date range' },
  { value: 'set', label: 'Is set' },
  { value: 'notset', label: 'Is not set' },
];

export const TIME_GRAIN_OPTIONS = [
  { value: '', label: 'No grain' },
  { value: 'day', label: 'Day' },
  { value: 'week', label: 'Week' },
  { value: 'month', label: 'Month' },
  { value: 'quarter', label: 'Quarter' },
  { value: 'year', label: 'Year' },
];

export const DATE_PRESETS = [
  { value: 'today', label: 'Today' },
  { value: 'yesterday', label: 'Yesterday' },
  { value: 'last_7_days', label: 'Last 7 days' },
  { value: 'last_30_days', label: 'Last 30 days' },
  { value: 'month_to_date', label: 'Month to date' },
  { value: 'year_to_date', label: 'Year to date' },
];

export const PALETTE_OPTIONS: PaletteOption[] = [
  { id: 'emerald', label: 'Emerald', colors: ['#10a37f', '#0ea5e9', '#f59e0b', '#f97316', '#ec4899', '#6366f1'] },
  { id: 'ocean', label: 'Ocean', colors: ['#0369a1', '#0284c7', '#0ea5e9', '#22d3ee', '#14b8a6', '#0f766e'] },
  { id: 'sunset', label: 'Sunset', colors: ['#f97316', '#fb7185', '#f43f5e', '#f59e0b', '#ef4444', '#be123c'] },
  { id: 'slate', label: 'Slate', colors: ['#334155', '#475569', '#64748b', '#94a3b8', '#0f172a', '#1e293b'] },
  { id: 'orchard', label: 'Orchard', colors: ['#65a30d', '#84cc16', '#16a34a', '#22c55e', '#15803d', '#4d7c0f'] },
];

export const DEFAULT_WIDGET_VISUAL_CONFIG: WidgetVisualConfig = {
  paletteId: PALETTE_OPTIONS[0].id,
  showGrid: true,
  showLegend: false,
  showDataLabels: false,
  lineCurve: 'smooth',
  lineStrokeWidth: 2.25,
  barRadius: 6,
  pieInnerRadius: 42,
  pieLabelMode: 'none',
};
