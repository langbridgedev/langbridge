import {
  DATE_PRESETS,
  DATE_FILTER_OPERATORS,
  FILTER_OPERATORS,
} from './types';
import type {
  FieldOption,
  FilterDraft,
  FilterOperatorOption,
} from './types';

const DATE_FIELD_TYPES = new Set(['date', 'datetime', 'timestamp', 'time']);
const VALUELESS_OPERATORS = new Set(['set', 'notset']);
const YEAR_PATTERN = /^\d{4}$/;
const YEAR_MONTH_PATTERN = /^\d{4}-(0[1-9]|1[0-2])$/;
const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const RANGE_OPERATORS = new Set(['indaterange', 'notindaterange']);
const DATE_RANGE_PRESET_VALUES = new Set(DATE_PRESETS.map((preset) => preset.value));

export const DATE_RANGE_FILTER_PRESETS = [
  { value: 'no_filter', label: 'No Filter' },
  ...DATE_PRESETS,
  { value: 'custom_between', label: 'Custom: Between' },
  { value: 'custom_before', label: 'Custom: Before date' },
  { value: 'custom_after', label: 'Custom: After date' },
  { value: 'custom_on', label: 'Custom: On date' },
] as const;

export type DateFilterPreset = (typeof DATE_RANGE_FILTER_PRESETS)[number]['value'];

export type DateRangeFilterState = {
  preset: DateFilterPreset;
  from: string;
  to: string;
};

export function isDateLikeField(field?: FieldOption | null): boolean {
  if (!field || field.kind !== 'dimension') {
    return false;
  }
  const normalizedType = (field.type || '').trim().toLowerCase();
  return DATE_FIELD_TYPES.has(normalizedType);
}

export function getFilterOperatorsForField(field?: FieldOption | null): FilterOperatorOption[] {
  return isDateLikeField(field) ? DATE_FILTER_OPERATORS : FILTER_OPERATORS;
}

export function getDefaultFilterOperator(field?: FieldOption | null): string {
  const operators = getFilterOperatorsForField(field);
  return operators[0]?.value || 'equals';
}

export function ensureOperatorForField(
  operator: string,
  field?: FieldOption | null,
): string {
  const operators = getFilterOperatorsForField(field);
  const normalized = operator.trim().toLowerCase();
  if (operators.some((entry) => entry.value === normalized)) {
    return normalized;
  }
  return getDefaultFilterOperator(field);
}

export function isValuelessOperator(operator: string): boolean {
  return VALUELESS_OPERATORS.has((operator || '').trim().toLowerCase());
}

export function getFilterValuePlaceholder(
  field: FieldOption | null | undefined,
  operator: string,
): string {
  if (!isDateLikeField(field)) {
    return 'Value';
  }
  const normalizedOperator = (operator || '').trim().toLowerCase();
  if (normalizedOperator === 'indaterange' || normalizedOperator === 'notindaterange') {
    return 'YYYY-MM-DD,YYYY-MM-DD or last_30_days';
  }
  if (normalizedOperator === 'beforedate' || normalizedOperator === 'afterdate') {
    return 'YYYY-MM-DD';
  }
  return 'YYYY-MM-DD or YYYY';
}

export function usesDateRangePresetInput(
  field: FieldOption | null | undefined,
  operator: string,
): boolean {
  return isDateLikeField(field) && RANGE_OPERATORS.has((operator || '').trim().toLowerCase());
}

export function parseDateRangeFilterValues(rawValues: string): DateRangeFilterState {
  const trimmed = (rawValues || '').trim();
  if (!trimmed) {
    return { preset: 'no_filter', from: '', to: '' };
  }

  if (DATE_RANGE_PRESET_VALUES.has(trimmed)) {
    return { preset: trimmed as DateFilterPreset, from: '', to: '' };
  }

  if (trimmed.startsWith('before:')) {
    return { preset: 'custom_before', from: trimmed.slice('before:'.length), to: '' };
  }
  if (trimmed.startsWith('after:')) {
    return { preset: 'custom_after', from: trimmed.slice('after:'.length), to: '' };
  }
  if (trimmed.startsWith('on:')) {
    return { preset: 'custom_on', from: trimmed.slice('on:'.length), to: '' };
  }

  const parts = trimmed
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  if (parts.length === 2 && ISO_DATE_PATTERN.test(parts[0]) && ISO_DATE_PATTERN.test(parts[1])) {
    return { preset: 'custom_between', from: parts[0], to: parts[1] };
  }
  if (parts.length === 1 && ISO_DATE_PATTERN.test(parts[0])) {
    return { preset: 'custom_on', from: parts[0], to: '' };
  }

  const range = normalizeYearOrMonthRange(trimmed);
  if (range) {
    return { preset: 'custom_between', from: range[0], to: range[1] };
  }

  return { preset: 'no_filter', from: '', to: '' };
}

export function serializeDateRangeFilterValues(state: DateRangeFilterState): string {
  const preset = state.preset;
  if (preset === 'no_filter') {
    return '';
  }
  if (DATE_RANGE_PRESET_VALUES.has(preset)) {
    return preset;
  }
  if (preset === 'custom_between') {
    return state.from && state.to ? `${state.from},${state.to}` : '';
  }
  if (preset === 'custom_before') {
    return state.from ? `before:${state.from}` : '';
  }
  if (preset === 'custom_after') {
    return state.from ? `after:${state.from}` : '';
  }
  if (preset === 'custom_on') {
    return state.from ? `on:${state.from}` : '';
  }
  return '';
}

type SemanticFilterItem = {
  member: string;
  operator: string;
  values?: string[];
};

export function toSemanticFilter(
  filter: FilterDraft,
  field?: FieldOption | null,
): SemanticFilterItem | null {
  const member = filter.member.trim();
  if (!member) {
    return null;
  }
  const operator = ensureOperatorForField(filter.operator || 'equals', field);
  if (isValuelessOperator(operator)) {
    return { member, operator };
  }
  const values = splitFilterValues(filter.values);
  if (values.length === 0) {
    return null;
  }
  if (!isDateLikeField(field)) {
    return { member, operator, values };
  }
  return normalizeDateFilter(member, operator, values);
}

function splitFilterValues(rawValues: string): string[] {
  return rawValues
    .split(',')
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function normalizeDateFilter(member: string, operator: string, values: string[]): SemanticFilterItem {
  if (values.length === 1) {
    const mapped = normalizeSingleDateValue(operator, values[0]);
    if (mapped) {
      return { member, operator: mapped.operator, values: mapped.values };
    }
  }
  return { member, operator, values };
}

function normalizeSingleDateValue(
  operator: string,
  value: string,
): { operator: string; values: string[] } | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  if (operator === 'equals' || operator === 'notequals') {
    const range = normalizeYearOrMonthRange(trimmed);
    if (range) {
      return {
        operator: operator === 'equals' ? 'indaterange' : 'notindaterange',
        values: range,
      };
    }
    if (ISO_DATE_PATTERN.test(trimmed)) {
      return {
        operator: operator === 'equals' ? 'indaterange' : 'notindaterange',
        values: [`on:${trimmed}`],
      };
    }
  }

  if (operator === 'indaterange' || operator === 'notindaterange') {
    const range = normalizeYearOrMonthRange(trimmed);
    if (range) {
      return { operator, values: range };
    }
    if (ISO_DATE_PATTERN.test(trimmed)) {
      return { operator, values: [`on:${trimmed}`] };
    }
  }

  return null;
}

function normalizeYearOrMonthRange(value: string): string[] | null {
  if (YEAR_PATTERN.test(value)) {
    return [`${value}-01-01`, `${value}-12-31`];
  }
  if (YEAR_MONTH_PATTERN.test(value)) {
    const [yearString, monthString] = value.split('-');
    const year = Number(yearString);
    const month = Number(monthString);
    const lastDay = new Date(Date.UTC(year, month, 0)).getUTCDate();
    return [`${value}-01`, `${value}-${String(lastDay).padStart(2, '0')}`];
  }
  return null;
}
