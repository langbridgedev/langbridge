import { useMemo } from 'react';
import { Filter, Plus, RotateCcw, Sparkles, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';

import type { BiWidget, FieldOption, FilterDraft } from '../types';
import { FieldSelect } from './FieldSelect';
import {
  DATE_RANGE_FILTER_PRESETS,
  ensureOperatorForField,
  getDefaultFilterOperator,
  getFilterOperatorsForField,
  getFilterValuePlaceholder,
  isValuelessOperator,
  parseDateRangeFilterValues,
  serializeDateRangeFilterValues,
  usesDateRangePresetInput,
} from '../filterUtils';

type FilterBarProps = {
  fields: FieldOption[];
  globalFilters: FilterDraft[];
  onGlobalFiltersChange: (filters: FilterDraft[]) => void;
  onApplyFilters: () => void;
  activeWidget: BiWidget | null;
  isEditMode: boolean;
};

export function FilterBar({
  fields,
  globalFilters,
  onGlobalFiltersChange,
  onApplyFilters,
  activeWidget,
  isEditMode,
}: FilterBarProps) {
  const activeLabel = useMemo(() => activeWidget?.title || 'No widget selected', [activeWidget]);
  const fieldById = useMemo(() => {
    const lookup = new Map<string, FieldOption>();
    fields.forEach((field) => lookup.set(field.id, field));
    return lookup;
  }, [fields]);

  const addFilter = () => {
    const defaultField = fields[0];
    onGlobalFiltersChange([
      ...globalFilters,
      {
        id: makeLocalId(),
        member: defaultField?.id || '',
        operator: getDefaultFilterOperator(defaultField),
        values: '',
      },
    ]);
  };

  const updateFilter = (id: string, updates: Partial<FilterDraft>) => {
    onGlobalFiltersChange(globalFilters.map((filter) => (filter.id === id ? { ...filter, ...updates } : filter)));
  };

  const removeFilter = (id: string) => {
    onGlobalFiltersChange(globalFilters.filter((filter) => filter.id !== id));
  };

  const clearAll = () => onGlobalFiltersChange([]);

  return (
    <section className="border-b border-[color:var(--panel-border)] px-5 py-3 lg:px-6">
      <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-2.5 shadow-[0_8px_18px_-18px_rgba(15,23,42,0.55)]">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex min-w-0 items-center gap-2">
            <span className="inline-flex h-7 w-7 items-center justify-center rounded-lg border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] text-[color:var(--text-muted)]">
              <Filter className="h-3.5 w-3.5" />
            </span>
            <div className="min-w-0">
              <p className="truncate text-xs font-semibold uppercase tracking-[0.16em] text-[color:var(--text-secondary)]">
                Dashboard Filters
              </p>
              <p className="truncate text-[11px] text-[color:var(--text-muted)]">
                Active widget: {activeLabel}
                {activeWidget ? ` • ${activeWidget.filters.length} local override(s)` : ''}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" className="h-7 gap-1 px-2 text-[11px]" onClick={addFilter} disabled={!isEditMode}>
              <Plus className="h-3 w-3" /> Add filter
            </Button>
            <Button variant="outline" size="sm" className="h-7 gap-1 px-2 text-[11px]" onClick={clearAll} disabled={globalFilters.length === 0}>
              <RotateCcw className="h-3 w-3" /> Reset
            </Button>
            <Button size="sm" className="h-7 gap-1 rounded-full px-3 text-[11px]" onClick={onApplyFilters}>
              <Sparkles className="h-3 w-3" /> Apply
            </Button>
          </div>
        </div>

        {globalFilters.length > 0 ? (
          <div className="mt-3 grid gap-2 lg:grid-cols-2 2xl:grid-cols-3">
            {globalFilters.map((filter) => {
              const selectedField = fieldById.get(filter.member);
              const operatorOptions = getFilterOperatorsForField(selectedField);
              const normalizedOperator = ensureOperatorForField(filter.operator, selectedField);
              const requiresValue = !isValuelessOperator(normalizedOperator);
              const usesDateRangeInput = usesDateRangePresetInput(selectedField, normalizedOperator);
              const dateRangeState = parseDateRangeFilterValues(filter.values);

              return (
                <div key={filter.id} className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-2 py-2">
                  <div className="flex items-center gap-2">
                    <div className="flex-1">
                      <FieldSelect
                        fields={fields}
                        value={filter.member}
                        onChange={(value) => {
                          const nextField = fieldById.get(value);
                          updateFilter(filter.id, {
                            member: value,
                            operator: ensureOperatorForField(filter.operator, nextField),
                          });
                        }}
                        className={`h-7 rounded-lg border-0 bg-transparent px-2 text-xs shadow-none ${
                          !isEditMode ? 'pointer-events-none opacity-70' : ''
                        }`}
                        inputClassName="text-xs"
                      />
                    </div>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-[color:var(--text-muted)] hover:text-red-500"
                      onClick={() => removeFilter(filter.id)}
                      aria-label="Remove global filter"
                      disabled={!isEditMode}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                  <div className="mt-2 space-y-2">
                    <Select
                      value={normalizedOperator}
                      onChange={(event) => updateFilter(filter.id, { operator: ensureOperatorForField(event.target.value, selectedField) })}
                      className="h-7 rounded-lg border-0 bg-[color:var(--panel-bg)] px-2 text-xs"
                      disabled={!isEditMode}
                    >
                      {operatorOptions.map((operator) => (
                        <option key={operator.value} value={operator.value}>
                          {operator.label}
                        </option>
                      ))}
                    </Select>
                    {usesDateRangeInput ? (
                      <>
                        <Select
                          value={dateRangeState.preset}
                          onChange={(event) =>
                            updateFilter(filter.id, {
                              values: serializeDateRangeFilterValues({
                                ...dateRangeState,
                                preset: event.target.value as typeof dateRangeState.preset,
                              }),
                            })
                          }
                          className="h-7 rounded-lg border-0 bg-[color:var(--panel-bg)] px-2 text-xs"
                          disabled={!isEditMode}
                        >
                          {DATE_RANGE_FILTER_PRESETS.map((option) => (
                            <option key={option.value} value={option.value}>
                              {option.label}
                            </option>
                          ))}
                        </Select>
                        {dateRangeState.preset === 'custom_between' ? (
                          <div className="grid grid-cols-2 gap-2">
                            <Input
                              type="date"
                              value={dateRangeState.from}
                              onChange={(event) =>
                                updateFilter(filter.id, {
                                  values: serializeDateRangeFilterValues({
                                    ...dateRangeState,
                                    from: event.target.value,
                                  }),
                                })
                              }
                              className="h-7 rounded-lg border-0 bg-[color:var(--panel-bg)] text-xs"
                              disabled={!isEditMode}
                            />
                            <Input
                              type="date"
                              value={dateRangeState.to}
                              onChange={(event) =>
                                updateFilter(filter.id, {
                                  values: serializeDateRangeFilterValues({
                                    ...dateRangeState,
                                    to: event.target.value,
                                  }),
                                })
                              }
                              className="h-7 rounded-lg border-0 bg-[color:var(--panel-bg)] text-xs"
                              disabled={!isEditMode}
                            />
                          </div>
                        ) : null}
                        {(dateRangeState.preset === 'custom_before' || dateRangeState.preset === 'custom_after' || dateRangeState.preset === 'custom_on') ? (
                          <Input
                            type="date"
                            value={dateRangeState.from}
                            onChange={(event) =>
                              updateFilter(filter.id, {
                                values: serializeDateRangeFilterValues({
                                  ...dateRangeState,
                                  from: event.target.value,
                                }),
                              })
                            }
                            className="h-7 rounded-lg border-0 bg-[color:var(--panel-bg)] text-xs"
                            disabled={!isEditMode}
                          />
                        ) : null}
                      </>
                    ) : (
                      <Input
                        value={filter.values}
                        onChange={(event) => updateFilter(filter.id, { values: event.target.value })}
                        placeholder={getFilterValuePlaceholder(selectedField, normalizedOperator)}
                        className="h-7 rounded-lg border-0 bg-[color:var(--panel-bg)] text-xs"
                        disabled={!isEditMode || !requiresValue}
                      />
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="mt-3 rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)]/40 px-3 py-2 text-[11px] text-[color:var(--text-muted)]">
            No global filters configured. Add filters here to drive all widgets consistently.
          </div>
        )}
      </div>
    </section>
  );
}

function makeLocalId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return Math.random().toString(36).slice(2, 11);
}
