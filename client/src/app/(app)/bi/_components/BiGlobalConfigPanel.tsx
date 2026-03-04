import { X, Plus, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import type { FieldOption, FilterDraft } from '../types';
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

interface BiGlobalConfigPanelProps {
  onClose: () => void;
  dashboardName: string;
  setDashboardName: (name: string) => void;
  dashboardDescription: string;
  setDashboardDescription: (description: string) => void;
  refreshMode: 'manual' | 'live';
  setRefreshMode: (mode: 'manual' | 'live') => void;
  lastRefreshedAt: string | null;
  fields: FieldOption[];
  globalFilters: FilterDraft[];
  setGlobalFilters: (filters: FilterDraft[]) => void;
  onApplyGlobalFilters: () => void;
}

export function BiGlobalConfigPanel({
  onClose,
  dashboardName,
  setDashboardName,
  dashboardDescription,
  setDashboardDescription,
  refreshMode,
  setRefreshMode,
  lastRefreshedAt,
  fields,
  globalFilters,
  setGlobalFilters,
  onApplyGlobalFilters,
}: BiGlobalConfigPanelProps) {
  const getFilterField = (member: string) => fields.find((field) => field.id === member);

  const handleAddGlobalFilter = () => {
    const defaultField = fields[0];
    setGlobalFilters([
      ...globalFilters,
      {
        id: Math.random().toString(36).substr(2, 9),
        member: defaultField?.id || '',
        operator: getDefaultFilterOperator(defaultField),
        values: '',
      },
    ]);
  };

  const handleUpdateGlobalFilter = (id: string, updates: Partial<FilterDraft>) => {
    setGlobalFilters(globalFilters.map(f => (f.id === id ? { ...f, ...updates } : f)));
  };

  const handleRemoveGlobalFilter = (id: string) => {
    setGlobalFilters(globalFilters.filter(f => f.id !== id));
  };

  return (
    <div className="flex flex-col h-full bg-background w-full">
      <div className="p-4 border-b border-[var(--border-light)] flex items-center justify-between bg-muted/10">
        <div className="flex-1 mr-4">
          <Input
            value={dashboardName}
            onChange={(e) => setDashboardName(e.target.value)}
            className="h-8 font-semibold bg-transparent border-transparent hover:border-border focus:border-primary px-2 transition-colors"
            placeholder="Dashboard name"
          />
        </div>
        <Button variant="ghost" size="icon" onClick={onClose} className="h-7 w-7 text-muted-foreground">
          <X className="h-4 w-4" />
        </Button>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-6 custom-scrollbar">
        <section className="space-y-3">
          <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Description</Label>
          <Textarea
            value={dashboardDescription}
            onChange={(event) => setDashboardDescription(event.target.value)}
            className="min-h-20 text-xs"
            placeholder="Summarize what this dashboard tracks."
          />
        </section>
        <section className="space-y-3">
          <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Refresh Mode
          </Label>
          <Select
            value={refreshMode}
            onChange={(event) => setRefreshMode(event.target.value === 'live' ? 'live' : 'manual')}
            className="h-8 text-xs"
          >
            <option value="manual">Manual refresh</option>
            <option value="live">Live on load</option>
          </Select>
          <p className="text-xs text-muted-foreground">
            {refreshMode === 'live'
              ? 'Dashboard refreshes automatically when opened.'
              : 'Dashboard keeps cached data until you manually refresh.'}
          </p>
          <p className="text-xs text-muted-foreground">
            Last refreshed: {lastRefreshedAt ? new Date(lastRefreshedAt).toLocaleString() : 'Never'}
          </p>
        </section>
        <section className="space-y-3">
          <div className="flex items-center justify-between">
            <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Global Filters</Label>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={handleAddGlobalFilter} className="h-6 gap-1 px-2 text-xs">
                <Plus className="h-3 w-3" /> Add
              </Button>
              <Button variant="outline" size="sm" onClick={onApplyGlobalFilters} className="h-6 px-2 text-xs">
                Apply
              </Button>
            </div>
          </div>
          <p className="text-xs text-muted-foreground">Merged into every chart query and run against all widgets.</p>
          {globalFilters.length === 0 && (
            <div className="text-xs text-muted-foreground italic">No global filters applied</div>
          )}
          <div className="space-y-2">
            {globalFilters.map(filter => (
              (() => {
                const selectedField = getFilterField(filter.member);
                const operatorOptions = getFilterOperatorsForField(selectedField);
                const normalizedOperator = ensureOperatorForField(filter.operator, selectedField);
                const requiresValue = !isValuelessOperator(normalizedOperator);
                const usesDateRangeInput = usesDateRangePresetInput(selectedField, normalizedOperator);
                const dateRangeState = parseDateRangeFilterValues(filter.values);
                return (
                  <div key={filter.id} className="bg-muted/30 p-2 rounded-lg space-y-2 border border-border">
                    <div className="flex items-center gap-2">
                      <FieldSelect
                        value={filter.member}
                        onChange={(value) => {
                          const nextField = getFilterField(value);
                          handleUpdateGlobalFilter(filter.id, {
                            member: value,
                            operator: ensureOperatorForField(filter.operator, nextField),
                          });
                        }}
                        fields={fields}
                        className="h-7 text-xs"
                        inputClassName="text-xs"
                      />
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => handleRemoveGlobalFilter(filter.id)}
                        className="h-7 w-7 text-destructive hover:bg-destructive/10"
                      >
                        <Trash2 className="h-3 w-3" />
                      </Button>
                    </div>
                    <div className="space-y-2">
                      <Select
                        value={normalizedOperator}
                        onChange={(e) => handleUpdateGlobalFilter(filter.id, { operator: ensureOperatorForField(e.target.value, selectedField) })}
                        className="h-7 text-xs"
                      >
                        {operatorOptions.map((op) => (
                          <option key={op.value} value={op.value}>{op.label}</option>
                        ))}
                      </Select>
                      {usesDateRangeInput ? (
                        <>
                          <Select
                            value={dateRangeState.preset}
                            onChange={(e) =>
                              handleUpdateGlobalFilter(filter.id, {
                                values: serializeDateRangeFilterValues({
                                  ...dateRangeState,
                                  preset: e.target.value as typeof dateRangeState.preset,
                                }),
                              })
                            }
                            className="h-7 text-xs"
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
                                onChange={(e) =>
                                  handleUpdateGlobalFilter(filter.id, {
                                    values: serializeDateRangeFilterValues({
                                      ...dateRangeState,
                                      from: e.target.value,
                                    }),
                                  })
                                }
                                className="h-7 text-xs"
                              />
                              <Input
                                type="date"
                                value={dateRangeState.to}
                                onChange={(e) =>
                                  handleUpdateGlobalFilter(filter.id, {
                                    values: serializeDateRangeFilterValues({
                                      ...dateRangeState,
                                      to: e.target.value,
                                    }),
                                  })
                                }
                                className="h-7 text-xs"
                              />
                            </div>
                          ) : null}
                          {(dateRangeState.preset === 'custom_before' || dateRangeState.preset === 'custom_after' || dateRangeState.preset === 'custom_on') ? (
                            <Input
                              type="date"
                              value={dateRangeState.from}
                              onChange={(e) =>
                                handleUpdateGlobalFilter(filter.id, {
                                  values: serializeDateRangeFilterValues({
                                    ...dateRangeState,
                                    from: e.target.value,
                                  }),
                                })
                              }
                              className="h-7 text-xs"
                            />
                          ) : null}
                        </>
                      ) : (
                        <Input
                          value={filter.values}
                          onChange={(e) => handleUpdateGlobalFilter(filter.id, { values: e.target.value })}
                          className="h-7 text-xs"
                          placeholder={getFilterValuePlaceholder(selectedField, normalizedOperator)}
                          disabled={!requiresValue}
                        />
                      )}
                    </div>
                  </div>
                );
              })()
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}
