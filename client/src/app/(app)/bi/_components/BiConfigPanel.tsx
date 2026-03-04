import { X, Plus, Trash2, Type, Hash } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Select } from '@/components/ui/select';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';
import { FieldSelect } from './FieldSelect';
import { 
  ChartType,
  WidgetSize,
  WidgetVisualConfig,
  FilterDraft, 
  OrderByDraft, 
  TIME_GRAIN_OPTIONS, 
  DATE_PRESETS,
  FieldOption,
  PALETTE_OPTIONS,
} from '../types';
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

interface BiConfigPanelProps {
  onClose: () => void;
  // Widget Meta
  title: string;
  setTitle: (title: string) => void;
  // Visual Props
  chartX: string;
  setChartX: (x: string) => void;
  chartY: string;
  setChartY: (y: string) => void;
  chartType: ChartType;
  setChartType: (type: ChartType) => void;
  widgetSize: WidgetSize;
  setWidgetSize: (size: WidgetSize) => void;
  visualConfig: WidgetVisualConfig;
  setVisualConfig: (config: WidgetVisualConfig) => void;
  // Data Props
  fields: FieldOption[];
  selectedDimensions: string[];
  selectedMeasures: string[];
  onRemoveField: (id: string, kind: 'dimension' | 'measure') => void;
  filters: FilterDraft[];
  setFilters: (filters: FilterDraft[]) => void;
  orderBys: OrderByDraft[];
  setOrderBys: (orders: OrderByDraft[]) => void;
  limit: number;
  setLimit: (limit: number) => void;
  timeDimension: string;
  setTimeDimension: (id: string) => void;
  timeGrain: string;
  setTimeGrain: (grain: string) => void;
  timeRangePreset: string;
  setTimeRangePreset: (preset: string) => void;
  timeRangeFrom: string;
  setTimeRangeFrom: (value: string) => void;
  timeRangeTo: string;
  setTimeRangeTo: (value: string) => void;
  onExportCsv: () => void;
  onShowSql: () => void;
}

export function BiConfigPanel({
  onClose,
  title,
  setTitle,
  chartX,
  setChartX,
  chartY,
  setChartY,
  chartType,
  setChartType,
  widgetSize,
  setWidgetSize,
  visualConfig,
  setVisualConfig,
  fields,
  selectedDimensions,
  selectedMeasures,
  onRemoveField,
  filters,
  setFilters,
  orderBys,
  setOrderBys,
  limit,
  setLimit,
  timeDimension,
  setTimeDimension,
  timeGrain,
  setTimeGrain,
  timeRangePreset,
  setTimeRangePreset,
  timeRangeFrom,
  setTimeRangeFrom,
  timeRangeTo,
  setTimeRangeTo,
  onExportCsv,
  onShowSql
}: BiConfigPanelProps) {
  const timeFields = fields.filter((field) => {
    if (field.kind !== 'dimension') {
      return false;
    }
    const normalizedType = (field.type || '').toLowerCase();
    return normalizedType === 'date' || normalizedType === 'datetime' || normalizedType === 'timestamp' || normalizedType === 'time';
  });
  const activeTimeRangePreset = timeRangePreset || 'no_filter';
  
  // Field lookup for labels
  const getFieldLabel = (id: string) => fields.find(f => f.id === id)?.label || id;
  const getFilterField = (member: string) => fields.find((field) => field.id === member);

  const handleAddFilter = () => {
    const defaultField = fields[0];
    setFilters([...filters, { 
      id: Math.random().toString(36).substr(2, 9), 
      member: defaultField?.id || '', 
      operator: getDefaultFilterOperator(defaultField), 
      values: '' 
    }]);
  };

  const handleUpdateFilter = (id: string, updates: Partial<FilterDraft>) => {
    setFilters(filters.map(f => f.id === id ? { ...f, ...updates } : f));
  };

  const handleRemoveFilter = (id: string) => {
    setFilters(filters.filter(f => f.id !== id));
  };


  const handleAddOrder = () => {
    setOrderBys([...orderBys, { 
      id: Math.random().toString(36).substr(2, 9), 
      member: fields[0]?.id || '', 
      direction: 'desc' 
    }]);
  };

  const handleUpdateOrder = (id: string, updates: Partial<OrderByDraft>) => {
    setOrderBys(orderBys.map(o => o.id === id ? { ...o, ...updates } : o));
  };

  const handleRemoveOrder = (id: string) => {
    setOrderBys(orderBys.filter(o => o.id !== id));
  };

  const updateVisualConfig = (updates: Partial<WidgetVisualConfig>) => {
    setVisualConfig({ ...visualConfig, ...updates });
  };

  return (
    <div className="flex flex-col h-full bg-background w-full">
      <div className="p-4 border-b border-[var(--border-light)] flex items-center justify-between bg-muted/10">
        <div className="flex-1 mr-4">
          <Input 
            value={title} 
            onChange={(e) => setTitle(e.target.value)} 
            className="h-8 font-semibold bg-transparent border-transparent hover:border-border focus:border-primary px-2 transition-colors"
            placeholder="Widget Title"
          />
        </div>
        <Button variant="ghost" size="icon" onClick={onClose} className="h-7 w-7 text-muted-foreground">
          <X className="h-4 w-4" />
        </Button>
      </div>

      <Tabs defaultValue="data" className="flex-1 flex flex-col overflow-hidden">
        <div className="px-4 pt-4">
          <TabsList className="w-full grid grid-cols-2">
            <TabsTrigger value="data">Data & Logic</TabsTrigger>
            <TabsTrigger value="visual">Visuals</TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="visual" className="flex-1 overflow-y-auto p-6 space-y-8 custom-scrollbar">
          <section>
            <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-4 block">Chart Type</label>
            <Select value={chartType} onChange={(e) => setChartType(e.target.value as ChartType)}>
              <option value="bar">Bar</option>
              <option value="line">Line</option>
              <option value="pie">Pie</option>
              <option value="table">Table</option>
            </Select>
          </section>

          <section>
            <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-4 block">Widget Size</label>
            <Select value={widgetSize} onChange={(e) => setWidgetSize(e.target.value as WidgetSize)}>
              <option value="small">Small</option>
              <option value="wide">Wide</option>
              <option value="tall">Tall</option>
              <option value="large">Large</option>
            </Select>
          </section>

          <section>
            <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-4 block">Field Mapping</label>
            {chartType === 'table' ? (
              <p className="text-xs text-muted-foreground">
                Tables show all returned columns. Use query settings to control columns and order.
              </p>
            ) : (
              <div className="space-y-4">
                <div>
                  <span className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">X Field</span>
                  <Select 
                    value={chartX} 
                    onChange={(e) => setChartX(e.target.value)}
                    placeholder="Select X Axis"
                  >
                    <option value="">Auto</option>
                    {selectedDimensions.map(id => (
                      <option key={id} value={id}>{getFieldLabel(id)}</option>
                    ))}
                  </Select>
                </div>
                <div>
                  <span className="text-[11px] font-semibold text-muted-foreground mb-1.5 block">Y Field</span>
                  <Select 
                    value={chartY} 
                    onChange={(e) => setChartY(e.target.value)}
                    placeholder="Select Y Axis"
                  >
                     <option value="">Auto</option>
                     {selectedMeasures.map(id => (
                      <option key={id} value={id}>{getFieldLabel(id)}</option>
                    ))}
                  </Select>
                </div>
              </div>
            )}
          </section>

          <section>
            <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-4 block">Palette</label>
            <div className="grid grid-cols-2 gap-2">
              {PALETTE_OPTIONS.map((palette) => (
                <button
                  key={palette.id}
                  type="button"
                  className={cn(
                    'flex items-center gap-2 rounded-lg border px-2 py-2 text-left transition',
                    visualConfig.paletteId === palette.id
                      ? 'border-primary bg-primary/5 ring-1 ring-primary/40'
                      : 'border-border hover:border-primary/50',
                  )}
                  onClick={() => updateVisualConfig({ paletteId: palette.id })}
                >
                  <span className="flex items-center gap-1">
                    {palette.colors.slice(0, 4).map((color) => (
                      <span
                        key={`${palette.id}-${color}`}
                        className="h-3 w-3 rounded-full border border-white/40"
                        style={{ backgroundColor: color }}
                      />
                    ))}
                  </span>
                  <span className="text-[11px] font-medium text-muted-foreground">{palette.label}</span>
                </button>
              ))}
            </div>
          </section>

          <section className="space-y-3">
            <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-2 block">Display</label>
            <label className="flex items-center justify-between rounded-lg border border-border px-3 py-2 text-xs">
              <span>Show grid</span>
              <input
                type="checkbox"
                checked={visualConfig.showGrid}
                onChange={(event) => updateVisualConfig({ showGrid: event.target.checked })}
              />
            </label>
            <label className="flex items-center justify-between rounded-lg border border-border px-3 py-2 text-xs">
              <span>Show legend</span>
              <input
                type="checkbox"
                checked={visualConfig.showLegend}
                onChange={(event) => updateVisualConfig({ showLegend: event.target.checked })}
              />
            </label>
            <label className="flex items-center justify-between rounded-lg border border-border px-3 py-2 text-xs">
              <span>Show value labels</span>
              <input
                type="checkbox"
                checked={visualConfig.showDataLabels}
                onChange={(event) => updateVisualConfig({ showDataLabels: event.target.checked })}
              />
            </label>
          </section>

          {chartType === 'line' ? (
            <section className="space-y-3">
              <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-2 block">Line Style</label>
              <Select
                value={visualConfig.lineCurve}
                onChange={(event) =>
                  updateVisualConfig({ lineCurve: event.target.value as WidgetVisualConfig['lineCurve'] })
                }
              >
                <option value="smooth">Smooth</option>
                <option value="linear">Linear</option>
                <option value="step">Step</option>
              </Select>
              <div>
                <div className="mb-1 flex items-center justify-between text-[11px] text-muted-foreground">
                  <span>Stroke width</span>
                  <span>{visualConfig.lineStrokeWidth.toFixed(1)}</span>
                </div>
                <Input
                  type="range"
                  min={1}
                  max={6}
                  step={0.5}
                  value={visualConfig.lineStrokeWidth}
                  onChange={(event) => updateVisualConfig({ lineStrokeWidth: Number(event.target.value) })}
                />
              </div>
            </section>
          ) : null}

          {chartType === 'bar' ? (
            <section className="space-y-3">
              <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-2 block">Bar Style</label>
              <div>
                <div className="mb-1 flex items-center justify-between text-[11px] text-muted-foreground">
                  <span>Corner radius</span>
                  <span>{Math.round(visualConfig.barRadius)}px</span>
                </div>
                <Input
                  type="range"
                  min={0}
                  max={14}
                  step={1}
                  value={visualConfig.barRadius}
                  onChange={(event) => updateVisualConfig({ barRadius: Number(event.target.value) })}
                />
              </div>
            </section>
          ) : null}

          {chartType === 'pie' ? (
            <section className="space-y-3">
              <label className="text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground mb-2 block">Pie Style</label>
              <div>
                <div className="mb-1 flex items-center justify-between text-[11px] text-muted-foreground">
                  <span>Donut hole</span>
                  <span>{Math.round(visualConfig.pieInnerRadius)}%</span>
                </div>
                <Input
                  type="range"
                  min={0}
                  max={70}
                  step={1}
                  value={visualConfig.pieInnerRadius}
                  onChange={(event) => updateVisualConfig({ pieInnerRadius: Number(event.target.value) })}
                />
              </div>
              <Select
                value={visualConfig.pieLabelMode}
                onChange={(event) =>
                  updateVisualConfig({ pieLabelMode: event.target.value as WidgetVisualConfig['pieLabelMode'] })
                }
              >
                <option value="none">No slice labels</option>
                <option value="name">Slice name</option>
                <option value="value">Slice value</option>
                <option value="percent">Slice percent</option>
              </Select>
            </section>
          ) : null}
        </TabsContent>

        <TabsContent value="data" className="flex-1 overflow-y-auto p-4 space-y-6 custom-scrollbar">
          
          {/* Dimensions & Measures */}
          <section className="space-y-4">
             <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
                   <Type className="h-3 w-3" /> Dimensions
                </Label>
                {selectedDimensions.length === 0 && <div className="text-xs text-muted-foreground italic px-2">No dimensions selected</div>}
                <div className="space-y-1">
                   {selectedDimensions.map(id => (
                      <div key={id} className="flex items-center justify-between px-2 py-1.5 bg-muted/30 rounded-md text-xs border border-border group">
                         <span className="truncate">{getFieldLabel(id)}</span>
                         <button onClick={() => onRemoveField(id, 'dimension')} className="text-muted-foreground hover:text-destructive opacity-0 group-hover:opacity-100 transition-opacity">
                            <X className="h-3 w-3" />
                         </button>
                      </div>
                   ))}
                </div>
             </div>

             <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
                   <Hash className="h-3 w-3" /> Measures
                </Label>
                {selectedMeasures.length === 0 && <div className="text-xs text-muted-foreground italic px-2">No measures selected</div>}
                <div className="space-y-1">
                   {selectedMeasures.map(id => (
                      <div key={id} className="flex items-center justify-between px-2 py-1.5 bg-muted/30 rounded-md text-xs border border-border group">
                         <span className="truncate">{getFieldLabel(id)}</span>
                         <button onClick={() => onRemoveField(id, 'measure')} className="text-muted-foreground hover:text-destructive opacity-0 group-hover:opacity-100 transition-opacity">
                            <X className="h-3 w-3" />
                         </button>
                      </div>
                   ))}
                </div>
             </div>
          </section>

          <div className="h-[1px] bg-border my-2"></div>

          {/* Time Dimension */}
          <section className="space-y-3">
            <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Time Dimension</Label>
            <FieldSelect
              value={timeDimension}
              onChange={setTimeDimension}
              fields={timeFields}
              allowEmpty
              emptyLabel="None"
              placeholder="Select time field"
              className="h-9 text-xs"
              inputClassName="text-xs"
            />
            {timeDimension && (
              <div className="grid grid-cols-2 gap-2">
                 <Select 
                    value={timeGrain} 
                    onChange={(e) => setTimeGrain(e.target.value)}
                  >
                    {TIME_GRAIN_OPTIONS.map(o => (
                      <option key={o.value} value={o.value}>{o.label}</option>
                    ))}
                  </Select>
                  <Select 
                    value={activeTimeRangePreset} 
                    onChange={(e) => setTimeRangePreset(e.target.value)}
                  >
                    <option value="no_filter">No Filter</option>
                    {DATE_PRESETS.map(o => (
                      <option key={o.value} value={o.value}>{o.label}</option>
                    ))}
                    <option value="custom_between">Custom: Between</option>
                    <option value="custom_before">Custom: Before date</option>
                    <option value="custom_after">Custom: After date</option>
                    <option value="custom_on">Custom: On date</option>
                  </Select>
              </div>
            )}
            {timeDimension && activeTimeRangePreset === 'custom_between' ? (
              <div className="grid grid-cols-2 gap-2">
                <Input
                  type="date"
                  value={timeRangeFrom}
                  onChange={(event) => setTimeRangeFrom(event.target.value)}
                  placeholder="From date"
                  className="h-9 text-xs"
                />
                <Input
                  type="date"
                  value={timeRangeTo}
                  onChange={(event) => setTimeRangeTo(event.target.value)}
                  placeholder="To date"
                  className="h-9 text-xs"
                />
              </div>
            ) : null}
            {timeDimension && (activeTimeRangePreset === 'custom_before' || activeTimeRangePreset === 'custom_after' || activeTimeRangePreset === 'custom_on') ? (
              <Input
                type="date"
                value={timeRangeFrom}
                onChange={(event) => setTimeRangeFrom(event.target.value)}
                placeholder="Select date"
                className="h-9 text-xs"
              />
            ) : null}
          </section>

          {/* Filters */}
          <section className="space-y-3">
            <div className="flex items-center justify-between">
               <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Filters</Label>
               <Button variant="outline" size="sm" onClick={handleAddFilter} className="h-6 gap-1 px-2 text-xs">
                 <Plus className="h-3 w-3" /> Add
               </Button>
            </div>
            {filters.length === 0 && <div className="text-xs text-muted-foreground italic">No filters applied</div>}
            <div className="space-y-2">
              {filters.map(filter => (
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
                            handleUpdateFilter(filter.id, {
                              member: value,
                              operator: ensureOperatorForField(filter.operator, nextField),
                            });
                          }}
                          fields={fields}
                          className="h-7 text-xs"
                          inputClassName="text-xs"
                        />
                        <Button variant="ghost" size="icon" onClick={() => handleRemoveFilter(filter.id)} className="h-7 w-7 text-destructive hover:bg-destructive/10">
                          <Trash2 className="h-3 w-3" />
                        </Button>
                      </div>
                      <div className="space-y-2">
                        <Select 
                          value={normalizedOperator} 
                          onChange={(e) => handleUpdateFilter(filter.id, { operator: ensureOperatorForField(e.target.value, selectedField) })}
                          className="h-7 text-xs"
                        >
                          {operatorOptions.map((op) => <option key={op.value} value={op.value}>{op.label}</option>)}
                        </Select>
                        {usesDateRangeInput ? (
                          <>
                            <Select
                              value={dateRangeState.preset}
                              onChange={(e) =>
                                handleUpdateFilter(filter.id, {
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
                                    handleUpdateFilter(filter.id, {
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
                                    handleUpdateFilter(filter.id, {
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
                                  handleUpdateFilter(filter.id, {
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
                            onChange={(e) => handleUpdateFilter(filter.id, { values: e.target.value })}
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


          {/* Sorting */}
           <section className="space-y-3">
            <div className="flex items-center justify-between">
               <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Sorting</Label>
               <Button variant="outline" size="sm" onClick={handleAddOrder} className="h-6 gap-1 px-2 text-xs">
                 <Plus className="h-3 w-3" /> Add
               </Button>
            </div>
            {orderBys.length === 0 && <div className="text-xs text-muted-foreground italic">Default sorting</div>}
            <div className="space-y-2">
              {orderBys.map(order => (
                <div key={order.id} className="flex items-center gap-2 bg-muted/30 p-1.5 rounded-lg border border-border">
                   <FieldSelect
                      value={order.member}
                      onChange={(value) => handleUpdateOrder(order.id, { member: value })}
                      fields={fields}
                      className="h-7 text-xs flex-1"
                      inputClassName="text-xs"
                    />
                    <Select 
                      value={order.direction} 
                      onChange={(e) => handleUpdateOrder(order.id, { direction: e.target.value as 'asc' | 'desc' })}
                      className="h-7 text-xs w-20"
                    >
                      <option value="asc">Asc</option>
                      <option value="desc">Desc</option>
                    </Select>
                    <Button variant="ghost" size="icon" onClick={() => handleRemoveOrder(order.id)} className="h-7 w-7 text-muted-foreground">
                      <Trash2 className="h-3 w-3" />
                    </Button>
                </div>
              ))}
            </div>
          </section>

          {/* Limit */}
          <section className="space-y-3">
             <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Row Limit</Label>
             <Input 
               type="number" 
               value={limit} 
               onChange={(e) => setLimit(Number(e.target.value))} 
               className="h-8 text-sm"
               min={1}
               max={10000}
             />
          </section>

          {/* Actions */}
          <section className="pt-4 border-t border-border space-y-2">
            <Button variant="outline" size="sm" className="w-full justify-start" onClick={onExportCsv}>
               Download CSV
            </Button>
            <Button variant="outline" size="sm" className="w-full justify-start" onClick={onShowSql}>
               View SQL
            </Button>
          </section>

        </TabsContent>
      </Tabs>
    </div>
  );
}
