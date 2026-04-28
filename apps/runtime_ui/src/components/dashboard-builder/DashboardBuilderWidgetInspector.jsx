import { Copy, Download, RefreshCw, Trash2 } from "lucide-react";

import { FederationDiagnosticsPanel } from "../FederationDiagnosticsPanel";
import { PageEmpty, Panel } from "../PagePrimitives";
import { formatValue } from "../../lib/format";
import {
  DASHBOARD_BUILDER_PALETTES,
  DASHBOARD_BUILDER_TIME_GRAINS,
  DASHBOARD_BUILDER_TIME_PRESETS,
  createFilterDraft,
  createOrderDraft,
  ensureOperatorForField,
  getFilterValuePlaceholder,
  getDashboardBuilderPalette,
  isValuelessFilter,
  parseDateRangeFilterValues,
  serializeDateRangeFilterValues,
  usesDateRangePresetInput,
} from "../../lib/dashboardBuilder";
import { DashboardBuilderFieldSelect } from "./DashboardBuilderFieldSelect";

function FilterSection({
  title,
  filters,
  fields,
  fieldTypesByValue,
  onAdd,
  onPatch,
  onRemove,
}) {
  const defaultOperators = [
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
  const dateOperators = [
    { value: "indaterange", label: "In date range" },
    { value: "notindaterange", label: "Not in date range" },
    { value: "set", label: "Is set" },
    { value: "notset", label: "Is not set" },
  ];

  return (
    <div className="field-group">
      <div className="field-group-header">
        <strong>{title}</strong>
        <button className="ghost-button" type="button" onClick={onAdd}>
          Add filter
        </button>
      </div>

      {filters.length > 0 ? (
        <div className="page-stack">
          {filters.map((filter) => {
            const field = fields.find((item) => item.id === filter.member) || null;
            const normalizedOperator = ensureOperatorForField(
              filter.operator,
              field,
              fieldTypesByValue,
            );
            const operatorOptions = usesDateRangePresetInput(
              field,
              fieldTypesByValue,
              "indaterange",
            )
              ? dateOperators
              : defaultOperators;
            const dateRangeState = parseDateRangeFilterValues(filter.values);
            const datePresetInput = usesDateRangePresetInput(
              field,
              fieldTypesByValue,
              normalizedOperator,
            );

            return (
              <div key={filter.id} className="field-group">
                <DashboardBuilderFieldSelect
                  fields={fields}
                  value={filter.member}
                  onChange={(value) => {
                    const nextField = fields.find((item) => item.id === value) || null;
                    onPatch(filter.id, {
                      member: value,
                      operator: ensureOperatorForField(
                        filter.operator,
                        nextField,
                        fieldTypesByValue,
                      ),
                    });
                  }}
                />

                <div className="form-grid compact">
                  <label className="field">
                    <span>Operator</span>
                    <select
                      className="select-input"
                      value={normalizedOperator}
                      onChange={(event) =>
                        onPatch(filter.id, {
                          operator: ensureOperatorForField(
                            event.target.value,
                            field,
                            fieldTypesByValue,
                          ),
                        })
                      }
                    >
                      {operatorOptions.map((operator) => (
                        <option key={operator.value} value={operator.value}>
                          {operator.label}
                        </option>
                      ))}
                    </select>
                  </label>

                  {datePresetInput ? (
                    <label className="field">
                      <span>Preset</span>
                      <select
                        className="select-input"
                        value={dateRangeState.preset}
                        onChange={(event) =>
                          onPatch(filter.id, {
                            values: serializeDateRangeFilterValues({
                              ...dateRangeState,
                              preset: event.target.value,
                            }),
                          })
                        }
                      >
                        {DASHBOARD_BUILDER_TIME_PRESETS.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </label>
                  ) : (
                    <label className="field">
                      <span>Value</span>
                      <input
                        className="text-input"
                        type="text"
                        value={filter.values}
                        onChange={(event) => onPatch(filter.id, { values: event.target.value })}
                        placeholder={getFilterValuePlaceholder(
                          field,
                          fieldTypesByValue,
                          normalizedOperator,
                        )}
                        disabled={isValuelessFilter(normalizedOperator)}
                      />
                    </label>
                  )}
                </div>

                {datePresetInput &&
                (dateRangeState.preset === "custom_between" ||
                  dateRangeState.preset === "custom_before" ||
                  dateRangeState.preset === "custom_after" ||
                  dateRangeState.preset === "custom_on") ? (
                  <div className="form-grid compact">
                    <label className="field">
                      <span>
                        {dateRangeState.preset === "custom_between" ? "From" : "Date"}
                      </span>
                      <input
                        className="text-input"
                        type="date"
                        value={dateRangeState.from}
                        onChange={(event) =>
                          onPatch(filter.id, {
                            values: serializeDateRangeFilterValues({
                              ...dateRangeState,
                              from: event.target.value,
                            }),
                          })
                        }
                      />
                    </label>
                    {dateRangeState.preset === "custom_between" ? (
                      <label className="field">
                        <span>To</span>
                        <input
                          className="text-input"
                          type="date"
                          value={dateRangeState.to}
                          onChange={(event) =>
                            onPatch(filter.id, {
                              values: serializeDateRangeFilterValues({
                                ...dateRangeState,
                                to: event.target.value,
                              }),
                            })
                          }
                        />
                      </label>
                    ) : null}
                  </div>
                ) : null}

                <div className="page-actions">
                  <button
                    className="ghost-button danger-button"
                    type="button"
                    onClick={() => onRemove(filter.id)}
                  >
                    Remove filter
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <PageEmpty title="No widget filters" message="Add filters to scope the active widget query." />
      )}
    </div>
  );
}

function OrderSection({ orders, fields, onAdd, onPatch, onRemove }) {
  return (
    <div className="field-group">
      <div className="field-group-header">
        <strong>Ordering</strong>
        <button className="ghost-button" type="button" onClick={onAdd}>
          Add order
        </button>
      </div>

      {orders.length > 0 ? (
        <div className="page-stack">
          {orders.map((order) => (
            <div key={order.id} className="field-group">
              <div className="form-grid compact">
                <label className="field">
                  <span>Field</span>
                  <DashboardBuilderFieldSelect
                    fields={fields}
                    value={order.member}
                    onChange={(value) => onPatch(order.id, { member: value })}
                  />
                </label>
                <label className="field">
                  <span>Direction</span>
                  <select
                    className="select-input"
                    value={order.direction}
                    onChange={(event) => onPatch(order.id, { direction: event.target.value })}
                  >
                    <option value="desc">Descending</option>
                    <option value="asc">Ascending</option>
                  </select>
                </label>
              </div>
              <div className="page-actions">
                <button
                  className="ghost-button danger-button"
                  type="button"
                  onClick={() => onRemove(order.id)}
                >
                  Remove order
                </button>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <PageEmpty title="No ordering" message="Add order rules to control the result sort." />
      )}
    </div>
  );
}

export function DashboardBuilderWidgetInspector({
  activeBoard,
  activeWidget,
  selectedModel,
  activeWidgetDimensions,
  activeWidgetMeasures,
  dashboardBuilderEditMode,
  fieldOptions,
  dimensionFieldOptions,
  measureFieldOptions,
  dateDimensionOptions,
  fieldTypesByValue,
  formatSemanticMember,
  onUpdateWidget,
  onRunWidget,
  onRemoveWidget,
  onAssignField,
  onExportWidget,
  onCopyGeneratedSql,
}) {
  return (
    <div className="detail-stack dashboard-builder-inspector-stack">
      <Panel
        title={activeWidget?.title || "Widget"}
        className="dashboard-builder-inspector-panel dashboard-builder-compact-panel"
        actions={
          activeWidget ? (
            <div className="panel-actions-inline">
              <button
                className="primary-button"
                type="button"
                onClick={() => void onRunWidget(activeWidget)}
                disabled={!selectedModel || activeWidgetMeasures.length === 0}
              >
                <RefreshCw className="button-icon" aria-hidden="true" />
                Run
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={onRemoveWidget}
                disabled={!dashboardBuilderEditMode}
              >
                <Trash2 className="button-icon" aria-hidden="true" />
                Delete
              </button>
            </div>
          ) : null
        }
      >
        {activeWidget && activeBoard ? (
          <div className="detail-stack">
            <div className="dashboard-builder-panel-meta">
              <span>{selectedModel || "No model"}</span>
              <span>{formatValue(activeWidgetDimensions.length)} dimensions</span>
              <span>{formatValue(activeWidgetMeasures.length)} measures</span>
              <span>{getDashboardBuilderPalette(activeWidget.visualConfig?.paletteId).label}</span>
              <span>{formatValue(activeWidget.lastRunAt || "Not run yet")}</span>
            </div>

            <div className="form-grid compact">
              <label className="field">
                <span>Title</span>
                <input
                  className="text-input"
                  type="text"
                  value={activeWidget.title}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, { title: event.target.value })
                  }
                  disabled={!dashboardBuilderEditMode}
                />
              </label>
              <label className="field">
                <span>Chart</span>
                <select
                  className="select-input"
                  value={activeWidget.chartType}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, {
                      chartType: event.target.value,
                    })
                  }
                  disabled={!dashboardBuilderEditMode}
                >
                  <option value="bar">Bar</option>
                  <option value="line">Line</option>
                  <option value="pie">Pie</option>
                  <option value="table">Table</option>
                </select>
              </label>
              <label className="field">
                <span>Size</span>
                <select
                  className="select-input"
                  value={activeWidget.size}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, { size: event.target.value })
                  }
                  disabled={!dashboardBuilderEditMode}
                >
                  <option value="small">Small</option>
                  <option value="wide">Wide</option>
                  <option value="tall">Tall</option>
                  <option value="large">Large</option>
                </select>
              </label>
              <label className="field">
                <span>Palette</span>
                <select
                  className="select-input"
                  value={activeWidget.visualConfig?.paletteId || DASHBOARD_BUILDER_PALETTES[0].id}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, {
                      visualConfig: {
                        ...activeWidget.visualConfig,
                        paletteId: event.target.value,
                      },
                    })
                  }
                  disabled={!dashboardBuilderEditMode}
                >
                  {DASHBOARD_BUILDER_PALETTES.map((palette) => (
                    <option key={palette.id} value={palette.id}>
                      {palette.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>X field</span>
                <DashboardBuilderFieldSelect
                  fields={dimensionFieldOptions}
                  value={activeWidget.chartX || ""}
                  onChange={(value) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, { chartX: value })
                  }
                  allowEmpty
                  emptyLabel="Auto"
                />
              </label>
              <label className="field">
                <span>Y field</span>
                <DashboardBuilderFieldSelect
                  fields={measureFieldOptions}
                  value={activeWidget.chartY || ""}
                  onChange={(value) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, { chartY: value })
                  }
                  allowEmpty
                  emptyLabel="Auto"
                />
              </label>
              <label className="field">
                <span>Rows</span>
                <input
                  className="text-input"
                  type="number"
                  min="1"
                  value={activeWidget.limit}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, {
                      limit: event.target.value,
                    })
                  }
                  disabled={!dashboardBuilderEditMode}
                />
              </label>
              <label className="field">
                <span>Time field</span>
                <DashboardBuilderFieldSelect
                  fields={dateDimensionOptions}
                  value={activeWidget.timeDimension || ""}
                  onChange={(value) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, {
                      timeDimension: value,
                      timeGrain: value ? activeWidget.timeGrain : "",
                    })
                  }
                  allowEmpty
                  emptyLabel="No time dimension"
                />
              </label>
              <label className="field">
                <span>Grain</span>
                <select
                  className="select-input"
                  value={activeWidget.timeGrain}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, {
                      timeGrain: event.target.value,
                    })
                  }
                  disabled={!dashboardBuilderEditMode || !activeWidget.timeDimension}
                >
                  {DASHBOARD_BUILDER_TIME_GRAINS.map((item) => (
                    <option key={item.value || "none"} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Time range</span>
                <select
                  className="select-input"
                  value={activeWidget.timeRangePreset || "no_filter"}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, {
                      timeRangePreset:
                        event.target.value === "no_filter" ? "" : event.target.value,
                    })
                  }
                  disabled={!dashboardBuilderEditMode || !activeWidget.timeDimension}
                >
                  {DASHBOARD_BUILDER_TIME_PRESETS.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              {["custom_between", "custom_before", "custom_after", "custom_on"].includes(
                activeWidget.timeRangePreset,
              ) ? (
                <>
                  <label className="field">
                    <span>
                      {activeWidget.timeRangePreset === "custom_between" ? "From" : "Date"}
                    </span>
                    <input
                      className="text-input"
                      type="date"
                      value={activeWidget.timeRangeFrom}
                      onChange={(event) =>
                        onUpdateWidget(activeBoard.id, activeWidget.id, {
                          timeRangeFrom: event.target.value,
                        })
                      }
                      disabled={!dashboardBuilderEditMode}
                    />
                  </label>
                  {activeWidget.timeRangePreset === "custom_between" ? (
                    <label className="field">
                      <span>To</span>
                      <input
                        className="text-input"
                        type="date"
                        value={activeWidget.timeRangeTo}
                        onChange={(event) =>
                          onUpdateWidget(activeBoard.id, activeWidget.id, {
                            timeRangeTo: event.target.value,
                          })
                        }
                        disabled={!dashboardBuilderEditMode}
                      />
                    </label>
                  ) : null}
                </>
              ) : null}
            </div>

            <div className="field field-full dashboard-builder-selection-field">
              <div className="dashboard-builder-selection-head">
                <span>Dimensions</span>
                <small>{formatValue(activeWidgetDimensions.length)} selected</small>
              </div>
              {activeWidgetDimensions.length > 0 ? (
                <div className="field-pill-list dashboard-builder-selected-field-list">
                  {activeWidgetDimensions.map((value) => (
                    <button
                      key={value}
                      className={`field-pill active ${!dashboardBuilderEditMode ? "static" : ""}`}
                      type="button"
                      onClick={() => onAssignField(value, "dimension")}
                      disabled={!dashboardBuilderEditMode}
                      title={value}
                    >
                      {formatSemanticMember(value)}
                    </button>
                  ))}
                </div>
              ) : (
                <div className="empty-box dashboard-builder-selection-empty">Select dimensions from the field library.</div>
              )}
            </div>

            <div className="field field-full dashboard-builder-selection-field">
              <div className="dashboard-builder-selection-head">
                <span>Measures</span>
                <small>{formatValue(activeWidgetMeasures.length)} selected</small>
              </div>
              {activeWidgetMeasures.length > 0 ? (
                <div className="field-pill-list dashboard-builder-selected-field-list">
                  {activeWidgetMeasures.map((value) => (
                    <button
                      key={value}
                      className={`field-pill active ${!dashboardBuilderEditMode ? "static" : ""}`}
                      type="button"
                      onClick={() => onAssignField(value, "measure")}
                      disabled={!dashboardBuilderEditMode}
                      title={value}
                    >
                      {formatSemanticMember(value)}
                    </button>
                  ))}
                </div>
              ) : (
                <div className="empty-box dashboard-builder-selection-empty">Select measures from the field library.</div>
              )}
            </div>

            <FilterSection
              title="Widget filters"
              filters={Array.isArray(activeWidget.filters) ? activeWidget.filters : []}
              fields={fieldOptions}
              fieldTypesByValue={fieldTypesByValue}
              onAdd={() =>
                onUpdateWidget(activeBoard.id, activeWidget.id, {
                  filters: [...activeWidget.filters, createFilterDraft({ member: fieldOptions[0]?.id || "" })],
                })
              }
              onPatch={(filterId, updates) =>
                onUpdateWidget(activeBoard.id, activeWidget.id, {
                  filters: activeWidget.filters.map((filter) =>
                    filter.id === filterId ? { ...filter, ...updates } : filter,
                  ),
                })
              }
              onRemove={(filterId) =>
                onUpdateWidget(activeBoard.id, activeWidget.id, {
                  filters: activeWidget.filters.filter((filter) => filter.id !== filterId),
                })
              }
            />

            <OrderSection
              orders={Array.isArray(activeWidget.orderBys) ? activeWidget.orderBys : []}
              fields={[...dimensionFieldOptions, ...measureFieldOptions]}
              onAdd={() =>
                onUpdateWidget(activeBoard.id, activeWidget.id, {
                  orderBys: [...activeWidget.orderBys, createOrderDraft({ member: measureFieldOptions[0]?.id || dimensionFieldOptions[0]?.id || "" })],
                })
              }
              onPatch={(orderId, updates) =>
                onUpdateWidget(activeBoard.id, activeWidget.id, {
                  orderBys: activeWidget.orderBys.map((order) =>
                    order.id === orderId ? { ...order, ...updates } : order,
                  ),
                })
              }
              onRemove={(orderId) =>
                onUpdateWidget(activeBoard.id, activeWidget.id, {
                  orderBys: activeWidget.orderBys.filter((order) => order.id !== orderId),
                })
              }
            />

            <div className="panel-actions-inline">
              <button
                className="ghost-button"
                type="button"
                onClick={onExportWidget}
                disabled={!activeWidget.result}
              >
                <Download className="button-icon" aria-hidden="true" />
                Export CSV
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={onCopyGeneratedSql}
                disabled={!activeWidget.result?.generated_sql}
              >
                <Copy className="button-icon" aria-hidden="true" />
                Copy SQL
              </button>
            </div>

            {dashboardBuilderEditMode || activeWidget.description ? (
              <details className="diagnostics-disclosure">
                <summary>Notes</summary>
                <textarea
                  className="textarea-input"
                  value={activeWidget.description}
                  onChange={(event) =>
                    onUpdateWidget(activeBoard.id, activeWidget.id, {
                      description: event.target.value,
                    })
                  }
                  disabled={!dashboardBuilderEditMode}
                />
              </details>
            ) : null}
            {activeWidget.result?.generated_sql ? (
              <details className="diagnostics-disclosure">
                <summary>Generated SQL</summary>
                <pre className="code-block compact">{activeWidget.result.generated_sql}</pre>
              </details>
            ) : null}
            {activeWidget.result?.federation_diagnostics ? (
              <FederationDiagnosticsPanel
                diagnostics={activeWidget.result.federation_diagnostics}
                title="Federation diagnostics"
                description="Inspect how this semantic widget executed across sources."
              />
            ) : null}
          </div>
        ) : (
          <PageEmpty
            title="No active widget"
            message="Select or create a widget to configure the dashboard."
          />
        )}
      </Panel>
    </div>
  );
}
