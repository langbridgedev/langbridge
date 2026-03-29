import { useDeferredValue, useEffect, useState } from "react";
import {
  Activity,
  Copy,
  Download,
  Edit3,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";

import { ChartPreview } from "../components/ChartPreview";
import { ResultTable } from "../components/ResultTable";
import { PageEmpty, Panel } from "../components/PagePrimitives";
import { readStoredJson } from "../hooks/usePersistentState";
import { useAsyncData } from "../hooks/useAsyncData";
import { fetchSemanticModel, fetchSemanticModels, querySemantic } from "../lib/runtimeApi";
import { formatValue, getErrorMessage } from "../lib/format";
import {
  BI_PALETTES,
  BI_TIME_GRAINS,
  buildBiQueryPayload,
  createBiBoard,
  createBiWidget,
  enrichBiResult,
  getBiPalette,
  isDateLikeField,
  loadBiStudioState,
} from "../lib/bi";
import {
  copyTextToClipboard,
  downloadTextFile,
  extractSemanticDatasets,
  extractSemanticFields,
  renderJson,
  toCsvText,
} from "../lib/runtimeUi";

function getSelectedMembers(values) {
  return Array.isArray(values)
    ? values.filter((value) => String(value || "").trim())
    : [];
}

function getPrimarySelectedMember(values) {
  return getSelectedMembers(values)[0] || "";
}

function toggleSelectedMember(values, nextValue) {
  const normalized = getSelectedMembers(values);
  const value = String(nextValue || "").trim();
  if (!value) {
    return normalized;
  }
  return normalized.includes(value)
    ? normalized.filter((item) => item !== value)
    : [...normalized, value];
}

function formatSemanticMember(value) {
  const parts = String(value || "")
    .split(".")
    .filter(Boolean);
  return parts[parts.length - 1] || String(value || "");
}

function summarizeSelectedMembers(values, prefix) {
  const selected = getSelectedMembers(values).map((value) => formatSemanticMember(value));
  if (selected.length === 0) {
    return "";
  }
  if (selected.length === 1) {
    return `${prefix}: ${selected[0]}`;
  }
  return `${prefix}: ${selected[0]} +${selected.length - 1}`;
}

export function BiPage() {
  const modelsState = useAsyncData(fetchSemanticModels);
  const models = Array.isArray(modelsState.data?.items) ? modelsState.data.items : [];
  const [studioState, setStudioState] = useState(() => loadBiStudioState(readStoredJson));
  const [activeWidgetId, setActiveWidgetId] = useState("");
  const [detail, setDetail] = useState(null);
  const [detailError, setDetailError] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);
  const [fieldSearch, setFieldSearch] = useState("");
  const [studioNotice, setStudioNotice] = useState("");
  const [biEditMode, setBiEditMode] = useState(true);
  const deferredFieldSearch = useDeferredValue(fieldSearch);

  const boards = studioState.boards;
  const defaultModelName = models.find((item) => item.default)?.name || models[0]?.name || "";
  const activeBoard =
    boards.find((board) => board.id === studioState.activeBoardId) || boards[0] || null;
  const activeWidget =
    activeBoard?.widgets.find((widget) => widget.id === activeWidgetId) ||
    activeBoard?.widgets[0] ||
    null;
  const selectedModel = activeBoard?.selectedModel || "";
  const fields = extractSemanticFields(detail);
  const semanticDatasets = extractSemanticDatasets(detail);
  const activeWidgetDimensions = getSelectedMembers(activeWidget?.dimensions);
  const activeWidgetMeasures = getSelectedMembers(activeWidget?.measures);
  const dateDimensionOptions = fields.dimensions.filter((item) =>
    isDateLikeField(item, Object.fromEntries(fields.dimensions.map((field) => [field.value, field.type]))),
  );
  const runnableCount =
    activeBoard?.widgets.filter((widget) => getSelectedMembers(widget.measures).length > 0).length || 0;
  const totalWidgets = boards.reduce((count, board) => count + board.widgets.length, 0);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const snapshot = {
      activeBoardId: studioState.activeBoardId,
      boards: studioState.boards.map((board) => ({
        ...board,
        widgets: board.widgets.map(({ result, running, error, ...widget }) => widget),
      })),
    };
    window.localStorage.setItem("langbridge.runtime_ui.bi_studio", JSON.stringify(snapshot));
  }, [studioState]);

  useEffect(() => {
    if (boards.length === 0) {
      const board = createBiBoard({ selectedModel: defaultModelName });
      setStudioState({ boards: [board], activeBoardId: board.id });
      setActiveWidgetId(board.widgets[0]?.id || "");
      return;
    }
    if (!boards.some((board) => board.id === studioState.activeBoardId)) {
      setStudioState((current) => ({ ...current, activeBoardId: current.boards[0]?.id || "" }));
    }
  }, [boards, defaultModelName, studioState.activeBoardId]);

  useEffect(() => {
    if (!activeBoard) {
      setActiveWidgetId("");
      return;
    }
    if (!activeBoard.selectedModel || !models.some((item) => item.name === activeBoard.selectedModel)) {
      updateBoard(activeBoard.id, { selectedModel: defaultModelName });
    }
    if (activeBoard.widgets.length === 0) {
      setActiveWidgetId("");
      return;
    }
    if (!activeBoard.widgets.some((widget) => widget.id === activeWidgetId)) {
      setActiveWidgetId(activeBoard.widgets[0].id);
    }
  }, [activeBoard, activeWidgetId, defaultModelName, models]);

  useEffect(() => {
    let cancelled = false;

    async function loadModelDetail() {
      if (!selectedModel) {
        setDetail(null);
        setDetailError("");
        setDetailLoading(false);
        return;
      }
      setDetailLoading(true);
      setDetailError("");
      try {
        const payload = await fetchSemanticModel(selectedModel);
        if (!cancelled) {
          setDetail(payload);
        }
      } catch (caughtError) {
        if (!cancelled) {
          setDetail(null);
          setDetailError(getErrorMessage(caughtError));
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadModelDetail();
    return () => {
      cancelled = true;
    };
  }, [selectedModel]);

  const filteredSemanticDatasets = semanticDatasets
    .map((dataset) => {
      const search = String(deferredFieldSearch || "").trim().toLowerCase();
      if (!search) {
        return dataset;
      }
      return {
        ...dataset,
        dimensions: dataset.dimensions.filter((item) =>
          String(item?.name || "").toLowerCase().includes(search),
        ),
        measures: dataset.measures.filter((item) =>
          String(item?.name || "").toLowerCase().includes(search),
        ),
      };
    })
    .filter(
      (dataset) =>
        !deferredFieldSearch ||
        String(dataset.name).toLowerCase().includes(String(deferredFieldSearch).toLowerCase()) ||
        dataset.dimensions.length > 0 ||
        dataset.measures.length > 0,
    );

  function updateBoard(boardId, updates) {
    setStudioState((current) => ({
      ...current,
      boards: current.boards.map((board) => (board.id === boardId ? { ...board, ...updates } : board)),
    }));
  }

  function updateWidget(boardId, widgetId, updates) {
    setStudioState((current) => ({
      ...current,
      boards: current.boards.map((board) =>
        board.id === boardId
          ? {
              ...board,
              widgets: board.widgets.map((widget) =>
                widget.id === widgetId ? { ...widget, ...updates } : widget,
              ),
            }
          : board,
      ),
    }));
  }

  function createBoard() {
    const board = createBiBoard({
      name: `Runtime dashboard ${boards.length + 1}`,
      selectedModel: selectedModel || defaultModelName,
    });
    setStudioState((current) => ({ boards: [board, ...current.boards], activeBoardId: board.id }));
    setActiveWidgetId(board.widgets[0]?.id || "");
    setStudioNotice("Created a local dashboard draft.");
  }

  function removeBoard() {
    if (!activeBoard) {
      return;
    }
    const remaining = boards.filter((board) => board.id !== activeBoard.id);
    if (remaining.length === 0) {
      const freshBoard = createBiBoard({ selectedModel: defaultModelName });
      setStudioState({ boards: [freshBoard], activeBoardId: freshBoard.id });
      setActiveWidgetId(freshBoard.widgets[0]?.id || "");
      setStudioNotice("Reset the BI studio to a fresh local dashboard.");
      return;
    }
    setStudioState({ boards: remaining, activeBoardId: remaining[0].id });
    setActiveWidgetId(remaining[0].widgets[0]?.id || "");
    setStudioNotice("Removed the selected dashboard.");
  }

  function duplicateBoard() {
    if (!activeBoard) {
      return;
    }
    const board = createBiBoard({
      name: `${activeBoard.name} copy`,
      description: activeBoard.description,
      selectedModel: activeBoard.selectedModel,
      lastRefreshedAt: activeBoard.lastRefreshedAt,
      widgets: activeBoard.widgets.map((widget) => ({
        ...widget,
        id: `widget-${Math.random().toString(36).slice(2, 10)}`,
      })),
    });
    setStudioState((current) => ({ boards: [board, ...current.boards], activeBoardId: board.id }));
    setActiveWidgetId(board.widgets[0]?.id || "");
    setStudioNotice("Duplicated the active dashboard into a new local draft.");
  }

  function addWidget() {
    if (!activeBoard) {
      return;
    }
    const widget = createBiWidget({
      title: `Widget ${activeBoard.widgets.length + 1}`,
      description: "Local runtime widget powered by semantic query execution.",
      dimensions:
        activeWidgetDimensions.length > 0
          ? activeWidgetDimensions
          : fields.dimensions[0]?.value
            ? [fields.dimensions[0].value]
            : [],
      measures:
        activeWidgetMeasures.length > 0
          ? activeWidgetMeasures
          : fields.measures[0]?.value
            ? [fields.measures[0].value]
            : [],
      timeDimension: activeWidget?.timeDimension || dateDimensionOptions[0]?.value || "",
    });
    updateBoard(activeBoard.id, { widgets: [...activeBoard.widgets, widget] });
    setActiveWidgetId(widget.id);
    setStudioNotice("Added a widget to the dashboard canvas.");
  }

  function removeWidget() {
    if (!activeBoard || !activeWidget) {
      return;
    }
    const remainingWidgets = activeBoard.widgets.filter((widget) => widget.id !== activeWidget.id);
    updateBoard(activeBoard.id, { widgets: remainingWidgets });
    setActiveWidgetId(remainingWidgets[0]?.id || "");
    setStudioNotice("Removed the active widget.");
  }

  function assignField(value, kind) {
    if (!activeBoard) {
      return;
    }
    const memberKey = kind === "dimension" ? "dimensions" : "measures";
    const target = activeWidget || activeBoard.widgets[0];
    if (!target) {
      const widget = createBiWidget({
        title: "Widget 1",
        description: "Created from the semantic field library.",
        dimensions:
          kind === "dimension"
            ? [value]
            : fields.dimensions[0]?.value
              ? [fields.dimensions[0].value]
              : [],
        measures:
          kind === "measure"
            ? [value]
            : fields.measures[0]?.value
              ? [fields.measures[0].value]
              : [],
      });
      updateBoard(activeBoard.id, { widgets: [...activeBoard.widgets, widget] });
      setActiveWidgetId(widget.id);
      setStudioNotice(`Created a widget from the selected ${kind}.`);
      return;
    }
    setActiveWidgetId(target.id);
    const currentSelection = getSelectedMembers(target[memberKey]);
    const isAssigned = currentSelection.includes(value);
    updateWidget(activeBoard.id, target.id, {
      [memberKey]: toggleSelectedMember(currentSelection, value),
    });
    setStudioNotice(`${isAssigned ? "Removed" : "Added"} ${kind} ${isAssigned ? "from" : "to"} ${target.title}.`);
  }

  async function runWidget(widget) {
    if (!activeBoard || !selectedModel || getSelectedMembers(widget?.measures).length === 0) {
      return;
    }
    updateWidget(activeBoard.id, widget.id, { running: true, error: "" });
    try {
      const response = await querySemantic(buildBiQueryPayload(activeBoard, widget));
      updateWidget(activeBoard.id, widget.id, {
        running: false,
        error: "",
        lastRunAt: new Date().toISOString(),
        result: enrichBiResult(response),
      });
      updateBoard(activeBoard.id, { lastRefreshedAt: new Date().toISOString() });
    } catch (caughtError) {
      updateWidget(activeBoard.id, widget.id, {
        running: false,
        result: null,
        error: getErrorMessage(caughtError),
      });
    }
  }

  async function runAllWidgets() {
    const widgets =
      activeBoard?.widgets.filter((widget) => getSelectedMembers(widget.measures).length > 0) || [];
    if (widgets.length === 0) {
      setStudioNotice("Add a measure to at least one widget before refreshing the dashboard.");
      return;
    }
    await Promise.all(widgets.map((widget) => runWidget(widget)));
    setStudioNotice("Refreshed all runnable widgets against the local runtime.");
  }

  async function copyGeneratedSql() {
    if (!activeWidget?.result?.generated_sql) {
      return;
    }
    try {
      await copyTextToClipboard(activeWidget.result.generated_sql);
      setStudioNotice("Copied generated SQL to the clipboard.");
    } catch (caughtError) {
      setStudioNotice(getErrorMessage(caughtError));
    }
  }

  function exportWidget() {
    if (!activeWidget?.result) {
      return;
    }
    downloadTextFile(
      `${activeWidget.title.toLowerCase().replaceAll(/\s+/g, "-") || "runtime-widget"}.csv`,
      toCsvText(activeWidget.result),
      "text/csv;charset=utf-8",
    );
    setStudioNotice("Downloaded the active widget as CSV.");
  }

  function exportBoard() {
    if (!activeBoard) {
      return;
    }
    downloadTextFile(
      `${activeBoard.name.toLowerCase().replaceAll(/\s+/g, "-") || "runtime-dashboard"}.json`,
      renderJson({
        exported_at: new Date().toISOString(),
        dashboard: activeBoard,
      }),
      "application/json;charset=utf-8",
    );
    setStudioNotice("Exported the active dashboard as local JSON.");
  }

  return (
    <div className="page-stack bi-shell">
      <section className="surface-panel bi-command-bar">
        <div className="bi-command-bar-main">
          <div className="bi-command-bar-copy">
            <p className="eyebrow">BI Studio</p>
            <h2>{activeBoard?.name || "Runtime dashboard"}</h2>
            <div className="bi-command-bar-meta">
              <span className="chip">{selectedModel || "No model"}</span>
              <span className="chip">{formatValue(activeBoard?.widgets.length || 0)} widgets</span>
              <span className="chip">{formatValue(runnableCount)} runnable</span>
              <span className={`chip bi-mode-chip ${biEditMode ? "active" : ""}`.trim()}>
                {biEditMode ? "Edit" : "View"}
              </span>
            </div>
          </div>

          <div className="bi-command-bar-controls">
            <label className="field bi-compact-field">
              <span>Dashboard</span>
              <select
                className="select-input bi-dashboard-select"
                value={activeBoard?.id || ""}
                onChange={(event) =>
                  setStudioState((current) => ({ ...current, activeBoardId: event.target.value }))
                }
              >
                {boards.map((board) => (
                  <option key={board.id} value={board.id}>
                    {board.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="field bi-compact-field">
              <span>Model</span>
              <select
                className="select-input bi-dashboard-select"
                value={selectedModel}
                onChange={(event) =>
                  activeBoard
                    ? updateBoard(activeBoard.id, {
                        selectedModel: event.target.value,
                        lastRefreshedAt: null,
                      })
                    : null
                }
                disabled={!activeBoard || !biEditMode}
              >
                {models.map((item) => (
                  <option key={item.id || item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>

        <div className="bi-command-bar-actions">
          <button
            className={`ghost-button bi-mode-toggle ${biEditMode ? "active" : ""}`}
            type="button"
            onClick={() => setBiEditMode((current) => !current)}
          >
            {biEditMode ? <Edit3 className="button-icon" aria-hidden="true" /> : <Activity className="button-icon" aria-hidden="true" />}
            {biEditMode ? "Edit" : "View"}
          </button>
          <button className="primary-button" type="button" onClick={addWidget} disabled={!activeBoard || !biEditMode}>
            <Plus className="button-icon" aria-hidden="true" />
            Add widget
          </button>
          <button
            className="ghost-button"
            type="button"
            onClick={() => (activeWidget ? void runWidget(activeWidget) : undefined)}
            disabled={!activeWidget || !selectedModel || activeWidgetMeasures.length === 0}
          >
            <Activity className="button-icon" aria-hidden="true" />
            Run active
          </button>
          <button
            className="ghost-button"
            type="button"
            onClick={() => void runAllWidgets()}
            disabled={!activeBoard || !selectedModel || runnableCount === 0}
          >
            <RefreshCw className="button-icon" aria-hidden="true" />
            Run all
          </button>
          <button className="ghost-button" type="button" onClick={exportBoard} disabled={!activeBoard}>
            <Download className="button-icon" aria-hidden="true" />
            Export
          </button>
        </div>
      </section>

      {studioNotice ? <div className="callout bi-studio-notice bi-status-strip"><span>{studioNotice}</span></div> : null}

      <section className="bi-studio-grid bi-cloud-grid">
        <div className="detail-stack bi-sidebar-stack">
          <Panel
            title="Dashboards"
            className="bi-sidebar-panel bi-compact-panel"
            actions={
              <div className="panel-actions-inline">
                <button className="ghost-button" type="button" onClick={createBoard}>
                  <Plus className="button-icon" aria-hidden="true" />
                  New
                </button>
                <button className="ghost-button" type="button" onClick={duplicateBoard} disabled={!activeBoard}>
                  <Copy className="button-icon" aria-hidden="true" />
                  Duplicate
                </button>
                <button className="ghost-button" type="button" onClick={removeBoard} disabled={!activeBoard}>
                  <Trash2 className="button-icon" aria-hidden="true" />
                  Delete
                </button>
              </div>
            }
          >
            <div className="bi-panel-meta">
              <span>{formatValue(boards.length)} dashboards</span>
              <span>{formatValue(totalWidgets)} widgets</span>
              <span>{formatValue(activeBoard?.lastRefreshedAt || "Not run yet")}</span>
            </div>
            <div className="board-list bi-board-list">
              {boards.map((board) => (
                <button
                  key={board.id}
                  className={`list-card ${board.id === activeBoard?.id ? "active" : ""}`}
                  type="button"
                  onClick={() => setStudioState((current) => ({ ...current, activeBoardId: board.id }))}
                >
                  <div className="bi-board-card-top">
                    <strong>{board.name}</strong>
                    <span className="chip">{board.widgets.length}</span>
                  </div>
                  <small>{board.selectedModel || "No model selected"}</small>
                </button>
              ))}
            </div>
            {activeBoard ? (
              <label className="field">
                <span>Name</span>
                <input
                  className="text-input"
                  type="text"
                  value={activeBoard.name}
                  onChange={(event) => updateBoard(activeBoard.id, { name: event.target.value })}
                  disabled={!biEditMode}
                />
              </label>
            ) : null}
          </Panel>

          <Panel title="Fields" className="bi-sidebar-panel bi-compact-panel">
            <div className="bi-panel-meta">
              <span>{formatValue(semanticDatasets.length)} datasets</span>
              <span>{formatValue(fields.dimensions.length)} dimensions</span>
              <span>{formatValue(fields.measures.length)} measures</span>
            </div>
            <label className="field">
              <input
                className="text-input"
                type="search"
                value={fieldSearch}
                onChange={(event) => setFieldSearch(event.target.value)}
                placeholder="Find field"
              />
            </label>
            {detailError ? <div className="error-banner">{detailError}</div> : null}
            {detailLoading ? (
              <div className="empty-box">Loading semantic model...</div>
            ) : filteredSemanticDatasets.length > 0 ? (
              <div className="field-section-list">
                {filteredSemanticDatasets.map((dataset) => (
                  <div key={dataset.name} className="field-group bi-field-group">
                    <div className="field-group-header">
                      <strong>{dataset.name}</strong>
                      <span>{dataset.dimensions.length}D / {dataset.measures.length}M</span>
                    </div>
                    <div className="field-pill-list">
                      {dataset.dimensions.map((item) => {
                        const value = `${dataset.name}.${item.name}`;
                        return (
                          <button
                            key={value}
                            className={`field-pill ${activeWidgetDimensions.includes(value) ? "active" : ""} ${!biEditMode ? "static" : ""}`}
                            type="button"
                            onClick={() => assignField(value, "dimension")}
                            disabled={!biEditMode}
                          >
                            {item.name}
                          </button>
                        );
                      })}
                      {dataset.measures.map((item) => {
                        const value = `${dataset.name}.${item.name}`;
                        return (
                          <button
                            key={value}
                            className={`field-pill ${activeWidgetMeasures.includes(value) ? "active" : ""} ${!biEditMode ? "static" : ""}`}
                            type="button"
                            onClick={() => assignField(value, "measure")}
                            disabled={!biEditMode}
                          >
                            {item.name}
                          </button>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <PageEmpty title="No fields found" message="Adjust the search or select a semantic model with exposed fields." />
            )}
          </Panel>
        </div>

        <Panel
          title="Canvas"
          className="bi-canvas-panel bi-compact-panel"
          actions={
            <div className="panel-actions-inline">
              <span className="chip">{activeBoard?.name || "No dashboard"}</span>
            </div>
          }
        >
          {activeBoard ? (
            <div className="detail-stack">
              <div className="bi-panel-meta bi-canvas-meta">
                <span>{formatValue(activeBoard.widgets.length)} widgets</span>
                <span>{selectedModel || "No model selected"}</span>
                <span>{biEditMode ? "Editable" : "Preview"}</span>
              </div>
              {activeBoard.widgets.length > 0 ? (
                <div className="widget-canvas bi-widget-canvas">
                  {activeBoard.widgets.map((widget) => (
                    <article key={widget.id} className={`widget-tile bi-widget-tile widget-size-${widget.size} ${widget.id === activeWidget?.id ? "active" : ""}`}>
                      <div className="bi-widget-head">
                        <button className="widget-tile-header" type="button" onClick={() => setActiveWidgetId(widget.id)}>
                          <div className="bi-widget-title-block">
                            <strong>{widget.title}</strong>
                            <div className="bi-widget-metrics">
                              <span className="chart-kind">{widget.chartType}</span>
                              {getSelectedMembers(widget.dimensions).length > 0 ? (
                                <span className="chip">{summarizeSelectedMembers(widget.dimensions, "D")}</span>
                              ) : null}
                              {getSelectedMembers(widget.measures).length > 0 ? (
                                <span className="chip">{summarizeSelectedMembers(widget.measures, "M")}</span>
                              ) : null}
                            </div>
                          </div>
                        </button>
                        <button className="ghost-button bi-widget-run-button" type="button" onClick={() => void runWidget(widget)} disabled={!selectedModel || getSelectedMembers(widget.measures).length === 0}>
                          <RefreshCw className="button-icon" aria-hidden="true" />
                          Run
                        </button>
                      </div>
                      {widget.error ? <div className="error-banner">{widget.error}</div> : null}
                      {widget.running ? (
                        <div className="empty-box">Running semantic query...</div>
                      ) : widget.result ? (
                        <div className="detail-stack">
                          <ChartPreview
                            title={widget.title}
                            result={widget.result}
                            metadata={Array.isArray(widget.result?.metadata) ? widget.result.metadata : []}
                            visualization={{
                              chartType: widget.chartType,
                              x: getPrimarySelectedMember(widget.dimensions),
                              y: getSelectedMembers(widget.measures),
                            }}
                            preferredDimension={getPrimarySelectedMember(widget.dimensions)}
                            preferredMeasure={getPrimarySelectedMember(widget.measures)}
                            themeColors={getBiPalette(widget.visualConfig?.paletteId).colors}
                          />
                          {widget.id === activeWidget?.id || widget.chartType === "table" ? (
                            <ResultTable result={widget.result} maxPreviewRows={6} />
                          ) : null}
                        </div>
                      ) : (
                        <PageEmpty title="No result" message="Pick fields and run the widget." />
                      )}
                      <div className="bi-widget-foot">
                        <span>{formatValue(widget.result?.rowCount || 0)} rows</span>
                        <span>{formatValue(widget.lastRunAt || "Not run yet")}</span>
                      </div>
                    </article>
                  ))}
                </div>
              ) : (
                <PageEmpty
                  title="No widgets yet"
                  message="Add a widget to begin composing a runtime dashboard."
                  action={
                    <button className="primary-button" type="button" onClick={addWidget} disabled={!biEditMode}>
                      Add widget
                    </button>
                  }
                />
              )}
            </div>
          ) : (
            <PageEmpty title="No dashboard selected" message="Create or select a dashboard to continue." />
          )}
        </Panel>

        <div className="detail-stack bi-inspector-stack">
          <Panel
            title={activeWidget?.title || "Widget"}
            className="bi-inspector-panel bi-compact-panel"
            actions={
              activeWidget ? (
                <div className="panel-actions-inline">
                  <button className="primary-button" type="button" onClick={() => runWidget(activeWidget)} disabled={!selectedModel || activeWidgetMeasures.length === 0}>
                    <RefreshCw className="button-icon" aria-hidden="true" />
                    Run
                  </button>
                  <button className="ghost-button" type="button" onClick={removeWidget} disabled={!biEditMode}>
                    <Trash2 className="button-icon" aria-hidden="true" />
                    Delete
                  </button>
                </div>
              ) : null
            }
          >
            {activeWidget && activeBoard ? (
              <div className="detail-stack">
                <div className="bi-panel-meta">
                  <span>{selectedModel || "No model"}</span>
                  <span>{formatValue(activeWidgetDimensions.length)} dimensions</span>
                  <span>{formatValue(activeWidgetMeasures.length)} measures</span>
                  <span>{getBiPalette(activeWidget.visualConfig?.paletteId).label}</span>
                  <span>{formatValue(activeWidget.lastRunAt || "Not run yet")}</span>
                </div>
                <div className="form-grid compact">
                  <label className="field">
                    <span>Title</span>
                    <input className="text-input" type="text" value={activeWidget.title} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { title: event.target.value })} disabled={!biEditMode} />
                  </label>
                  <label className="field">
                    <span>Chart</span>
                    <select className="select-input" value={activeWidget.chartType} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { chartType: event.target.value })} disabled={!biEditMode}>
                      <option value="bar">Bar</option>
                      <option value="line">Line</option>
                      <option value="pie">Pie</option>
                      <option value="table">Table</option>
                    </select>
                  </label>
                  <label className="field">
                    <span>Size</span>
                    <select className="select-input" value={activeWidget.size} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { size: event.target.value })} disabled={!biEditMode}>
                      <option value="small">Small</option>
                      <option value="wide">Wide</option>
                      <option value="tall">Tall</option>
                      <option value="large">Large</option>
                    </select>
                  </label>
                  <label className="field">
                    <span>Palette</span>
                    <select className="select-input" value={activeWidget.visualConfig?.paletteId || BI_PALETTES[0].id} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { visualConfig: { ...activeWidget.visualConfig, paletteId: event.target.value } })} disabled={!biEditMode}>
                      {BI_PALETTES.map((palette) => (
                        <option key={palette.id} value={palette.id}>
                          {palette.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <div className="field field-full bi-selection-field">
                    <div className="bi-selection-head">
                      <span>Dimensions</span>
                      <small>{formatValue(activeWidgetDimensions.length)} selected</small>
                    </div>
                    {activeWidgetDimensions.length > 0 ? (
                      <div className="field-pill-list bi-selected-field-list">
                        {activeWidgetDimensions.map((value) => (
                          <button
                            key={value}
                            className={`field-pill active ${!biEditMode ? "static" : ""}`}
                            type="button"
                            onClick={() => assignField(value, "dimension")}
                            disabled={!biEditMode}
                            title={value}
                          >
                            {formatSemanticMember(value)}
                          </button>
                        ))}
                      </div>
                    ) : (
                      <div className="empty-box bi-selection-empty">
                        {detailLoading ? "Loading dimensions..." : "Select dimensions from the field library."}
                      </div>
                    )}
                  </div>
                  <div className="field field-full bi-selection-field">
                    <div className="bi-selection-head">
                      <span>Measures</span>
                      <small>{formatValue(activeWidgetMeasures.length)} selected</small>
                    </div>
                    {activeWidgetMeasures.length > 0 ? (
                      <div className="field-pill-list bi-selected-field-list">
                        {activeWidgetMeasures.map((value) => (
                          <button
                            key={value}
                            className={`field-pill active ${!biEditMode ? "static" : ""}`}
                            type="button"
                            onClick={() => assignField(value, "measure")}
                            disabled={!biEditMode}
                            title={value}
                          >
                            {formatSemanticMember(value)}
                          </button>
                        ))}
                      </div>
                    ) : (
                      <div className="empty-box bi-selection-empty">
                        {detailLoading ? "Loading measures..." : "Select measures from the field library."}
                      </div>
                    )}
                  </div>
                  <label className="field">
                    <span>Rows</span>
                    <input className="text-input" type="number" min="1" value={activeWidget.limit} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { limit: event.target.value })} disabled={!biEditMode} />
                  </label>
                  <label className="field">
                    <span>Time field</span>
                    <select className="select-input" value={activeWidget.timeDimension} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { timeDimension: event.target.value })} disabled={!biEditMode}>
                      <option value="">No time dimension</option>
                      {dateDimensionOptions.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="field">
                    <span>Grain</span>
                    <select className="select-input" value={activeWidget.timeGrain} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { timeGrain: event.target.value })} disabled={!biEditMode || !activeWidget.timeDimension}>
                      {BI_TIME_GRAINS.map((item) => (
                        <option key={item.value || "none"} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <div className="panel-actions-inline">
                  <button className="ghost-button" type="button" onClick={exportWidget} disabled={!activeWidget.result}>
                    <Download className="button-icon" aria-hidden="true" />
                    Export CSV
                  </button>
                  <button className="ghost-button" type="button" onClick={copyGeneratedSql} disabled={!activeWidget.result?.generated_sql}>
                    <Copy className="button-icon" aria-hidden="true" />
                    Copy SQL
                  </button>
                </div>
                {biEditMode || activeWidget.description ? (
                  <details className="diagnostics-disclosure">
                    <summary>Notes</summary>
                    <textarea className="textarea-input" value={activeWidget.description} onChange={(event) => updateWidget(activeBoard.id, activeWidget.id, { description: event.target.value })} disabled={!biEditMode} />
                  </details>
                ) : null}
                {activeWidget.result?.generated_sql ? (
                  <details className="diagnostics-disclosure">
                    <summary>Generated SQL</summary>
                    <pre className="code-block compact">{activeWidget.result.generated_sql}</pre>
                  </details>
                ) : null}
              </div>
            ) : (
              <PageEmpty title="No active widget" message="Select or create a widget to configure the dashboard." />
            )}
          </Panel>
        </div>
      </section>
    </div>
  );
}
