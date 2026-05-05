import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";

import { AnalyticsChartPreview } from "../../components/analytics/AnalyticsChartPreview.jsx";
import { AnalyticsResultTable } from "../../components/analytics/AnalyticsResultTable.jsx";
import {
  copyText,
  csvForResult,
  downloadText,
  safeFileName,
} from "../../components/analytics/analyticsArtifacts.js";
import { getErrorMessage, formatDateTime, formatValue } from "../../lib/format.js";
import {
  getDashboardSemanticModel,
  listDashboardSemanticModels,
  readDashboardState,
  runDashboardWidget,
  writeDashboardState,
} from "../../services/dashboardService.js";
import {
  DASHBOARD_PALETTES,
  DASHBOARD_TIME_GRAINS,
  DASHBOARD_TIME_PRESETS,
  DASHBOARD_WIDGET_TYPES,
  canRunDashboardWidget,
  createDashboardBoard,
  createDashboardFilter,
  createDashboardOrder,
  createDashboardWidget,
  duplicateDashboardBoard,
  extractDashboardSemanticResources,
  fieldLabel,
  isDateLikeDashboardField,
  normalizeDashboardImportPayload,
  normalizeDashboardState,
  removeDashboardBoard,
  reorderDashboardWidgets,
  serializeDashboardExport,
  touchDashboardBoard,
  touchDashboardWidget,
} from "./dashboardModel.js";

const EMPTY_RESOURCE_STATE = {
  datasets: [],
  modelMetrics: [],
  fields: [],
  dimensions: [],
  measures: [],
  metrics: [],
};

const FILTER_OPERATORS = [
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

const DATE_FILTER_OPERATORS = [
  { value: "indaterange", label: "In date range" },
  { value: "notindaterange", label: "Not in date range" },
  { value: "set", label: "Is set" },
  { value: "notset", label: "Is not set" },
];

const DASHBOARD_PANEL_CLOSED = "closed";
const DASHBOARD_PANEL_CONTEXT = "context";
const DASHBOARD_PANEL_CREATE_WIDGET = "create-widget";
const DASHBOARD_PANEL_EDIT_WIDGET = "edit-widget";

const DATE_RANGE_OPERATORS = new Set(["indaterange", "notindaterange"]);
const CUSTOM_TIME_PRESETS = new Set(["custom_between", "custom_before", "custom_after", "custom_on"]);

export function DashboardBoardPage({ dashboardId }) {
  const [studioState, setStudioState] = useState(() => readDashboardState());
  const [activeWidgetId, setActiveWidgetId] = useState("");
  const [models, setModels] = useState([]);
  const [modelsLoading, setModelsLoading] = useState(true);
  const [modelsError, setModelsError] = useState("");
  const [modelDetail, setModelDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [notice, setNotice] = useState("");
  const [fieldSearch, setFieldSearch] = useState("");
  const [panelMode, setPanelMode] = useState(DASHBOARD_PANEL_CLOSED);
  const [layoutMode, setLayoutMode] = useState(false);
  const [draggedWidgetId, setDraggedWidgetId] = useState("");
  const [widgetMenu, setWidgetMenu] = useState(null);
  const [copiedKey, setCopiedKey] = useState("");
  const deferredFieldSearch = useDeferredValue(fieldSearch);

  const boards = studioState.boards;
  const activeBoard =
    boards.find((board) => board.id === studioState.activeBoardId) || boards[0] || null;
  const activeWidget =
    activeBoard?.widgets.find((widget) => widget.id === activeWidgetId) ||
    activeBoard?.widgets[0] ||
    null;
  const defaultModelName = models.find((model) => model.default)?.name || models[0]?.name || "";
  const availableModelRefs = useMemo(
    () => new Set(models.flatMap((model) => [model.name, model.id].filter(Boolean))),
    [models],
  );
  const selectedModel = activeBoard?.selectedModel || "";
  const semanticResources = useMemo(
    () => (modelDetail ? extractDashboardSemanticResources(modelDetail) : EMPTY_RESOURCE_STATE),
    [modelDetail],
  );
  const measureFields = useMemo(
    () => [...semanticResources.measures, ...semanticResources.metrics],
    [semanticResources.measures, semanticResources.metrics],
  );
  const dateFields = useMemo(
    () => semanticResources.dimensions.filter((field) => isDateLikeDashboardField(field)),
    [semanticResources.dimensions],
  );
  const filteredDatasets = useMemo(
    () => filterSemanticDatasets(semanticResources, deferredFieldSearch),
    [semanticResources, deferredFieldSearch],
  );
  const runnableCount =
    activeBoard?.widgets.filter((widget) => canRunDashboardWidget(widget, activeBoard)).length || 0;
  const panelOpen = panelMode !== DASHBOARD_PANEL_CLOSED;
  const widgetMenuTarget =
    widgetMenu && activeBoard?.widgets.find((widget) => widget.id === widgetMenu.widgetId);

  useEffect(() => {
    writeDashboardState(studioState);
  }, [studioState]);

  useEffect(() => {
    if (!notice || typeof window === "undefined") {
      return undefined;
    }
    const timeoutId = window.setTimeout(() => setNotice(""), 3200);
    return () => window.clearTimeout(timeoutId);
  }, [notice]);

  useEffect(() => {
    if (!widgetMenu || typeof window === "undefined") {
      return undefined;
    }

    function closeOnClick() {
      setWidgetMenu(null);
    }

    function closeOnEscape(event) {
      if (event.key === "Escape") {
        setWidgetMenu(null);
      }
    }

    window.addEventListener("click", closeOnClick);
    window.addEventListener("resize", closeOnClick);
    window.addEventListener("scroll", closeOnClick, true);
    window.addEventListener("keydown", closeOnEscape);
    return () => {
      window.removeEventListener("click", closeOnClick);
      window.removeEventListener("resize", closeOnClick);
      window.removeEventListener("scroll", closeOnClick, true);
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, [widgetMenu]);

  useEffect(() => {
    let cancelled = false;
    setModelsLoading(true);
    setModelsError("");
    listDashboardSemanticModels()
      .then((items) => {
        if (!cancelled) {
          setModels(items);
        }
      })
      .catch((error) => {
        if (!cancelled) {
          setModels([]);
          setModelsError(getErrorMessage(error));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setModelsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!dashboardId) {
      return;
    }
    if (boards.some((board) => board.id === dashboardId) && studioState.activeBoardId !== dashboardId) {
      setStudioState((current) => ({ ...current, activeBoardId: dashboardId }));
    }
  }, [boards, dashboardId, studioState.activeBoardId]);

  useEffect(() => {
    if (!activeBoard) {
      return;
    }
    if (!activeBoard.selectedModel && defaultModelName) {
      updateBoard(activeBoard.id, { selectedModel: defaultModelName });
    }
    if (
      activeBoard.selectedModel &&
      defaultModelName &&
      availableModelRefs.size > 0 &&
      !availableModelRefs.has(activeBoard.selectedModel)
    ) {
      updateBoard(activeBoard.id, { selectedModel: defaultModelName });
    }
    if (!activeWidget || !activeBoard.widgets.some((widget) => widget.id === activeWidgetId)) {
      setActiveWidgetId(activeBoard.widgets[0]?.id || "");
    }
  }, [activeBoard, activeWidget, activeWidgetId, availableModelRefs, defaultModelName]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedModel) {
      setModelDetail(null);
      setDetailError("");
      setDetailLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setDetailLoading(true);
    setDetailError("");
    getDashboardSemanticModel(selectedModel)
      .then((payload) => {
        if (!cancelled) {
          setModelDetail(payload);
        }
      })
      .catch((error) => {
        if (!cancelled) {
          setModelDetail(null);
          setDetailError(getErrorMessage(error));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setDetailLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [selectedModel]);

  function updateBoard(boardId, updates) {
    setStudioState((current) => ({
      ...current,
      boards: current.boards.map((board) =>
        board.id === boardId ? touchDashboardBoard(board, updates) : board,
      ),
    }));
  }

  function updateWidget(boardId, widgetId, updates) {
    setStudioState((current) => ({
      ...current,
      boards: current.boards.map((board) =>
        board.id === boardId
          ? touchDashboardBoard(board, {
              widgets: board.widgets.map((widget) =>
                widget.id === widgetId ? touchDashboardWidget(widget, updates) : widget,
              ),
            })
          : board,
      ),
    }));
  }

  function selectBoard(boardId) {
    setStudioState((current) => ({ ...current, activeBoardId: boardId }));
    const board = boards.find((item) => item.id === boardId);
    setActiveWidgetId(board?.widgets[0]?.id || "");
    setPanelMode(DASHBOARD_PANEL_CLOSED);
  }

  function addBoard() {
    const board = createDashboardBoard({
      name: `Runtime dashboard ${boards.length + 1}`,
      selectedModel: selectedModel || defaultModelName,
      widgets: [createDashboardWidget(defaultWidgetSeed(semanticResources, "bar"))],
    });
    setStudioState((current) => ({
      boards: [board, ...current.boards],
      activeBoardId: board.id,
    }));
    setActiveWidgetId(board.widgets[0]?.id || "");
    setPanelMode(DASHBOARD_PANEL_CLOSED);
    setNotice("Created a dashboard draft.");
  }

  function duplicateBoard() {
    if (!activeBoard) {
      return;
    }
    const board = duplicateDashboardBoard(activeBoard);
    setStudioState((current) => ({
      schemaVersion: current.schemaVersion,
      boards: [board, ...current.boards],
      activeBoardId: board.id,
    }));
    setActiveWidgetId(board.widgets[0]?.id || "");
    setPanelMode(DASHBOARD_PANEL_CLOSED);
    setNotice("Duplicated dashboard draft.");
  }

  function deleteBoard() {
    if (!activeBoard) {
      return;
    }
    const next = removeDashboardBoard(studioState, activeBoard.id);
    setStudioState(next);
    setActiveWidgetId(next.boards[0]?.widgets[0]?.id || "");
    setPanelMode(DASHBOARD_PANEL_CLOSED);
    setNotice(boards.length === 1 ? "Reset to a fresh dashboard." : "Deleted dashboard draft.");
  }

  function resetDashboards() {
    const next = normalizeDashboardState(null);
    const seededBoard = {
      ...next.boards[0],
      selectedModel: selectedModel || defaultModelName,
    };
    setStudioState({
      ...next,
      activeBoardId: seededBoard.id,
      boards: [seededBoard],
    });
    setActiveWidgetId(seededBoard.widgets[0]?.id || "");
    setPanelMode(DASHBOARD_PANEL_CLOSED);
    setNotice("Reset dashboard drafts.");
  }

  async function importDashboardJson(file) {
    if (!file) {
      return;
    }
    try {
      const payload = JSON.parse(await file.text());
      const imported = normalizeDashboardImportPayload(payload);
      const existingIds = new Set(boards.map((board) => board.id));
      const boardsToImport = imported.boards.map((board) =>
        existingIds.has(board.id) ? duplicateDashboardBoard(board, { name: board.name }) : board,
      );
      setStudioState((current) => ({
        schemaVersion: imported.schemaVersion,
        boards: [...boardsToImport, ...current.boards],
        activeBoardId: boardsToImport[0]?.id || current.activeBoardId,
      }));
      setActiveWidgetId(boardsToImport[0]?.widgets[0]?.id || "");
      setPanelMode(DASHBOARD_PANEL_CLOSED);
      setNotice(`Imported ${imported.boards.length} dashboard${imported.boards.length === 1 ? "" : "s"}.`);
    } catch (error) {
      setNotice(`Import failed: ${getErrorMessage(error)}`);
    }
  }

  function selectModel(modelName) {
    if (!activeBoard) {
      return;
    }
    updateBoard(activeBoard.id, {
      selectedModel: modelName,
      lastRefreshedAt: null,
      widgets: activeBoard.widgets.map((widget) =>
        touchDashboardWidget(widget, {
          result: null,
          error: "",
          running: false,
          lastRunAt: null,
        }),
      ),
    });
  }

  function addWidget(type = "bar") {
    if (!activeBoard) {
      return;
    }
    const widget = createDashboardWidget(defaultWidgetSeed(semanticResources, type, activeBoard.widgets.length + 1));
    updateBoard(activeBoard.id, { widgets: [...activeBoard.widgets, widget] });
    setActiveWidgetId(widget.id);
    setPanelMode(DASHBOARD_PANEL_CREATE_WIDGET);
    setNotice(`Added ${type === "stat" ? "KPI" : type} widget.`);
  }

  function removeWidget(widgetId = activeWidget?.id) {
    if (!activeBoard || !widgetId) {
      return;
    }
    const widgets = activeBoard.widgets.filter((widget) => widget.id !== widgetId);
    updateBoard(activeBoard.id, { widgets });
    if (activeWidgetId === widgetId) {
      setActiveWidgetId(widgets[0]?.id || "");
      setPanelMode(DASHBOARD_PANEL_CLOSED);
    }
    setNotice("Removed widget.");
  }

  function assignField(field) {
    if (!activeBoard || !field) {
      return;
    }
    const target = activeWidget || activeBoard.widgets[0];
    const bucket = field.kind === "dimension" ? "dimensions" : "measures";
    if (!target) {
      const widget = createDashboardWidget({
        ...defaultWidgetSeed(semanticResources, "bar"),
        [bucket]: [field.value],
      });
      updateBoard(activeBoard.id, { widgets: [...activeBoard.widgets, widget] });
      setActiveWidgetId(widget.id);
      return;
    }
    const selected = new Set(target[bucket] || []);
    if (selected.has(field.value)) {
      selected.delete(field.value);
    } else {
      selected.add(field.value);
    }
    const nextValues = [...selected];
    const nextWidget = { ...target, [bucket]: nextValues };
    updateWidget(activeBoard.id, target.id, {
      [bucket]: nextValues,
      chartX: bucket === "dimensions" ? resolveSelectedChartBinding(target.chartX, chartDimensionMembers(nextWidget)) : target.chartX,
      chartY: bucket === "measures" ? resolveSelectedChartBinding(target.chartY, nextValues) : target.chartY,
    });
  }

  async function handleRunWidget(widget) {
    if (!activeBoard || !canRunDashboardWidget(widget, activeBoard)) {
      setNotice("Select a semantic model and at least one measure or metric before running.");
      return;
    }
    const boardSnapshot = activeBoard;
    updateWidget(activeBoard.id, widget.id, { running: true, error: "" });
    try {
      const result = await runDashboardWidget(boardSnapshot, widget);
      const completedAt = new Date().toISOString();
      updateWidget(boardSnapshot.id, widget.id, {
        running: false,
        error: "",
        result,
        lastRunAt: completedAt,
      });
      updateBoard(boardSnapshot.id, { lastRefreshedAt: completedAt });
    } catch (error) {
      updateWidget(boardSnapshot.id, widget.id, {
        running: false,
        result: null,
        error: getErrorMessage(error),
      });
    }
  }

  async function handleRunAll() {
    const boardSnapshot = activeBoard;
    const widgets = boardSnapshot?.widgets.filter((widget) => canRunDashboardWidget(widget, boardSnapshot)) || [];
    if (widgets.length === 0) {
      setNotice("No runnable widgets yet.");
      return;
    }
    widgets.forEach((widget) => updateWidget(boardSnapshot.id, widget.id, { running: true, error: "" }));
    await Promise.all(
      widgets.map(async (widget) => {
        try {
          const result = await runDashboardWidget(boardSnapshot, widget);
          updateWidget(boardSnapshot.id, widget.id, {
            running: false,
            error: "",
            result,
            lastRunAt: new Date().toISOString(),
          });
        } catch (error) {
          updateWidget(boardSnapshot.id, widget.id, {
            running: false,
            result: null,
            error: getErrorMessage(error),
          });
        }
      }),
    );
    updateBoard(boardSnapshot.id, { lastRefreshedAt: new Date().toISOString() });
    setNotice("Refreshed runnable widgets.");
  }

  function handleDrop(targetWidgetId) {
    if (!activeBoard || !draggedWidgetId || draggedWidgetId === targetWidgetId) {
      setDraggedWidgetId("");
      return;
    }
    updateBoard(activeBoard.id, {
      widgets: reorderDashboardWidgets(activeBoard.widgets, draggedWidgetId, targetWidgetId),
    });
    setDraggedWidgetId("");
  }

  function moveWidget(widgetId, direction) {
    if (!activeBoard) {
      return;
    }
    const index = activeBoard.widgets.findIndex((widget) => widget.id === widgetId);
    const target = index + direction;
    if (index < 0 || target < 0 || target >= activeBoard.widgets.length) {
      return;
    }
    const next = [...activeBoard.widgets];
    [next[index], next[target]] = [next[target], next[index]];
    updateBoard(activeBoard.id, { widgets: next });
  }

  async function copyGeneratedSql() {
    if (!activeWidget?.result?.generated_sql) {
      return;
    }
    if (await copyText(activeWidget.result.generated_sql)) {
      setCopiedKey("sql");
      window.setTimeout(() => setCopiedKey(""), 1200);
    }
  }

  function exportWidgetCsv(widget) {
    if (!widget?.result) {
      return;
    }
    downloadText({
      fileName: safeFileName(widget.title || "dashboard-widget", "csv"),
      mimeType: "text/csv;charset=utf-8",
      text: csvForResult(widget.result),
    });
  }

  function exportBoardJson() {
    if (!activeBoard) {
      return;
    }
    downloadText({
      fileName: safeFileName(activeBoard.name || "runtime-dashboard", "json"),
      mimeType: "application/json",
      text: JSON.stringify(serializeDashboardExport(activeBoard), null, 2),
    });
  }

  function toggleLayoutMode() {
    setLayoutMode((current) => !current);
  }

  function toggleContextPanel() {
    setPanelMode((current) =>
      current === DASHBOARD_PANEL_CONTEXT ? DASHBOARD_PANEL_CLOSED : DASHBOARD_PANEL_CONTEXT,
    );
  }

  function closePanel() {
    setPanelMode(DASHBOARD_PANEL_CLOSED);
  }

  function openWidgetMenu(event, widget) {
    event.preventDefault();
    event.stopPropagation();
    const maxX = typeof window === "undefined" ? event.clientX : window.innerWidth - 148;
    const maxY = typeof window === "undefined" ? event.clientY : window.innerHeight - 92;
    setActiveWidgetId(widget.id);
    setWidgetMenu({
      widgetId: widget.id,
      x: Math.max(8, Math.min(event.clientX, maxX)),
      y: Math.max(8, Math.min(event.clientY, maxY)),
    });
  }

  function runWidgetFromMenu(widget) {
    setWidgetMenu(null);
    void handleRunWidget(widget);
  }

  function removeWidgetFromMenu(widget) {
    setWidgetMenu(null);
    removeWidget(widget.id);
  }

  return (
    <section className="workspace-page workspace-page--dashboard dashboard-builder-next">
      <DashboardToolbar
        activeBoard={activeBoard}
        boards={boards}
        models={models}
        selectedModel={selectedModel}
        modelsLoading={modelsLoading}
        layoutMode={layoutMode}
        panelMode={panelMode}
        runnableCount={runnableCount}
        onSelectBoard={selectBoard}
        onSelectModel={selectModel}
        onCreateBoard={addBoard}
        onDuplicateBoard={duplicateBoard}
        onDeleteBoard={deleteBoard}
        onResetBoards={resetDashboards}
        onImportJson={(file) => void importDashboardJson(file)}
        onToggleLayout={toggleLayoutMode}
        onToggleContext={toggleContextPanel}
        onAddWidget={() => addWidget("bar")}
        onRunAll={() => void handleRunAll()}
        onExport={exportBoardJson}
      />

      {modelsError ? <div className="query-inline-error">{modelsError}</div> : null}
      {notice ? <div className="dashboard-notice">{notice}</div> : null}

      <div className={dashboardLayoutClass(panelOpen)}>
        <DashboardCanvas
          activeBoard={activeBoard}
          activeWidget={panelOpen ? activeWidget : null}
          semanticResources={semanticResources}
          layoutMode={layoutMode}
          draggedWidgetId={draggedWidgetId}
          onSelectWidget={(widgetId) => {
            setActiveWidgetId(widgetId);
            setPanelMode(DASHBOARD_PANEL_EDIT_WIDGET);
          }}
          onRunWidget={(widget) => void handleRunWidget(widget)}
          onAddWidget={addWidget}
          onDragStart={setDraggedWidgetId}
          onDrop={handleDrop}
          onMoveWidget={moveWidget}
          onExportWidget={exportWidgetCsv}
          onOpenWidgetMenu={openWidgetMenu}
        />

        {panelMode === DASHBOARD_PANEL_CONTEXT ? (
          <DashboardFieldDrawer
            resources={semanticResources}
            filteredDatasets={filteredDatasets}
            fieldSearch={fieldSearch}
            detailLoading={detailLoading}
            detailError={detailError}
            activeWidget={activeWidget}
            editMode
            onSearchChange={setFieldSearch}
            onAssignField={assignField}
            onAddWidget={addWidget}
            onClose={closePanel}
            onBackToWidget={
              activeWidget ? () => setPanelMode(DASHBOARD_PANEL_EDIT_WIDGET) : null
            }
          />
        ) : null}

        {panelMode === DASHBOARD_PANEL_CREATE_WIDGET || panelMode === DASHBOARD_PANEL_EDIT_WIDGET ? (
          <DashboardInspector
            activeBoard={activeBoard}
            activeWidget={activeWidget}
            semanticResources={semanticResources}
            measureFields={measureFields}
            dateFields={dateFields}
            editMode
            panelMode={panelMode}
            copiedKey={copiedKey}
            onUpdateBoard={updateBoard}
            onUpdateWidget={updateWidget}
            onRunWidget={(widget) => void handleRunWidget(widget)}
            onRemoveWidget={removeWidget}
            onAssignField={assignField}
            onOpenContext={() => setPanelMode(DASHBOARD_PANEL_CONTEXT)}
            onClose={closePanel}
            onCopySql={() => void copyGeneratedSql()}
            onExportWidget={() => exportWidgetCsv(activeWidget)}
          />
        ) : null}
      </div>

      {widgetMenuTarget && typeof document !== "undefined"
        ? createPortal(
            <DashboardWidgetContextMenu
              widget={widgetMenuTarget}
              board={activeBoard}
              x={widgetMenu.x}
              y={widgetMenu.y}
              onRun={() => runWidgetFromMenu(widgetMenuTarget)}
              onDelete={() => removeWidgetFromMenu(widgetMenuTarget)}
            />,
            document.body,
          )
        : null}
    </section>
  );
}

function DashboardToolbar({
  activeBoard,
  boards,
  models,
  selectedModel,
  modelsLoading,
  layoutMode,
  panelMode,
  runnableCount,
  onSelectBoard,
  onSelectModel,
  onCreateBoard,
  onDuplicateBoard,
  onDeleteBoard,
  onResetBoards,
  onImportJson,
  onToggleLayout,
  onToggleContext,
  onAddWidget,
  onRunAll,
  onExport,
}) {
  return (
    <div className="dashboard-command-bar">
      <div className="dashboard-command-title">
        <strong>{activeBoard?.name || "Runtime dashboard"}</strong>
        <span>
          {selectedModel || "No semantic model"} / {activeBoard?.widgets.length || 0} widgets / {runnableCount} runnable
        </span>
      </div>
      <div className="dashboard-command-controls">
        <div className="dashboard-command-selects">
          <select
            aria-label="Dashboard"
            value={activeBoard?.id || ""}
            onChange={(event) => onSelectBoard(event.target.value)}
          >
            {boards.map((board) => (
              <option key={board.id} value={board.id}>
                {board.name}
              </option>
            ))}
          </select>
          <select
            aria-label="Semantic model"
            value={selectedModel}
            disabled={modelsLoading || !activeBoard}
            onChange={(event) => onSelectModel(event.target.value)}
          >
            {!selectedModel ? <option value="">Choose model</option> : null}
            {models.map((model) => (
              <option key={model.id || model.name} value={model.name}>
                {model.name}
              </option>
            ))}
          </select>
        </div>
        <div className="dashboard-command-primary-actions">
          <button className="primary-action" type="button" onClick={onAddWidget} disabled={!activeBoard}>
            Add chart
          </button>
          <button type="button" onClick={onToggleLayout} disabled={!activeBoard}>
            {layoutMode ? "Done layout" : "Layout"}
          </button>
          <button type="button" onClick={onToggleContext} disabled={!activeBoard}>
            {panelMode === DASHBOARD_PANEL_CONTEXT ? "Hide context" : "Context"}
          </button>
          <button type="button" onClick={onRunAll} disabled={runnableCount === 0}>Refresh</button>
        </div>
        <details className="dashboard-command-menu">
          <summary>More</summary>
          <div className="dashboard-command-menu-popover">
            <button type="button" onClick={onCreateBoard}>New dashboard</button>
            <button type="button" onClick={onDuplicateBoard} disabled={!activeBoard}>Duplicate</button>
            <button type="button" onClick={onExport} disabled={!activeBoard}>Export JSON</button>
            <label className="dashboard-upload-button">
              Import JSON
              <input
                type="file"
                accept="application/json"
                onChange={(event) => {
                  const file = event.target.files?.[0] || null;
                  if (file) {
                    onImportJson(file);
                  }
                  event.target.value = "";
                }}
              />
            </label>
            <button type="button" onClick={onResetBoards}>Reset drafts</button>
            <button type="button" onClick={onDeleteBoard} disabled={!activeBoard}>Delete dashboard</button>
          </div>
        </details>
      </div>
    </div>
  );
}

function DashboardFieldDrawer({
  resources,
  filteredDatasets,
  fieldSearch,
  detailLoading,
  detailError,
  activeWidget,
  editMode,
  onSearchChange,
  onAssignField,
  onAddWidget,
  onClose,
  onBackToWidget,
}) {
  const [expandedGroups, setExpandedGroups] = useState({});
  const hasSearch = Boolean(String(fieldSearch || "").trim());

  function isGroupOpen(groupId, index = 0) {
    if (hasSearch) {
      return true;
    }
    return Object.prototype.hasOwnProperty.call(expandedGroups, groupId)
      ? expandedGroups[groupId]
      : index === 0;
  }

  function toggleGroup(groupId, index = 0) {
    const currentlyOpen = isGroupOpen(groupId, index);
    setExpandedGroups((current) => ({ ...current, [groupId]: !currentlyOpen }));
  }

  function setAllGroups(open) {
    const next = filteredDatasets.reduce((accumulator, dataset) => {
      accumulator[dataset.name] = open;
      return accumulator;
    }, {});
    if (resources.modelMetrics.length > 0) {
      next["model-metrics"] = open;
    }
    setExpandedGroups(next);
  }

  return (
    <aside className="dashboard-field-drawer">
      <div className="dashboard-panel-head">
        <div>
          <p className="eyebrow">Fields</p>
          <h3>Semantic context</h3>
        </div>
        <div className="dashboard-panel-actions">
          <span>{resources.fields.length}</span>
          {onBackToWidget ? <button type="button" onClick={onBackToWidget}>Widget</button> : null}
          {onClose ? <button type="button" onClick={onClose}>Close</button> : null}
        </div>
      </div>
      <input
        type="search"
        value={fieldSearch}
        onChange={(event) => onSearchChange(event.target.value)}
        placeholder="Find model, table, metric, or field"
      />
      {detailError ? <div className="query-inline-error">{detailError}</div> : null}
      {detailLoading ? <div className="dashboard-empty-card">Loading semantic model.</div> : null}
      <div className="dashboard-quick-add">
        {["bar", "line", "stat", "table", "note"].map((type) => (
          <button key={type} type="button" disabled={!editMode} onClick={() => onAddWidget(type)}>
            + {type === "stat" ? "KPI" : type}
          </button>
        ))}
      </div>
      <div className="dashboard-field-toolbar">
        <button type="button" onClick={() => setAllGroups(true)}>Expand all</button>
        <button type="button" onClick={() => setAllGroups(false)}>Collapse all</button>
      </div>
      <div className="dashboard-field-list">
        {filteredDatasets.map((dataset, index) => (
          <FieldDatasetGroup
            key={dataset.name}
            dataset={dataset}
            expanded={isGroupOpen(dataset.name, index)}
            activeWidget={activeWidget}
            editMode={editMode}
            onToggle={() => toggleGroup(dataset.name, index)}
            onAssignField={onAssignField}
          />
        ))}
        {resources.modelMetrics.length > 0 ? (
          <FieldStandaloneGroup
            id="model-metrics"
            title="Model metrics"
            count={resources.modelMetrics.length}
            expanded={isGroupOpen("model-metrics", filteredDatasets.length)}
            fields={resources.modelMetrics}
            activeWidget={activeWidget}
            editMode={editMode}
            onToggle={() => toggleGroup("model-metrics", filteredDatasets.length)}
            onAssignField={onAssignField}
          />
        ) : null}
        {!detailLoading && filteredDatasets.length === 0 && resources.modelMetrics.length === 0 ? (
          <div className="dashboard-empty-card">No fields found for this model.</div>
        ) : null}
      </div>
    </aside>
  );
}

function FieldDatasetGroup({
  dataset,
  expanded,
  activeWidget,
  editMode,
  onToggle,
  onAssignField,
}) {
  const sections = [
    { id: "dimensions", title: "Dimensions", fields: dataset.dimensions },
    { id: "measures", title: "Measures", fields: dataset.measures },
    { id: "metrics", title: "Metrics", fields: dataset.metrics },
  ].filter((section) => section.fields.length > 0);
  return (
    <div className={`dashboard-field-group ${expanded ? "expanded" : ""}`}>
      <button className="dashboard-field-group-head" type="button" onClick={onToggle}>
        <span className="dashboard-field-caret" aria-hidden="true" />
        <strong>{dataset.name}</strong>
        <small>{dataset.dimensions.length}D / {dataset.measures.length + dataset.metrics.length}M</small>
      </button>
      {dataset.relationName ? <p>{dataset.relationName}</p> : null}
      {expanded ? (
        <div className="dashboard-field-group-body">
          {sections.map((section) => (
            <FieldSection
              key={`${dataset.name}-${section.id}`}
              title={section.title}
              fields={section.fields}
              activeWidget={activeWidget}
              editMode={editMode}
              onAssignField={onAssignField}
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}

function FieldStandaloneGroup({
  title,
  count,
  expanded,
  fields,
  activeWidget,
  editMode,
  onToggle,
  onAssignField,
}) {
  return (
    <div className={`dashboard-field-group ${expanded ? "expanded" : ""}`}>
      <button className="dashboard-field-group-head" type="button" onClick={onToggle}>
        <span className="dashboard-field-caret" aria-hidden="true" />
        <strong>{title}</strong>
        <small>{count}</small>
      </button>
      {expanded ? (
        <div className="dashboard-field-group-body">
          <FieldSection
            title="Metrics"
            fields={fields}
            activeWidget={activeWidget}
            editMode={editMode}
            onAssignField={onAssignField}
          />
        </div>
      ) : null}
    </div>
  );
}

function FieldSection({ title, fields, activeWidget, editMode, onAssignField }) {
  return (
    <section className="dashboard-field-section">
      <div className="dashboard-field-section-head">
        <span>{title}</span>
        <small>{fields.length}</small>
      </div>
      <FieldButtons
        fields={fields}
        activeWidget={activeWidget}
        editMode={editMode}
        onAssignField={onAssignField}
      />
    </section>
  );
}

function FieldButtons({ fields, activeWidget, editMode, onAssignField }) {
  return (
    <div className="dashboard-field-buttons">
      {fields.map((field) => {
        const selected = field.kind === "dimension"
          ? activeWidget?.dimensions?.includes(field.value)
          : activeWidget?.measures?.includes(field.value);
        return (
          <button
            key={field.id}
            className={selected ? "active" : ""}
            type="button"
            disabled={!editMode}
            title={field.qualifiedLabel}
            onClick={() => onAssignField(field)}
          >
            <span>{field.label}</span>
            <small>{field.kind}</small>
          </button>
        );
      })}
    </div>
  );
}

function DashboardCanvas({
  activeBoard,
  activeWidget,
  semanticResources,
  layoutMode,
  draggedWidgetId,
  onSelectWidget,
  onRunWidget,
  onAddWidget,
  onDragStart,
  onDrop,
  onMoveWidget,
  onExportWidget,
  onOpenWidgetMenu,
}) {
  if (!activeBoard) {
    return <main className="dashboard-canvas-next"><div className="dashboard-empty-card">No dashboard selected.</div></main>;
  }
  return (
    <main className="dashboard-canvas-next">
      <div className="dashboard-canvas-head">
        <div>
          <h2>{activeBoard.name}</h2>
          <p>{activeBoard.description || "Build a lightweight semantic dashboard from governed runtime fields."}</p>
        </div>
        <span>{activeBoard.lastRefreshedAt ? `Updated ${formatDateTime(activeBoard.lastRefreshedAt)}` : "Not refreshed"}</span>
      </div>

      {activeBoard.widgets.length > 0 ? (
        <div className="dashboard-tile-grid">
          {activeBoard.widgets.map((widget, index) => (
            <DashboardWidgetTile
              key={widget.id}
              widget={widget}
              index={index}
              total={activeBoard.widgets.length}
              active={widget.id === activeWidget?.id}
              board={activeBoard}
              semanticResources={semanticResources}
              layoutMode={layoutMode}
              dragged={widget.id === draggedWidgetId}
              onSelect={() => onSelectWidget(widget.id)}
              onRun={() => onRunWidget(widget)}
              onDragStart={() => onDragStart(widget.id)}
              onDrop={() => onDrop(widget.id)}
              onMoveWidget={onMoveWidget}
              onExportWidget={() => onExportWidget(widget)}
              onOpenMenu={(event) => onOpenWidgetMenu(event, widget)}
            />
          ))}
        </div>
      ) : (
        <div className="dashboard-empty-card">
          <strong>No widgets yet</strong>
          <span>Add a chart, KPI, table, or note to start the dashboard.</span>
          <button type="button" onClick={() => onAddWidget("bar")}>Add chart</button>
        </div>
      )}
    </main>
  );
}

function DashboardWidgetTile({
  widget,
  index,
  total,
  active,
  board,
  semanticResources,
  layoutMode,
  dragged,
  onSelect,
  onRun,
  onDragStart,
  onDrop,
  onMoveWidget,
  onExportWidget,
  onOpenMenu,
}) {
  const measure = resolveSelectedChartBinding(widget.chartY, widget.measures);
  const dimension = resolveSelectedChartBinding(widget.chartX, chartDimensionMembers(widget));
  const palette = DASHBOARD_PALETTES.find((item) => item.id === widget.paletteId) || DASHBOARD_PALETTES[0];
  return (
    <article
      className={`dashboard-tile ${active ? "active" : ""} ${dragged ? "dragged" : ""}`}
      draggable={layoutMode}
      onContextMenu={onOpenMenu}
      onDragStart={onDragStart}
      onDragOver={(event) => event.preventDefault()}
      onDrop={onDrop}
    >
      <div className="dashboard-tile-head">
        <button type="button" onClick={onSelect}>
          <span className={layoutMode ? "dashboard-drag-handle" : "dashboard-widget-kind"}>
            {layoutMode ? "Drag" : widget.type}
          </span>
          <strong>{widget.title}</strong>
        </button>
        <div>
          {layoutMode ? (
            <>
              <button type="button" disabled={index === 0} onClick={() => onMoveWidget(widget.id, -1)}>Up</button>
              <button type="button" disabled={index === total - 1} onClick={() => onMoveWidget(widget.id, 1)}>Down</button>
            </>
          ) : null}
          <button type="button" disabled={!canRunDashboardWidget(widget, board)} onClick={onRun}>
            {widget.running ? "Running" : "Run"}
          </button>
        </div>
      </div>
      <div className="dashboard-tile-meta">
        <span>{widget.type === "stat" ? "KPI" : widget.type}</span>
        {widget.measures.length > 0 ? <span>{widget.measures.map(fieldLabel).join(", ")}</span> : null}
        {widget.dimensions.length > 0 ? <span>by {widget.dimensions.map(fieldLabel).join(", ")}</span> : null}
      </div>
      {widget.error ? <div className="query-inline-error">{widget.error}</div> : null}
      <DashboardWidgetBody
        widget={widget}
        result={widget.result}
        dimension={dimension}
        measure={measure}
        semanticResources={semanticResources}
        palette={palette}
      />
      <div className="dashboard-tile-foot">
        <span>{widget.result ? `${formatValue(widget.result.rowCount)} rows` : "No result"}</span>
        <span>{widget.lastRunAt ? formatDateTime(widget.lastRunAt) : "Not run"}</span>
        {widget.result ? <button type="button" onClick={onExportWidget}>CSV</button> : null}
      </div>
    </article>
  );
}

function DashboardWidgetContextMenu({ widget, board, x, y, onRun, onDelete }) {
  const canRun = canRunDashboardWidget(widget, board);
  return (
    <div
      className="dashboard-widget-context-menu"
      style={{ left: x, top: y }}
      role="menu"
      aria-label={`${widget.title} actions`}
      onClick={(event) => event.stopPropagation()}
      onContextMenu={(event) => event.preventDefault()}
    >
      <button type="button" role="menuitem" disabled={!canRun} onClick={onRun}>
        Run
      </button>
      <button type="button" role="menuitem" onClick={onDelete}>
        Delete
      </button>
    </div>
  );
}

function DashboardWidgetBody({ widget, result, dimension, measure, semanticResources, palette }) {
  const chartDimension = chartDataDimensionKey(widget, dimension);
  if (widget.type === "note") {
    return <div className="dashboard-note">{widget.noteMarkdown || widget.description || "Write a note in the inspector."}</div>;
  }
  if (widget.running) {
    return <div className="dashboard-empty-card">Running semantic query...</div>;
  }
  if (!result) {
    return <div className="dashboard-empty-card">Pick fields and run this widget.</div>;
  }
  if (widget.type === "table") {
    return (
      <div className="dashboard-table-preview">
        <AnalyticsResultTable result={result} maxPreviewRows={8} />
      </div>
    );
  }
  return (
    <div className="dashboard-chart-preview">
      <AnalyticsChartPreview
        title={widget.title}
        result={result}
        metadata={result.metadata}
        visualization={{
          chartType: widget.type,
          x: chartDimension,
          y: widget.chartY ? [widget.chartY] : widget.measures,
        }}
        preferredDimension={chartDimension || dimension || semanticResources.dimensions[0]?.value}
        preferredMeasure={measure || measureFieldsFallback(semanticResources)}
        themeColors={palette.colors}
      />
    </div>
  );
}

function DashboardInspector({
  activeBoard,
  activeWidget,
  semanticResources,
  measureFields,
  dateFields,
  editMode,
  panelMode,
  copiedKey,
  onUpdateBoard,
  onUpdateWidget,
  onRunWidget,
  onRemoveWidget,
  onAssignField,
  onOpenContext,
  onClose,
  onCopySql,
  onExportWidget,
}) {
  if (!activeBoard || !activeWidget) {
    return <aside className="dashboard-inspector-next"><div className="dashboard-empty-card">Select a widget.</div></aside>;
  }
  const chartDimensionFieldOptions = selectedChartDimensionFields(activeWidget, semanticResources);
  const chartMeasureFieldOptions = selectedChartMeasureFields(activeWidget, measureFields);
  const chartXValue = resolveSelectedChartBinding(activeWidget.chartX, chartDimensionMembers(activeWidget));
  const chartYValue = resolveSelectedChartBinding(activeWidget.chartY, activeWidget.measures);
  return (
    <aside className="dashboard-inspector-next">
      <div className="dashboard-panel-head">
        <div>
          <p className="eyebrow">{panelMode === DASHBOARD_PANEL_CREATE_WIDGET ? "New widget" : "Widget"}</p>
          <h3>{activeWidget.title}</h3>
        </div>
        <div className="dashboard-panel-actions">
          <button type="button" onClick={onOpenContext}>Fields</button>
          <button type="button" disabled={!canRunDashboardWidget(activeWidget, activeBoard)} onClick={() => onRunWidget(activeWidget)}>
            Run
          </button>
          <button type="button" onClick={onClose}>Close</button>
        </div>
      </div>

      <div className="dashboard-inspector-section">
        <label>
          Dashboard name
          <input value={activeBoard.name} disabled={!editMode} onChange={(event) => onUpdateBoard(activeBoard.id, { name: event.target.value })} />
        </label>
        <label>
          Title
          <input value={activeWidget.title} disabled={!editMode} onChange={(event) => onUpdateWidget(activeBoard.id, activeWidget.id, { title: event.target.value })} />
        </label>
        <div className="dashboard-form-grid">
          <label>
            Type
            <select value={activeWidget.type} disabled={!editMode} onChange={(event) => onUpdateWidget(activeBoard.id, activeWidget.id, { type: event.target.value })}>
              {DASHBOARD_WIDGET_TYPES.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
            </select>
          </label>
          <label>
            Palette
            <select value={activeWidget.paletteId} disabled={!editMode} onChange={(event) => onUpdateWidget(activeBoard.id, activeWidget.id, { paletteId: event.target.value })}>
              {DASHBOARD_PALETTES.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
            </select>
          </label>
          <label>
            Limit
            <input type="number" min="1" value={activeWidget.limit} disabled={!editMode} onChange={(event) => onUpdateWidget(activeBoard.id, activeWidget.id, { limit: event.target.value })} />
          </label>
        </div>
      </div>

      <FilterEditor
        title="Dashboard filters"
        filters={activeBoard.globalFilters}
        filterLogic={activeBoard.globalFilterLogic}
        fields={semanticResources.fields}
        editMode={editMode}
        onLogicChange={(globalFilterLogic) => onUpdateBoard(activeBoard.id, { globalFilterLogic })}
        onAdd={() => onUpdateBoard(activeBoard.id, { globalFilters: [...activeBoard.globalFilters, createDashboardFilter(defaultFilterSeed(semanticResources.fields))] })}
        onPatch={(filterId, updates) => onUpdateBoard(activeBoard.id, { globalFilters: activeBoard.globalFilters.map((filter) => filter.id === filterId ? { ...filter, ...updates } : filter) })}
        onRemove={(filterId) => onUpdateBoard(activeBoard.id, { globalFilters: activeBoard.globalFilters.filter((filter) => filter.id !== filterId) })}
      />

      {activeWidget.type === "note" ? (
        <div className="dashboard-inspector-section">
          <label>
            Markdown note
            <textarea value={activeWidget.noteMarkdown} disabled={!editMode} onChange={(event) => onUpdateWidget(activeBoard.id, activeWidget.id, { noteMarkdown: event.target.value })} />
          </label>
        </div>
      ) : (
        <>
          <SelectedFields
            title="Dimensions"
            fields={activeWidget.dimensions}
            allFields={semanticResources.dimensions}
            editMode={editMode}
            onAssignField={onAssignField}
          />
          <SelectedFields
            title="Measures and metrics"
            fields={activeWidget.measures}
            allFields={measureFields}
            editMode={editMode}
            onAssignField={onAssignField}
          />
          <div className="dashboard-inspector-section">
            <div className="dashboard-form-grid">
              <label>
                X field
                <FieldSelect fields={chartDimensionFieldOptions} value={chartXValue} onChange={(value) => onUpdateWidget(activeBoard.id, activeWidget.id, { chartX: value })} disabled={!editMode} allowEmpty emptyLabel="Auto" />
              </label>
              <label>
                Y field
                <FieldSelect fields={chartMeasureFieldOptions} value={chartYValue} onChange={(value) => onUpdateWidget(activeBoard.id, activeWidget.id, { chartY: value })} disabled={!editMode} allowEmpty emptyLabel="Auto" />
              </label>
            </div>
          </div>
          <TimeDimensionEditor
            widget={activeWidget}
            dateFields={dateFields}
            editMode={editMode}
            onPatch={(updates) => {
              const nextWidget = { ...activeWidget, ...updates };
              onUpdateWidget(activeBoard.id, activeWidget.id, {
                ...updates,
                chartX: resolveSelectedChartBinding(nextWidget.chartX, chartDimensionMembers(nextWidget)),
              });
            }}
          />
          <FilterEditor
            title="Widget filters"
            filters={activeWidget.filters}
            filterLogic={activeWidget.filterLogic}
            fields={semanticResources.fields}
            editMode={editMode}
            onLogicChange={(filterLogic) => onUpdateWidget(activeBoard.id, activeWidget.id, { filterLogic })}
            onAdd={() => onUpdateWidget(activeBoard.id, activeWidget.id, { filters: [...activeWidget.filters, createDashboardFilter(defaultFilterSeed(semanticResources.fields))] })}
            onPatch={(filterId, updates) => onUpdateWidget(activeBoard.id, activeWidget.id, { filters: activeWidget.filters.map((filter) => filter.id === filterId ? { ...filter, ...updates } : filter) })}
            onRemove={(filterId) => onUpdateWidget(activeBoard.id, activeWidget.id, { filters: activeWidget.filters.filter((filter) => filter.id !== filterId) })}
          />
          <OrderEditor
            orders={activeWidget.orderBys}
            fields={[...chartDimensionFieldOptions, ...chartMeasureFieldOptions]}
            editMode={editMode}
            onAdd={() => onUpdateWidget(activeBoard.id, activeWidget.id, { orderBys: [...activeWidget.orderBys, createDashboardOrder({ member: activeWidget.measures[0] || activeWidget.dimensions[0] || activeWidget.timeDimension || "" })] })}
            onPatch={(orderId, updates) => onUpdateWidget(activeBoard.id, activeWidget.id, { orderBys: activeWidget.orderBys.map((order) => order.id === orderId ? { ...order, ...updates } : order) })}
            onRemove={(orderId) => onUpdateWidget(activeBoard.id, activeWidget.id, { orderBys: activeWidget.orderBys.filter((order) => order.id !== orderId) })}
          />
        </>
      )}

      <div className="dashboard-inspector-actions">
        <button type="button" disabled={!activeWidget.result} onClick={onExportWidget}>Export CSV</button>
        <button type="button" disabled={!activeWidget.result?.generated_sql} onClick={onCopySql}>{copiedKey === "sql" ? "Copied" : "Copy SQL"}</button>
        <button type="button" disabled={!editMode} onClick={onRemoveWidget}>Delete</button>
      </div>

      <DashboardExecutionDetails widget={activeWidget} />

      {activeWidget.result?.generated_sql ? (
        <details className="dashboard-details">
          <summary>Generated SQL</summary>
          <pre>{activeWidget.result.generated_sql}</pre>
        </details>
      ) : null}
      {activeWidget.result?.federation_diagnostics ? (
        <details className="dashboard-details">
          <summary>Execution diagnostics</summary>
          <DashboardDiagnosticsSummary diagnostics={activeWidget.result.federation_diagnostics} />
        </details>
      ) : null}
    </aside>
  );
}

function DashboardExecutionDetails({ widget }) {
  if (!widget?.result && !widget?.lastRunAt && !widget?.error) {
    return null;
  }
  const result = widget.result || {};
  return (
    <div className="dashboard-execution-card">
      <div className="dashboard-section-head">
        <strong>Execution</strong>
        <span>{widget.running ? "Running" : widget.error ? "Needs attention" : "Ready"}</span>
      </div>
      <dl className="dashboard-execution-grid">
        <div>
          <dt>Rows</dt>
          <dd>{result.rowCount !== undefined ? formatValue(result.rowCount) : "None"}</dd>
        </div>
        <div>
          <dt>Duration</dt>
          <dd>{formatDuration(result.duration_ms)}</dd>
        </div>
        <div>
          <dt>Last run</dt>
          <dd>{widget.lastRunAt ? formatDateTime(widget.lastRunAt) : "Not run"}</dd>
        </div>
        <div>
          <dt>SQL</dt>
          <dd>{result.generated_sql ? "Available" : "None"}</dd>
        </div>
      </dl>
      {widget.error ? <div className="query-inline-error">{widget.error}</div> : null}
    </div>
  );
}

function DashboardDiagnosticsSummary({ diagnostics }) {
  const stages = Array.isArray(diagnostics?.stages) ? diagnostics.stages : [];
  const sources = Array.isArray(diagnostics?.sources) ? diagnostics.sources : [];
  const engines = Array.isArray(diagnostics?.engines) ? diagnostics.engines : [];
  const summaryItems = [
    { label: "Stages", value: stages.length },
    { label: "Sources", value: sources.length },
    { label: "Engines", value: engines.length },
  ].filter((item) => item.value > 0);
  if (summaryItems.length === 0) {
    return <div className="dashboard-diagnostics-summary">Diagnostics were returned for this run.</div>;
  }
  return (
    <div className="dashboard-diagnostics-summary">
      {summaryItems.map((item) => (
        <span key={item.label}>{item.label}: {formatValue(item.value)}</span>
      ))}
    </div>
  );
}

function SelectedFields({ title, fields, allFields, editMode, onAssignField }) {
  return (
    <div className="dashboard-inspector-section">
      <div className="dashboard-section-head">
        <strong>{title}</strong>
        <span>{fields.length}</span>
      </div>
      {fields.length > 0 ? (
        <div className="dashboard-selected-fields">
          {fields.map((value) => {
            const field = allFields.find((item) => item.value === value) || { value, label: fieldLabel(value), kind: title.includes("Dimension") ? "dimension" : "measure" };
            return (
              <button key={value} type="button" disabled={!editMode} onClick={() => onAssignField(field)}>
                {fieldLabel(value)}
              </button>
            );
          })}
        </div>
      ) : (
        <div className="dashboard-empty-card">Select fields from the field drawer.</div>
      )}
    </div>
  );
}

function TimeDimensionEditor({ widget, dateFields, editMode, onPatch }) {
  const timeRangePreset = widget.timeRangePreset || "";
  const showCustomDates = CUSTOM_TIME_PRESETS.has(timeRangePreset);
  return (
    <div className="dashboard-inspector-section dashboard-time-section">
      <div className="dashboard-section-head">
        <strong>Time dimension</strong>
        <span>{widget.timeDimension ? "Enabled" : "None"}</span>
      </div>
      <div className="dashboard-form-grid">
        <label>
          Date field
          <FieldSelect
            fields={dateFields}
            value={widget.timeDimension}
            disabled={!editMode}
            allowEmpty
            emptyLabel="No time dimension"
            onChange={(value) =>
              onPatch({
                timeDimension: value,
                timeGrain: value ? widget.timeGrain : "",
                timeRangePreset: value ? widget.timeRangePreset : "",
                timeRangeFrom: value ? widget.timeRangeFrom : "",
                timeRangeTo: value ? widget.timeRangeTo : "",
              })
            }
          />
        </label>
        <label>
          Grain
          <select
            value={widget.timeGrain}
            disabled={!editMode || !widget.timeDimension}
            onChange={(event) => onPatch({ timeGrain: event.target.value })}
          >
            {DASHBOARD_TIME_GRAINS.map((item) => <option key={item.value || "none"} value={item.value}>{item.label}</option>)}
          </select>
        </label>
        <label>
          Range
          <select
            value={timeRangePreset}
            disabled={!editMode || !widget.timeDimension}
            onChange={(event) => {
              const preset = event.target.value;
              onPatch({
                timeRangePreset: preset,
                timeRangeFrom: CUSTOM_TIME_PRESETS.has(preset) ? widget.timeRangeFrom : "",
                timeRangeTo: preset === "custom_between" ? widget.timeRangeTo : "",
              });
            }}
          >
            {DASHBOARD_TIME_PRESETS.map((item) => <option key={item.value || "none"} value={item.value}>{item.label}</option>)}
          </select>
        </label>
        {showCustomDates ? (
          <label>
            {timeRangePreset === "custom_between" ? "From" : "Date"}
            <input
              type="date"
              value={widget.timeRangeFrom}
              disabled={!editMode || !widget.timeDimension}
              onChange={(event) => onPatch({ timeRangeFrom: event.target.value })}
            />
          </label>
        ) : null}
        {timeRangePreset === "custom_between" ? (
          <label>
            To
            <input
              type="date"
              value={widget.timeRangeTo}
              disabled={!editMode || !widget.timeDimension}
              onChange={(event) => onPatch({ timeRangeTo: event.target.value })}
            />
          </label>
        ) : null}
      </div>
      {dateFields.length === 0 ? (
        <div className="dashboard-empty-card">No date-like dimensions were found in this semantic model.</div>
      ) : null}
    </div>
  );
}

function FilterEditor({ title, filters, filterLogic = "and", fields, editMode, onAdd, onPatch, onLogicChange, onRemove }) {
  return (
    <div className="dashboard-inspector-section">
      <div className="dashboard-section-head">
        <strong>{title}</strong>
        <button type="button" disabled={!editMode || fields.length === 0} onClick={onAdd}>Add</button>
      </div>
      <div className="dashboard-filter-logic" aria-label="Filter match logic">
        <button
          className={filterLogic !== "or" ? "active" : ""}
          type="button"
          disabled={!editMode}
          onClick={() => onLogicChange("and")}
        >
          Match all
          <span>AND</span>
        </button>
        <button
          className={filterLogic === "or" ? "active" : ""}
          type="button"
          disabled={!editMode}
          onClick={() => onLogicChange("or")}
        >
          Match any
          <span>OR</span>
        </button>
      </div>
      {filters.length > 0 ? (
        <div className="dashboard-filter-stack">
          {filters.map((filter, index) => (
            <FilterCard
              key={filter.id}
              filter={filter}
              index={index}
              filterLogic={filterLogic}
              fields={fields}
              editMode={editMode}
              onPatch={onPatch}
              onRemove={onRemove}
            />
          ))}
        </div>
      ) : <div className="dashboard-empty-card">No filters.</div>}
    </div>
  );
}

function FilterCard({ filter, index, filterLogic, fields, editMode, onPatch, onRemove }) {
  const selectedField = fields.find((field) => field.value === filter.member || field.id === filter.member) || null;
  const normalizedOperator = normalizeFilterOperator(filter.operator, selectedField);
  const operators = isDateLikeDashboardField(selectedField) ? DATE_FILTER_OPERATORS : FILTER_OPERATORS;
  const dateRangeInput = DATE_RANGE_OPERATORS.has(normalizedOperator);
  const dateRangeState = parseDateRangeFilterValues(filter.values);
  const valueDisabled = ["set", "notset"].includes(normalizedOperator);
  return (
    <div className="dashboard-filter-card">
      <div className="dashboard-filter-joiner">{index === 0 ? "Where" : filterLogic.toUpperCase()}</div>
      <div className="dashboard-filter-main">
        <FieldSelect
          fields={fields}
          value={filter.member}
          disabled={!editMode}
          onChange={(value) => {
            const nextField = fields.find((field) => field.value === value || field.id === value) || null;
            onPatch(filter.id, {
              member: value,
              operator: normalizeFilterOperator(filter.operator, nextField),
            });
          }}
        />
        <select
          value={normalizedOperator}
          disabled={!editMode}
          onChange={(event) => onPatch(filter.id, { operator: normalizeFilterOperator(event.target.value, selectedField) })}
        >
          {operators.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
        </select>
        {dateRangeInput ? (
          <select
            value={dateRangeState.preset}
            disabled={!editMode}
            onChange={(event) => onPatch(filter.id, { values: serializeDateRangeFilterValues({ ...dateRangeState, preset: event.target.value }) })}
          >
            {DASHBOARD_TIME_PRESETS.map((item) => <option key={item.value || "none"} value={item.value}>{item.label}</option>)}
          </select>
        ) : (
          <input
            value={filter.values}
            disabled={!editMode || valueDisabled}
            onChange={(event) => onPatch(filter.id, { values: event.target.value })}
            placeholder="Value or comma list"
          />
        )}
        <button type="button" disabled={!editMode} onClick={() => onRemove(filter.id)}>Remove</button>
      </div>
      {dateRangeInput && CUSTOM_TIME_PRESETS.has(dateRangeState.preset) ? (
        <div className="dashboard-filter-date-row">
          <label>
            {dateRangeState.preset === "custom_between" ? "From" : "Date"}
            <input
              type="date"
              value={dateRangeState.from}
              disabled={!editMode}
              onChange={(event) => onPatch(filter.id, { values: serializeDateRangeFilterValues({ ...dateRangeState, from: event.target.value }) })}
            />
          </label>
          {dateRangeState.preset === "custom_between" ? (
            <label>
              To
              <input
                type="date"
                value={dateRangeState.to}
                disabled={!editMode}
                onChange={(event) => onPatch(filter.id, { values: serializeDateRangeFilterValues({ ...dateRangeState, to: event.target.value }) })}
              />
            </label>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function OrderEditor({ orders, fields, editMode, onAdd, onPatch, onRemove }) {
  return (
    <div className="dashboard-inspector-section">
      <div className="dashboard-section-head">
        <strong>Ordering</strong>
        <button type="button" disabled={!editMode || fields.length === 0} onClick={onAdd}>Add</button>
      </div>
      {orders.length > 0 ? orders.map((order) => (
        <div className="dashboard-filter-editor" key={order.id}>
          <FieldSelect fields={fields} value={order.member} disabled={!editMode} onChange={(value) => onPatch(order.id, { member: value })} />
          <select value={order.direction} disabled={!editMode} onChange={(event) => onPatch(order.id, { direction: event.target.value })}>
            <option value="desc">Descending</option>
            <option value="asc">Ascending</option>
          </select>
          <button type="button" disabled={!editMode} onClick={() => onRemove(order.id)}>Remove</button>
        </div>
      )) : <div className="dashboard-empty-card">Default order uses the first selected measure.</div>}
    </div>
  );
}

function FieldSelect({ fields, value, onChange, disabled, allowEmpty = false, emptyLabel = "Auto" }) {
  return (
    <select value={value || ""} disabled={disabled} onChange={(event) => onChange(event.target.value)}>
      {allowEmpty ? <option value="">{emptyLabel}</option> : null}
      {fields.map((field) => (
        <option key={field.id} value={field.value}>
          {field.qualifiedLabel || field.value}
        </option>
      ))}
    </select>
  );
}

function formatDuration(value) {
  const duration = Number(value);
  if (!Number.isFinite(duration)) {
    return "Unknown";
  }
  return duration < 1000 ? `${Math.round(duration)} ms` : `${(duration / 1000).toFixed(2)} s`;
}

function defaultFilterSeed(fields) {
  const field = fields[0] || null;
  return {
    member: field?.value || "",
    operator: isDateLikeDashboardField(field) ? "indaterange" : "equals",
  };
}

function chartDimensionMembers(widget) {
  return uniqueValues([...(widget?.dimensions || []), widget?.timeDimension]);
}

function chartDataDimensionKey(widget, dimension) {
  const selectedDimension = String(dimension || "").trim();
  const timeDimension = String(widget?.timeDimension || "").trim();
  const grain = String(widget?.timeGrain || "").trim().toLowerCase();
  if (selectedDimension && timeDimension && selectedDimension === timeDimension && grain) {
    return `${selectedDimension}_${grain}`;
  }
  return selectedDimension;
}

function selectedChartDimensionFields(widget, resources) {
  const dimensionFields = chartDimensionMembers(widget);
  return dimensionFields.map((value) => {
    const field = findDashboardField(resources.dimensions, value, "dimension");
    if (value === widget?.timeDimension && widget?.timeGrain) {
      return {
        ...field,
        label: `${field.label} (${widget.timeGrain})`,
        qualifiedLabel: `${field.qualifiedLabel || field.value} (${widget.timeGrain})`,
      };
    }
    return field;
  });
}

function selectedChartMeasureFields(widget, measureFields) {
  return (widget?.measures || []).map((value) => findDashboardField(measureFields, value, "measure"));
}

function findDashboardField(fields, value, kind) {
  const found = fields.find((field) => field.value === value || field.id === value);
  if (found) {
    return found;
  }
  return {
    id: value,
    value,
    label: fieldLabel(value),
    qualifiedLabel: value,
    kind,
  };
}

function uniqueValues(values) {
  const seen = new Set();
  return values
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .filter((value) => {
      if (seen.has(value)) {
        return false;
      }
      seen.add(value);
      return true;
    });
}

function resolveSelectedChartBinding(value, selectedMembers) {
  const normalized = String(value || "").trim();
  if (normalized && selectedMembers.includes(normalized)) {
    return normalized;
  }
  return selectedMembers[0] || "";
}

function normalizeFilterOperator(operator, field) {
  const normalized = String(operator || "").trim().toLowerCase();
  if (isDateLikeDashboardField(field)) {
    return DATE_FILTER_OPERATORS.some((item) => item.value === normalized) ? normalized : "indaterange";
  }
  if (DATE_RANGE_OPERATORS.has(normalized)) {
    return "equals";
  }
  return FILTER_OPERATORS.some((item) => item.value === normalized) ? normalized : "equals";
}

function parseDateRangeFilterValues(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return { preset: "", from: "", to: "" };
  }
  const parts = raw.split(",").map((item) => item.trim()).filter(Boolean);
  if (parts.length === 2) {
    return { preset: "custom_between", from: parts[0], to: parts[1] };
  }
  if (raw.startsWith("between:")) {
    const [from = "", to = ""] = raw.slice("between:".length).split("..");
    return { preset: "custom_between", from, to };
  }
  if (raw.startsWith("before:")) {
    return { preset: "custom_before", from: raw.slice("before:".length), to: "" };
  }
  if (raw.startsWith("after:")) {
    return { preset: "custom_after", from: raw.slice("after:".length), to: "" };
  }
  if (raw.startsWith("on:")) {
    return { preset: "custom_on", from: raw.slice("on:".length), to: "" };
  }
  return { preset: raw, from: "", to: "" };
}

function serializeDateRangeFilterValues(state) {
  const preset = String(state?.preset || "").trim();
  const from = String(state?.from || "").trim();
  const to = String(state?.to || "").trim();
  if (preset === "custom_between") {
    return from && to ? `${from},${to}` : "";
  }
  if (preset === "custom_before") {
    return from ? `before:${from}` : "";
  }
  if (preset === "custom_after") {
    return from ? `after:${from}` : "";
  }
  if (preset === "custom_on") {
    return from ? `on:${from}` : "";
  }
  return preset;
}

function defaultWidgetSeed(resources, type = "bar", index = 1) {
  if (type === "note") {
    return {
      title: `Note ${index}`,
      type: "note",
      noteMarkdown: "Add context for this dashboard.",
    };
  }
  const dimension = resources.dimensions[0]?.value || "";
  const measure = [...resources.measures, ...resources.metrics][0]?.value || "";
  return {
    title: type === "stat" ? `KPI ${index}` : `Widget ${index}`,
    type,
    dimensions: dimension && type !== "stat" ? [dimension] : [],
    measures: measure ? [measure] : [],
    chartX: dimension,
    chartY: measure,
  };
}

function filterSemanticDatasets(resources, searchValue) {
  const search = String(searchValue || "").trim().toLowerCase();
  if (!search) {
    return resources.datasets;
  }
  return resources.datasets
    .map((dataset) => ({
      ...dataset,
      dimensions: filterDashboardFields(dataset.dimensions, search, dataset.name),
      measures: filterDashboardFields(dataset.measures, search, dataset.name),
      metrics: filterDashboardFields(dataset.metrics, search, dataset.name),
    }))
    .filter(
      (dataset) =>
        dataset.name.toLowerCase().includes(search) ||
        dataset.dimensions.length > 0 ||
        dataset.measures.length > 0 ||
        dataset.metrics.length > 0,
    );
}

function filterDashboardFields(fields, search, datasetName) {
  const datasetMatches = String(datasetName || "").toLowerCase().includes(search);
  if (datasetMatches) {
    return fields;
  }
  return fields.filter((field) =>
    [field.label, field.qualifiedLabel, field.value, field.kind]
      .filter(Boolean)
      .some((value) => String(value).toLowerCase().includes(search)),
  );
}

function measureFieldsFallback(resources) {
  return [...resources.measures, ...resources.metrics][0]?.value || "";
}

function dashboardLayoutClass(panelOpen) {
  return [
    "dashboard-builder-layout-next",
    panelOpen ? "with-panel" : "without-panel",
  ].join(" ");
}
