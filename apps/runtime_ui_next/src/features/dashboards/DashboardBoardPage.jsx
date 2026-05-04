import { useEffect, useState } from "react";

import { getDashboardBoard } from "../../services/dashboardService.js";

export function DashboardBoardPage({ navigate }) {
  const [dashboardSource, setDashboardSource] = useState("semantic");
  const [board, setBoard] = useState(null);
  const sourceLabel = dashboardSource === "semantic" ? "growth_performance" : "customer_month_revenue";

  useEffect(() => {
    void getDashboardBoard().then(setBoard);
  }, []);

  if (!board) {
    return <section className="empty-state"><p>Loading dashboard...</p></section>;
  }

  return (
    <section className="workspace-page workspace-page--dashboard dashboard-workspace-minimal">
      <div className="dashboard-board-header">
        <div className="dashboard-board-title">
          <h2>{board.title}</h2>
        </div>
        <div className="dashboard-board-actions">
          <button type="button" onClick={() => navigate("/chat")}>Open chat</button>
          <button type="button">Refresh all</button>
          <button type="button">Export JSON</button>
          <button className="primary-action" type="button">Publish</button>
        </div>
      </div>

      <div className="dashboard-builder-strip">
        <div className="dashboard-compose-input">
          <input type="text" value="Add Q3 channel revenue and gross margin" readOnly />
        </div>
        <div className="dashboard-builder-controls">
          <label className="query-control">
            Source
            <select value={dashboardSource} onChange={(event) => setDashboardSource(event.target.value)}>
              <option value="semantic">Semantic model</option>
              <option value="dataset">Dataset</option>
            </select>
          </label>
          <label className="query-control">
            {dashboardSource === "semantic" ? "Model" : "Dataset"}
            <select value={sourceLabel} readOnly>
              <option value={sourceLabel}>{sourceLabel}</option>
            </select>
          </label>
          <label className="query-control">
            Chart
            <select defaultValue="bar">
              <option value="kpi">KPI</option>
              <option value="bar">Bar</option>
              <option value="line">Line</option>
              <option value="pie">Pie</option>
              <option value="table">Table</option>
            </select>
          </label>
          <button className="primary-action" type="button">Add tile</button>
        </div>
      </div>

      <div className="dashboard-filter-row">
        {board.filters.map((filter) => <button key={filter} type="button">{filter}</button>)}
        <button type="button">+ Add filter</button>
        <button type="button">+ Chart</button>
        <button type="button">+ KPI</button>
        <button type="button">+ Note</button>
      </div>

      <div className="dashboard-simple-canvas">
        {board.tiles.map((tile) => (
          <DashboardTile key={tile.id} tile={tile} sourceLabel={sourceLabel} />
        ))}
      </div>
    </section>
  );
}

function DashboardTile({ tile, sourceLabel }) {
  return (
    <article className={`dashboard-simple-widget ${tile.size === "wide" ? "dashboard-simple-widget--wide" : ""}`}>
      <div className="dashboard-widget-header">
        <div>
          <span className="drag-handle">Drag</span>
          <h3>{tile.title}</h3>
        </div>
        <div>
          {tile.size === "wide" ? <button type="button">Wide</button> : null}
          <button type="button">{tile.type === "Table" ? "CSV" : "Export"}</button>
          {tile.size === "wide" ? <button type="button">Edit</button> : null}
        </div>
      </div>
      {tile.size === "wide" ? (
        <div className="mock-chart dashboard-simple-chart">
          <i style={{ height: "88%" }} />
          <i style={{ height: "48%" }} />
          <i style={{ height: "78%" }} />
          <i style={{ height: "45%" }} />
          <i style={{ height: "74%" }} />
          <i style={{ height: "42%" }} />
        </div>
      ) : (
        <p>{tile.description}</p>
      )}
      <div className="dashboard-widget-footer">
        <span>{sourceLabel}</span>
        <span>{tile.type}</span>
        {tile.rows ? <span>{tile.rows} rows</span> : null}
        {tile.lastRun ? <span>Last run {tile.lastRun}</span> : null}
      </div>
    </article>
  );
}
