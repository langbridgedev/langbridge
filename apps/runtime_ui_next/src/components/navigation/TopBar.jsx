export function TopBar({ activeRoute, navigate }) {
  return (
    <header className={`top-bar ${activeRoute.workspace === "chat" ? "top-bar--chat" : ""}`}>
      <div>
        {activeRoute.workspace !== "chat" ? (
          <p className="eyebrow">{activeRoute.workspace === "configure" ? "Configuration" : "Analyst workspace"}</p>
        ) : null}
        <h1>{activeRoute.label}</h1>
      </div>
      <div className="top-actions">
        <button type="button" onClick={() => navigate("/query-workspace")}>Query</button>
        <button type="button" onClick={() => navigate("/dashboards")}>Dashboard</button>
        <button type="button" onClick={() => navigate("/configure/connectors")}>Configure</button>
      </div>
    </header>
  );
}
