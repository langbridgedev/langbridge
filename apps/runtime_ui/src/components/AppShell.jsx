import { useMemo } from "react";
import { Link, NavLink, useLocation } from "react-router-dom";
import {
  Blocks,
  Bot,
  Cable,
  Database,
  LayoutDashboard,
  MessageSquareText,
  Settings2,
  Sparkles,
  Table2,
} from "lucide-react";

const NAV_SECTIONS = [
  {
    label: "Command",
    items: [
      { to: "/", label: "Overview", icon: LayoutDashboard, description: "Runtime health, counts, and quick actions." },
    ],
  },
  {
    label: "Model",
    items: [
      { to: "/connectors", label: "Connectors", icon: Cable, description: "Connector inventory and sync operations." },
      { to: "/datasets", label: "Datasets", icon: Database, description: "Runtime datasets and preview surfaces." },
      { to: "/semantic-models", label: "Semantic Models", icon: Blocks, description: "Semantic layer inventory and YAML detail." },
    ],
  },
  {
    label: "Operate",
    items: [
      { to: "/sql", label: "SQL Workspace", icon: Table2, description: "Direct SQL and dataset-backed workbench." },
      { to: "/agents", label: "Agents", icon: Bot, description: "Configured agent definitions and tool bindings." },
      { to: "/chat", label: "Chat", icon: MessageSquareText, description: "Threaded runtime chat on top of local agents." },
      { to: "/bi", label: "BI", icon: Sparkles, description: "Lightweight semantic query and chart previews." },
    ],
  },
  {
    label: "System",
    items: [
      { to: "/settings", label: "Settings", icon: Settings2, description: "Runtime identity, auth mode, and capabilities." },
    ],
  },
];

function matchNav(pathname, target) {
  if (target === "/") {
    return pathname === "/";
  }
  return pathname === target || pathname.startsWith(`${target}/`);
}

export function AppShell({ session, authStatus, onLogout, children }) {
  const location = useLocation();

  const activePage = useMemo(() => {
    const flatItems = NAV_SECTIONS.flatMap((section) => section.items);
    return flatItems.find((item) => matchNav(location.pathname, item.to)) || flatItems[0];
  }, [location.pathname]);

  return (
    <div className="app-shell">
      <aside className="side-nav">
        <Link className="brand-mark" to="/">
          <span className="brand-mark-icon">L</span>
          <span>
            <strong>Langbridge Runtime</strong>
            <small>single-workspace UI</small>
          </span>
        </Link>

        <nav className="nav-groups" aria-label="Runtime navigation">
          {NAV_SECTIONS.map((section) => (
            <div key={section.label} className="nav-group">
              <p className="nav-group-label">{section.label}</p>
              <div className="nav-list">
                {section.items.map((item) => {
                  const Icon = item.icon;
                  return (
                    <NavLink
                      key={item.to}
                      to={item.to}
                      className={({ isActive }) => `nav-item ${isActive ? "active" : ""}`}
                      end={item.to === "/"}
                    >
                      <div className="nav-item-top">
                        <span className="nav-icon">
                          <Icon className="nav-icon-svg" aria-hidden="true" />
                        </span>
                        <span>{item.label}</span>
                      </div>
                      <small>{item.description}</small>
                    </NavLink>
                  );
                })}
              </div>
            </div>
          ))}
        </nav>

        <div className="side-runtime-card">
          <p className="side-runtime-kicker">Runtime posture</p>
          <strong>{authStatus?.auth_enabled ? "Session scoped access" : "Direct local access"}</strong>
          <span>
            {authStatus?.auth_enabled
              ? `Auth mode: ${authStatus.auth_mode}`
              : "Auth is disabled for this runtime."}
          </span>
          <div className="side-runtime-actions">
            <Link className="side-runtime-link" to="/sql">
              SQL
            </Link>
            <Link className="side-runtime-link" to="/chat">
              Chat
            </Link>
            <Link className="side-runtime-link" to="/bi">
              BI
            </Link>
          </div>
        </div>

        <div className="side-note">
          <p>Runtime-owned shell</p>
          <p>Cloud org/project navigation is intentionally removed in this app.</p>
        </div>
      </aside>

      <div className="shell-main">
        <header className="shell-header">
          <div>
            <p className="eyebrow">Runtime Workspace</p>
            <h1>{activePage.label}</h1>
            <p className="header-copy">{activePage.description}</p>
          </div>
          <div className="header-actions">
            <div className="session-card">
              <strong>{session?.username || "Direct access"}</strong>
              <span>
                {authStatus?.auth_enabled
                  ? `Auth: ${authStatus.auth_mode}`
                  : "Auth disabled"}
              </span>
            </div>
            {authStatus?.auth_enabled && authStatus?.auth_mode === "local" ? (
              <button className="ghost-button" type="button" onClick={onLogout}>
                Sign out
              </button>
            ) : null}
          </div>
        </header>

        <nav className="mobile-nav" aria-label="Runtime navigation">
          {NAV_SECTIONS.flatMap((section) => section.items).map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) => `mobile-nav-item ${isActive ? "active" : ""}`}
                end={item.to === "/"}
              >
                <Icon className="mobile-nav-icon" aria-hidden="true" />
                {item.label}
              </NavLink>
            );
          })}
        </nav>

        <main className="page-shell">{children}</main>
      </div>
    </div>
  );
}
