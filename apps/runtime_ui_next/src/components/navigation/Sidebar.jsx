import { useEffect, useState } from "react";

import { NavIcon } from "./NavIcon.jsx";
import { getEmptyNavigationState, getNavigationState } from "../../services/navigationService.js";
import { classNames } from "../../utils/classNames.js";

const primaryNav = [
  { id: "chat", label: "Chats", path: "/chat", icon: "chat" },
  { id: "query", label: "Query", path: "/query-workspace", icon: "terminal" },
  { id: "dashboards", label: "Dashboards", path: "/dashboards", icon: "dashboard" },
];

export function Sidebar({ activeRoute, navigate, session, authStatus, onLogout, collapsed, setCollapsed, theme, setTheme }) {
  const [navigation, setNavigation] = useState(getEmptyNavigationState);
  const canSignOut = Boolean(authStatus?.auth_enabled);

  useEffect(() => {
    let cancelled = false;
    setNavigation(getEmptyNavigationState());
    void getNavigationState(activeRoute.workspace).then((nextNavigation) => {
      if (!cancelled) {
        setNavigation(nextNavigation);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [activeRoute.workspace]);

  return (
    <aside className="side-rail">
      <div className="rail-top">
        <button
          className="sidebar-icon-button"
          type="button"
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          onClick={() => setCollapsed((current) => !current)}
        >
          <NavIcon name="menu" />
        </button>
        <button className="new-chat-button" type="button" onClick={() => navigate("/chat")}>
          <span className="rail-icon"><NavIcon name="plus" /></span>
          <span className="rail-label">New chat</span>
        </button>
      </div>

      <nav className="rail-section" aria-label="Primary workspace">
        {primaryNav.map((item) => (
          <button
            key={item.id}
            className={classNames("rail-item", activeRoute.workspace === item.id && "active")}
            type="button"
            onClick={() => navigate(item.path)}
          >
            <span className="rail-icon"><NavIcon name={item.icon} /></span>
            <strong className="rail-label">{item.label}</strong>
          </button>
        ))}
      </nav>

      <nav className="rail-section rail-history" aria-label="Workspace history">
        {/* {navigation.projects ? (
          <div className="rail-history-section rail-projects">
            <p>Projects</p>
            {navigation.projects.items.map((item) => (
              <button key={item.id} className="project-link" type="button" onClick={() => navigate(item.path)}>
                <span className="project-copy rail-label">
                  <strong>{item.name}</strong>
                  <small>{item.meta}</small>
                </span>
              </button>
            ))}
          </div>
        ) : null} */}

        {navigation.recents ? (
          <div className="rail-history-section rail-recents">
            <p>Recent {navigation.recents.label}</p>
            {navigation.recents.items.map((item) => (
              <button key={item.id} className="thread-link" type="button" onClick={() => navigate(item.path)}>
                <span className="rail-label">{item.title}</span>
              </button>
            ))}
          </div>
        ) : null}
      </nav>

      <div className="rail-bottom">
        {session && canSignOut ? (
          <div className="session-pill rail-label">
            <strong>{session.display_name || session.username || "Operator"}</strong>
            <span>Runtime session</span>
          </div>
        ) : null}
        <button
          className="theme-button"
          type="button"
          onClick={() => setTheme((current) => (current === "light" ? "dark" : "light"))}
        >
          <span className="rail-icon"><NavIcon name={theme === "light" ? "moon" : "sun"} /></span>
          <span className="rail-label">{theme === "light" ? "Dark mode" : "Light mode"}</span>
        </button>
        <button
          className={classNames("configure-button", activeRoute.workspace === "configure" && "active")}
          type="button"
          onClick={() => navigate("/configure/connectors")}
        >
          <span className="rail-icon"><NavIcon name="settings" /></span>
          <span className="rail-label">Configure</span>
        </button>
        {canSignOut ? (
          <button className="theme-button" type="button" onClick={onLogout}>
            <span className="rail-icon"><NavIcon name="logout" /></span>
            <span className="rail-label">Sign out</span>
          </button>
        ) : null}
        <span className="runtime-pill rail-label">Langbridge API</span>
      </div>
    </aside>
  );
}
