import { useEffect, useRef, useState } from "react";
import { Link, NavLink, useLocation } from "react-router-dom";
import { Box, ChevronLeft, ChevronRight } from "lucide-react";

import { NAV_SECTIONS } from "../lib/routes";
import { ThemeToggle } from "./ThemeToggle";

export function AppShell({ session, authStatus, onLogout, children }) {
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(false);
  const [utilityMenuOpen, setUtilityMenuOpen] = useState(false);
  const desktopUtilityRef = useRef(null);
  const mobileUtilityRef = useRef(null);
  const authEnabled = Boolean(authStatus?.auth_enabled);
  const identityName = session?.username || (authEnabled ? "Runtime user" : "Direct access");
  const identitySubcopy = session?.email || (authEnabled ? "Signed into this runtime" : "Authentication disabled");
  const authBadge = authEnabled ? `${authStatus?.auth_mode || "configured"} auth` : "Open runtime";
  const isChatRoute = location.pathname === "/chat" || location.pathname.startsWith("/chat/");
  const shellClassName = [
    "app-shell",
    collapsed ? "nav-collapsed" : "",
    isChatRoute ? "chat-route" : "",
  ]
    .filter(Boolean)
    .join(" ");
  const identityInitials = identityName
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() || "")
    .join("");

  useEffect(() => {
    setUtilityMenuOpen(false);
  }, [location.pathname, collapsed]);

  useEffect(() => {
    function handlePointerDown(event) {
      const target = event.target;
      const insideDesktop = desktopUtilityRef.current?.contains(target);
      const insideMobile = mobileUtilityRef.current?.contains(target);

      if (!insideDesktop && !insideMobile) {
        setUtilityMenuOpen(false);
      }
    }

    function handleKeyDown(event) {
      if (event.key === "Escape") {
        setUtilityMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);

    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  const flatItems = NAV_SECTIONS.flatMap((section) => section.items);
  const mobileItems = flatItems.filter((item) => item.mobile !== false);
  const utilityPanel = (className = "", compact = false, ref = null) => (
    <div className={`shell-utility-panel ${className}`.trim()} ref={ref}>
      <button
        className={`session-trigger ${compact ? "compact" : ""}`.trim()}
        type="button"
        onClick={() => setUtilityMenuOpen((current) => !current)}
        aria-haspopup="dialog"
        aria-expanded={utilityMenuOpen}
        title={identityName}
      >
        <span className="session-avatar" aria-hidden="true">
          {identityInitials || "LB"}
        </span>
        {!compact ? (
          <span className="session-trigger-name">{identityName}</span>
        ) : null}
      </button>

      {utilityMenuOpen ? (
        <div className="session-menu" role="dialog" aria-label="User menu">
          <div className="session-menu-header">
            <strong>{identityName}</strong>
            <span>{identitySubcopy}</span>
            <span className={`session-state ${authEnabled ? "enabled" : "disabled"}`.trim()}>
              {authBadge}
            </span>
          </div>
          <div className="shell-utility-actions" style={{ marginTop: "1rem" }}>
            <ThemeToggle className="side-utility-button" onToggle={() => setUtilityMenuOpen(false)} />
            {authStatus?.auth_enabled && authStatus?.login_allowed ? (
              <button
                className="ghost-button side-utility-button"
                type="button"
                onClick={() => {
                  setUtilityMenuOpen(false);
                  onLogout();
                }}
              >
                Sign out
              </button>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );

  return (
    <div className={shellClassName}>
      <aside className={`side-nav ${collapsed ? "collapsed" : ""}`.trim()}>
        <div className="side-nav-top">
          <div className="brand-row">
            <Link className="brand-mark" to="/" title="Langbridge Runtime">
              <span className="brand-mark-icon">
                <Box className="nav-icon-svg" aria-hidden="true" />
              </span>
              {!collapsed ? (
                <span>
                  <strong>Langbridge Runtime</strong>
                </span>
              ) : null}
            </Link>
          </div>

          <nav className="nav-groups" aria-label="Runtime navigation">
            {NAV_SECTIONS.map((section) => (
              <div key={section.label} className="nav-group">
                {!collapsed ? <p className="nav-group-label">{section.label}</p> : null}
                <div className="nav-list">
                  {section.items.map((item) => {
                    const Icon = item.icon;
                    return (
                      <NavLink
                        key={item.to}
                        to={item.to}
                        className={({ isActive }) => `nav-item ${isActive ? "active" : ""}`}
                        end={item.to === "/"}
                        title={item.label}
                      >
                        <div className="nav-item-top">
                          <span className="nav-icon">
                            <Icon className="nav-icon-svg" aria-hidden="true" />
                          </span>
                          {!collapsed ? <span>{item.label}</span> : null}
                        </div>
                        {/* {!collapsed ? <small>{item.description}</small> : null} */}
                      </NavLink>
                    );
                  })}
                </div>
              </div>
            ))}
          </nav>
        </div>

        <div className="side-nav-bottom">
          {utilityPanel("side-utility-stack", collapsed, desktopUtilityRef)}
          <button
            className="nav-collapse-button"
            type="button"
            onClick={() => setCollapsed((current) => !current)}
            aria-label={collapsed ? "Expand navigation" : "Collapse navigation"}
            title={collapsed ? "Expand navigation" : "Collapse navigation"}
          >
            {collapsed ? (
              <ChevronRight className="nav-icon-svg" aria-hidden="true" />
            ) : (
              <ChevronLeft className="nav-icon-svg" aria-hidden="true" />
            )}
          </button>
        </div>
      </aside>

      <div className="shell-main">
        <nav className="mobile-nav" aria-label="Runtime navigation">
          {mobileItems.map((item) => {
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

        {utilityPanel("mobile-utility-panel", false, mobileUtilityRef)}

        <main className="page-shell">{children}</main>
      </div>
    </div>
  );
}
