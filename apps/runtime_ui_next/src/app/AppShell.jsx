import { Sidebar } from "../components/navigation/Sidebar.jsx";
import { TopBar } from "../components/navigation/TopBar.jsx";
import { classNames } from "../utils/classNames.js";

export function AppShell({
  activeRoute,
  path,
  navigate,
  session,
  authStatus,
  onLogout,
  sidebarCollapsed,
  setSidebarCollapsed,
  theme,
  setTheme,
  children,
}) {
  return (
    <div className={classNames("app-shell", sidebarCollapsed && "app-shell--collapsed")} data-theme={theme}>
      <Sidebar
        activeRoute={activeRoute}
        path={path}
        navigate={navigate}
        session={session}
        authStatus={authStatus}
        onLogout={onLogout}
        collapsed={sidebarCollapsed}
        setCollapsed={setSidebarCollapsed}
        theme={theme}
        setTheme={setTheme}
      />
      <main className={classNames("main-surface", `main-surface--${activeRoute.workspace}`)}>
        {activeRoute.workspace !== "chat" ? (
          <TopBar activeRoute={activeRoute} navigate={navigate} />
        ) : null}
        <div className="page-transition">
          {children}
        </div>
      </main>
    </div>
  );
}
