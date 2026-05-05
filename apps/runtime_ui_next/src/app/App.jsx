import { useCallback, useEffect, useMemo, useState } from "react";
import { Navigate, Route, Routes, useLocation, useNavigate, useParams } from "react-router-dom";

import { AppShell } from "./AppShell.jsx";
import { defaultRoute, matchRoute } from "./routes.js";
import {
  readSidebarPreference,
  readThemePreference,
  writeSidebarPreference,
  writeThemePreference,
} from "../services/preferencesService.js";
import { useRuntimeAuth } from "../hooks/useRuntimeAuth.js";
import {
  BootstrapScreen,
  ErrorScreen,
  LoadingScreen,
  LoginScreen,
  UnsupportedAuthScreen,
} from "../features/auth/AuthScreens.jsx";
import { ChatIndexPage } from "../features/chat/ChatIndexPage.jsx";
import { ChatPage } from "../features/chat/ChatPage.jsx";
import { QueryWorkspacePage } from "../features/query-workspace/QueryWorkspacePage.jsx";
import { DashboardBoardPage } from "../features/dashboards/DashboardBoardPage.jsx";
import { ConfigurationPage } from "../features/configuration/ConfigurationPage.jsx";

function DashboardRoute({ navigate }) {
  const { dashboardId } = useParams();
  return <DashboardBoardPage dashboardId={dashboardId} navigate={navigate} />;
}

function ConfigurationRoute({ navigate, authStatus, session }) {
  const { section = "connectors" } = useParams();
  return (
    <ConfigurationPage
      section={section}
      navigate={navigate}
      authStatus={authStatus}
      session={session}
    />
  );
}

function RuntimeRoutes({ authStatus, session, onLogout }) {
  const location = useLocation();
  const routerNavigate = useNavigate();
  const [sidebarCollapsed, setSidebarCollapsed] = useState(readSidebarPreference);
  const [theme, setTheme] = useState(readThemePreference);
  const activeRoute = useMemo(
    () => matchRoute(location.pathname) || defaultRoute,
    [location.pathname],
  );
  const navigate = useCallback(
    (nextPath) => {
      if (!nextPath || nextPath === location.pathname) {
        return;
      }
      routerNavigate(nextPath);
    },
    [location.pathname, routerNavigate],
  );

  useEffect(() => {
    writeThemePreference(theme);
  }, [theme]);

  useEffect(() => {
    writeSidebarPreference(sidebarCollapsed);
  }, [sidebarCollapsed]);

  return (
    <AppShell
      activeRoute={activeRoute}
      path={location.pathname}
      navigate={navigate}
      session={session}
      authStatus={authStatus}
      onLogout={onLogout}
      sidebarCollapsed={sidebarCollapsed}
      setSidebarCollapsed={setSidebarCollapsed}
      theme={theme}
      setTheme={setTheme}
    >
      <Routes>
        <Route path="/" element={<Navigate to="/chat" replace />} />
        <Route path="/c" element={<Navigate to="/chat" replace />} />
        <Route path="/chat" element={<ChatIndexPage />} />
        <Route path="/chat/:threadId" element={<ChatPage />} />
        <Route path="/query" element={<Navigate to="/query-workspace" replace />} />
        <Route path="/sql" element={<Navigate to="/query-workspace" replace />} />
        <Route path="/query-workspace" element={<QueryWorkspacePage navigate={navigate} />} />
        <Route path="/dashboards" element={<DashboardRoute navigate={navigate} />} />
        <Route path="/dashboards/:dashboardId" element={<DashboardRoute navigate={navigate} />} />
        <Route path="/connectors" element={<Navigate to="/configure/connectors" replace />} />
        <Route path="/datasets" element={<Navigate to="/configure/datasets" replace />} />
        <Route path="/semantic-models" element={<Navigate to="/configure/semantic-models" replace />} />
        <Route path="/agents" element={<Navigate to="/configure/agents" replace />} />
        <Route path="/security" element={<Navigate to="/configure/security" replace />} />
        <Route path="/configure" element={<Navigate to="/configure/connectors" replace />} />
        <Route
          path="/configure/:section"
          element={<ConfigurationRoute navigate={navigate} authStatus={authStatus} session={session} />}
        />
        <Route path="*" element={<Navigate to="/chat" replace />} />
      </Routes>
    </AppShell>
  );
}

export function App() {
  const { state, submitting, submitError, reloadAuth, onBootstrap, onLogin, onLogout } =
    useRuntimeAuth();

  if (state.phase === "loading") {
    return <LoadingScreen />;
  }

  if (state.phase === "error") {
    return <ErrorScreen error={state.error} onRetry={() => void reloadAuth()} />;
  }

  if (state.phase === "unsupported") {
    return <UnsupportedAuthScreen authStatus={state.authStatus} onRetry={() => void reloadAuth()} />;
  }

  if (state.phase === "bootstrap") {
    return <BootstrapScreen error={submitError} submitting={submitting} onSubmit={(form) => void onBootstrap(form)} />;
  }

  if (state.phase === "login") {
    return <LoginScreen error={submitError} submitting={submitting} onSubmit={(form) => void onLogin(form)} />;
  }

  return (
    <RuntimeRoutes
      authStatus={state.authStatus}
      session={state.session}
      onLogout={() => void onLogout()}
    />
  );
}
