import { Navigate, Route, Routes } from "react-router-dom";

import { AppShell } from "./components/AppShell";
import {
  BootstrapScreen,
  ErrorScreen,
  LoadingScreen,
  LoginScreen,
  UnsupportedAuthScreen,
} from "./components/AuthScreens";
import { useRuntimeAuth } from "./hooks/useRuntimeAuth";
import { AgentsPage } from "./pages/AgentsPage";
import { BiPage } from "./pages/BiPage";
import { ChatIndexPage } from "./pages/ChatIndexPage";
import { ChatPage } from "./pages/ChatPage";
import { ConnectorsPage } from "./pages/ConnectorsPage";
import { DatasetsPage } from "./pages/DatasetsPage";
import { OverviewPage } from "./pages/OverviewPage";
import { SemanticModelsPage } from "./pages/SemanticModelsPage";
import { SettingsPage } from "./pages/SettingsPage";
import { SqlPage } from "./pages/SqlPage";

function RuntimeRoutes({ authStatus, session, onLogout }) {
  return (
    <AppShell session={session} authStatus={authStatus} onLogout={onLogout}>
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/connectors" element={<ConnectorsPage />} />
        <Route path="/connectors/:id" element={<ConnectorsPage />} />
        <Route path="/datasets" element={<DatasetsPage />} />
        <Route path="/datasets/:id" element={<DatasetsPage />} />
        <Route path="/semantic-models" element={<SemanticModelsPage />} />
        <Route path="/semantic-models/:id" element={<SemanticModelsPage />} />
        <Route path="/sql" element={<SqlPage />} />
        <Route path="/agents" element={<AgentsPage />} />
        <Route path="/agents/:id" element={<AgentsPage />} />
        <Route path="/chat" element={<ChatIndexPage />} />
        <Route path="/chat/:threadId" element={<ChatPage />} />
        <Route path="/bi" element={<BiPage />} />
        <Route
          path="/settings"
          element={<SettingsPage authStatus={authStatus} session={session} />}
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}

export default function App() {
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
    return (
      <BootstrapScreen
        error={submitError}
        submitting={submitting}
        onSubmit={(form) => void onBootstrap(form)}
      />
    );
  }

  if (state.phase === "login") {
    return (
      <LoginScreen
        error={submitError}
        submitting={submitting}
        onSubmit={(form) => void onLogin(form)}
      />
    );
  }

  return (
    <RuntimeRoutes
      authStatus={state.authStatus}
      session={state.session}
      onLogout={() => void onLogout()}
    />
  );
}
