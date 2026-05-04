import { startTransition, useCallback, useEffect, useState } from "react";

import {
  bootstrapAdmin,
  fetchAuthBootstrapStatus,
  fetchAuthMe,
  login,
  logout,
} from "../lib/runtimeApi";
import { getErrorMessage } from "../lib/format";

export function useRuntimeAuth() {
  const [state, setState] = useState({
    phase: "loading",
    authStatus: null,
    session: null,
    error: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");

  const loadAuthState = useCallback(async () => {
    startTransition(() => {
      setState((current) => ({
        ...current,
        phase: "loading",
        error: "",
      }));
    });

    try {
      const authStatus = await fetchAuthBootstrapStatus();

      if (!authStatus.auth_enabled) {
        const me = await fetchAuthMe();
        startTransition(() => {
          setState({
            phase: "ready",
            authStatus,
            session: me.user || null,
            error: "",
          });
        });
        return;
      }

      if (!authStatus.login_allowed) {
        startTransition(() => {
          setState({
            phase: "unsupported",
            authStatus,
            session: null,
            error: "",
          });
        });
        return;
      }

      if (authStatus.bootstrap_required) {
        startTransition(() => {
          setState({
            phase: "bootstrap",
            authStatus,
            session: null,
            error: "",
          });
        });
        return;
      }

      try {
        const me = await fetchAuthMe();
        startTransition(() => {
          setState({
            phase: "ready",
            authStatus,
            session: me.user || null,
            error: "",
          });
        });
      } catch (caughtError) {
        if (caughtError?.status === 401) {
          startTransition(() => {
            setState({
              phase: "login",
              authStatus,
              session: null,
              error: "",
            });
          });
          return;
        }
        throw caughtError;
      }
    } catch (caughtError) {
      startTransition(() => {
        setState({
          phase: "error",
          authStatus: null,
          session: null,
          error: getErrorMessage(caughtError),
        });
      });
    }
  }, []);

  useEffect(() => {
    void loadAuthState();
  }, [loadAuthState]);

  const handleBootstrap = useCallback(
    async (form) => {
      setSubmitting(true);
      setSubmitError("");
      try {
        await bootstrapAdmin(form);
        await loadAuthState();
      } catch (caughtError) {
        setSubmitError(getErrorMessage(caughtError));
      } finally {
        setSubmitting(false);
      }
    },
    [loadAuthState],
  );

  const handleLogin = useCallback(
    async (form) => {
      setSubmitting(true);
      setSubmitError("");
      try {
        await login(form);
        await loadAuthState();
      } catch (caughtError) {
        setSubmitError(getErrorMessage(caughtError));
      } finally {
        setSubmitting(false);
      }
    },
    [loadAuthState],
  );

  const handleLogout = useCallback(async () => {
    await logout();
    await loadAuthState();
  }, [loadAuthState]);

  return {
    state,
    submitting,
    submitError,
    reloadAuth: loadAuthState,
    onBootstrap: handleBootstrap,
    onLogin: handleLogin,
    onLogout: handleLogout,
  };
}
