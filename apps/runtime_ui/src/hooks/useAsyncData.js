import { useCallback, useEffect, useState } from "react";

import { getErrorMessage } from "../lib/format";

export function useAsyncData(loader, dependencies = [], options = {}) {
  const { enabled = true, initialData = null } = options;
  const [data, setData] = useState(initialData);
  const [loading, setLoading] = useState(Boolean(enabled));
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    if (!enabled) {
      setLoading(false);
      return initialData;
    }
    setLoading(true);
    setError("");
    try {
      const nextData = await loader();
      setData(nextData);
      return nextData;
    } catch (caughtError) {
      setError(getErrorMessage(caughtError));
      return null;
    } finally {
      setLoading(false);
    }
  }, [enabled, initialData, loader, ...dependencies]);

  useEffect(() => {
    void load();
  }, [load]);

  return {
    data,
    loading,
    error,
    reload: load,
    setData,
  };
}
