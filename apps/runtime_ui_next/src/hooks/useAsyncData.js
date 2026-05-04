import { useCallback, useEffect, useRef, useState } from "react";

import { getErrorMessage } from "../lib/format";

export function useAsyncData(loader, dependencies = [], options = {}) {
  const { enabled = true, initialData = null } = options;
  const initialDataRef = useRef(initialData);
  const [data, setData] = useState(initialData);
  const [loading, setLoading] = useState(Boolean(enabled));
  const [error, setError] = useState("");

  useEffect(() => {
    initialDataRef.current = initialData;
  }, [initialData]);

  const load = useCallback(async () => {
    if (!enabled) {
      setLoading(false);
      return initialDataRef.current;
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
  }, [enabled, loader, ...dependencies]);

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
