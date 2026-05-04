import { runtimeRequest } from "../lib/runtimeApi.js";

export class LangbridgeApiError extends Error {
  constructor(message, { status = 0, payload = null } = {}) {
    super(message);
    this.name = "LangbridgeApiError";
    this.status = status;
    this.payload = payload;
  }
}

export async function langbridgeRequest(path, options = {}) {
  try {
    return await runtimeRequest(path, options);
  } catch (error) {
    if (error instanceof LangbridgeApiError) {
      throw error;
    }
    throw new LangbridgeApiError(error?.message || "Runtime API request failed.", {
      status: error?.status || 0,
      payload: error?.payload || error,
    });
  }
}

export async function langbridgeList(path, options = {}) {
  return getItems(await langbridgeRequest(path, options));
}

export function getItems(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (Array.isArray(payload?.items)) {
    return payload.items;
  }
  if (Array.isArray(payload?.data)) {
    return payload.data;
  }
  return [];
}
