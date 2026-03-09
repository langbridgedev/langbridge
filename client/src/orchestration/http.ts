export class ApiError extends Error {
  readonly status: number;
  readonly details: unknown;
  readonly code?: string;
  readonly suggestions: string[];
  readonly fieldErrors: Record<string, string>;

  constructor(
    message: string,
    options: {
      status: number;
      details?: unknown;
      code?: string;
      suggestions?: string[];
      fieldErrors?: Record<string, string>;
    },
  ) {
    super(message);
    this.name = 'ApiError';
    this.status = options.status;
    this.details = options.details;
    this.code = options.code;
    this.suggestions = options.suggestions ?? [];
    this.fieldErrors = options.fieldErrors ?? {};
  }
}

type RuntimeWindow = Window & {
  __LANGBRIDGE_BACKEND_URL__?: string;
};

function getApiBase(): string {
  if (typeof window !== 'undefined') {
    const runtimeApiBase = (window as RuntimeWindow).__LANGBRIDGE_BACKEND_URL__;
    if (runtimeApiBase) {
      return runtimeApiBase;
    }
  }

  return process.env.BACKEND_URL ?? process.env.NEXT_PUBLIC_BACKEND_URL ?? '';
}

function resolveUrl(path: string): string {
  if (path.startsWith('http://') || path.startsWith('https://')) {
    return path;
  }
  const apiBase = getApiBase();
  if (!apiBase) {
    return path;
  }
  return `${apiBase}${path}`;
}

export function resolveApiUrl(path: string): string {
  return resolveUrl(path);
}

export type ApiRequestOptions = RequestInit & {
  skipJsonParse?: boolean;
};

function toSuggestions(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => String(item).trim()).filter(Boolean);
}

function toFieldErrors(value: unknown): Record<string, string> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .map(([key, item]) => [key, String(item).trim()] as const)
      .filter(([, item]) => item.length > 0),
  );
}

export async function apiFetch<T = unknown>(path: string, options: ApiRequestOptions = {}): Promise<T> {
  const { headers, skipJsonParse, ...init } = options;
  const response = await fetch(resolveUrl(path), {
    credentials: 'include',
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init.body instanceof FormData ? {} : { 'Content-Type': 'application/json' }),
      ...(headers ?? {}),
    },
  });

  if (skipJsonParse) {
    if (!response.ok) {
      throw new ApiError(response.statusText || 'Request failed', {
        status: response.status,
      });
    }
    return undefined as T;
  }

  let payload: unknown = null;
  const contentType = response.headers.get('content-type');

  try {
    if (contentType && contentType.includes('application/json')) {
      payload = await response.json();
    } else {
      const text = await response.text();
      payload = text ? text : null;
    }
  } catch (error) {
    if (response.ok) {
      throw error;
    }
  }

  if (!response.ok) {
    const structuredError =
      typeof payload === 'object' && payload && 'error' in payload && typeof (payload as Record<string, unknown>).error === 'object'
        ? ((payload as Record<string, unknown>).error as Record<string, unknown>)
        : null;
    const message =
      typeof structuredError?.message === 'string'
        ? structuredError.message
        : typeof payload === 'object' && payload && 'detail' in payload
          ? String((payload as Record<string, unknown>).detail)
          : typeof payload === 'string' && payload
            ? payload
            : response.statusText || 'Request failed';
    throw new ApiError(message, {
      status: response.status,
      details: payload,
      code: typeof structuredError?.code === 'string' ? structuredError.code : undefined,
      suggestions: toSuggestions(structuredError?.suggestions),
      fieldErrors: toFieldErrors(structuredError?.fieldErrors),
    });
  }

  return payload as T;
}
