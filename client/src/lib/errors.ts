import { ApiError } from '@/orchestration/http';

export type ErrorTone = 'error' | 'warning';
export type ErrorContext = 'generic' | 'sql.execution' | 'dataset.preview' | 'dataset.form' | 'schema.browser';

export interface DisplayError {
  title: string;
  message: string;
  suggestions: string[];
  technicalDetails?: string;
  errorCode?: string;
  tone: ErrorTone;
  fieldErrors?: Record<string, string>;
}

type StructuredErrorPayload = {
  code?: string;
  message?: string;
  details?: unknown;
  suggestions?: unknown;
  fieldErrors?: unknown;
};

const PATH_PATTERN = /(?:[A-Za-z]:)?(?:[\\/][^\\/\s]+){2,}/g;
const URL_PATTERN = /https?:\/\/[^\s)]+/g;

function sanitizeUserText(value: string): string {
  return value
    .replace(URL_PATTERN, 'an internal service')
    .replace(PATH_PATTERN, 'an internal path')
    .replace(/\s+/g, ' ')
    .trim();
}

function stringifyDetails(value: unknown): string | undefined {
  if (value == null) {
    return undefined;
  }
  if (typeof value === 'string') {
    return value.trim() || undefined;
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

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

function extractStructuredPayload(error: unknown): {
  status?: number;
  code?: string;
  message?: string;
  details?: string;
  suggestions: string[];
  fieldErrors: Record<string, string>;
} {
  if (error instanceof ApiError) {
    const payload = error.details as { error?: StructuredErrorPayload; detail?: unknown } | undefined;
    const structured = payload?.error;
    return {
      status: error.status,
      code: structured?.code ?? error.code,
      message: structured?.message ?? error.message,
      details: stringifyDetails(structured?.details ?? payload?.detail ?? error.details),
      suggestions: structured?.suggestions?.length ? toSuggestions(structured.suggestions) : error.suggestions,
      fieldErrors:
        Object.keys(toFieldErrors(structured?.fieldErrors)).length > 0
          ? toFieldErrors(structured?.fieldErrors)
          : error.fieldErrors,
    };
  }

  if (error instanceof Error) {
    return {
      message: error.message,
      details: error.message,
      suggestions: [],
      fieldErrors: {},
    };
  }

  return {
    message: undefined,
    details: stringifyDetails(error),
    suggestions: [],
    fieldErrors: {},
  };
}

function inferDefaultTone(status?: number): ErrorTone {
  if (status && status < 500) {
    return 'warning';
  }
  return 'error';
}

function buildGenericError(
  message: string | undefined,
  details: string | undefined,
  suggestions: string[],
  status?: number,
  code?: string,
  fieldErrors?: Record<string, string>,
): DisplayError {
  const safeMessage = sanitizeUserText(message || 'Something went wrong while processing the request.');
  return {
    title: status && status < 500 ? 'Request could not be completed' : 'Something went wrong',
    message: safeMessage,
    suggestions:
      suggestions.length > 0
        ? suggestions
        : [
            'Review the current input and try again.',
            'Refresh the page if the issue persists.',
            'Copy the technical details when reporting the issue.',
          ],
    technicalDetails: details,
    errorCode: code,
    tone: inferDefaultTone(status),
    fieldErrors,
  };
}

export function createDisplayError(error: DisplayError): DisplayError {
  return error;
}

export function toDisplayError(error: unknown, context: ErrorContext = 'generic'): DisplayError {
  const { status, code, message, details, suggestions, fieldErrors } = extractStructuredPayload(error);
  const rawMessage = message || details || 'Something went wrong while processing the request.';
  const normalizedMessage = sanitizeUserText(rawMessage);
  const normalizedDetails = details || message;
  const lower = rawMessage.toLowerCase();

  if (
    context === 'dataset.preview'
    && (
      lower.includes('no files found that match the pattern')
      || lower.includes('no such file or directory')
      || lower.includes('file.parquet')
      || lower.includes('/cache/datasets/')
    )
  ) {
    return {
      title: 'Dataset preview failed',
      message: 'Langbridge could not find the data file for this dataset.',
      suggestions: [
        'Re-run the dataset sync so the preview file is recreated.',
        'Check the connector configuration and dataset path settings.',
        'Restore a previous dataset revision if the current asset is incomplete.',
      ],
      technicalDetails: normalizedDetails,
      errorCode: code || 'DATASET_FILE_NOT_FOUND',
      tone: 'warning',
    };
  }

  if (context === 'dataset.preview') {
    return {
      title: 'Dataset preview failed',
      message: 'Langbridge could not load a preview for this dataset.',
      suggestions:
        suggestions.length > 0
          ? suggestions
          : [
              'Verify that the dataset sync completed successfully.',
              'Check the dataset configuration and selected preview limit.',
              'Retry the preview after refreshing the dataset details.',
            ],
      technicalDetails: normalizedDetails,
      errorCode: code,
      tone: inferDefaultTone(status),
    };
  }

  if (context === 'dataset.form') {
    return {
      title: 'Dataset creation failed',
      message:
        Object.keys(fieldErrors).length > 0
          ? 'Some dataset fields need attention before Langbridge can create the asset.'
          : normalizedMessage || 'Langbridge could not create the dataset with the current configuration.',
      suggestions:
        suggestions.length > 0
          ? suggestions
          : [
              'Review the required fields and selected connection.',
              'Check the schema, table, or SQL configuration for this dataset.',
              'Try again after correcting the highlighted fields.',
            ],
      technicalDetails: normalizedDetails,
      errorCode: code,
      tone: 'warning',
      fieldErrors,
    };
  }

  if (context === 'schema.browser') {
    return {
      title: 'Schema browser unavailable',
      message: 'Langbridge could not load schema metadata for this connection.',
      suggestions:
        suggestions.length > 0
          ? suggestions
          : [
              'Test the connection and confirm the source is reachable.',
              'Check that the selected credentials can read schema metadata.',
              'Refresh the browser after the connector is updated.',
            ],
      technicalDetails: normalizedDetails,
      errorCode: code,
      tone: inferDefaultTone(status),
    };
  }

  if (context === 'sql.execution') {
    let contextualMessage = 'The query could not be executed.';
    const contextualSuggestions = suggestions.length > 0 ? suggestions : [];

    if (lower.includes('syntax error') || lower.includes('parser error')) {
      contextualMessage = 'The SQL statement contains invalid syntax.';
      contextualSuggestions.push(
        'Review the SQL syntax, aliases, and clause order.',
        'Check that referenced tables and columns exist in the selected source.',
      );
    } else if (lower.includes('permission') || lower.includes('forbidden') || lower.includes('not authorized')) {
      contextualMessage = 'The current workspace policy or connection permissions blocked this query.';
      contextualSuggestions.push(
        'Confirm that the selected source and schemas are allowed by workspace policy.',
        'Use a connection with access to the requested data.',
      );
    } else if (lower.includes('dataset') || lower.includes('file') || lower.includes('parquet')) {
      contextualMessage = 'The query referenced data that Langbridge could not access during execution.';
      contextualSuggestions.push(
        'Rebuild or resync the affected dataset before rerunning the query.',
        'Check federated dataset bindings and connector health.',
      );
    } else {
      contextualSuggestions.push(
        'Review the SQL text and selected connection.',
        'Retry the query after checking source availability.',
      );
    }

    return {
      title: 'Query execution failed',
      message: contextualMessage,
      suggestions: Array.from(new Set(contextualSuggestions)),
      technicalDetails: normalizedDetails,
      errorCode: code,
      tone: inferDefaultTone(status),
    };
  }

  return buildGenericError(message, details, suggestions, status, code, fieldErrors);
}
