import { apiFetch } from '../http';

const BASE_PATH = '/api/v1/jobs';

function requireOrganizationId(organizationId: string): string {
  if (!organizationId) {
    throw new Error('Organization id is required.');
  }
  return organizationId;
}

function basePath(organizationId: string): string {
  return `${BASE_PATH}/${requireOrganizationId(organizationId)}`;
}

export type JobEventVisibility = 'public' | 'internal';

export type AgentJobEvent = {
  id: string;
  eventType: string;
  visibility: JobEventVisibility;
  message: string;
  source?: string | null;
  details: Record<string, unknown>;
  createdAt?: string | null;
};

export type AgentJobFinalResponse = {
  result?: unknown;
  visualization?: unknown;
  summary?: string | null;
};

export type AgentJobState = {
  id: string;
  jobType: string;
  status: string;
  progress: number;
  error?: Record<string, unknown> | null;
  createdAt?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  events: AgentJobEvent[];
  finalResponse?: AgentJobFinalResponse | null;
  thinkingBreakdown?: Record<string, unknown> | null;
  hasInternalEvents?: boolean;
};

export type AgentJobCancelResponse = {
  accepted: boolean;
  status: string;
};

export async function fetchAgentJobState(
  organizationId: string,
  jobId: string,
  includeInternal = false,
): Promise<AgentJobState> {
  const params = new URLSearchParams();
  if (includeInternal) {
    params.set('include_internal', 'true');
  }
  const query = params.toString();
  return apiFetch<AgentJobState>(`${basePath(organizationId)}/${jobId}${query ? `?${query}` : ''}`);
}

export async function cancelAgentJob(
  organizationId: string,
  jobId: string,
): Promise<AgentJobCancelResponse> {
  if (!jobId) {
    throw new Error('Job id is required.');
  }
  return apiFetch<AgentJobCancelResponse>(`${basePath(organizationId)}/${jobId}/cancel`, {
    method: 'POST',
  });
}
