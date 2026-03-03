import { apiFetch } from '@/orchestration/http';

import type {
  CreateSemanticModelPayload,
  SemanticModelAgenticJobCreatePayload,
  SemanticModelAgenticJobCreateResponse,
  SemanticModelCatalogResponse,
  SemanticModelKind,
  SemanticModel,
  SemanticModelRecord,
  SemanticModelSelectionGeneratePayload,
  SemanticModelSelectionGenerateResponse,
  UpdateSemanticModelPayload,
} from './types';

const BASE_PATH = '/api/v1/semantic-model';

function requireOrganizationId(organizationId: string): string {
  if (!organizationId) {
    throw new Error('Organization id is required.');
  }
  return organizationId;
}

function basePath(organizationId: string): string {
  return `${BASE_PATH}/${requireOrganizationId(organizationId)}`;
}

export async function previewSemanticModel(
  organizationId: string,
  projectId?: string,
): Promise<SemanticModel> {
  const params = new URLSearchParams();
  if (projectId) {
    params.set('project_id', projectId);
  }
  const suffix = params.toString();
  return apiFetch<SemanticModel>(
    `${basePath(organizationId)}/preview${suffix ? `?${suffix}` : ''}`,
  );
}

export async function listSemanticModels(
  organizationId: string,
  projectId?: string,
  modelKind: SemanticModelKind = 'all',
): Promise<SemanticModelRecord[]> {
  const params = new URLSearchParams();
  if (projectId) {
    params.set('project_id', projectId);
  }
  params.set('model_kind', modelKind);
  const suffix = params.toString();
  return apiFetch<SemanticModelRecord[]>(
    `${basePath(organizationId)}${suffix ? `?${suffix}` : ''}`,
  );
}

export async function createSemanticModel(
  organizationId: string,
  payload: CreateSemanticModelPayload,
): Promise<SemanticModelRecord> {
  const body: CreateSemanticModelPayload = {
    ...payload,
    organizationId: requireOrganizationId(organizationId),
  };
  if (body.projectId?.length === 0) {
    body.projectId = undefined;
  }
  return apiFetch<SemanticModelRecord>(basePath(organizationId), {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function updateSemanticModel(
  modelId: string,
  organizationId: string,
  payload: UpdateSemanticModelPayload,
): Promise<SemanticModelRecord> {
  const body: UpdateSemanticModelPayload = { ...payload };
  if (body.projectId?.length === 0) {
    body.projectId = undefined;
  }
  return apiFetch<SemanticModelRecord>(`${basePath(organizationId)}/${modelId}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
}

export async function deleteSemanticModel(
  modelId: string,
  organizationId: string,
): Promise<void> {
  await apiFetch<void>(`${basePath(organizationId)}/${modelId}`, {
    method: 'DELETE',
    skipJsonParse: true,
  });
}

export async function fetchSemanticModel(
  modelId: string,
  organizationId: string,
): Promise<SemanticModelRecord> {
  return apiFetch<SemanticModelRecord>(`${basePath(organizationId)}/${modelId}`);
}

export async function fetchSemanticModels(
  organizationId: string,
): Promise<SemanticModelRecord[]> {
  return apiFetch<SemanticModelRecord[]>(basePath(organizationId));
}

export async function previewSemanticModelYaml(
  organizationId: string,
  projectId?: string,
): Promise<string> {
  const params = new URLSearchParams();
  if (projectId) {
    params.set('project_id', projectId);
  }
  const suffix = params.toString();
  return await apiFetch<string>(
    `${basePath(organizationId)}/preview/yaml${suffix ? `?${suffix}` : ''}`,
  );
}

export async function fetchSemanticModelYaml(
  modelId: string,
  organizationId: string,
): Promise<string> {
  return await apiFetch<string>(`${basePath(organizationId)}/${modelId}/yaml`);
}

export async function generateSemanticModelYaml(
  organizationId: string,
  connectorId: string,
): Promise<string> {
  if (!connectorId) {
    throw new Error('Connector id is required to generate a semantic model.');
  }
  const params = new URLSearchParams({ connector_id: connectorId });
  return apiFetch<string>(`${basePath(organizationId)}/generate/yaml?${params.toString()}`);
}

export async function fetchSemanticModelCatalog(
  organizationId: string,
  connectorId: string,
): Promise<SemanticModelCatalogResponse> {
  if (!connectorId) {
    throw new Error('Connector id is required to load semantic model catalog.');
  }
  const params = new URLSearchParams({ connector_id: connectorId });
  return apiFetch<SemanticModelCatalogResponse>(`${basePath(organizationId)}/catalog?${params.toString()}`);
}

export async function generateSemanticModelYamlFromSelection(
  organizationId: string,
  payload: SemanticModelSelectionGeneratePayload,
): Promise<SemanticModelSelectionGenerateResponse> {
  return apiFetch<SemanticModelSelectionGenerateResponse>(`${basePath(organizationId)}/generate/yaml`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function startAgenticSemanticModelJob(
  organizationId: string,
  payload: SemanticModelAgenticJobCreatePayload,
): Promise<SemanticModelAgenticJobCreateResponse> {
  const body: SemanticModelAgenticJobCreatePayload = { ...payload };
  if (body.projectId?.length === 0) {
    body.projectId = undefined;
  }
  return apiFetch<SemanticModelAgenticJobCreateResponse>(`${basePath(organizationId)}/agentic/jobs`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}
