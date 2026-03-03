import type {
  Organization,
  OrganizationInvite,
  Project,
  ProjectInvite,
  OrganizationEnvironmentSetting,
  OrganizationEnvironmentSettingCatalogEntry,
  RuntimeInstance,
  RuntimeRegistrationToken,
} from './types';
import { apiFetch } from '../http';

const BASE_PATH = '/api/v1/organizations';
const RUNTIMES_BASE_PATH = '/api/v1/runtimes';

export async function fetchOrganizations(): Promise<Organization[]> {
  return apiFetch<Organization[]>(BASE_PATH);
}

export async function createOrganization(payload: { name: string }): Promise<Organization> {
  return apiFetch<Organization>(BASE_PATH, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function createProject(
  organizationId: string,
  payload: { name: string },
): Promise<Project> {
  return apiFetch<Project>(`${BASE_PATH}/${organizationId}/projects`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function inviteToOrganization(
  organizationId: string,
  payload: { username: string },
): Promise<OrganizationInvite> {
  return apiFetch<OrganizationInvite>(`${BASE_PATH}/${organizationId}/invites`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function inviteToProject(
  organizationId: string,
  projectId: string,
  payload: { username: string },
): Promise<ProjectInvite> {
  return apiFetch<ProjectInvite>(`${BASE_PATH}/${organizationId}/projects/${projectId}/invites`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function fetchOrganizationEnvironmentKeys(): Promise<string[]> {
  return apiFetch<string[]>(`${BASE_PATH}/environment/keys`);
}

export async function fetchOrganizationEnvironmentCatalog(): Promise<OrganizationEnvironmentSettingCatalogEntry[]> {
  return apiFetch<OrganizationEnvironmentSettingCatalogEntry[]>(`${BASE_PATH}/environment/catalog`);
}

export async function fetchOrganizationEnvironmentSettings(
  organizationId: string,
): Promise<OrganizationEnvironmentSetting[]> {
  return apiFetch<OrganizationEnvironmentSetting[]>(`${BASE_PATH}/${organizationId}/environment`);
}

export async function setOrganizationEnvironmentSetting(
  organizationId: string,
  settingKey: string,
  settingValue: string,
): Promise<OrganizationEnvironmentSetting> {
  return apiFetch<OrganizationEnvironmentSetting>(`${BASE_PATH}/${organizationId}/environment/${settingKey}`, {
    method: 'POST',
    body: JSON.stringify({
      settingKey,
      settingValue,
    }),
  });
}

export async function deleteOrganizationEnvironmentSetting(
  organizationId: string,
  settingKey: string,
): Promise<void> {
  await apiFetch<void>(`${BASE_PATH}/${organizationId}/environment/${settingKey}`, {
    method: 'DELETE',
    skipJsonParse: true,
  });
}

export async function createRuntimeRegistrationToken(
  organizationId: string,
): Promise<RuntimeRegistrationToken> {
  return apiFetch<RuntimeRegistrationToken>(`${RUNTIMES_BASE_PATH}/${organizationId}/tokens`, {
    method: 'POST',
  });
}

export async function fetchRuntimeInstances(
  organizationId: string,
): Promise<RuntimeInstance[]> {
  return apiFetch<RuntimeInstance[]>(`${RUNTIMES_BASE_PATH}/${organizationId}/instances`);
}

export type {
  InviteStatus,
  Organization,
  OrganizationInvite,
  OrganizationEnvironmentSetting,
  OrganizationEnvironmentSettingCatalogEntry,
  Project,
  ProjectInvite,
  RuntimeInstance,
  RuntimeRegistrationToken,
} from './types';
