export type InviteStatus = 'pending' | 'accepted' | 'declined';
export type SettingScope = 'organization' | 'workspace' | 'user';
export type SettingDataType = 'string' | 'number' | 'boolean' | 'json' | 'list';

export interface Project {
  id: string;
  name: string;
  organizationId: string;
}

export interface Organization {
  id: string;
  name: string;
  memberCount: number;
  projects: Project[];
}

export interface OrganizationInvite {
  id: string;
  status: InviteStatus;
  inviteeUsername: string;
}

export interface ProjectInvite {
  id: string;
  status: InviteStatus;
  inviteeId: string;
}

export interface OrganizationEnvironmentSetting {
  settingKey: string;
  settingValue: string;
  category?: string | null;
  displayName?: string | null;
  description?: string | null;
  scope?: SettingScope | string | null;
  isLocked?: boolean;
  isInherited?: boolean;
  lastUpdatedBy?: string | null;
  lastUpdatedAt?: string | null;
  dataType?: SettingDataType | string | null;
  options?: string[] | null;
  placeholder?: string | null;
  multiline?: boolean | null;
  defaultValue?: string | null;
  helperText?: string | null;
  isAdvanced?: boolean;
}

export interface OrganizationEnvironmentSettingCatalogEntry {
  settingKey: string;
  displayName: string;
  description: string;
  category: string;
  scope: SettingScope | string;
  dataType: SettingDataType | string;
  options?: string[] | null;
  placeholder?: string | null;
  multiline?: boolean;
  defaultValue?: string | null;
  isLocked?: boolean;
  isInherited?: boolean;
  helperText?: string | null;
  isAdvanced?: boolean;
}

export interface RuntimeRegistrationToken {
  registrationToken: string;
  expiresAt: string;
}

export interface RuntimeInstance {
  epId: string;
  tenantId: string;
  displayName: string | null;
  status: string;
  tags: string[];
  capabilities: Record<string, unknown>;
  metadata: Record<string, unknown>;
  registeredAt: string;
  lastSeenAt: string | null;
  updatedAt: string | null;
}
