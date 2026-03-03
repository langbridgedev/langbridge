import type {
  OrganizationEnvironmentSetting,
  OrganizationEnvironmentSettingCatalogEntry,
} from '@/orchestration/organizations';

export const SETTINGS_CATEGORIES = [
  'General',
  'Security & Access',
  'Execution & Runtime',
  'AI / LLM',
  'Connectors',
  'Limits & Quotas',
  'Notifications',
  'Audit & Compliance',
] as const;

export type SettingsCategory = (typeof SETTINGS_CATEGORIES)[number];

export const ALL_CATEGORIES = 'All categories';

const CATEGORY_FALLBACK: SettingsCategory = 'General';

const SENSITIVE_KEYWORDS = [
  'token',
  'secret',
  'password',
  'api_key',
  'key',
  'connection',
  'credential',
];

export type SettingViewModel = {
  settingKey: string;
  displayName: string;
  description: string;
  category: SettingsCategory;
  scope: string;
  dataType: string;
  options: string[];
  placeholder?: string | null;
  multiline: boolean;
  defaultValue?: string | null;
  helperText?: string | null;
  isAdvanced: boolean;
  isLocked: boolean;
  isInherited: boolean;
  isConfigured: boolean;
  settingValue: string;
  lastUpdatedBy?: string | null;
  lastUpdatedAt?: string | null;
};

function normalizeCategory(category?: string | null): SettingsCategory {
  if (!category) {
    return CATEGORY_FALLBACK;
  }
  const match = SETTINGS_CATEGORIES.find((item) => item.toLowerCase() === category.toLowerCase());
  return match ?? CATEGORY_FALLBACK;
}

function normalizeDisplayName(settingKey: string): string {
  return settingKey
    .split('_')
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ');
}

function normalizeDataType(dataType?: string | null): string {
  if (!dataType) {
    return 'string';
  }
  return dataType.toLowerCase();
}

export function isSensitiveSetting(settingKey: string): boolean {
  const lowered = settingKey.toLowerCase();
  return SENSITIVE_KEYWORDS.some((keyword) => lowered.includes(keyword));
}

export function buildSettingsViewModel(
  catalog: OrganizationEnvironmentSettingCatalogEntry[],
  settings: OrganizationEnvironmentSetting[],
): SettingViewModel[] {
  const explicitByKey = new Map(settings.map((setting) => [setting.settingKey, setting]));
  const records: SettingViewModel[] = [];

  for (const entry of catalog) {
    const explicit = explicitByKey.get(entry.settingKey);
    explicitByKey.delete(entry.settingKey);

    const explicitValue = explicit?.settingValue ?? '';
    const hasExplicitValue = explicitValue.trim().length > 0;
    const effectiveValue = hasExplicitValue ? explicitValue : (entry.defaultValue ?? '');

    records.push({
      settingKey: entry.settingKey,
      displayName: entry.displayName,
      description: entry.description,
      category: normalizeCategory(entry.category),
      scope: entry.scope || 'organization',
      dataType: normalizeDataType(explicit?.dataType ?? entry.dataType),
      options: explicit?.options ?? entry.options ?? [],
      placeholder: explicit?.placeholder ?? entry.placeholder ?? null,
      multiline: Boolean(explicit?.multiline ?? entry.multiline),
      defaultValue: explicit?.defaultValue ?? entry.defaultValue ?? null,
      helperText: explicit?.helperText ?? entry.helperText ?? null,
      isAdvanced: Boolean(explicit?.isAdvanced ?? entry.isAdvanced),
      isLocked: Boolean(explicit?.isLocked ?? entry.isLocked),
      isInherited: Boolean(explicit?.isInherited ?? (!hasExplicitValue && Boolean(entry.defaultValue))),
      isConfigured: hasExplicitValue,
      settingValue: effectiveValue,
      lastUpdatedBy: explicit?.lastUpdatedBy ?? null,
      lastUpdatedAt: explicit?.lastUpdatedAt ?? null,
    });
  }

  for (const [, explicit] of explicitByKey.entries()) {
    records.push({
      settingKey: explicit.settingKey,
      displayName: explicit.displayName || normalizeDisplayName(explicit.settingKey),
      description: explicit.description || 'Custom organization setting.',
      category: normalizeCategory(explicit.category),
      scope: explicit.scope || 'organization',
      dataType: normalizeDataType(explicit.dataType),
      options: explicit.options ?? [],
      placeholder: explicit.placeholder ?? null,
      multiline: Boolean(explicit.multiline),
      defaultValue: explicit.defaultValue ?? null,
      helperText: explicit.helperText ?? null,
      isAdvanced: Boolean(explicit.isAdvanced),
      isLocked: Boolean(explicit.isLocked),
      isInherited: Boolean(explicit.isInherited),
      isConfigured: explicit.settingValue.trim().length > 0,
      settingValue: explicit.settingValue,
      lastUpdatedBy: explicit.lastUpdatedBy ?? null,
      lastUpdatedAt: explicit.lastUpdatedAt ?? null,
    });
  }

  const indexByCategory = new Map(
    SETTINGS_CATEGORIES.map((category, index) => [category, index]),
  );

  return records.sort((left, right) => {
    const leftCategory = indexByCategory.get(left.category) ?? 0;
    const rightCategory = indexByCategory.get(right.category) ?? 0;
    if (leftCategory !== rightCategory) {
      return leftCategory - rightCategory;
    }
    return left.displayName.localeCompare(right.displayName);
  });
}

export function summarizeSettingValue(record: Pick<SettingViewModel, 'settingKey' | 'settingValue' | 'dataType'>): string {
  const raw = (record.settingValue || '').trim();
  const dataType = normalizeDataType(record.dataType);
  if (!raw) {
    return 'Not set';
  }
  if (isSensitiveSetting(record.settingKey)) {
    return 'Configured';
  }
  if (dataType === 'boolean') {
    return raw.toLowerCase() === 'true' ? 'Enabled' : 'Disabled';
  }
  if (dataType === 'json') {
    try {
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        const size = Object.keys(parsed).length;
        return `${size} field${size === 1 ? '' : 's'}`;
      }
    } catch {
      // Fall through to plain text summary
    }
  }
  if (dataType === 'list') {
    const items = raw
      .split(/[,\n]/)
      .map((item) => item.trim())
      .filter(Boolean);
    if (items.length === 0) {
      return 'Not set';
    }
    if (items.length === 1) {
      return items[0];
    }
    return `${items[0]} (+${items.length - 1} more)`;
  }
  if (raw.length > 72) {
    return `${raw.slice(0, 69)}...`;
  }
  return raw;
}

export function validateSettingValue(
  record: Pick<SettingViewModel, 'dataType' | 'options'>,
  value: string,
): string | null {
  const trimmed = value.trim();
  const dataType = normalizeDataType(record.dataType);
  if (trimmed.length === 0) {
    return null;
  }

  const options = record.options ?? [];
  if (options.length > 0 && !options.includes(trimmed)) {
    return `Choose one of: ${options.join(', ')}`;
  }

  if (dataType === 'number') {
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) {
      return 'Enter a valid number.';
    }
  }

  if (dataType === 'boolean' && !['true', 'false'].includes(trimmed.toLowerCase())) {
    return "Enter 'true' or 'false'.";
  }

  if (dataType === 'json') {
    try {
      JSON.parse(trimmed);
    } catch {
      return 'Enter valid JSON.';
    }
  }

  return null;
}

export function normalizeSettingValueForSave(
  record: Pick<SettingViewModel, 'dataType'>,
  value: string,
): string {
  const dataType = normalizeDataType(record.dataType);
  const trimmed = value.trim();

  if (dataType === 'boolean') {
    return trimmed.toLowerCase() === 'true' ? 'true' : 'false';
  }
  if (dataType === 'list') {
    return trimmed
      .split(/[,\n]/)
      .map((item) => item.trim())
      .filter(Boolean)
      .join(', ');
  }
  return trimmed;
}

export function filterSettings(
  records: SettingViewModel[],
  search: string,
  selectedCategory: SettingsCategory | typeof ALL_CATEGORIES,
): SettingViewModel[] {
  const lowered = search.trim().toLowerCase();
  const terms = lowered ? lowered.split(/\s+/).filter(Boolean) : [];

  return records.filter((record) => {
    if (selectedCategory !== ALL_CATEGORIES && record.category !== selectedCategory) {
      return false;
    }
    if (terms.length === 0) {
      return true;
    }

    const haystack = [
      record.settingKey,
      record.displayName,
      record.description,
      record.category,
      record.scope,
      summarizeSettingValue(record),
    ]
      .join(' ')
      .toLowerCase();

    return terms.every((term) => haystack.includes(term));
  });
}
