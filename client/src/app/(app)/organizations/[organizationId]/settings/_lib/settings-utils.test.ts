import { describe, expect, it } from 'vitest';

import {
  ALL_CATEGORIES,
  buildSettingsViewModel,
  filterSettings,
  validateSettingValue,
} from './settings-utils';

describe('settings-utils', () => {
  it('filters settings by search text and category', () => {
    const records = buildSettingsViewModel(
      [
        {
          settingKey: 'llm_enabled',
          displayName: 'LLM enabled',
          description: 'Allow LLM-assisted workflows.',
          category: 'AI / LLM',
          scope: 'organization',
          dataType: 'boolean',
          options: ['true', 'false'],
          multiline: false,
          defaultValue: 'false',
          isLocked: false,
          isInherited: false,
          isAdvanced: false,
        },
        {
          settingKey: 'query_timeout_seconds',
          displayName: 'Query timeout seconds',
          description: 'Max query runtime.',
          category: 'Limits & Quotas',
          scope: 'organization',
          dataType: 'number',
          multiline: false,
          isLocked: false,
          isInherited: false,
          isAdvanced: false,
        },
      ],
      [{ settingKey: 'llm_enabled', settingValue: 'true' }],
    );

    const bySearch = filterSettings(records, 'llm workflows', ALL_CATEGORIES);
    expect(bySearch).toHaveLength(1);
    expect(bySearch[0]?.settingKey).toBe('llm_enabled');

    const byCategory = filterSettings(records, '', 'Limits & Quotas');
    expect(byCategory).toHaveLength(1);
    expect(byCategory[0]?.settingKey).toBe('query_timeout_seconds');
  });

  it('validates number settings', () => {
    const error = validateSettingValue(
      { dataType: 'number', options: [] },
      'abc',
    );
    expect(error).toBe('Enter a valid number.');

    const ok = validateSettingValue(
      { dataType: 'number', options: [] },
      '120',
    );
    expect(ok).toBeNull();
  });
});
