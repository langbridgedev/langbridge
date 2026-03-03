import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { AddSettingModal } from './AddSettingModal';
import type { SettingViewModel } from '../_lib/settings-utils';

const catalog: SettingViewModel[] = [
  {
    settingKey: 'query_timeout_seconds',
    displayName: 'Query timeout seconds',
    description: 'Maximum runtime for a query.',
    category: 'Limits & Quotas',
    scope: 'organization',
    dataType: 'number',
    options: [],
    placeholder: '30',
    multiline: false,
    defaultValue: '30',
    helperText: 'Use seconds.',
    isAdvanced: false,
    isLocked: false,
    isInherited: false,
    isConfigured: false,
    settingValue: '',
    lastUpdatedBy: null,
    lastUpdatedAt: null,
  },
];

describe('AddSettingModal', () => {
  it('walks through type -> configure -> review and creates the setting', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onCreate = vi.fn().mockResolvedValue(undefined);

    render(
      <AddSettingModal
        open
        onOpenChange={onOpenChange}
        settingsCatalog={catalog}
        onCreate={onCreate}
      />,
    );

    await screen.findByTestId('add-setting-step-1');
    await user.click(screen.getByRole('button', { name: /Query timeout seconds/i }));
    await user.click(screen.getByRole('button', { name: 'Continue' }));

    await screen.findByTestId('add-setting-step-2');
    const input = screen.getByDisplayValue('30');
    fireEvent.change(input, { target: { value: '45' } });
    await user.click(screen.getByRole('button', { name: 'Review' }));

    await screen.findByTestId('add-setting-step-3');
    await user.click(screen.getByRole('button', { name: 'Create setting' }));

    await waitFor(() => {
      expect(onCreate).toHaveBeenCalledWith(catalog[0], '45');
      expect(onOpenChange).toHaveBeenCalledWith(false);
    });
  });
});
