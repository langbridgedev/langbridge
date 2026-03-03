import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { EditSettingDrawer } from './EditSettingDrawer';
import type { SettingViewModel } from '../_lib/settings-utils';

const baseSetting: SettingViewModel = {
  settingKey: 'support_email',
  displayName: 'Support email',
  description: 'Default support email address.',
  category: 'General',
  scope: 'organization',
  dataType: 'string',
  options: [],
  placeholder: 'support@company.com',
  multiline: false,
  defaultValue: null,
  helperText: null,
  isAdvanced: false,
  isLocked: false,
  isInherited: false,
  isConfigured: true,
  settingValue: 'support@old.example',
  lastUpdatedBy: null,
  lastUpdatedAt: null,
};

describe('EditSettingDrawer', () => {
  it('shows dirty state and saves edited values', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSave = vi.fn().mockResolvedValue(undefined);
    const onDuplicate = vi.fn();

    render(
      <EditSettingDrawer
        open
        setting={baseSetting}
        onOpenChange={onOpenChange}
        onSave={onSave}
        onDuplicate={onDuplicate}
      />,
    );

    await screen.findByText('Support email');
    const input = screen.getByDisplayValue('support@old.example');
    await user.clear(input);
    await user.type(input, 'support@new.example');

    expect(screen.getByText('Unsaved changes')).toBeInTheDocument();
    const saveButton = screen.getByRole('button', { name: 'Save changes' });
    expect(saveButton).toBeEnabled();

    await user.click(saveButton);
    await waitFor(() => {
      expect(onSave).toHaveBeenCalledWith(baseSetting, 'support@new.example');
      expect(onOpenChange).toHaveBeenCalledWith(false);
    });
  });
});
