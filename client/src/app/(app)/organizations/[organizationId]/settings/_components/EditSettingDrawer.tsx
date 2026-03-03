'use client';

import { useEffect, useMemo, useState } from 'react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Drawer, DrawerContent } from '@/components/ui/drawer';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import {
  normalizeSettingValueForSave,
  validateSettingValue,
  type SettingViewModel,
} from '../_lib/settings-utils';

interface EditSettingDrawerProps {
  open: boolean;
  setting: SettingViewModel | null;
  onOpenChange: (open: boolean) => void;
  onSave: (setting: SettingViewModel, value: string) => Promise<void> | void;
  onDuplicate: (setting: SettingViewModel, value: string) => void;
}

export function EditSettingDrawer({
  open,
  setting,
  onOpenChange,
  onSave,
  onDuplicate,
}: EditSettingDrawerProps) {
  const [draftValue, setDraftValue] = useState('');
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!setting) {
      setDraftValue('');
      setAdvancedOpen(false);
      setErrorMessage(null);
      return;
    }
    setDraftValue(setting.settingValue ?? '');
    setAdvancedOpen(false);
    setErrorMessage(null);
  }, [setting?.settingKey, setting?.settingValue, setting]);

  const isDirty = useMemo(() => {
    if (!setting) {
      return false;
    }
    return draftValue !== (setting.settingValue ?? '');
  }, [draftValue, setting]);

  const validationMessage = useMemo(() => {
    if (!setting) {
      return null;
    }
    return validateSettingValue(setting, draftValue);
  }, [draftValue, setting]);

  const canSave = Boolean(setting) && isDirty && !validationMessage && !isSaving && !setting?.isLocked;

  if (!setting) {
    return (
      <Drawer open={open} onOpenChange={onOpenChange}>
        <DrawerContent side="right" className="max-w-xl" />
      </Drawer>
    );
  }

  const handleSave = async () => {
    if (!canSave) {
      return;
    }
    setIsSaving(true);
    setErrorMessage(null);
    try {
      await onSave(setting, normalizeSettingValueForSave(setting, draftValue));
      onOpenChange(false);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to save this setting.';
      setErrorMessage(message);
    } finally {
      setIsSaving(false);
    }
  };

  const resetToDefault = () => {
    setDraftValue(setting.defaultValue ?? '');
  };

  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent side="right" className="max-w-xl">
        <div className="space-y-5">
          <div>
            <div className="mb-2 flex flex-wrap items-center gap-2">
              <Badge variant="secondary">{setting.category}</Badge>
              <Badge variant="secondary">
                {setting.scope === 'workspace' ? 'Workspace' : setting.scope === 'user' ? 'User' : 'Org'}
              </Badge>
              {setting.isLocked ? <Badge variant="destructive">Locked</Badge> : null}
              {setting.isInherited ? <Badge variant="warning">Inherited</Badge> : null}
            </div>
            <h2 className="text-xl font-semibold text-[color:var(--text-primary)]">{setting.displayName}</h2>
            <p className="mt-1 text-sm text-[color:var(--text-secondary)]">{setting.description}</p>
            <p className="mt-1 text-xs font-mono text-[color:var(--text-muted)]">{setting.settingKey}</p>
          </div>

          {isDirty ? (
            <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-700 dark:text-amber-200">
              Unsaved changes
            </div>
          ) : null}

          {setting.options.length > 0 ? (
            <Select value={draftValue} onChange={(event) => setDraftValue(event.target.value)} disabled={setting.isLocked}>
              <option value="">Not set</option>
              {setting.options.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </Select>
          ) : setting.multiline || setting.dataType === 'json' || setting.dataType === 'list' ? (
            <Textarea
              value={draftValue}
              onChange={(event) => setDraftValue(event.target.value)}
              placeholder={setting.placeholder ?? undefined}
              className={setting.dataType === 'json' ? 'min-h-[180px] font-mono text-xs' : 'min-h-[120px]'}
              disabled={setting.isLocked}
            />
          ) : (
            <Input
              value={draftValue}
              onChange={(event) => setDraftValue(event.target.value)}
              placeholder={setting.placeholder ?? undefined}
              disabled={setting.isLocked}
            />
          )}

          {validationMessage ? (
            <p className="text-sm text-rose-600 dark:text-rose-300">{validationMessage}</p>
          ) : null}
          {errorMessage ? (
            <p className="text-sm text-rose-600 dark:text-rose-300">{errorMessage}</p>
          ) : null}

          {setting.helperText ? <p className="text-xs text-[color:var(--text-muted)]">{setting.helperText}</p> : null}

          <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-3">
            <button
              type="button"
              className="text-sm font-semibold text-[color:var(--text-primary)]"
              onClick={() => setAdvancedOpen((current) => !current)}
            >
              {advancedOpen ? 'Hide advanced options' : 'Show advanced options'}
            </button>
            {advancedOpen ? (
              <div className="mt-3 space-y-2 text-xs text-[color:var(--text-muted)]">
                <p>Data type: {setting.dataType}</p>
                <p>Default value: {setting.defaultValue ?? 'none'}</p>
                <p>Last updated: {setting.lastUpdatedAt ? new Date(setting.lastUpdatedAt).toLocaleString() : 'Not available'}</p>
                <p>Last updated by: {setting.lastUpdatedBy ?? 'Not available'}</p>
              </div>
            ) : null}
          </div>

          <div className="flex flex-wrap items-center gap-2 pt-2">
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button onClick={handleSave} isLoading={isSaving} disabled={!canSave}>
              Save changes
            </Button>
            <Button
              variant="ghost"
              onClick={resetToDefault}
              disabled={setting.defaultValue == null || setting.isLocked}
            >
              Reset to default
            </Button>
            <Button
              variant="ghost"
              onClick={() => onDuplicate(setting, draftValue)}
              disabled={setting.isLocked}
            >
              Duplicate setting
            </Button>
          </div>
        </div>
      </DrawerContent>
    </Drawer>
  );
}
