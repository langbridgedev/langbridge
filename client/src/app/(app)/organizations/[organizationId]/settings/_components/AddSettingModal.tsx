'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  Bell,
  Gauge,
  Plug,
  Settings2,
  Shield,
  ShieldCheck,
  Sparkles,
  Cpu,
} from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import {
  isSensitiveSetting,
  normalizeSettingValueForSave,
  summarizeSettingValue,
  validateSettingValue,
  type SettingViewModel,
} from '../_lib/settings-utils';

interface AddSettingModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  settingsCatalog: SettingViewModel[];
  onCreate: (setting: SettingViewModel, value: string) => Promise<void> | void;
  initialSelection?: {
    settingKey: string;
    value?: string;
  } | null;
}

const CATEGORY_ICON_MAP: Record<string, typeof Settings2> = {
  General: Settings2,
  'Security & Access': Shield,
  'Execution & Runtime': Cpu,
  'AI / LLM': Sparkles,
  Connectors: Plug,
  'Limits & Quotas': Gauge,
  Notifications: Bell,
  'Audit & Compliance': ShieldCheck,
};

export function AddSettingModal({
  open,
  onOpenChange,
  settingsCatalog,
  onCreate,
  initialSelection,
}: AddSettingModalProps) {
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [search, setSearch] = useState('');
  const [selectedKey, setSelectedKey] = useState('');
  const [draftValue, setDraftValue] = useState('');
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isCreating, setIsCreating] = useState(false);

  useEffect(() => {
    if (!open) {
      setStep(1);
      setSearch('');
      setSelectedKey('');
      setDraftValue('');
      setAdvancedOpen(false);
      setErrorMessage(null);
      setIsCreating(false);
      return;
    }
    if (initialSelection?.settingKey) {
      setSelectedKey(initialSelection.settingKey);
      setDraftValue(initialSelection.value ?? '');
      setStep(2);
    }
  }, [open, initialSelection?.settingKey, initialSelection?.value]);

  const selectableSettings = useMemo(
    () => settingsCatalog.filter((item) => !item.isLocked),
    [settingsCatalog],
  );

  const filteredSettings = useMemo(() => {
    const lowered = search.trim().toLowerCase();
    if (!lowered) {
      return selectableSettings;
    }
    return selectableSettings.filter((setting) => {
      const haystack = [
        setting.displayName,
        setting.description,
        setting.category,
        setting.settingKey,
      ]
        .join(' ')
        .toLowerCase();
      return haystack.includes(lowered);
    });
  }, [search, selectableSettings]);

  const selectedSetting = useMemo(
    () => selectableSettings.find((item) => item.settingKey === selectedKey) ?? null,
    [selectableSettings, selectedKey],
  );

  useEffect(() => {
    if (!selectedSetting) {
      return;
    }
    if (step !== 2) {
      return;
    }
    if (draftValue !== '') {
      return;
    }
    setDraftValue(selectedSetting.defaultValue ?? '');
  }, [selectedSetting, step, draftValue]);

  const validationMessage = useMemo(() => {
    if (!selectedSetting) {
      return null;
    }
    return validateSettingValue(selectedSetting, draftValue);
  }, [selectedSetting, draftValue]);

  const handleContinueFromType = () => {
    if (!selectedSetting) {
      setErrorMessage('Choose a setting type to continue.');
      return;
    }
    setErrorMessage(null);
    if (draftValue.length === 0 && selectedSetting.defaultValue != null) {
      setDraftValue(selectedSetting.defaultValue);
    }
    setStep(2);
  };

  const handleContinueFromConfigure = () => {
    if (!selectedSetting) {
      setErrorMessage('Select a setting to configure.');
      return;
    }
    if (validationMessage) {
      setErrorMessage(validationMessage);
      return;
    }
    setErrorMessage(null);
    setStep(3);
  };

  const handleCreate = async () => {
    if (!selectedSetting) {
      setErrorMessage('Select a setting to create.');
      return;
    }
    if (validationMessage) {
      setErrorMessage(validationMessage);
      return;
    }
    setErrorMessage(null);
    setIsCreating(true);
    try {
      await onCreate(selectedSetting, normalizeSettingValueForSave(selectedSetting, draftValue));
      onOpenChange(false);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : 'Unable to create setting.');
    } finally {
      setIsCreating(false);
    }
  };

  const renderConfigField = () => {
    if (!selectedSetting) {
      return null;
    }
    if (selectedSetting.options.length > 0) {
      return (
        <Select value={draftValue} onChange={(event) => setDraftValue(event.target.value)}>
          <option value="">Not set</option>
          {selectedSetting.options.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </Select>
      );
    }

    if (
      selectedSetting.multiline ||
      selectedSetting.dataType === 'json' ||
      selectedSetting.dataType === 'list'
    ) {
      return (
        <Textarea
          value={draftValue}
          onChange={(event) => setDraftValue(event.target.value)}
          placeholder={selectedSetting.placeholder ?? undefined}
          className={selectedSetting.dataType === 'json' ? 'min-h-[180px] font-mono text-xs' : 'min-h-[120px]'}
        />
      );
    }

    return (
      <Input
        value={draftValue}
        onChange={(event) => setDraftValue(event.target.value)}
        placeholder={selectedSetting.placeholder ?? undefined}
      />
    );
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl" data-testid="add-setting-modal">
        <DialogHeader>
          <DialogTitle>Add setting</DialogTitle>
          <DialogDescription>
            Create organization settings with guided defaults and validation.
          </DialogDescription>
        </DialogHeader>

        <div className="mb-4 flex items-center gap-2 text-xs">
          <Badge variant={step === 1 ? 'default' : 'secondary'}>1. Type</Badge>
          <Badge variant={step === 2 ? 'default' : 'secondary'}>2. Configure</Badge>
          <Badge variant={step === 3 ? 'default' : 'secondary'}>3. Review</Badge>
        </div>

        {step === 1 ? (
          <div className="space-y-4" data-testid="add-setting-step-1">
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search setting types..."
            />
            <div className="grid gap-3 md:grid-cols-2">
              {filteredSettings.map((setting) => {
                const Icon = CATEGORY_ICON_MAP[setting.category] ?? Settings2;
                const selected = selectedKey === setting.settingKey;
                return (
                  <button
                    key={setting.settingKey}
                    type="button"
                    onClick={() => setSelectedKey(setting.settingKey)}
                    className={`rounded-2xl border p-4 text-left transition ${
                      selected
                        ? 'border-[color:var(--accent)] bg-[color:var(--accent-soft)]'
                        : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] hover:border-[color:var(--border-strong)]'
                    }`}
                  >
                    <div className="mb-2 flex items-center gap-2">
                      <span className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-[color:var(--chip-bg)]">
                        <Icon className="h-4 w-4" />
                      </span>
                      <Badge variant="secondary">{setting.category}</Badge>
                    </div>
                    <p className="text-sm font-semibold text-[color:var(--text-primary)]">{setting.displayName}</p>
                    <p className="mt-1 text-xs text-[color:var(--text-muted)]">{setting.description}</p>
                  </button>
                );
              })}
            </div>
          </div>
        ) : null}

        {step === 2 && selectedSetting ? (
          <div className="space-y-4" data-testid="add-setting-step-2">
            <div>
              <p className="text-sm font-semibold text-[color:var(--text-primary)]">{selectedSetting.displayName}</p>
              <p className="text-xs text-[color:var(--text-muted)]">{selectedSetting.description}</p>
            </div>

            {renderConfigField()}

            {selectedSetting.helperText ? (
              <p className="text-xs text-[color:var(--text-muted)]">{selectedSetting.helperText}</p>
            ) : null}

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
                  <p>Data type: {selectedSetting.dataType}</p>
                  <p>Default value: {selectedSetting.defaultValue ?? 'none'}</p>
                  <p>Scope: {selectedSetting.scope}</p>
                  <p>Category: {selectedSetting.category}</p>
                </div>
              ) : null}
            </div>
          </div>
        ) : null}

        {step === 3 && selectedSetting ? (
          <div className="space-y-4" data-testid="add-setting-step-3">
            <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
              <div className="mb-2 flex flex-wrap items-center gap-2">
                <Badge variant="secondary">{selectedSetting.category}</Badge>
                <Badge variant="secondary">
                  {selectedSetting.scope === 'workspace'
                    ? 'Workspace'
                    : selectedSetting.scope === 'user'
                      ? 'User'
                      : 'Org'}
                </Badge>
              </div>
              <p className="text-sm font-semibold text-[color:var(--text-primary)]">{selectedSetting.displayName}</p>
              <p className="mt-1 text-xs text-[color:var(--text-muted)]">{selectedSetting.description}</p>
              <p className="mt-3 text-xs text-[color:var(--text-muted)]">Value summary</p>
              <p className="text-sm text-[color:var(--text-primary)]">
                {isSensitiveSetting(selectedSetting.settingKey)
                  ? draftValue.trim()
                    ? 'Configured'
                    : 'Not set'
                  : summarizeSettingValue({
                      settingKey: selectedSetting.settingKey,
                      settingValue: draftValue,
                      dataType: selectedSetting.dataType,
                    })}
              </p>
            </div>
          </div>
        ) : null}

        {errorMessage ? (
          <p className="text-sm text-rose-600 dark:text-rose-300">{errorMessage}</p>
        ) : null}

        <DialogFooter>
          {step > 1 ? (
            <Button
              variant="outline"
              onClick={() => setStep((current) => (current === 3 ? 2 : 1))}
              disabled={isCreating}
            >
              Back
            </Button>
          ) : (
            <Button variant="outline" onClick={() => onOpenChange(false)} disabled={isCreating}>
              Cancel
            </Button>
          )}

          {step === 1 ? (
            <Button onClick={handleContinueFromType}>Continue</Button>
          ) : null}
          {step === 2 ? (
            <Button onClick={handleContinueFromConfigure}>Review</Button>
          ) : null}
          {step === 3 ? (
            <Button onClick={handleCreate} isLoading={isCreating}>
              Create setting
            </Button>
          ) : null}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
