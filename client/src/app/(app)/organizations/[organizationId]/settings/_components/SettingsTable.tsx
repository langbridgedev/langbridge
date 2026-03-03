'use client';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { summarizeSettingValue, type SettingViewModel } from '../_lib/settings-utils';

interface SettingsTableProps {
  records: SettingViewModel[];
  onEditSetting: (record: SettingViewModel) => void;
}

function renderStatusBadge(record: SettingViewModel) {
  if (record.dataType === 'boolean') {
    const enabled = record.settingValue.trim().toLowerCase() === 'true';
    return <Badge variant={enabled ? 'success' : 'secondary'}>{enabled ? 'Enabled' : 'Disabled'}</Badge>;
  }
  return <Badge variant={record.isConfigured ? 'success' : 'secondary'}>{record.isConfigured ? 'Configured' : 'Not set'}</Badge>;
}

export function SettingsTable({ records, onEditSetting }: SettingsTableProps) {
  if (records.length === 0) {
    return (
      <div className="rounded-2xl border border-dashed border-[color:var(--panel-border)] p-8 text-center text-sm text-[color:var(--text-muted)]">
        No settings match this filter.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)]">
      <table className="min-w-full text-sm">
        <thead className="bg-[color:var(--panel-alt)] text-[color:var(--text-secondary)]">
          <tr>
            <th className="px-4 py-3 text-left font-medium">Setting</th>
            <th className="px-4 py-3 text-left font-medium">Value</th>
            <th className="px-4 py-3 text-left font-medium">Scope</th>
            <th className="px-4 py-3 text-left font-medium">Status</th>
            <th className="px-4 py-3 text-left font-medium">Last updated</th>
            <th className="px-4 py-3 text-right font-medium">Actions</th>
          </tr>
        </thead>
        <tbody>
          {records.map((record) => (
            <tr key={record.settingKey} className="border-t border-[color:var(--panel-border)] align-top">
              <td className="px-4 py-3">
                <div>
                  <p className="font-semibold text-[color:var(--text-primary)]">{record.displayName}</p>
                  <p className="mt-1 text-xs text-[color:var(--text-muted)]">{record.description}</p>
                  <p className="mt-1 text-[10px] font-mono text-[color:var(--text-muted)]">{record.settingKey}</p>
                </div>
              </td>
              <td className="px-4 py-3 text-[color:var(--text-primary)]">
                <p>{summarizeSettingValue(record)}</p>
              </td>
              <td className="px-4 py-3">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="secondary">
                    {record.scope === 'workspace'
                      ? 'Workspace'
                      : record.scope === 'user'
                        ? 'User'
                        : 'Org'}
                  </Badge>
                  {record.isInherited ? <Badge variant="warning">Inherited</Badge> : null}
                  {record.isLocked ? <Badge variant="destructive">Locked</Badge> : null}
                </div>
              </td>
              <td className="px-4 py-3">{renderStatusBadge(record)}</td>
              <td className="px-4 py-3 text-xs text-[color:var(--text-muted)]">
                {record.lastUpdatedAt ? new Date(record.lastUpdatedAt).toLocaleString() : 'Not available'}
                {record.lastUpdatedBy ? <p className="mt-1">By {record.lastUpdatedBy}</p> : null}
              </td>
              <td className="px-4 py-3 text-right">
                <Button size="sm" variant="outline" onClick={() => onEditSetting(record)}>
                  Edit
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
