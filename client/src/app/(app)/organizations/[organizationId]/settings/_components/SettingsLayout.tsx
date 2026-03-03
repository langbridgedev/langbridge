'use client';

import Link from 'next/link';
import { ArrowLeft, Download, FileJson, FileText, Plus, ScrollText } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface SettingsLayoutProps {
  organizationId: string;
  organizationName: string;
  environmentLabel?: string | null;
  searchValue: string;
  onSearchChange: (value: string) => void;
  onAddSetting: () => void;
  onExportJson: () => void;
  onExportYaml: () => void;
  onViewAudit: () => void;
  quickActions: React.ReactNode;
  children: React.ReactNode;
}

export function SettingsLayout({
  organizationId,
  organizationName,
  environmentLabel,
  searchValue,
  onSearchChange,
  onAddSetting,
  onExportJson,
  onExportYaml,
  onViewAudit,
  quickActions,
  children,
}: SettingsLayoutProps) {
  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      <header className="surface-panel rounded-3xl p-6 shadow-soft">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="space-y-3">
            <Link
              href={`/organizations/${organizationId}`}
              className="inline-flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.18em] text-[color:var(--text-muted)]"
            >
              <ArrowLeft className="h-3.5 w-3.5" />
              Organization
            </Link>
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <h1 className="text-2xl font-semibold text-[color:var(--text-primary)] md:text-3xl">
                  Organization settings
                </h1>
                <Badge variant="secondary">{organizationName}</Badge>
                {environmentLabel ? <Badge variant="warning">{environmentLabel}</Badge> : null}
              </div>
              <p className="text-sm md:text-base">
                Configure organization-wide defaults, security controls, execution policy, and AI settings.
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Button size="sm" onClick={onAddSetting}>
              <Plus className="h-4 w-4" />
              Add setting
            </Button>
            <Button size="sm" variant="outline" onClick={onExportJson}>
              <FileJson className="h-4 w-4" />
              Export JSON
            </Button>
            <Button size="sm" variant="outline" onClick={onExportYaml}>
              <FileText className="h-4 w-4" />
              Export YAML
            </Button>
            <Button size="sm" variant="outline" onClick={onViewAudit}>
              <ScrollText className="h-4 w-4" />
              View audit
            </Button>
          </div>
        </div>
      </header>

      <section className="surface-panel rounded-3xl p-4 shadow-soft">
        <div className="grid gap-4 lg:grid-cols-[1.2fr_1fr_auto] lg:items-center">
          <Input
            value={searchValue}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder="Search settings by name, description, category, or key..."
          />
          <div className="text-xs text-[color:var(--text-muted)]">
            Changes are isolated to each setting. Use drawer Save/Cancel to avoid accidental updates.
          </div>
          <div className="flex justify-end">
            <Button size="sm" variant="ghost" onClick={onExportJson}>
              <Download className="h-4 w-4" />
              Export
            </Button>
          </div>
        </div>
      </section>

      {quickActions}

      {children}
    </div>
  );
}
