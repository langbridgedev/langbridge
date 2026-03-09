'use client';

import Link from 'next/link';

import { cn } from '@/lib/utils';

import type { DashboardQuickAction } from '../types';

interface QuickActionPanelProps {
  actions: DashboardQuickAction[];
}

export function QuickActionPanel({ actions }: QuickActionPanelProps) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
            Quick actions
          </p>
          <p className="mt-1 text-sm text-[color:var(--text-secondary)]">
            Move directly from setup to analysis without leaving the workspace.
          </p>
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {actions.map((action) => (
          <Link
            key={action.label}
            href={action.href}
            className={cn(
              'group rounded-2xl border px-4 py-4 transition hover:-translate-y-0.5',
              action.emphasis === 'primary'
                ? 'border-[color:var(--border-strong)] bg-[linear-gradient(135deg,var(--accent-soft),transparent_80%)] text-[color:var(--text-primary)] hover:border-[color:var(--border-strong-hover)]'
                : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] text-[color:var(--text-primary)] hover:border-[color:var(--border-strong)] hover:bg-[color:var(--panel-alt)]',
            )}
          >
            <div className="flex items-start gap-3">
              <span className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-[color:var(--chip-bg)] text-[color:var(--accent)]">
                <action.icon className="h-4 w-4" aria-hidden="true" />
              </span>
              <div className="min-w-0">
                <p className="text-sm font-semibold">{action.label}</p>
                <p className="mt-1 text-xs leading-5 text-[color:var(--text-muted)]">{action.description}</p>
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
