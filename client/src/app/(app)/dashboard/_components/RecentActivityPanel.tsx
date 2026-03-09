'use client';

import Link from 'next/link';

import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader } from '@/components/ui/card';

import type { DashboardActivityItem } from '../types';

interface RecentActivityPanelProps {
  items: DashboardActivityItem[];
}

export function RecentActivityPanel({ items }: RecentActivityPanelProps) {
  return (
    <Card className="rounded-[28px]">
      <CardHeader className="pb-4">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
          Recent activity
        </p>
        <h2 className="text-xl font-semibold text-[color:var(--text-primary)]">Resume active work</h2>
        <p className="text-sm text-[color:var(--text-secondary)]">
          Recent queries, datasets, models, agents, and threads across the current workspace scope.
        </p>
      </CardHeader>
      <CardContent className="space-y-3">
        {items.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-5 text-sm text-[color:var(--text-muted)]">
            Activity appears here once the workspace has queries, datasets, models, or investigation threads.
          </div>
        ) : (
          items.map((item) => (
            <Link
              key={item.id}
              href={item.href}
              className="group flex items-start justify-between gap-4 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-4 py-4 transition hover:border-[color:var(--border-strong)] hover:bg-[color:var(--panel-alt)]"
            >
              <div className="flex min-w-0 items-start gap-3">
                <span className="inline-flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-2xl bg-[color:var(--chip-bg)] text-[color:var(--accent)]">
                  <item.icon className="h-4 w-4" aria-hidden="true" />
                </span>
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <p className="truncate text-sm font-semibold text-[color:var(--text-primary)]">{item.title}</p>
                    <Badge variant="secondary">{item.kindLabel}</Badge>
                    {item.statusLabel ? <Badge variant={item.statusTone}>{item.statusLabel}</Badge> : null}
                  </div>
                  <p className="mt-1 text-sm leading-5 text-[color:var(--text-muted)]">{item.description}</p>
                </div>
              </div>
              <p className="flex-shrink-0 text-xs text-[color:var(--text-muted)]">{item.timestampLabel}</p>
            </Link>
          ))
        )}
      </CardContent>
    </Card>
  );
}
