'use client';

import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader } from '@/components/ui/card';

import type { DashboardOverviewMetric, DashboardStatusTone } from '../types';

interface WorkspaceOverviewProps {
  metrics: DashboardOverviewMetric[];
  statusLabel: string;
  statusDescription: string;
  statusTone: DashboardStatusTone;
}

export function WorkspaceOverview({
  metrics,
  statusLabel,
  statusDescription,
  statusTone,
}: WorkspaceOverviewProps) {
  return (
    <Card className="h-full rounded-[28px] bg-[linear-gradient(180deg,rgba(255,255,255,0.02),transparent)]">
      <CardHeader className="space-y-3 pb-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
              System overview
            </p>
            <p className="mt-1 text-sm text-[color:var(--text-secondary)]">{statusDescription}</p>
          </div>
          <Badge variant={statusTone}>{statusLabel}</Badge>
        </div>
      </CardHeader>
      <CardContent className="grid gap-3 sm:grid-cols-2">
        {metrics.map((metric) => (
          <div
            key={metric.label}
            className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"
          >
            <div className="flex items-center justify-between gap-2 text-[color:var(--text-muted)]">
              <p className="text-xs font-semibold uppercase tracking-[0.16em]">{metric.label}</p>
              <metric.icon className="h-4 w-4" aria-hidden="true" />
            </div>
            <p className="mt-3 text-2xl font-semibold text-[color:var(--text-primary)]">{metric.value}</p>
            <p className="mt-2 text-xs leading-5 text-[color:var(--text-muted)]">{metric.detail}</p>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}
