'use client';

import Link from 'next/link';

import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader } from '@/components/ui/card';

import type { DashboardExecutionItem, DashboardExecutionSummaryMetric } from '../types';

interface ExecutionStatusPanelProps {
  summary: DashboardExecutionSummaryMetric[];
  items: DashboardExecutionItem[];
}

export function ExecutionStatusPanel({ summary, items }: ExecutionStatusPanelProps) {
  return (
    <Card className="rounded-[28px]">
      <CardHeader className="pb-4">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
          Active work
        </p>
        <h2 className="text-xl font-semibold text-[color:var(--text-primary)]">Execution status</h2>
        <p className="text-sm text-[color:var(--text-secondary)]">
          Monitor running SQL jobs and connector sync activity from the workspace command surface.
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 sm:grid-cols-3">
          {summary.map((metric) => (
            <div
              key={metric.label}
              className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"
            >
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[color:var(--text-muted)]">
                {metric.label}
              </p>
              <p className="mt-2 text-xl font-semibold text-[color:var(--text-primary)]">{metric.value}</p>
              <p className="mt-1 text-xs leading-5 text-[color:var(--text-muted)]">{metric.detail}</p>
            </div>
          ))}
        </div>

        <div className="space-y-3">
          {items.length === 0 ? (
            <div className="rounded-2xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-5 text-sm text-[color:var(--text-muted)]">
              No live executions right now. New SQL jobs and connector syncs will appear here automatically.
            </div>
          ) : (
            items.map((item) => (
              <Link
                key={item.id}
                href={item.href}
                className="group block rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-4 py-4 transition hover:border-[color:var(--border-strong)] hover:bg-[color:var(--panel-alt)]"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="truncate text-sm font-semibold text-[color:var(--text-primary)]">{item.title}</p>
                      <Badge variant="secondary">{item.sourceLabel}</Badge>
                      <Badge variant={item.statusTone}>{item.statusLabel}</Badge>
                    </div>
                    <p className="mt-1 text-sm leading-5 text-[color:var(--text-muted)]">{item.description}</p>
                  </div>
                  <p className="flex-shrink-0 text-xs text-[color:var(--text-muted)]">{item.timestampLabel}</p>
                </div>
                {typeof item.progress === 'number' ? (
                  <div className="mt-3 space-y-2">
                    <div className="h-2 overflow-hidden rounded-full bg-[color:var(--chip-bg)]">
                      <div
                        className="h-full rounded-full bg-[color:var(--accent)] transition-[width]"
                        style={{ width: `${Math.max(4, Math.min(100, item.progress))}%` }}
                      />
                    </div>
                    <p className="text-xs text-[color:var(--text-muted)]">{Math.round(item.progress)}% complete</p>
                  </div>
                ) : null}
              </Link>
            ))
          )}
        </div>
      </CardContent>
    </Card>
  );
}
