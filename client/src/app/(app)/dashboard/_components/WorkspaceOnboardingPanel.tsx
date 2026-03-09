'use client';

import Link from 'next/link';
import { CheckCircle2, Circle } from 'lucide-react';

import { Card, CardContent, CardHeader } from '@/components/ui/card';

import type { DashboardOnboardingStep } from '../types';

interface WorkspaceOnboardingPanelProps {
  steps: DashboardOnboardingStep[];
}

export function WorkspaceOnboardingPanel({ steps }: WorkspaceOnboardingPanelProps) {
  return (
    <Card className="rounded-[28px] border-dashed">
      <CardHeader className="pb-4">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
          Getting started
        </p>
        <h2 className="text-xl font-semibold text-[color:var(--text-primary)]">Stand up the first analytics flow</h2>
        <p className="text-sm text-[color:var(--text-secondary)]">
          Wire the workspace once, then use LangBridge as the operational surface for federated analytics.
        </p>
      </CardHeader>
      <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {steps.map((step, index) => (
          <Link
            key={step.id}
            href={step.href}
            className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4 transition hover:border-[color:var(--border-strong)]"
          >
            <div className="flex items-start justify-between gap-3">
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[color:var(--text-muted)]">
                Step {index + 1}
              </p>
              {step.completed ? (
                <CheckCircle2 className="h-4 w-4 text-emerald-500" aria-hidden="true" />
              ) : (
                <Circle className="h-4 w-4 text-[color:var(--text-muted)]" aria-hidden="true" />
              )}
            </div>
            <p className="mt-3 text-sm font-semibold text-[color:var(--text-primary)]">{step.title}</p>
            <p className="mt-2 text-xs leading-5 text-[color:var(--text-muted)]">{step.description}</p>
          </Link>
        ))}
      </CardContent>
    </Card>
  );
}
