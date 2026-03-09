'use client';

import Link from 'next/link';
import { ArrowRight } from 'lucide-react';

import { Card, CardContent } from '@/components/ui/card';

import type { DashboardEntryCard } from '../types';

interface EntryCardGridProps {
  items: DashboardEntryCard[];
}

export function EntryCardGrid({ items }: EntryCardGridProps) {
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      {items.map((item) => (
        <Link key={item.title} href={item.href} className="group block">
          <Card className="h-full rounded-[28px]">
            <CardContent className="flex h-full flex-col p-6">
              <div className="flex items-start justify-between gap-3">
                <span className="inline-flex h-11 w-11 items-center justify-center rounded-2xl bg-[color:var(--chip-bg)] text-[color:var(--accent)]">
                  <item.icon className="h-5 w-5" aria-hidden="true" />
                </span>
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[color:var(--text-muted)]">
                  {item.metric}
                </p>
              </div>
              <div className="mt-5">
                <h3 className="text-lg font-semibold text-[color:var(--text-primary)]">{item.title}</h3>
                <p className="mt-2 text-sm leading-6 text-[color:var(--text-secondary)]">{item.description}</p>
              </div>
              <div className="mt-6 inline-flex items-center gap-2 text-sm font-semibold text-[color:var(--accent)]">
                {item.cta}
                <ArrowRight className="h-4 w-4 transition group-hover:translate-x-0.5" aria-hidden="true" />
              </div>
            </CardContent>
          </Card>
        </Link>
      ))}
    </div>
  );
}
