'use client';

import { useState } from 'react';
import { AlertTriangle, Copy, Info } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import type { DisplayError } from '@/lib/errors';

interface ErrorPanelProps extends DisplayError {
  className?: string;
}

const TONE_STYLES = {
  error: 'border-rose-400/40 bg-rose-100/50 text-rose-900 dark:bg-rose-950/20 dark:text-rose-100',
  warning: 'border-amber-400/40 bg-amber-100/60 text-amber-950 dark:bg-amber-950/20 dark:text-amber-100',
} as const;

export function ErrorPanel({
  title,
  message,
  suggestions,
  technicalDetails,
  errorCode,
  tone,
  className,
}: ErrorPanelProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    const payload = [
      title,
      message,
      errorCode ? `Code: ${errorCode}` : null,
      technicalDetails ? `Technical details:\n${technicalDetails}` : null,
    ]
      .filter(Boolean)
      .join('\n\n');

    try {
      await navigator.clipboard.writeText(payload);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  };

  return (
    <div className={cn('rounded-2xl border p-4 shadow-soft', TONE_STYLES[tone], className)}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-start gap-3">
          <span className="mt-0.5 inline-flex h-8 w-8 items-center justify-center rounded-full bg-white/40 dark:bg-black/20">
            {tone === 'warning' ? <Info className="h-4 w-4" aria-hidden="true" /> : <AlertTriangle className="h-4 w-4" aria-hidden="true" />}
          </span>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <p className="text-sm font-semibold">{title}</p>
              {errorCode ? <Badge variant="secondary">{errorCode}</Badge> : null}
            </div>
            <p className="mt-1 text-sm leading-6">{message}</p>
          </div>
        </div>
        <Button type="button" variant="ghost" size="sm" onClick={() => void handleCopy()}>
          <Copy className="h-4 w-4" aria-hidden="true" />
          {copied ? 'Copied' : 'Copy details'}
        </Button>
      </div>

      {suggestions.length > 0 ? (
        <div className="mt-4">
          <p className="text-xs font-semibold uppercase tracking-[0.16em] opacity-75">Suggested actions</p>
          <ul className="mt-2 space-y-1 text-sm">
            {suggestions.map((suggestion) => (
              <li key={suggestion}>- {suggestion}</li>
            ))}
          </ul>
        </div>
      ) : null}

      {technicalDetails ? (
        <details className="mt-4 rounded-xl border border-black/10 bg-white/40 px-3 py-2 dark:border-white/10 dark:bg-black/10">
          <summary className="cursor-pointer text-sm font-medium">Show technical details</summary>
          <pre className="mt-3 overflow-x-auto whitespace-pre-wrap break-words text-xs leading-5 opacity-90">
            {technicalDetails}
          </pre>
        </details>
      ) : null}
    </div>
  );
}
