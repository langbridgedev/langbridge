import type { LucideIcon } from 'lucide-react';

export type DataSource = {
  id: string;
  name: string;
  type: 'snowflake' | 'postgres' | 'mysql' | 'api';
  status: 'connected' | 'error' | 'pending';
  createdAt: string;
};

export type Agent = {
  id: string;
  name: string;
  kind: 'sql_analyst' | 'docs_qa' | 'hybrid';
  sourceIds: string[];
  createdAt: string;
};

export type DashboardStatusTone = 'default' | 'success' | 'destructive' | 'secondary' | 'warning';

export interface DashboardQuickAction {
  href: string;
  label: string;
  description: string;
  icon: LucideIcon;
  emphasis?: 'primary' | 'secondary';
}

export interface DashboardOverviewMetric {
  label: string;
  value: string;
  detail: string;
  icon: LucideIcon;
}

export interface DashboardActivityItem {
  id: string;
  href: string;
  title: string;
  description: string;
  kindLabel: string;
  timestampLabel: string;
  icon: LucideIcon;
  statusLabel?: string;
  statusTone?: DashboardStatusTone;
}

export interface DashboardExecutionSummaryMetric {
  label: string;
  value: string;
  detail: string;
}

export interface DashboardExecutionItem {
  id: string;
  href: string;
  title: string;
  description: string;
  sourceLabel: string;
  timestampLabel: string;
  statusLabel: string;
  statusTone: DashboardStatusTone;
  progress?: number | null;
}

export interface DashboardEntryCard {
  href: string;
  title: string;
  description: string;
  cta: string;
  metric: string;
  icon: LucideIcon;
}

export interface DashboardOnboardingStep {
  id: string;
  href: string;
  title: string;
  description: string;
  completed: boolean;
}
