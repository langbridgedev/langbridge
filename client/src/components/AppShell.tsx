'use client';

import { JSX, useMemo, useState, type ReactNode } from 'react';
import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import {
  LayoutDashboard,
  Database,
  Table2,
  FileCode2,
  BrainCircuit,
  BarChart3,
  Bot,
  Building2,
  ChevronLeft,
  ChevronRight,
  MessageSquareText,
  BookOpen,
  LifeBuoy,
  Settings,
  type LucideIcon,
} from 'lucide-react';

import { LogoutButton } from '@/components/LogoutButton';
import { ThemeToggle } from '@/components/ThemeToggle';
import { Button } from '@/components/ui/button';
import { Select } from '@/components/ui/select';
import { cn } from '@/lib/utils';
import { useWorkspaceScope } from '@/context/workspaceScope';

interface NavItem {
  href: string;
  label: string;
  description: string;
  icon: LucideIcon;
  children?: NavChild[];
}

interface NavChild {
  href: string;
  label: string;
  description?: string;
  icon?: LucideIcon;
}

const NAV_ITEMS: NavItem[] = [
  {
    href: '/dashboard',
    label: 'Command Center',
    description: 'Monitor data, queries, and orchestrations across your analytics workspace.',
    icon: LayoutDashboard,
  },
  {
    href: '/datasources',
    label: 'Data connections',
    description: 'Manage structured connectors and retrievers powering your agents.',
    icon: Database,
  },
  {
    href: '/datasets',
    label: 'Datasets',
    description: 'Curate governed virtual datasets between connectors and semantic models.',
    icon: Table2,
  },
  {
    href: '/sql',
    label: 'SQL',
    description: 'Write and run native SQL with policies, history, and saved artifacts.',
    icon: FileCode2,
  },
  {
    href: '/semantic-model',
    label: 'Semantic models',
    description: 'Build semantic layers and publish curated data models for agents.',
    icon: BrainCircuit,
    children: [
      {
        href: '/semantic-model',
        label: 'Semantic models',
        description: 'Build semantic layers and publish curated data models for agents.',
      },
      {
        href: '/semantic-model/unified',
        label: 'Unified semantic models',
        description: 'Compose relationships across semantic models and query across sources.',
      },
    ],
  },
  {
    href: '/bi',
    label: 'BI studio',
    description: 'Compose semantic queries and shape lightweight dashboards.',
    icon: BarChart3,
  },
  {
    href: '/agents',
    label: 'Agents',
    description: 'Build semantic layers and publish curated data models for agents.',
    icon: Bot,
    children: [
      {
        href: '/agents',
        label: 'Agents',
        description: 'Build semantic layers and publish curated data models for agents.',
      },
      {
        href: '/agents/llm',
        label: 'LLM connections',
        description: 'Register provider credentials for upcoming agent builders.',
      },
    ],
  },
  {
    href: '/organizations',
    label: 'Organizations & projects',
    description: 'Group teammates and resources into collaborative workspaces.',
    icon: Building2,
  },
  {
    href: '/chat',
    label: 'Threads',
    description: 'Revisit active threads and manage ongoing analysis sessions.',
    icon: MessageSquareText,
  },
  {
    href: '/settings',
    label: 'Settings',
    description: 'Update your personal preferences, notifications, and profile.',
    icon: Settings,
  },
];

export function AppShell({ children }: { children: ReactNode }): JSX.Element {
  const pathname = usePathname();
  const router = useRouter();
  const { selectedOrganization, selectedProject, selectedOrganizationId } = useWorkspaceScope();

  const [openParent, setOpenParent] = useState<string | null>(null);
  const [isNavCollapsed, setIsNavCollapsed] = useState(false);

  const navItems = useMemo(() => {
    const agentsBase = selectedOrganizationId ? `/agents/${selectedOrganizationId}` : '/agents';
    const datasourcesBase = selectedOrganizationId ? `/datasources/${selectedOrganizationId}` : '/datasources';
    const datasetsBase = selectedOrganizationId ? `/datasets/${selectedOrganizationId}` : '/datasets';
    const semanticModelBase = selectedOrganizationId ? `/semantic-model/${selectedOrganizationId}` : '/semantic-model';
    const biBase = selectedOrganizationId ? `/bi/${selectedOrganizationId}` : '/bi';
    const sqlBase = selectedOrganizationId ? `/sql/${selectedOrganizationId}` : '/sql';
    const chatBase = selectedOrganizationId ? `/chat/${selectedOrganizationId}` : '/chat';

    const remapChildren = (children: NavChild[] | undefined, base: string, prefix: string) =>
      children?.map((child) => {
        if (!child.href.startsWith(prefix)) {
          return child;
        }
        if (child.href === prefix) {
          return { ...child, href: base };
        }
        const suffix = child.href.replace(prefix, '');
        return { ...child, href: `${base}${suffix}` };
      });

    return NAV_ITEMS.map((item) => {
      if (item.href === '/agents') {
        const children = remapChildren(item.children, agentsBase, '/agents');
        return { ...item, href: agentsBase, children };
      }
      if (item.href === '/datasources') {
        return { ...item, href: datasourcesBase };
      }
      if (item.href === '/datasets') {
        return { ...item, href: datasetsBase };
      }
      if (item.href === '/semantic-model') {
        const children = remapChildren(item.children, semanticModelBase, '/semantic-model');
        return { ...item, href: semanticModelBase, children };
      }
      if (item.href === '/bi') {
        return { ...item, href: biBase };
      }
      if (item.href === '/sql') {
        return { ...item, href: sqlBase };
      }
      if (item.href === '/chat') {
        return { ...item, href: chatBase };
      }
      return item;
    });
  }, [selectedOrganizationId]);

  const activeNav = useMemo(() => {
    for (const item of navItems) {
      if (pathname === item.href || pathname.startsWith(`${item.href}/`)) {
        return { parent: item, label: item.label, description: item.description };
      }
      if (item.children) {
        const child = item.children.find(
          (childItem) => pathname === childItem.href || pathname.startsWith(`${childItem.href}/`),
        );
        if (child) {
          return {
            parent: item,
            label: child.label,
            description: child.description ?? item.description,
          };
        }
      }
    }
    return { parent: navItems[0], label: navItems[0].label, description: navItems[0].description };
  }, [navItems, pathname]);

  const scopeSummary = useMemo(() => {
    if (!selectedOrganization) {
      return null;
    }
    if (selectedProject) {
      return `${selectedOrganization.name} - ${selectedProject.name}`;
    }
    return selectedOrganization.name;
  }, [selectedOrganization, selectedProject]);

  return (
    <div className="min-h-screen bg-[color:var(--shell-bg)] text-[color:var(--text-primary)] transition-colors">
      <div className="flex min-h-screen flex-col lg:flex-row">
        <aside
          className={cn(
            'sticky top-0 hidden h-screen flex-shrink-0 flex-col border-r border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-3 py-6 text-sm text-[color:var(--text-secondary)] shadow-soft transition-[width,padding] duration-200 lg:flex',
            isNavCollapsed ? 'w-20' : 'w-64 px-5',
          )}
        >
          <div className={cn('flex items-center gap-2', isNavCollapsed ? 'justify-center' : 'justify-between')}>
            <Link
              href="/dashboard"
              title="LangBridge"
              className={cn(
                'inline-flex items-center rounded-full border border-[color:var(--border-strong)] bg-[color:var(--panel-alt)] text-sm font-semibold text-[color:var(--text-primary)] transition hover:border-[color:var(--border-strong-hover)] hover:text-[color:var(--text-primary)]',
                isNavCollapsed ? 'h-10 w-10 justify-center px-0' : 'gap-2 px-4 py-2',
              )}
            >
              <span className={cn(isNavCollapsed ? 'text-base' : '')}>L</span>
              <span className={cn(isNavCollapsed && 'sr-only')}>LangBridge</span>
            </Link>
            <button
              type="button"
              onClick={() => setIsNavCollapsed((current) => !current)}
              className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-[color:var(--panel-border)] text-[color:var(--text-secondary)] transition hover:bg-[color:var(--panel-alt)] hover:text-[color:var(--text-primary)]"
              aria-label={isNavCollapsed ? 'Expand navigation' : 'Collapse navigation'}
              title={isNavCollapsed ? 'Expand navigation' : 'Collapse navigation'}
            >
              {isNavCollapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
            </button>
          </div>
          <div className="mt-8 flex min-h-0 flex-1 flex-col">
            <nav className="min-h-0 flex-1 space-y-1 overflow-y-auto pr-1">
            {navItems.map((item) => {
              const hasChildren = Boolean(item.children && item.children.length > 0);
              const isParentActive =
                item.href === activeNav.parent.href ||
                (item.children ?? []).some(
                  (child) => pathname === child.href || pathname.startsWith(`${child.href}/`),
                );
              const isOpen = openParent === item.href || (hasChildren && isParentActive);

              if (!hasChildren || isNavCollapsed) {
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    title={item.label}
                    aria-label={item.label}
                    className={cn(
                      'flex rounded-xl px-3 py-2 transition hover:bg-[color:var(--panel-alt)] hover:text-[color:var(--text-primary)]',
                      isNavCollapsed ? 'justify-center' : 'items-center',
                      isParentActive
                        ? 'bg-[color:var(--panel-alt)] font-semibold text-[color:var(--text-primary)]'
                        : 'text-[color:var(--text-secondary)]',
                    )}
                  >
                    <item.icon className={cn('h-4 w-4', !isNavCollapsed && 'mr-3')} />
                    {isNavCollapsed ? <span className="sr-only">{item.label}</span> : item.label}
                  </Link>
                );
              }

              return (
                <div key={item.href} className="space-y-1">
                  <button
                    type="button"
                    onClick={() => setOpenParent((current) => (current === item.href ? null : item.href))}
                    title={item.label}
                    className={cn(
                      'flex w-full items-center justify-between rounded-xl px-3 py-2 text-left transition hover:bg-[color:var(--panel-alt)] hover:text-[color:var(--text-primary)]',
                      isParentActive
                        ? 'bg-[color:var(--panel-alt)] font-semibold text-[color:var(--text-primary)]'
                        : 'text-[color:var(--text-secondary)]',
                    )}
                  >
                    <div className="flex items-center">
                      <item.icon className="mr-3 h-4 w-4" />
                      <span>{item.label}</span>
                    </div>
                    <span className="text-xs">{isOpen ? 'v' : '>'}</span>
                  </button>
                  {isOpen ? (
                    <div className="ml-3 space-y-1 border-l border-[color:var(--panel-border)] pl-3">
                      {item.children?.map((child) => {
                        const childActive = pathname === child.href;
                        return (
                          <Link
                            key={child.href}
                            href={child.href}
                            className={cn(
                              'block rounded-xl px-3 py-2 text-sm transition hover:bg-[color:var(--panel-alt)] hover:text-[color:var(--text-primary)]',
                              childActive
                                ? 'bg-[color:var(--panel-alt)] font-medium text-[color:var(--text-primary)]'
                                : 'text-[color:var(--text-secondary)]',
                            )}
                          >
                            {child.label}
                          </Link>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              );
            })}
            </nav>
            <div className="mt-auto pt-6 space-y-3 text-xs text-[color:var(--text-muted)]">
              <Link
                href="/docs"
                title="Documentation"
                aria-label="Documentation"
                className={cn(
                  'inline-flex transition hover:text-[color:var(--text-primary)]',
                  isNavCollapsed ? 'justify-center rounded-xl px-3 py-2' : 'items-center gap-2',
                )}
              >
                <BookOpen className="h-4 w-4" />
                <span className={cn(isNavCollapsed && 'sr-only')}>Documentation</span>
              </Link>
              <hr />
              <Link
                href="/support"
                title="Support & feedback"
                aria-label="Support & feedback"
                className={cn(
                  'inline-flex transition hover:text-[color:var(--text-primary)]',
                  isNavCollapsed ? 'justify-center rounded-xl px-3 py-2' : 'items-center gap-2',
                )}
              >
                <LifeBuoy className="h-4 w-4" />
                <span className={cn(isNavCollapsed && 'sr-only')}>Support & feedback</span>
              </Link>
            </div>
          </div>
        </aside>

        <div className="flex flex-1 flex-col">
          <header className="border-b border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-6 py-4 shadow-soft">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="space-y-1">
                <h1 className="text-lg font-semibold text-[color:var(--text-primary)]">{activeNav.label}</h1>
                <p className="text-sm text-[color:var(--text-secondary)]">{activeNav.description}</p>
                {scopeSummary ? (
                  <p className="text-xs text-[color:var(--text-muted)]">Scope: {scopeSummary}</p>
                ) : null}
              </div>
              <div className="flex w-full flex-col items-stretch gap-3 sm:w-auto sm:flex-row sm:items-center sm:justify-end">
                <ScopeSelector />
                <div className="inline-flex items-center justify-end gap-3">
                  <ThemeToggle size="sm" />
                  <LogoutButton />
                  <Button variant="outline" size="sm" onClick={() => router.push('/docs/whats-new')}>
                    What&apos;s new
                  </Button>
                </div>
              </div>
            </div>

            <nav className="mt-4 flex gap-2 overflow-x-auto text-sm lg:hidden">
              {[...navItems.flatMap((item) => item.children ?? [item])].map((item) => {
                const isActive = pathname === item.href || pathname.startsWith(`${item.href}/`);
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      'whitespace-nowrap rounded-full px-3 py-1 transition',
                      isActive
                        ? 'bg-[color:var(--panel-alt)] text-[color:var(--text-primary)]'
                        : 'text-[color:var(--text-secondary)] hover:bg-[color:var(--panel-alt)] hover:text-[color:var(--text-primary)]',
                    )}
                  >
                    {item.label}
                  </Link>
                );
              })}
            </nav>
          </header>

          <div className="flex-1 overflow-y-auto">
            <main className="w-full px-6 py-8 page-enter max-w-none"
            >
              {children}
            </main>
          </div>
        </div>
      </div>
    </div>
  );
}

function ScopeSelector(): JSX.Element {
  const {
    organizations,
    loading,
    error,
    selectedOrganizationId,
    selectedProjectId,
    selectedOrganization,
    setSelectedOrganizationId,
    setSelectedProjectId,
  } = useWorkspaceScope();

  const projectOptions = useMemo(() => selectedOrganization?.projects ?? [], [selectedOrganization]);

  const organizationDisabled = loading || organizations.length === 0;
  const projectDisabled = loading || !selectedOrganization || projectOptions.length === 0;

  return (
    <div className="flex w-full flex-col gap-3 sm:w-auto sm:flex-row sm:items-end sm:gap-4">
      <div className="flex min-w-[200px] flex-col gap-1">
        <span className="text-xs font-medium text-[color:var(--text-muted)]">Organization</span>
        <Select
          value={selectedOrganizationId}
          onChange={(event) => setSelectedOrganizationId(event.target.value)}
          disabled={organizationDisabled}
          placeholder={loading ? 'Loading organizations...' : 'Select an organization'}
        >
          {organizations.map((organization) => (
            <option key={organization.id} value={organization.id}>
              {organization.name}
            </option>
          ))}
        </Select>
      </div>
      <div className="flex min-w-[200px] flex-col gap-1">
        <span className="text-xs font-medium text-[color:var(--text-muted)]">Project</span>
        <Select
          value={selectedProjectId}
          onChange={(event) => setSelectedProjectId(event.target.value)}
          disabled={projectDisabled}
          placeholder={
            !selectedOrganizationId
              ? 'Select an organization first'
              : projectOptions.length === 0
                ? 'No projects yet'
                : 'Select a project'
          }
        >
          {selectedOrganizationId ? (
            <option value="">
              All projects{selectedOrganization ? ` - ${selectedOrganization.name}` : ''}
            </option>
          ) : null}
          {projectOptions.map((project) => (
            <option key={project.id} value={project.id}>
              {project.name}
            </option>
          ))}
        </Select>
      </div>
      {error ? <span className="text-xs text-rose-500">Unable to load organizations.</span> : null}
    </div>
  );
}
