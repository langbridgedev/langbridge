'use client';

import { JSX, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useQuery } from '@tanstack/react-query';
import { ArrowRight, Database, Plus, RefreshCw, Search } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { useWorkspaceScope } from '@/context/workspaceScope';
import { fetchConnectors } from '@/orchestration/connectors';
import type { ConnectorResponse } from '@/orchestration/connectors';

const connectorsQueryKey = (organizationId: string) => ['connectors', organizationId] as const;

type DataConnectionsIndexProps = {
  params: { organizationId: string };
};

export default function DataConnectionsIndex({ params }: DataConnectionsIndexProps): JSX.Element {
  const router = useRouter();
  const { selectedOrganizationId, setSelectedOrganizationId } = useWorkspaceScope();
  const organizationId = params.organizationId;
  const [search, setSearch] = useState('');

  useEffect(() => {
    if (organizationId && organizationId !== selectedOrganizationId) {
      setSelectedOrganizationId(organizationId);
    }
  }, [organizationId, selectedOrganizationId, setSelectedOrganizationId]);

  const hasOrganization = Boolean(organizationId);

  const connectorsQuery = useQuery<ConnectorResponse[]>({
    queryKey: connectorsQueryKey(organizationId),
    queryFn: () => fetchConnectors(organizationId),
    enabled: hasOrganization,
  });

  const handleCreate = () => {
    router.push(`/datasources/${organizationId}/create`);
  };

  const connectors = useMemo(
    () =>
      (connectorsQuery.data ?? [])
        .filter((connector): connector is ConnectorResponse & { id: string } => Boolean(connector.id))
        .sort((a, b) => {
          const left = a.name.toLowerCase();
          const right = b.name.toLowerCase();
          if (left < right) {
            return -1;
          }
          if (left > right) {
            return 1;
          }
          return 0;
        }),
    [connectorsQuery.data],
  );

  const filteredConnectors = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) {
      return connectors;
    }
    return connectors.filter((connector) =>
      [connector.name, connector.connectorType ?? '', connector.description ?? '']
        .join(' ')
        .toLowerCase()
        .includes(term),
    );
  }, [connectors, search]);

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      <header className="surface-panel rounded-3xl p-6 shadow-soft">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div className="space-y-3">
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[color:var(--text-muted)]">
              Data connections
            </p>
            <div className="space-y-2">
              <h1 className="text-2xl font-semibold text-[color:var(--text-primary)] md:text-3xl">
                Connect structured data, unstructured data, and APIs.
              </h1>
              <p className="text-sm md:text-base">
                Catalog every warehouse, lake, and business system powering your agents.
              </p>
            </div>
          </div>
          <div className="flex flex-col items-start gap-4 sm:flex-row sm:items-center sm:justify-end">
            <Button onClick={handleCreate} size="sm" className="gap-2">
              <Plus className="h-4 w-4" aria-hidden="true" />
              New connection
            </Button>
          </div>
        </div>
      </header>

      <section className="surface-panel flex flex-1 flex-col rounded-3xl p-6 shadow-soft">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <h2 className="text-sm font-semibold text-[color:var(--text-primary)]">Connections</h2>
          <div className="flex flex-1 flex-col gap-3 sm:max-w-xl sm:flex-row sm:items-center sm:justify-end">
            <div className="relative w-full sm:max-w-sm">
              <Search className="pointer-events-none absolute left-3 top-3 h-4 w-4 text-[color:var(--text-muted)]" aria-hidden="true" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Search connections"
                className="pl-9"
                aria-label="Search connections"
              />
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => connectorsQuery.refetch()}
              disabled={connectorsQuery.isFetching || !hasOrganization}
              className="text-[color:var(--text-secondary)] hover:text-[color:var(--text-primary)]"
            >
              <RefreshCw className="mr-2 h-4 w-4" aria-hidden="true" /> Refresh
            </Button>
          </div>
        </div>

        <div className="mt-6 flex-1">
          {!hasOrganization ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 text-center text-[color:var(--text-muted)]">
              <p className="text-sm">Select an organization to view its connectors.</p>
              <p className="text-xs">Once a scope is active, your saved connections will appear here.</p>
            </div>
          ) : connectorsQuery.isLoading ? (
            <div className="space-y-3">
              {Array.from({ length: 4 }).map((_, index) => (
                <div
                  key={index}
                  className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-4"
                >
                  <Skeleton className="h-4 w-32" />
                </div>
              ))}
            </div>
          ) : connectorsQuery.isError ? (
            <div className="flex h-full flex-col items-center justify-center gap-4 text-center text-[color:var(--text-muted)]">
              <p className="text-sm">We couldn&apos;t load data connections right now.</p>
              <Button onClick={() => connectorsQuery.refetch()} variant="outline" size="sm">
                Try again
              </Button>
            </div>
          ) : connectors.length === 0 ? (
            <div className="flex h-full flex-col items-center justify-center gap-4 text-center text-[color:var(--text-muted)]">
              <div className="inline-flex items-center justify-center rounded-full border border-[color:var(--panel-border)] bg-[color:var(--chip-bg)] px-3 py-1 text-xs font-medium">
                No connectors yet
              </div>
              <div className="space-y-2">
                <p className="text-base font-semibold text-[color:var(--text-primary)]">No data connections found</p>
                <p className="text-sm">Create your first connector to power orchestrations.</p>
              </div>
              <Button onClick={handleCreate} size="sm" className="gap-2">
                <Plus className="h-4 w-4" aria-hidden="true" />
                Create connection
              </Button>
            </div>
          ) : filteredConnectors.length === 0 ? (
            <div className="flex h-full flex-col items-center justify-center gap-4 text-center text-[color:var(--text-muted)]">
              <div className="inline-flex items-center justify-center rounded-full border border-[color:var(--panel-border)] bg-[color:var(--chip-bg)] px-3 py-1 text-xs font-medium">
                No matches
              </div>
              <div className="space-y-2">
                <p className="text-base font-semibold text-[color:var(--text-primary)]">No connections match that search</p>
                <p className="text-sm">Try a connector name, type, or description keyword.</p>
              </div>
            </div>
          ) : (
            <ul className="space-y-3">
              {filteredConnectors.map((connector) => (
                <li key={connector.id}>
                  <Link
                    href={`/datasources/${organizationId}/${connector.id}`}
                    className="group flex items-center justify-between rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] px-4 py-4 transition hover:border-[color:var(--border-strong)] hover:bg-[color:var(--panel-alt)]"
                  >
                    <div className="flex items-center gap-3">
                      <span className="flex h-9 w-9 items-center justify-center rounded-full bg-[color:var(--chip-bg)] text-sm font-semibold text-[color:var(--text-primary)]">
                        <Database className="h-4 w-4" aria-hidden="true" />
                      </span>
                      <div>
                        <p className="text-sm font-semibold text-[color:var(--text-primary)]">{connector.name}</p>
                        <p className="text-xs text-[color:var(--text-muted)]">
                          {connector.connectorType ?? 'Unknown type'}
                          {connector.description ? ` - ${connector.description}` : ''}
                        </p>
                      </div>
                    </div>
                    <ArrowRight
                      className="h-4 w-4 text-[color:var(--text-muted)] transition group-hover:translate-x-1 group-hover:text-[color:var(--text-primary)]"
                      aria-hidden="true"
                    />
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </div>
      </section>
    </div>
  );
}
