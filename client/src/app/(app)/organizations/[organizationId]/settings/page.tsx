'use client';

import { useRouter } from 'next/navigation';
import { dump as toYaml } from 'js-yaml';
import { JSX, useCallback, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Database, Gauge, Rocket, Sparkles } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { ApiError } from '@/orchestration/http';
import {
  createProject,
  createRuntimeRegistrationToken,
  deleteOrganizationEnvironmentSetting,
  fetchOrganizationEnvironmentCatalog,
  fetchOrganizationEnvironmentSettings,
  fetchRuntimeInstances,
  inviteToOrganization,
  inviteToProject,
  setOrganizationEnvironmentSetting,
  type RuntimeInstance,
} from '@/orchestration/organizations';
import { useWorkspaceScope } from '@/context/workspaceScope';

import { AddSettingModal } from './_components/AddSettingModal';
import { CategorySidebar } from './_components/CategorySidebar';
import { EditSettingDrawer } from './_components/EditSettingDrawer';
import { SettingsLayout } from './_components/SettingsLayout';
import { SettingsTable } from './_components/SettingsTable';
import {
  ALL_CATEGORIES,
  SETTINGS_CATEGORIES,
  buildSettingsViewModel,
  filterSettings,
  type SettingViewModel,
  type SettingsCategory,
} from './_lib/settings-utils';

type OrganizationSettingsPageProps = {
  params: { organizationId: string };
};

const environmentSettingsQueryKey = (organizationId: string | null | undefined) =>
  ['organization-env-settings', organizationId] as const;
const environmentCatalogQueryKey = ['organization-env-catalog'] as const;
const runtimeInstancesQueryKey = (organizationId: string | null | undefined) =>
  ['organization-runtime-instances', organizationId] as const;

function resolveErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Something went wrong. Please try again.';
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) {
    return 'N/A';
  }
  const asDate = new Date(value);
  if (Number.isNaN(asDate.getTime())) {
    return value;
  }
  return asDate.toLocaleString();
}

function runtimeStatusVariant(status: string): 'success' | 'warning' | 'secondary' {
  if (status === 'active') {
    return 'success';
  }
  if (status === 'draining') {
    return 'warning';
  }
  return 'secondary';
}

function downloadAsFile(filename: string, content: string, mimeType: string): void {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export default function OrganizationSettingsPage({ params }: OrganizationSettingsPageProps): JSX.Element {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { organizations, loading, refreshOrganizations: reloadOrganizations } = useWorkspaceScope();

  const organization = useMemo(
    () => organizations.find((item) => item.id === params.organizationId) ?? null,
    [organizations, params.organizationId],
  );

  const projects = useMemo(() => organization?.projects ?? [], [organization]);

  const [feedback, setFeedback] = useState<string | null>(null);
  const [feedbackTone, setFeedbackTone] = useState<'positive' | 'negative'>('positive');
  const [searchValue, setSearchValue] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<SettingsCategory | typeof ALL_CATEGORIES>(ALL_CATEGORIES);
  const [editingSetting, setEditingSetting] = useState<SettingViewModel | null>(null);
  const [addSettingOpen, setAddSettingOpen] = useState(false);
  const [addSettingInitialSelection, setAddSettingInitialSelection] = useState<{
    settingKey: string;
    value?: string;
  } | null>(null);
  const [newProjectName, setNewProjectName] = useState('');
  const [organizationInvite, setOrganizationInvite] = useState('');
  const [projectInvites, setProjectInvites] = useState<Record<string, string>>({});
  const [latestRuntimeToken, setLatestRuntimeToken] = useState<{
    token: string;
    expiresAt: string;
  } | null>(null);

  const showFeedback = useCallback((message: string, tone: 'positive' | 'negative' = 'positive') => {
    setFeedback(message);
    setFeedbackTone(tone);
    const timeout = window.setTimeout(() => setFeedback(null), 5000);
    return () => window.clearTimeout(timeout);
  }, []);

  const environmentCatalogQuery = useQuery({
    queryKey: environmentCatalogQueryKey,
    queryFn: () => fetchOrganizationEnvironmentCatalog(),
    refetchOnWindowFocus: false,
  });

  const environmentSettingsQuery = useQuery({
    queryKey: environmentSettingsQueryKey(params.organizationId),
    queryFn: () => fetchOrganizationEnvironmentSettings(params.organizationId),
    enabled: Boolean(params.organizationId),
    refetchOnWindowFocus: false,
  });

  const runtimeInstancesQuery = useQuery<RuntimeInstance[]>({
    queryKey: runtimeInstancesQueryKey(params.organizationId),
    queryFn: () => fetchRuntimeInstances(params.organizationId),
    enabled: Boolean(params.organizationId),
    refetchOnWindowFocus: false,
  });

  const saveSettingMutation = useMutation({
    mutationFn: async ({
      organizationId,
      settingKey,
      settingValue,
    }: {
      organizationId: string;
      settingKey: string;
      settingValue: string;
    }) => setOrganizationEnvironmentSetting(organizationId, settingKey, settingValue),
  });

  const clearSettingMutation = useMutation({
    mutationFn: async ({
      organizationId,
      settingKey,
    }: {
      organizationId: string;
      settingKey: string;
    }) => deleteOrganizationEnvironmentSetting(organizationId, settingKey),
  });

  const createRuntimeTokenMutation = useMutation({
    mutationFn: async ({ organizationId }: { organizationId: string }) =>
      createRuntimeRegistrationToken(organizationId),
  });

  const settingsRecords = useMemo(() => {
    return buildSettingsViewModel(
      environmentCatalogQuery.data ?? [],
      environmentSettingsQuery.data ?? [],
    );
  }, [environmentCatalogQuery.data, environmentSettingsQuery.data]);

  const filteredSettings = useMemo(() => {
    return filterSettings(settingsRecords, searchValue, selectedCategory);
  }, [settingsRecords, searchValue, selectedCategory]);

  const categoryCounts = useMemo(() => {
    return SETTINGS_CATEGORIES.map((category) => ({
      category,
      count: settingsRecords.filter((record) => record.category === category).length,
    }));
  }, [settingsRecords]);

  const upsertSetting = useCallback(
    async (setting: SettingViewModel, value: string) => {
      const trimmed = value.trim();
      if (!trimmed) {
        await clearSettingMutation.mutateAsync({
          organizationId: params.organizationId,
          settingKey: setting.settingKey,
        });
        showFeedback(`${setting.displayName} cleared.`, 'positive');
      } else {
        await saveSettingMutation.mutateAsync({
          organizationId: params.organizationId,
          settingKey: setting.settingKey,
          settingValue: trimmed,
        });
        showFeedback(`${setting.displayName} saved.`, 'positive');
      }
      await queryClient.invalidateQueries({ queryKey: environmentSettingsQueryKey(params.organizationId) });
    },
    [clearSettingMutation, params.organizationId, queryClient, saveSettingMutation, showFeedback],
  );

  const handleDuplicateFromDrawer = useCallback((setting: SettingViewModel, value: string) => {
    setAddSettingInitialSelection({
      settingKey: setting.settingKey,
      value,
    });
    setAddSettingOpen(true);
  }, []);

  const handleCreateFromModal = useCallback(
    async (setting: SettingViewModel, value: string) => {
      await upsertSetting(setting, value);
    },
    [upsertSetting],
  );

  const handleExportJson = useCallback(() => {
    if (!organization) {
      return;
    }
    const payload = {
      organizationId: organization.id,
      organizationName: organization.name,
      exportedAt: new Date().toISOString(),
      settings: settingsRecords.map((record) => ({
        settingKey: record.settingKey,
        settingValue: record.settingValue,
        category: record.category,
        scope: record.scope,
        isConfigured: record.isConfigured,
        isInherited: record.isInherited,
      })),
    };
    downloadAsFile(
      `${organization.name.replace(/\s+/g, '-').toLowerCase()}-settings.json`,
      JSON.stringify(payload, null, 2),
      'application/json',
    );
    showFeedback('Settings exported as JSON.', 'positive');
  }, [organization, settingsRecords, showFeedback]);

  const handleExportYaml = useCallback(() => {
    if (!organization) {
      return;
    }
    const payload = {
      organizationId: organization.id,
      organizationName: organization.name,
      exportedAt: new Date().toISOString(),
      settings: settingsRecords.map((record) => ({
        settingKey: record.settingKey,
        settingValue: record.settingValue,
        category: record.category,
        scope: record.scope,
        isConfigured: record.isConfigured,
        isInherited: record.isInherited,
      })),
    };
    downloadAsFile(
      `${organization.name.replace(/\s+/g, '-').toLowerCase()}-settings.yaml`,
      toYaml(payload),
      'text/yaml',
    );
    showFeedback('Settings exported as YAML.', 'positive');
  }, [organization, settingsRecords, showFeedback]);

  const handleViewAudit = useCallback(() => {
    setSelectedCategory('Audit & Compliance');
    setSearchValue('');
  }, []);

  const handleOpenQuickActionSetting = useCallback(
    (settingKey: string) => {
      const target = settingsRecords.find((item) => item.settingKey === settingKey);
      if (!target) {
        showFeedback('Setting type is not available in this environment.', 'negative');
        return;
      }
      setEditingSetting(target);
    },
    [settingsRecords, showFeedback],
  );

  const handleCreateProject = useCallback(
    async (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      if (!organization) {
        showFeedback('Select a valid organization before creating a project.', 'negative');
        return;
      }
      const projectName = newProjectName.trim();
      if (!projectName) {
        showFeedback('Please provide a name for the project.', 'negative');
        return;
      }
      try {
        await createProject(organization.id, { name: projectName });
        setNewProjectName('');
        showFeedback(`Project "${projectName}" created.`, 'positive');
        await reloadOrganizations();
      } catch (error) {
        showFeedback(resolveErrorMessage(error), 'negative');
      }
    },
    [newProjectName, organization, reloadOrganizations, showFeedback],
  );

  const handleInviteToOrganization = useCallback(
    async (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      if (!organization) {
        showFeedback('Select a valid organization before inviting teammates.', 'negative');
        return;
      }
      const username = organizationInvite.trim();
      if (!username) {
        showFeedback('Enter a username to send an invite.', 'negative');
        return;
      }
      try {
        await inviteToOrganization(organization.id, { username });
        setOrganizationInvite('');
        showFeedback(`Invited ${username} to ${organization.name}.`, 'positive');
      } catch (error) {
        showFeedback(resolveErrorMessage(error), 'negative');
      }
    },
    [organization, organizationInvite, showFeedback],
  );

  const handleInviteToProject = useCallback(
    async (event: React.FormEvent<HTMLFormElement>, projectId: string, projectName: string) => {
      event.preventDefault();
      if (!organization) {
        showFeedback('Select a valid organization before inviting teammates.', 'negative');
        return;
      }
      const username = projectInvites[projectId]?.trim() ?? '';
      if (!username) {
        showFeedback('Enter a username to invite to the project.', 'negative');
        return;
      }
      try {
        await inviteToProject(organization.id, projectId, { username });
        setProjectInvites((current) => ({ ...current, [projectId]: '' }));
        showFeedback(`Invited ${username} to ${projectName}.`, 'positive');
      } catch (error) {
        showFeedback(resolveErrorMessage(error), 'negative');
      }
    },
    [organization, projectInvites, showFeedback],
  );

  const handleCreateRuntimeRegistrationToken = useCallback(async () => {
    try {
      const token = await createRuntimeTokenMutation.mutateAsync({
        organizationId: params.organizationId,
      });
      setLatestRuntimeToken({
        token: token.registrationToken,
        expiresAt: token.expiresAt,
      });
      showFeedback('Runtime registration token created.', 'positive');
    } catch (error) {
      showFeedback(resolveErrorMessage(error), 'negative');
    }
  }, [createRuntimeTokenMutation, params.organizationId, showFeedback]);

  const handleCopyRuntimeToken = useCallback(async () => {
    if (!latestRuntimeToken?.token) {
      return;
    }
    try {
      await navigator.clipboard.writeText(latestRuntimeToken.token);
      showFeedback('Runtime registration token copied to clipboard.', 'positive');
    } catch {
      showFeedback('Unable to copy token. Copy it manually.', 'negative');
    }
  }, [latestRuntimeToken, showFeedback]);

  const organizationLoading = loading && organizations.length === 0;
  const settingsLoading = environmentCatalogQuery.isLoading || environmentSettingsQuery.isLoading;
  const settingsError = environmentCatalogQuery.error || environmentSettingsQuery.error;

  const showExecutionPanels =
    selectedCategory === ALL_CATEGORIES || selectedCategory === 'Execution & Runtime';
  const showSecurityPanels =
    selectedCategory === ALL_CATEGORIES || selectedCategory === 'Security & Access';
  const showAuditPanel =
    selectedCategory === ALL_CATEGORIES || selectedCategory === 'Audit & Compliance';

  return (
    <>
      <SettingsLayout
        organizationId={params.organizationId}
        organizationName={organization?.name ?? 'Organization'}
        environmentLabel={process.env.NEXT_PUBLIC_ENVIRONMENT || null}
        searchValue={searchValue}
        onSearchChange={setSearchValue}
        onAddSetting={() => {
          setAddSettingInitialSelection(null);
          setAddSettingOpen(true);
        }}
        onExportJson={handleExportJson}
        onExportYaml={handleExportYaml}
        onViewAudit={handleViewAudit}
        quickActions={
          <section className="surface-panel rounded-3xl p-4 shadow-soft">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[color:var(--text-muted)]">
                  Quick actions
                </p>
                <p className="mt-1 text-sm">Jump to common configuration tasks.</p>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Button size="sm" variant="outline" onClick={() => router.push(`/datasources/${params.organizationId}/create`)}>
                  <Database className="h-4 w-4" />
                  Add connection
                </Button>
                <Button size="sm" variant="outline" onClick={() => handleOpenQuickActionSetting('execution_mode_default')}>
                  <Rocket className="h-4 w-4" />
                  Set default runtime
                </Button>
                <Button size="sm" variant="outline" onClick={() => handleOpenQuickActionSetting('llm_enabled')}>
                  <Sparkles className="h-4 w-4" />
                  Enable LLM
                </Button>
                <Button size="sm" variant="outline" onClick={() => setSelectedCategory('Limits & Quotas')}>
                  <Gauge className="h-4 w-4" />
                  Configure limits
                </Button>
              </div>
            </div>
          </section>
        }
      >
        {feedback ? (
          <div
            role="status"
            className={`rounded-lg border px-4 py-3 text-sm shadow-soft ${
              feedbackTone === 'positive'
                ? 'border-emerald-400/60 bg-emerald-500/10 text-emerald-800'
                : 'border-rose-400/60 bg-rose-500/10 text-rose-800'
            }`}
          >
            {feedback}
          </div>
        ) : null}

        {organizationLoading ? (
          <section className="surface-panel rounded-3xl p-6 shadow-soft">
            <div className="space-y-3">
              {Array.from({ length: 4 }).map((_, index) => (
                <div key={index} className="rounded-2xl border border-[color:var(--panel-border)] p-4">
                  <Skeleton className="h-4 w-40" />
                  <Skeleton className="mt-3 h-10 w-full" />
                </div>
              ))}
            </div>
          </section>
        ) : !organization ? (
          <section className="surface-panel rounded-3xl p-6 text-center shadow-soft">
            <p className="text-sm text-[color:var(--text-muted)]">We couldn&apos;t find that organization.</p>
          </section>
        ) : (
          <>
            <div className="grid gap-4 lg:grid-cols-[280px_1fr]">
              <CategorySidebar
                categories={categoryCounts}
                selectedCategory={selectedCategory}
                onSelectCategory={setSelectedCategory}
              />
              <section className="surface-panel rounded-3xl p-4 shadow-soft">
                {settingsLoading ? (
                  <div className="space-y-3">
                    {Array.from({ length: 6 }).map((_, index) => (
                      <div key={index} className="rounded-2xl border border-[color:var(--panel-border)] p-4">
                        <Skeleton className="h-4 w-44" />
                        <Skeleton className="mt-3 h-8 w-full" />
                      </div>
                    ))}
                  </div>
                ) : settingsError ? (
                  <div className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-6 text-center">
                    <p className="text-sm text-[color:var(--text-muted)]">{resolveErrorMessage(settingsError)}</p>
                  </div>
                ) : (
                  <SettingsTable records={filteredSettings} onEditSetting={setEditingSetting} />
                )}
              </section>
            </div>

            {showExecutionPanels ? (
              <>
                <section className="surface-panel rounded-3xl p-6 shadow-soft">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Runtime registration token</h2>
                      <p className="mt-1 text-sm">
                        Generate a one-time token for customer-runtime worker registration.
                      </p>
                    </div>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={handleCreateRuntimeRegistrationToken}
                      isLoading={createRuntimeTokenMutation.isPending}
                    >
                      Generate token
                    </Button>
                  </div>

                  {latestRuntimeToken ? (
                    <div className="mt-5 space-y-3 rounded-2xl border border-amber-400/40 bg-amber-500/10 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <p className="text-sm font-semibold text-[color:var(--text-primary)]">One-time token</p>
                        <Badge variant="warning">Expires {formatTimestamp(latestRuntimeToken.expiresAt)}</Badge>
                      </div>
                      <Input readOnly value={latestRuntimeToken.token} className="font-mono text-xs" />
                      <div className="flex justify-end">
                        <Button variant="secondary" size="sm" onClick={handleCopyRuntimeToken}>
                          Copy token
                        </Button>
                      </div>
                    </div>
                  ) : (
                    <div className="mt-5 rounded-2xl border border-dashed border-[color:var(--panel-border)] p-4 text-sm text-[color:var(--text-muted)]">
                      Generate a token when you are ready to bootstrap a customer-runtime worker.
                    </div>
                  )}
                </section>

                <section className="surface-panel rounded-3xl p-6 shadow-soft">
                  <div className="mb-4 flex items-center justify-between">
                    <div>
                      <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Runtime instances</h2>
                      <p className="mt-1 text-sm">Registered execution-plane instances for this organization.</p>
                    </div>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => runtimeInstancesQuery.refetch()}
                      disabled={runtimeInstancesQuery.isFetching}
                    >
                      Refresh
                    </Button>
                  </div>

                  {runtimeInstancesQuery.isLoading ? (
                    <div className="space-y-3">
                      {Array.from({ length: 2 }).map((_, index) => (
                        <div key={index} className="rounded-2xl border border-[color:var(--panel-border)] p-4">
                          <Skeleton className="h-4 w-48" />
                          <Skeleton className="mt-3 h-4 w-full" />
                        </div>
                      ))}
                    </div>
                  ) : runtimeInstancesQuery.error ? (
                    <p className="text-sm text-[color:var(--text-muted)]">
                      {resolveErrorMessage(runtimeInstancesQuery.error)}
                    </p>
                  ) : (runtimeInstancesQuery.data ?? []).length === 0 ? (
                    <p className="text-sm text-[color:var(--text-muted)]">No runtime instances have registered yet.</p>
                  ) : (
                    <ul className="space-y-3">
                      {(runtimeInstancesQuery.data ?? []).map((runtime) => (
                        <li
                          key={runtime.epId}
                          className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"
                        >
                          <div className="flex flex-wrap items-start justify-between gap-3">
                            <div>
                              <div className="flex flex-wrap items-center gap-2">
                                <p className="text-sm font-semibold text-[color:var(--text-primary)]">
                                  {runtime.displayName || `Runtime ${runtime.epId.slice(0, 8)}`}
                                </p>
                                <Badge variant={runtimeStatusVariant(runtime.status)}>{runtime.status}</Badge>
                              </div>
                              <p className="mt-1 text-[10px] font-mono text-[color:var(--text-muted)]">{runtime.epId}</p>
                            </div>
                            <div className="text-right text-xs text-[color:var(--text-muted)]">
                              <p>Last seen: {formatTimestamp(runtime.lastSeenAt)}</p>
                              <p>Registered: {formatTimestamp(runtime.registeredAt)}</p>
                            </div>
                          </div>
                        </li>
                      ))}
                    </ul>
                  )}
                </section>
              </>
            ) : null}

            {showSecurityPanels ? (
              <section className="surface-panel rounded-3xl p-6 shadow-soft">
                <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Security & access</h2>
                <p className="mt-1 text-sm">Invite teammates and manage workspace-level project access.</p>

                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <form
                    onSubmit={handleInviteToOrganization}
                    className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"
                  >
                    <h3 className="text-sm font-medium text-[color:var(--text-primary)]">Invite to organization</h3>
                    <p className="mt-1 text-xs text-[color:var(--text-muted)]">
                      Add an existing LangBridge user by username.
                    </p>
                    <div className="mt-3 flex gap-2">
                      <Input
                        value={organizationInvite}
                        onChange={(event) => setOrganizationInvite(event.target.value)}
                        placeholder="username"
                      />
                      <Button type="submit" size="sm">
                        Invite
                      </Button>
                    </div>
                  </form>

                  <form
                    onSubmit={handleCreateProject}
                    className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"
                  >
                    <h3 className="text-sm font-medium text-[color:var(--text-primary)]">Create project</h3>
                    <p className="mt-1 text-xs text-[color:var(--text-muted)]">
                      Projects scope connectors, agents, and semantic models.
                    </p>
                    <div className="mt-3 flex gap-2">
                      <Input
                        value={newProjectName}
                        onChange={(event) => setNewProjectName(event.target.value)}
                        placeholder="Project name"
                      />
                      <Button type="submit" size="sm">
                        Create
                      </Button>
                    </div>
                  </form>
                </div>

                <div className="mt-5">
                  <h3 className="text-sm font-semibold text-[color:var(--text-primary)]">Projects</h3>
                  {projects.length === 0 ? (
                    <p className="mt-2 text-sm text-[color:var(--text-muted)]">
                      No projects yet. Create one to start organizing work.
                    </p>
                  ) : (
                    <ul className="mt-3 space-y-3">
                      {projects.map((project) => (
                        <li
                          key={project.id}
                          className="rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4"
                        >
                          <h4 className="text-sm font-medium text-[color:var(--text-primary)]">{project.name}</h4>
                          <p className="text-xs text-[color:var(--text-muted)]">Project ID: {project.id}</p>
                          <form
                            onSubmit={(event) => handleInviteToProject(event, project.id, project.name)}
                            className="mt-3 flex gap-2"
                          >
                            <Input
                              value={projectInvites[project.id] ?? ''}
                              onChange={(event) =>
                                setProjectInvites((current) => ({
                                  ...current,
                                  [project.id]: event.target.value,
                                }))
                              }
                              placeholder="username"
                            />
                            <Button type="submit" size="sm" variant="outline">
                              Invite
                            </Button>
                          </form>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </section>
            ) : null}

            {showAuditPanel ? (
              <section className="surface-panel rounded-3xl p-6 shadow-soft">
                <h2 className="text-lg font-semibold text-[color:var(--text-primary)]">Audit & compliance</h2>
                <p className="mt-1 text-sm">
                  Audit-lite shows latest known editor metadata captured when settings are updated.
                </p>
                <div className="mt-4 rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] p-4">
                  <ul className="space-y-2 text-sm">
                    {settingsRecords
                      .filter((record) => record.lastUpdatedAt)
                      .sort((left, right) => (left.lastUpdatedAt && right.lastUpdatedAt
                        ? new Date(right.lastUpdatedAt).getTime() - new Date(left.lastUpdatedAt).getTime()
                        : 0))
                      .slice(0, 8)
                      .map((record) => (
                        <li key={record.settingKey} className="flex flex-wrap items-center justify-between gap-2">
                          <span className="text-[color:var(--text-primary)]">{record.displayName}</span>
                          <span className="text-xs text-[color:var(--text-muted)]">
                            {formatTimestamp(record.lastUpdatedAt)} {record.lastUpdatedBy ? `by ${record.lastUpdatedBy}` : ''}
                          </span>
                        </li>
                      ))}
                  </ul>
                  {settingsRecords.filter((record) => record.lastUpdatedAt).length === 0 ? (
                    <p className="text-sm text-[color:var(--text-muted)]">
                      No audit metadata recorded yet for settings updates.
                    </p>
                  ) : null}
                </div>
              </section>
            ) : null}
          </>
        )}
      </SettingsLayout>

      <EditSettingDrawer
        open={Boolean(editingSetting)}
        setting={editingSetting}
        onOpenChange={(open) => {
          if (!open) {
            setEditingSetting(null);
          }
        }}
        onSave={upsertSetting}
        onDuplicate={handleDuplicateFromDrawer}
      />

      <AddSettingModal
        open={addSettingOpen}
        onOpenChange={(open) => {
          setAddSettingOpen(open);
          if (!open) {
            setAddSettingInitialSelection(null);
          }
        }}
        settingsCatalog={settingsRecords}
        onCreate={handleCreateFromModal}
        initialSelection={addSettingInitialSelection}
      />
    </>
  );
}
