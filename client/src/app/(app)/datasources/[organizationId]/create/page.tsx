'use client';

import { JSX, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { LucideIcon } from 'lucide-react';
import {
  Atom,
  Box,
  Braces,
  Cloud,
  Database,
  Plug,
  Search,
  Server,
  Snowflake as SnowflakeIcon,
} from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select } from '@/components/ui/select';
import { Spinner } from '@/components/ui/spinner';
import { Textarea } from '@/components/ui/textarea';
import { cn } from '@/lib/utils';
import { ApiError } from '@/orchestration/http';
import {
  createConnector,
  fetchConnectorSchema,
  fetchConnectorTypes,
  type ConnectorConfigEntry,
  type ConnectorConfigSchema,
  type ConnectorResponse,
  type CreateConnectorPayload,
} from '@/orchestration/connectors';
import type { Project } from '@/orchestration/organizations';
import { useWorkspaceScope } from '@/context/workspaceScope';

type FeedbackTone = 'positive' | 'negative';

interface FormFields {
  name: string;
  description: string;
  version: string;
  label: string;
  icon: string;
  organizationId: string;
  projectId: string;
}

interface FeedbackState {
  message: string;
  tone: FeedbackTone;
}

interface ConnectorTypeCardProps {
  type: string;
  schema?: ConnectorConfigSchema | null;
  isSelected: boolean;
  isLoading: boolean;
  onSelect: (type: string) => void;
}

const CONNECTOR_ICON_MAP: Record<string, LucideIcon> = {
  SNOWFLAKE: SnowflakeIcon,
  POSTGRES: Database,
  MYSQL: Database,
  MARIADB: Database,
  MONGODB: Database,
  REDSHIFT: Database,
  BIGQUERY: Database,
  SQLSERVER: Server,
  ORACLE: Database,
  ELASTICSEARCH: Search,
  RESTAPI: Braces,
  GRAPHQL: Atom,
  SALESFORCE: Cloud,
  ZAPIER: Plug,
  GENERIC: Box,
};

function resolveErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Something went wrong. Please try again.';
}

type DataConnectionsPageProps = {
  params: { organizationId: string };
};

export default function DataConnectionsPage({ params }: DataConnectionsPageProps): JSX.Element {
  const [connectorTypes, setConnectorTypes] = useState<string[]>([]);
  const [schemasByType, setSchemasByType] = useState<Record<string, ConnectorConfigSchema>>({});
  const [selectedType, setSelectedType] = useState('');
  const [typesLoading, setTypesLoading] = useState(true);
  const [configValues, setConfigValues] = useState<Record<string, string>>({});
  const [formFields, setFormFields] = useState<FormFields>({
    name: '',
    description: '',
    version: '',
    label: '',
    icon: '',
    organizationId: '',
    projectId: '',
  });
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState<FeedbackState | null>(null);
  const [createdConnector, setCreatedConnector] = useState<ConnectorResponse | null>(null);
  const feedbackTimeout = useRef<number | undefined>(undefined);
  const lastInitializedType = useRef<string | null>(null);
  const pendingSchemaRequests = useRef<Map<string, Promise<ConnectorConfigSchema>>>(new Map());

  const showFeedback = useCallback((message: string, tone: FeedbackTone = 'positive') => {
    setFeedback({ message, tone });
    if (feedbackTimeout.current) {
      window.clearTimeout(feedbackTimeout.current);
    }
    feedbackTimeout.current = window.setTimeout(() => setFeedback(null), 5000);
  }, []);

  useEffect(() => {
    return () => {
      if (feedbackTimeout.current) {
        window.clearTimeout(feedbackTimeout.current);
      }
    };
  }, []);

  const routeOrganizationId = params.organizationId;

  const loadSchemaForType = useCallback(
    async (type: string, options: { silent?: boolean } = {}): Promise<ConnectorConfigSchema | null> => {
      if (!routeOrganizationId) {
        return null;
      }
      const normalized = type.trim();
      if (!normalized) {
        return null;
      }
      if (schemasByType[normalized]) {
        return schemasByType[normalized];
      }
      const pending = pendingSchemaRequests.current.get(normalized);
      if (pending) {
        return pending;
      }
      const request = fetchConnectorSchema(routeOrganizationId, normalized)
        .then((schema) => {
          setSchemasByType((current) => {
            if (current[normalized]) {
              return current;
            }
            return { ...current, [normalized]: schema };
          });
          return schema;
        })
        .catch((error) => {
          if (!options.silent) {
            showFeedback(resolveErrorMessage(error), 'negative');
          }
          throw error;
        });
      pendingSchemaRequests.current.set(normalized, request);
      request.finally(() => {
        pendingSchemaRequests.current.delete(normalized);
      });
      return request;
    },
    [routeOrganizationId, schemasByType, showFeedback],
  );

  useEffect(() => {
    async function loadConnectorTypes(): Promise<void> {
      if (!routeOrganizationId) {
        return;
      }
      setTypesLoading(true);
      try {
        const types = await fetchConnectorTypes(routeOrganizationId);
        setConnectorTypes(types);
      } catch (error) {
        showFeedback(resolveErrorMessage(error), 'negative');
      } finally {
        setTypesLoading(false);
      }
    }

    void loadConnectorTypes();
  }, [routeOrganizationId, showFeedback]);

  useEffect(() => {
    async function loadConnectorSchemas(): Promise<void> {
      if (connectorTypes.length === 0) {
        return;
      }
      await Promise.all(
        connectorTypes.map((type) =>
          loadSchemaForType(type, { silent: true }).catch(() => undefined),
        ),
      );
    }

    void loadConnectorSchemas();
  }, [connectorTypes, loadSchemaForType]);

  const {
    organizations,
    loading: organizationsLoading,
    selectedOrganizationId: activeOrganizationId,
    selectedProjectId: activeProjectId,
    setSelectedOrganizationId,
  } = useWorkspaceScope();

  useEffect(() => {
    if (routeOrganizationId && routeOrganizationId !== activeOrganizationId) {
      setSelectedOrganizationId(routeOrganizationId);
    }
  }, [activeOrganizationId, routeOrganizationId, setSelectedOrganizationId]);

  useEffect(() => {
    setFormFields((current) => {
      const nextOrganization = routeOrganizationId || activeOrganizationId || '';
      const nextProject = activeProjectId ?? '';
      if (current.organizationId === nextOrganization && current.projectId === nextProject) {
        return current;
      }
      return { ...current, organizationId: nextOrganization, projectId: nextProject };
    });
  }, [activeOrganizationId, activeProjectId, routeOrganizationId]);

  const selectedSchema = selectedType ? schemasByType[selectedType] ?? null : null;
  const isSchemaLoading = Boolean(selectedType && !selectedSchema);

  useEffect(() => {
    if (!selectedType) {
      lastInitializedType.current = null;
      return;
    }
    if (!selectedSchema) {
      return;
    }
    if (lastInitializedType.current === selectedType) {
      return;
    }

    setFormFields((current) => ({
      ...current,
      name: selectedSchema.name ?? '',
      description: selectedSchema.description ?? '',
      version: selectedSchema.version ?? '',
      label: selectedSchema.label ?? '',
      icon: selectedSchema.icon ?? '',
    }));

    const defaults: Record<string, string> = {};
    selectedSchema.config.forEach((entry) => {
      const fallback = entry.default ?? entry.value ?? '';
      defaults[entry.field] = fallback === null || fallback === undefined ? '' : String(fallback);
    });
    setConfigValues(defaults);
    lastInitializedType.current = selectedType;
  }, [selectedSchema, selectedType]);

  useEffect(() => {
    if (!selectedType) {
      return;
    }
    void loadSchemaForType(selectedType).catch(() => undefined);
  }, [selectedType, loadSchemaForType]);

  const sortedConnectorTypes = useMemo(() => {
    return [...connectorTypes].sort((a, b) => a.localeCompare(b));
  }, [connectorTypes]);

  const configEntries = useMemo(
    () => selectedSchema?.config ?? [],
    [selectedSchema],
  );

  const organizationId = routeOrganizationId || formFields.organizationId;
  const projectId = formFields.projectId;

  const projectOptions = useMemo(() => {
    if (organizations.length === 0) {
      return [] as Array<Project & { organizationName: string }>;
    }
    if (organizationId) {
      const org = organizations.find((item) => item.id === organizationId);
      if (!org) {
        return [];
      }
      return org.projects.map((project) => ({ ...project, organizationName: org.name }));
    }
    return organizations.flatMap((org) =>
      org.projects.map((project) => ({ ...project, organizationName: org.name })),
    );
  }, [organizations, organizationId]);

  const handleTypeSelect = useCallback(
    (type: string) => {
      if (type === selectedType) {
        return;
      }
      lastInitializedType.current = null;
      setSelectedType(type);
      setCreatedConnector(null);
    },
    [selectedType],
  );

  function handleConfigChange(field: string, value: string): void {
    setConfigValues((current) => ({
      ...current,
      [field]: value,
    }));
  }

  function handleFieldChange<K extends keyof FormFields>(field: K, value: FormFields[K]): void {
    setFormFields((current) => ({
      ...current,
      [field]: value,
    }));
  }

  function handleOrganizationSelect(value: string): void {
    setFormFields((current) => {
      const updated: FormFields = {
        ...current,
        organizationId: value,
      };
      if (value) {
        const organization = organizations.find((item) => item.id === value);
        const hasSelectedProject =
          organization?.projects.some((project) => project.id === current.projectId) ?? false;
        if (!hasSelectedProject) {
          updated.projectId = '';
        }
      } else {
        updated.organizationId = '';
      }
      return updated;
    });
  }

  function handleProjectSelect(value: string): void {
    setFormFields((current) => {
      if (!value) {
        return {
          ...current,
          projectId: '',
        };
      }
      const owningOrganization = organizations.find((org) =>
        org.projects.some((project) => project.id === value),
      );
      const updated: FormFields = {
        ...current,
        projectId: value,
      };
      if (owningOrganization && owningOrganization.id !== current.organizationId) {
        updated.organizationId = owningOrganization.id;
      }
      return updated;
    });
  }

  function validateBeforeSubmit(): string | null {
    if (!selectedType) {
      return 'Choose a connector type before continuing.';
    }
    if (!selectedSchema) {
      return 'Hold on while we finish loading the connector schema.';
    }
    const trimmedName = formFields.name.trim();
    if (!trimmedName) {
      return 'Provide a name for the connector.';
    }
    if (!formFields.organizationId.trim() && !formFields.projectId.trim()) {
      return 'Add an organization or project so we know where to attach this connector.';
    }
    const missingRequired = configEntries
      .filter((entry) => entry.required)
      .filter((entry) => !configValues[entry.field]?.trim());
    if (missingRequired.length > 0) {
      const labels = missingRequired.map((entry) => entry.label ?? entry.field);
      return `Fill in the required field${labels.length > 1 ? 's' : ''}: ${labels.join(', ')}.`;
    }
    return null;
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    const validationError = validateBeforeSubmit();
    if (validationError) {
      showFeedback(validationError, 'negative');
      return;
    }

    if (!organizationId) {
      showFeedback('Select an organization before creating a connector.', 'negative');
      return;
    }

    const cleanedConfig: Record<string, string> = {};
    configEntries.forEach((entry) => {
      const rawValue = configValues[entry.field] ?? '';
      const trimmedValue = rawValue.trim();
      if (trimmedValue) {
        cleanedConfig[entry.field] = trimmedValue;
      }
    });

    const payload: CreateConnectorPayload = {
      name: formFields.name.trim(),
      connectorType: selectedType,
      config: { config: cleanedConfig },
    };

    const description = formFields.description.trim();
    if (description) {
      payload.description = description;
    }
    const version = formFields.version.trim();
    if (version) {
      payload.version = version;
    }
    const label = formFields.label.trim();
    if (label) {
      payload.label = label;
    }
    const icon = formFields.icon.trim();
    if (icon) {
      payload.icon = icon;
    }
    const organizationIdValue = formFields.organizationId.trim();
    if (organizationIdValue) {
      payload.organizationId = organizationIdValue;
    }
    const projectIdValue = formFields.projectId.trim();
    if (projectIdValue) {
      payload.projectId = projectIdValue;
    }

    setSubmitting(true);
    setCreatedConnector(null);

    try {
      const connector = await createConnector(organizationId, payload);
      setCreatedConnector(connector);
      showFeedback(`Connector "${connector.name}" created successfully.`, 'positive');
    } catch (error) {
      showFeedback(resolveErrorMessage(error), 'negative');
    } finally {
      setSubmitting(false);
    }
  }

  function renderConfigInput(entry: ConnectorConfigEntry): JSX.Element {
    const value = configValues[entry.field] ?? '';

    if (entry.valueList && entry.valueList.length > 0) {
      return (
        <Select
          id={`config-${entry.field}`}
          placeholder={`Select ${entry.label ?? entry.field}`}
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        >
          {entry.valueList.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </Select>
      );
    }

    if (entry.type === 'password') {
      return (
        <Input
          id={`config-${entry.field}`}
          type="password"
          autoComplete="off"
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        />
      );
    }

    if (entry.type === 'number') {
      return (
        <Input
          id={`config-${entry.field}`}
          type="number"
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        />
      );
    }

    if (entry.type === 'boolean') {
      return (
        <Select
          id={`config-${entry.field}`}
          placeholder={`Select ${entry.label ?? entry.field}`}
          value={value || ''}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        >
          <option value="true">True</option>
          <option value="false">False</option>
        </Select>
      );
    }

    if (entry.type === 'textarea') {
      return (
        <Textarea
          id={`config-${entry.field}`}
          value={value}
          onChange={(event) => handleConfigChange(entry.field, event.target.value)}
        />
      );
    }

    return (
      <Input
        id={`config-${entry.field}`}
        value={value}
        onChange={(event) => handleConfigChange(entry.field, event.target.value)}
      />
    );
  }

  function renderSchemaDetails(currentSchema: ConnectorConfigSchema): JSX.Element | null {
    if (!currentSchema) {
      return null;
    }

    return (
      <div className="flex flex-wrap items-center gap-3 text-sm text-[color:var(--text-secondary)]">
        <Badge variant="secondary" className="uppercase tracking-wide">
          {selectedType}
        </Badge>
        <Badge variant="warning" className="capitalize">
          {currentSchema.connectorType}
        </Badge>
        <span>
          Version{' '}
          <strong className="font-semibold text-[color:var(--text-primary)]">
            {currentSchema.version}
          </strong>
        </span>
      </div>
    );
  }

  return (
    <div className="space-y-6 text-[color:var(--text-secondary)]">
      {feedback ? (
        <div
          role="status"
          className={cn(
            'rounded-lg border px-4 py-3 text-sm shadow-soft',
            feedback.tone === 'positive'
              ? 'border-emerald-400/60 bg-emerald-500/10 text-emerald-700'
              : 'border-rose-400/60 bg-rose-500/10 text-rose-800',
          )}
        >
          {feedback.message}
        </div>
      ) : null}

      <section className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
        <header className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-base font-semibold text-[color:var(--text-primary)]">Choose a connector type</h2>
            <p>Click a card to load its schema and generate a tailored form automatically.</p>
          </div>
          {typesLoading ? <Spinner className="h-5 w-5 text-[color:var(--text-secondary)]" /> : null}
        </header>

        {sortedConnectorTypes.length > 0 ? (
          <div className="mt-5 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {sortedConnectorTypes.map((type) => (
              <ConnectorTypeCard
                key={type}
                type={type}
                schema={schemasByType[type]}
                isSelected={selectedType === type}
                isLoading={selectedType === type && isSchemaLoading}
                onSelect={handleTypeSelect}
              />
            ))}
          </div>
        ) : (
          <div className="mt-5 flex items-center gap-2 rounded-lg border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-alt)] px-4 py-6 text-sm">
            <Spinner className="h-4 w-4 text-[color:var(--text-secondary)]" />
            Fetching connector types...
          </div>
        )}

        {selectedSchema ? <div className="mt-6">{renderSchemaDetails(selectedSchema)}</div> : null}
      </section>

      {selectedType ? (
        isSchemaLoading ? (
          <section className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 text-center text-sm shadow-soft">
            Loading the schema for {selectedType}...
          </section>
        ) : selectedSchema ? (
          <section className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
            <form className="space-y-8" onSubmit={handleSubmit}>
              <div className="space-y-4">
                <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                  <div>
                    <h2 className="text-base font-semibold text-[color:var(--text-primary)]">Connector details</h2>
                    <p>
                      Give the connector a friendly name and optional metadata that appears in dashboards.
                    </p>
                  </div>
                  {selectedSchema.name ? (
                    <Badge variant="secondary" className="text-xs">
                      Default name: {selectedSchema.name}
                    </Badge>
                  ) : null}
                </div>

                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="connector-name" className="text-[color:var(--text-secondary)]">
                      Name
                    </Label>
                    <Input
                      id="connector-name"
                      value={formFields.name}
                      onChange={(event) => handleFieldChange('name', event.target.value)}
                      placeholder={selectedSchema.name}
                      required
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="connector-description" className="text-[color:var(--text-secondary)]">
                    Description
                  </Label>
                  <Textarea
                    id="connector-description"
                    value={formFields.description}
                    onChange={(event) => handleFieldChange('description', event.target.value)}
                    placeholder={selectedSchema.description}
                  />
                </div>
              </div>

              <div className="space-y-4">
                <div>
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Connection scope</h3>
                  <p className="text-sm">
                    Add the organization or project that should own this connector. Provide at least one ID.
                  </p>
                </div>
                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="organization-id" className="text-[color:var(--text-secondary)]">
                      Organization
                    </Label>
                    <Select
                      id="organization-id"
                      value={organizationId}
                      placeholder={organizationsLoading ? 'Loading organizations...' : 'Select an organization'}
                      disabled={organizationsLoading || organizations.length === 0 || Boolean(routeOrganizationId)}
                      onChange={(event) => handleOrganizationSelect(event.target.value)}
                    >
                      {organizations.map((organization) => (
                        <option key={organization.id} value={organization.id}>
                          {organization.name}
                        </option>
                      ))}
                    </Select>
                    {organizations.length === 0 && !organizationsLoading ? (
                      <p className="text-xs text-[color:var(--text-muted)]">
                        You do not have any organizations yet. Create one to continue.
                      </p>
                    ) : null}
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="project-id" className="text-[color:var(--text-secondary)]">
                      Project
                    </Label>
                    <Select
                      id="project-id"
                      value={projectId}
                      placeholder={
                        organizationId
                          ? projectOptions.length > 0
                            ? 'Select a project'
                            : 'No projects found for this organization'
                          : 'Select a project'
                      }
                      disabled={projectOptions.length === 0}
                      onChange={(event) => handleProjectSelect(event.target.value)}
                    >
                      <option value="">-- No project (workspace) --</option>
                      {projectOptions.map((project) => (
                        <option key={project.id} value={project.id}>
                          {organizationId ? project.name : `${project.organizationName} - ${project.name}`}
                        </option>
                      ))}
                    </Select>
                  </div>
                </div>
              </div>

              <div className="space-y-4">
                <div>
                  <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Connector configuration</h3>
                  <p className="text-sm">Fill in the required connection fields. We will validate the schema before saving.</p>
                </div>

                <div className="grid gap-4 sm:grid-cols-2">
                  {configEntries.map((entry) => (
                    <div key={entry.field} className="space-y-2">
                      <Label
                        htmlFor={`config-${entry.field}`}
                        className="flex items-center gap-2 text-[color:var(--text-secondary)]"
                      >
                        <span>{entry.label ?? entry.field}</span>
                        {entry.required ? (
                          <span className="rounded-full bg-rose-100 px-2 py-0.5 text-xs font-medium text-rose-700">
                            Required
                          </span>
                        ) : null}
                      </Label>
                      {renderConfigInput(entry)}
                      {entry.description ? (
                        <p className="text-xs text-[color:var(--text-muted)]">{entry.description}</p>
                      ) : null}
                    </div>
                  ))}
                </div>
              </div>

              <div className="flex items-center justify-between gap-3 border-t border-[color:var(--panel-border)] pt-4 text-xs">
                <div className="text-[color:var(--text-muted)]">
                  We will validate the connection before saving it to your workspace.
                </div>
                <Button
                  type="submit"
                  isLoading={submitting}
                  loadingText="Creating..."
                  disabled={submitting || isSchemaLoading || !selectedSchema}
                >
                  Create connector
                </Button>
              </div>
            </form>
          </section>
        ) : null
      ) : (
        <section className="rounded-xl border border-dashed border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 text-center text-sm shadow-soft">
          Select a connector card above to generate its form.
        </section>
      )}

      {createdConnector ? (
        <section className="rounded-xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] p-6 shadow-soft">
          <h3 className="text-base font-semibold text-[color:var(--text-primary)]">Connector ready to use</h3>
          <p className="mt-1 text-sm">
            We saved {createdConnector.name}. Keep the identifiers handy in case you need to update this connector later.
          </p>
          <dl className="mt-4 grid gap-3 text-sm sm:grid-cols-2">
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Connector ID</dt>
              <dd className="mt-1 font-mono text-xs">{createdConnector.id ?? 'Not returned'}</dd>
            </div>
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Connector type</dt>
              <dd className="mt-1 font-mono text-xs uppercase">
                {createdConnector.connectorType ?? selectedType}
              </dd>
            </div>
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Organization</dt>
              <dd className="mt-1 font-mono text-xs">{createdConnector.organizationId ?? 'Not provided'}</dd>
            </div>
            <div>
              <dt className="font-medium text-[color:var(--text-primary)]">Project</dt>
              <dd className="mt-1 font-mono text-xs">{createdConnector.projectId ?? 'Not provided'}</dd>
            </div>
          </dl>
        </section>
      ) : null}
    </div>
  );
}

function ConnectorTypeCard({ type, schema, isSelected, isLoading, onSelect }: ConnectorTypeCardProps) {
  const IconComponent = CONNECTOR_ICON_MAP[type] ?? Plug;
  const title = schema?.label ?? type.toLowerCase().replace(/_/g, ' ');
  const description =
    schema?.description ??
    `Configure and launch a ${type.toLowerCase().replace(/_/g, ' ')} connector in minutes.`;

  return (
    <button
      type="button"
      onClick={() => onSelect(type)}
      className={cn(
        'group flex h-full flex-col justify-between gap-4 rounded-xl border p-4 text-left transition hover:-translate-y-1 hover:border-[color:var(--accent)] hover:shadow-lg focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent)]',
        isSelected
          ? 'border-[color:var(--accent)] bg-[color:var(--panel-alt)] shadow-lg'
          : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] shadow-soft',
      )}
    >
      <div className="flex items-start gap-4">
        <span className={cn(
          'flex h-12 w-12 items-center justify-center rounded-full bg-[color:var(--panel-alt)] text-[color:var(--accent)] transition group-hover:scale-105',
          isSelected ? 'bg-[color:var(--accent-soft)] text-[color:var(--accent-strong)]' : '',
        )}>
          <IconComponent className="h-6 w-6" aria-hidden />
        </span>
        <div className="flex-1">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-semibold capitalize text-[color:var(--text-primary)]">
              {title}
            </h3>
            {schema?.version ? (
              <Badge variant="secondary" className="text-xs">
                v{schema.version}
              </Badge>
            ) : null}
          </div>
          <p className="mt-1 line-clamp-3 text-xs text-[color:var(--text-secondary)]">{description}</p>
        </div>
      </div>
      <div className="flex items-center justify-between text-xs text-[color:var(--text-secondary)]">
        <span className="uppercase tracking-wide">Select</span>
        {isLoading ? <Spinner className="h-4 w-4 text-[color:var(--text-secondary)]" /> : <span>{'>'}</span>}
      </div>
    </button>
  );
}
