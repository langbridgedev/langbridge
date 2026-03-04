'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ChevronDown, ChevronUp, History, Pencil, RefreshCw, Send, Sparkles } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { Skeleton } from '@/components/ui/skeleton';
import { Spinner } from '@/components/ui/spinner';
import { Textarea } from '@/components/ui/textarea';
import { useToast } from '@/components/ui/toast';
import { formatRelativeDate } from '@/lib/utils';
import { fetchAgentDefinitions, type AgentDefinition } from '@/orchestration/agents';
import { fetchAgentJobState, type AgentJobEvent } from '@/orchestration/jobs';
import {
  fetchThread,
  listThreadMessages,
  runThreadChat,
  updateThread,
  type Thread,
  type ThreadChatResponse,
  type ThreadMessage,
  type ThreadTabularResult,
  type ThreadVisualizationSpec,
} from '@/orchestration/threads';

import { ResultTable } from './ResultTable';
import { VisualizationPreview } from './VisualizationPreview';

type ConversationTurn = {
  id: string;
  prompt: string;
  createdAt: string;
  status: 'pending' | 'ready' | 'error';
  jobId?: string;
  jobStatus?: string;
  awaitingFinalPayload?: boolean;
  events?: AgentJobEvent[];
  internalEvents?: AgentJobEvent[];
  hasInternalEvents?: boolean;
  thinkingBreakdown?: Record<string, unknown> | null;
  showThinking?: boolean;
  loadingThinking?: boolean;
  agentId?: string;
  agentLabel?: string;
  summary?: string | null;
  result?: ThreadTabularResult | null;
  visualization?: ThreadVisualizationSpec | null;
  errorMessage?: string;
};

type ChatInterfaceProps = {
  threadId: string;
  organizationId: string;
};

const readTextField = (value: unknown): string | undefined => {
  return typeof value === 'string' ? value : undefined;
};

const extractAgentMeta = (message: ThreadMessage | undefined) => {
  if (!message) {
    return { agentId: undefined, agentLabel: undefined };
  }
  const snapshot = message.modelSnapshot ?? {};
  const agentId = readTextField(snapshot.agent_id ?? snapshot.agentId);
  const agentLabel = readTextField(snapshot.agent_name ?? snapshot.agentName);
  return { agentId, agentLabel };
};

const buildTurnsFromMessages = (messages: ThreadMessage[]): ConversationTurn[] => {
  if (messages.length === 0) {
    return [];
  }
  const assistantByParent = new Map<string, ThreadMessage>();
  messages
    .filter((message) => message.role === 'assistant')
    .forEach((message) => {
      if (message.parentMessageId) {
        assistantByParent.set(message.parentMessageId, message);
      }
    });

  return messages
    .filter((message) => message.role === 'user')
    .map((message, index) => {
      const assistant = assistantByParent.get(message.id);
      const content = message.content ?? {};
      const assistantContent = assistant?.content ?? {};
      const { agentId, agentLabel } = extractAgentMeta(assistant ?? message);
      const errorMessage = readTextField((assistant?.error as Record<string, unknown> | undefined)?.message);

      return {
        id: message.id ?? `history-${index}`,
        prompt: readTextField(content.text) ?? '',
        createdAt: message.createdAt ?? new Date().toISOString(),
        status: assistant ? (assistant.error ? 'error' : 'ready') : 'pending',
        agentId,
        agentLabel,
        summary: readTextField(assistantContent.summary) ?? null,
        result: (assistantContent.result as ThreadTabularResult | null | undefined) ?? null,
        visualization: (assistantContent.visualization as ThreadVisualizationSpec | null | undefined) ?? null,
        errorMessage: errorMessage || undefined,
      };
    });
};

const TERMINAL_JOB_STATUSES = new Set(['succeeded', 'failed', 'cancelled']);
const JOB_STATUS_POLL_INTERVAL_MS = 1500;

const isTerminalJobStatus = (status: string | undefined): boolean => {
  if (!status) {
    return false;
  }
  return TERMINAL_JOB_STATUSES.has(status);
};

const getErrorMessage = (error: unknown): string => {
  if (error && typeof error === 'object' && 'message' in error && typeof error.message === 'string') {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return 'An unexpected error occurred.';
};

const asRecord = (value: unknown): Record<string, unknown> | null => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
};

const normalizeTabularResult = (value: unknown): ThreadTabularResult | null => {
  const payload = asRecord(value);
  if (!payload) {
    return null;
  }
  return {
    columns: Array.isArray(payload.columns) ? (payload.columns as string[]) : [],
    rows: Array.isArray(payload.rows) ? payload.rows : [],
    rowCount:
      typeof payload.rowCount === 'number'
        ? payload.rowCount
        : typeof payload.row_count === 'number'
          ? payload.row_count
          : null,
    elapsedMs:
      typeof payload.elapsedMs === 'number'
        ? payload.elapsedMs
        : typeof payload.elapsed_ms === 'number'
          ? payload.elapsed_ms
          : null,
  };
};

const normalizeVisualization = (value: unknown): ThreadVisualizationSpec | null => {
  const payload = asRecord(value);
  if (!payload) {
    return null;
  }
  return payload as ThreadVisualizationSpec;
};

const getVisualizationChartType = (visualization: ThreadVisualizationSpec | null | undefined): string | null => {
  if (!visualization || typeof visualization !== 'object') {
    return null;
  }
  const raw = (visualization as Record<string, unknown>).chartType ?? (visualization as Record<string, unknown>).chart_type;
  if (typeof raw !== 'string') {
    return null;
  }
  return raw.trim().toLowerCase() || null;
};

const hasRenderableVisualization = (visualization: ThreadVisualizationSpec | null | undefined): boolean => {
  const chartType = getVisualizationChartType(visualization);
  if (!chartType) {
    return false;
  }
  return chartType !== 'table';
};

const extractSqlStatements = (payload: Record<string, unknown> | null): string[] => {
  if (!payload) {
    return [];
  }
  const candidates = [
    payload.sql,
    payload.sql_executable,
    payload.sqlExecutable,
    payload.sql_canonical,
    payload.sqlCanonical,
    payload.source_sql,
    payload.sourceSql,
  ];
  return candidates
    .filter((entry): entry is string => typeof entry === 'string' && entry.trim().length > 0)
    .map((entry) => entry.trim());
};

const cleanEventType = (eventType: string): string => {
  return eventType
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/_/g, ' ')
    .trim();
};

const toClockTime = (timestamp?: string | null): string => {
  if (!timestamp) {
    return '';
  }
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};

export function ChatInterface({ threadId, organizationId }: ChatInterfaceProps) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [composer, setComposer] = useState('');
  const [turns, setTurns] = useState<ConversationTurn[]>([]);
  const [selectedAgentId, setSelectedAgentId] = useState('');
  const [threadTitle, setThreadTitle] = useState<string | null>(null);
  const [renameOpen, setRenameOpen] = useState(false);
  const [renameValue, setRenameValue] = useState('');
  const [renameError, setRenameError] = useState<string | null>(null);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  const agentDefinitionsQuery = useQuery<AgentDefinition[]>({
    queryKey: ['agent-definitions', organizationId],
    enabled: Boolean(organizationId),
    queryFn: () => fetchAgentDefinitions(organizationId),
  });

  const historyQuery = useQuery<ThreadMessage[]>({
    queryKey: ['thread-messages', organizationId, threadId],
    queryFn: () => listThreadMessages(organizationId, threadId),
  });

  const threadQuery = useQuery<Thread>({
    queryKey: ['thread', organizationId, threadId],
    queryFn: () => fetchThread(organizationId, threadId),
  });

  const agentOptions = useMemo(() => {
    return (agentDefinitionsQuery.data ?? []).slice().sort((a, b) => a.name.localeCompare(b.name));
  }, [agentDefinitionsQuery.data]);

  const agentLabelById = useMemo(() => {
    return new Map(agentOptions.map((agent) => [agent.id, agent.name]));
  }, [agentOptions]);

  const selectedAgent = useMemo(() => {
    return agentOptions.find((agent) => agent.id === selectedAgentId) ?? null;
  }, [agentOptions, selectedAgentId]);

  const sendMessageMutation = useMutation<
    ThreadChatResponse,
    Error,
    { content: string; turnId: string; agentId: string }
  >({
    mutationFn: ({ content, agentId }) => runThreadChat(organizationId, threadId, content, agentId),
    onSuccess: (data, variables) => {
      setTurns((previous) =>
        previous.map((turn) =>
          turn.id === variables.turnId
            ? data.jobId
              ? {
                  ...turn,
                  status: 'pending',
                  jobId: data.jobId,
                  jobStatus: data.jobStatus ?? 'queued',
                  awaitingFinalPayload: false,
                  events: [],
                  hasInternalEvents: false,
                  thinkingBreakdown: null,
                  showThinking: false,
                  errorMessage: undefined,
                }
              : {
                  ...turn,
                  status: 'ready',
                  summary: data.summary ?? 'No summary was provided.',
                  result: data.result ?? null,
                  visualization: data.visualization ?? null,
                  errorMessage: undefined,
                }
            : turn,
        ),
      );
    },
    onError: (error, variables) => {
      setTurns((previous) =>
        previous.map((turn) =>
          turn.id === variables.turnId
            ? { ...turn, status: 'error', errorMessage: error.message || 'Unable to complete this request.' }
            : turn,
        ),
      );
      toast({
        title: 'Request failed',
        description: error.message || 'The assistant was unable to respond.',
        variant: 'destructive',
      });
    },
  });

  const renameThreadMutation = useMutation<Thread, Error, { title: string }>({
    mutationFn: ({ title }) => updateThread(organizationId, threadId, { title }),
    onSuccess: (updated) => {
      setThreadTitle(updated.title ?? null);
      setRenameOpen(false);
      setRenameValue('');
      setRenameError(null);
      queryClient.invalidateQueries({ queryKey: ['thread', organizationId, threadId] });
      queryClient.invalidateQueries({ queryKey: ['chat-threads', organizationId] });
      toast({ title: 'Thread renamed', description: 'Your thread title has been updated.' });
    },
    onError: (error) => {
      const message = error.message || 'Unable to rename this thread.';
      setRenameError(message);
      toast({ title: 'Rename failed', description: message, variant: 'destructive' });
    },
  });

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [turns.length, sendMessageMutation.isPending]);

  useEffect(() => {
    setTurns([]);
    setComposer('');
    setHistoryLoaded(false);
    const storageKey = `thread-agent:${threadId}`;
    const storedAgentId = window.localStorage.getItem(storageKey);
    setSelectedAgentId(storedAgentId ?? '');
    setThreadTitle(null);
  }, [threadId]);

  useEffect(() => {
    const storageKey = `thread-agent:${threadId}`;
    if (!selectedAgentId) {
      window.localStorage.removeItem(storageKey);
      return;
    }
    window.localStorage.setItem(storageKey, selectedAgentId);
  }, [selectedAgentId, threadId]);

  useEffect(() => {
    if (!agentDefinitionsQuery.isSuccess) {
      return;
    }
    if (agentOptions.length === 0) {
      setSelectedAgentId('');
      return;
    }
    if (selectedAgentId && !selectedAgent) {
      setSelectedAgentId('');
    }
  }, [agentDefinitionsQuery.isSuccess, agentOptions.length, selectedAgent, selectedAgentId]);

  const lastUpdated = useMemo(() => {
    const readyTurns = turns.filter((turn) => turn.status === 'ready');
    if (readyTurns.length === 0) {
      return null;
    }
    return readyTurns[readyTurns.length - 1].createdAt;
  }, [turns]);

  const hasPendingTurn = turns.some((turn) => turn.status === 'pending');
  const isSending = sendMessageMutation.isPending || hasPendingTurn;
  const hasAgents = agentOptions.length > 0;
  const isLoadingAgents = agentDefinitionsQuery.isLoading;
  const agentStatusLabel =
    selectedAgent?.name ?? (isLoadingAgents ? 'Loading agents...' : hasAgents ? 'Select an agent' : 'No agents available');
  const agentsBasePath = organizationId ? `/agents/${organizationId}` : '/agents';

  const submitMessage = () => {
    const trimmed = composer.trim();
    if (!trimmed || isSending) {
      return;
    }
    if (!selectedAgentId) {
      toast({
        title: hasAgents ? 'Select an agent' : 'No agents available',
        description: hasAgents
          ? 'Choose an agent before sending a prompt.'
          : 'Create an agent to start this thread.',
        variant: 'destructive',
      });
      return;
    }

    const turnId = `turn-${Date.now()}`;
    const createdAt = new Date().toISOString();
    const agentLabel = selectedAgent?.name ?? 'Unknown agent';
    const agentId = selectedAgentId;

    setTurns((previous) => [
      ...previous,
      {
        id: turnId,
        prompt: trimmed,
        createdAt,
        status: 'pending',
        jobStatus: 'queued',
        awaitingFinalPayload: false,
        events: [],
        internalEvents: [],
        showThinking: false,
        agentId,
        agentLabel,
      },
    ]);

    sendMessageMutation.mutate({ content: trimmed, turnId, agentId });
    setComposer('');
  };

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    submitMessage();
  };

  const handleComposerKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      submitMessage();
    }
  };

  const handleRegenerate = () => {
    if (turns.length === 0) {
      toast({
        title: 'Nothing to regenerate',
        description: 'Send a prompt before trying to regenerate the response.',
        variant: 'destructive',
      });
      return;
    }

    const lastTurn = [...turns].reverse().find((turn) => turn.status !== 'pending');
    if (!lastTurn) {
      toast({
        title: 'Still processing',
        description: 'Wait for the current response to finish before regenerating.',
        variant: 'destructive',
      });
      return;
    }
    setComposer(lastTurn.prompt);
  };

  const resetConversation = () => {
    setTurns([]);
  };

  const toggleThinkingBreakdown = async (turnId: string) => {
    const targetTurn = turns.find((turn) => turn.id === turnId);
    if (!targetTurn) {
      return;
    }
    if (targetTurn.showThinking) {
      setTurns((previous) =>
        previous.map((turn) => (turn.id === turnId ? { ...turn, showThinking: false } : turn)),
      );
      return;
    }
    if (!targetTurn.jobId) {
      setTurns((previous) =>
        previous.map((turn) => (turn.id === turnId ? { ...turn, showThinking: true } : turn)),
      );
      return;
    }
    if (targetTurn.thinkingBreakdown || (targetTurn.internalEvents ?? []).length > 0) {
      setTurns((previous) =>
        previous.map((turn) => (turn.id === turnId ? { ...turn, showThinking: true } : turn)),
      );
      return;
    }

    setTurns((previous) =>
      previous.map((turn) =>
        turn.id === turnId ? { ...turn, loadingThinking: true, showThinking: true } : turn,
      ),
    );

    try {
      const job = await fetchAgentJobState(organizationId, targetTurn.jobId, true);
      const internalEvents = (job.events ?? []).filter((event) => event.visibility === 'internal');
      setTurns((previous) =>
        previous.map((turn) =>
          turn.id === turnId
            ? {
                ...turn,
                loadingThinking: false,
                showThinking: true,
                jobStatus: job.status,
                internalEvents,
                thinkingBreakdown: job.thinkingBreakdown ?? null,
                hasInternalEvents: Boolean(job.hasInternalEvents),
              }
            : turn,
        ),
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to load thinking details.';
      setTurns((previous) =>
        previous.map((turn) =>
          turn.id === turnId
            ? { ...turn, loadingThinking: false, showThinking: false, errorMessage: message }
            : turn,
        ),
      );
      toast({
        title: 'Could not load thinking details',
        description: message,
        variant: 'destructive',
      });
    }
  };

  useEffect(() => {
    if (historyLoaded || !historyQuery.isSuccess) {
      return;
    }
    const historyTurns = buildTurnsFromMessages(historyQuery.data ?? []);
    const historyIds = new Set(historyTurns.map((turn) => turn.id));
    const mergedTurns =
      turns.length > 0 ? [...historyTurns, ...turns.filter((turn) => !historyIds.has(turn.id))] : historyTurns;
    setTurns(mergedTurns);
    setHistoryLoaded(true);
    if (!selectedAgentId) {
      const lastAgentId = [...historyTurns].reverse().find((turn) => turn.agentId)?.agentId;
      if (lastAgentId) {
        setSelectedAgentId(lastAgentId);
      }
    }
  }, [historyLoaded, historyQuery.data, historyQuery.isSuccess, selectedAgentId, turns]);

  useEffect(() => {
    if (!threadQuery.data) {
      return;
    }
    setThreadTitle(threadQuery.data.title ?? null);
  }, [threadQuery.data]);

  useEffect(() => {
    const pendingTurns = turns.filter(
      (turn) =>
        turn.status === 'pending' &&
        turn.jobId &&
        (!isTerminalJobStatus(turn.jobStatus) || Boolean(turn.awaitingFinalPayload)),
    );
    if (pendingTurns.length === 0) {
      return;
    }

    let cancelled = false;
    let isPolling = false;

    const pollJobs = async () => {
      if (isPolling) {
        return;
      }
      isPolling = true;
      let shouldRefreshHistory = false;
      try {
        await Promise.all(
          pendingTurns.map(async (turn) => {
            if (!turn.jobId) {
              return;
            }
            try {
              const job = await fetchAgentJobState(organizationId, turn.jobId, false);
              if (cancelled) {
                return;
              }

              const resultPayload = normalizeTabularResult(job.finalResponse?.result);
              const visualizationPayload = normalizeVisualization(job.finalResponse?.visualization);
              const errorPayload = asRecord(job.error);
              const isFailed = job.status === 'failed' || job.status === 'cancelled';
              const isReady = job.status === 'succeeded';
              const hasFinalPayload = job.finalResponse !== null && job.finalResponse !== undefined;
              const finishedAtMs = job.finishedAt ? Date.parse(job.finishedAt) : Number.NaN;
              const finalizeWaitExpired =
                Number.isFinite(finishedAtMs) && Date.now() - finishedAtMs > 10_000;

              setTurns((previous) =>
                previous.map((currentTurn) => {
                  if (currentTurn.id !== turn.id) {
                    return currentTurn;
                  }

                  if (isFailed) {
                    return {
                      ...currentTurn,
                      status: 'error',
                      jobStatus: job.status,
                      awaitingFinalPayload: false,
                      events: job.events ?? currentTurn.events ?? [],
                      hasInternalEvents: Boolean(job.hasInternalEvents),
                      errorMessage: getErrorMessage(errorPayload?.message ?? errorPayload ?? 'Request failed.'),
                    };
                  }

                  if (isReady) {
                    if (!hasFinalPayload && !finalizeWaitExpired) {
                      return {
                        ...currentTurn,
                        status: 'pending',
                        jobStatus: job.status,
                        awaitingFinalPayload: true,
                        events: job.events ?? currentTurn.events ?? [],
                        hasInternalEvents: Boolean(job.hasInternalEvents),
                      };
                    }

                    return {
                      ...currentTurn,
                      status: 'ready',
                      jobStatus: job.status,
                      awaitingFinalPayload: false,
                      events: job.events ?? currentTurn.events ?? [],
                      hasInternalEvents: Boolean(job.hasInternalEvents),
                      summary: job.finalResponse?.summary ?? currentTurn.summary ?? 'Response completed.',
                      result: resultPayload ?? currentTurn.result ?? null,
                      visualization: visualizationPayload ?? currentTurn.visualization ?? null,
                      errorMessage: undefined,
                    };
                  }

                  return {
                    ...currentTurn,
                    jobStatus: job.status,
                    awaitingFinalPayload: false,
                    events: job.events ?? currentTurn.events ?? [],
                    hasInternalEvents: Boolean(job.hasInternalEvents),
                  };
                }),
              );

              if ((isReady && (hasFinalPayload || finalizeWaitExpired)) || isFailed) {
                shouldRefreshHistory = true;
              }
            } catch {
              if (cancelled) {
                return;
              }
            }
          }),
        );

        if (shouldRefreshHistory) {
          queryClient.invalidateQueries({ queryKey: ['thread-messages', organizationId, threadId] });
        }
      } finally {
        isPolling = false;
      }
    };

    const intervalId = window.setInterval(pollJobs, JOB_STATUS_POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [organizationId, queryClient, threadId, turns]);

  useEffect(() => {
    if (!renameOpen) {
      setRenameError(null);
      return;
    }
    setRenameValue(threadTitle ?? '');
  }, [renameOpen, threadTitle]);

  const handleRenameSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = renameValue.trim();
    if (!trimmed) {
      setRenameError('Title is required.');
      return;
    }
    renameThreadMutation.mutate({ title: trimmed });
  };

  const threadLabel = threadTitle?.trim() || `Thread ${threadId.slice(0, 8)}`;

  return (
    <section className="flex h-[calc(100vh-8rem)] flex-col gap-3 py-2 text-[color:var(--text-secondary)] transition-colors">
      <header className="flex flex-wrap items-center justify-between gap-4 rounded-2xl border border-[color:var(--panel-border)]/60 bg-[color:var(--panel-bg)]/60 px-3 py-3">
        <div className="flex flex-wrap items-center gap-3">
          <Badge
            variant="secondary"
            className="border border-[color:var(--panel-border)] bg-[color:var(--chip-bg)] text-[color:var(--text-secondary)]"
          >
            Thread
          </Badge>
          <span className="text-xs font-semibold text-[color:var(--text-primary)]">{threadLabel}</span>
          <span className="truncate text-[10px] uppercase tracking-[0.2em] text-[color:var(--text-muted)]" title={threadId}>
            {threadId}
          </span>
          <span className="text-xs text-[color:var(--text-muted)]">Agent: {agentStatusLabel}</span>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <Select
            value={selectedAgentId}
            onChange={(event) => setSelectedAgentId(event.target.value)}
            disabled={isLoadingAgents || agentOptions.length === 0}
            aria-label="Select active agent"
            className="h-9 min-w-[200px]"
          >
            {isLoadingAgents ? (
              <option value="" disabled>
                Loading agents...
              </option>
            ) : agentOptions.length === 0 ? (
              <option value="" disabled>
                No agents available
              </option>
            ) : (
              <option value="" disabled>
                Select an agent
              </option>
            )}
            {agentOptions.map((agent) => (
              <option key={agent.id} value={agent.id}>
                {agent.name}
              </option>
            ))}
          </Select>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => router.push(`${agentsBasePath}/definitions`)}
            disabled={agentDefinitionsQuery.isLoading}
          >
            Manage agents
          </Button>
          <Dialog open={renameOpen} onOpenChange={setRenameOpen}>
            <DialogTrigger asChild>
              <Button type="button" variant="ghost" size="sm" className="gap-2">
                <Pencil className="h-4 w-4" aria-hidden="true" />
                Rename
              </Button>
            </DialogTrigger>
            <DialogContent>
              <form onSubmit={handleRenameSubmit} className="space-y-4">
                <DialogHeader>
                  <DialogTitle>Rename thread</DialogTitle>
                </DialogHeader>
                <div className="space-y-2">
                  <Input
                    value={renameValue}
                    onChange={(event) => setRenameValue(event.target.value)}
                    placeholder="e.g. Q4 pipeline review"
                    autoFocus
                  />
                  {renameError ? <p className="text-xs text-rose-500">{renameError}</p> : null}
                </div>
                <DialogFooter className="gap-2 sm:gap-3">
                  <DialogClose asChild>
                    <Button type="button" variant="ghost">
                      Cancel
                    </Button>
                  </DialogClose>
                  <Button type="submit" disabled={renameThreadMutation.isPending}>
                    {renameThreadMutation.isPending ? 'Saving...' : 'Save'}
                  </Button>
                </DialogFooter>
              </form>
            </DialogContent>
          </Dialog>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={resetConversation}
            disabled={turns.length === 0 && !isSending}
          >
            <History className="h-4 w-4" aria-hidden="true" />
            Clear
          </Button>
        </div>
      </header>

      <div className="flex min-h-0 flex-1 flex-col gap-6">
        <div className="flex min-h-0 flex-1 flex-col">
          <div
            className="flex flex-col gap-3 border-b border-[color:var(--panel-border)]/60 px-3 py-3 md:flex-row md:items-center md:justify-between"
            aria-live="polite"
          >
            <div>
              <h2 className="text-sm font-semibold text-[color:var(--text-primary)]">Thread timeline</h2>
              <p className="text-xs text-[color:var(--text-muted)]">
                Messages, summaries, and artifacts generated for this thread.
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-3 text-xs text-[color:var(--text-muted)]">
              <div className="flex items-center gap-2">
                <span
                  className={`inline-flex h-2 w-2 rounded-full ${isSending ? 'bg-amber-400' : 'bg-emerald-400'}`}
                  aria-hidden="true"
                />
                {isSending ? 'Generating response...' : 'Standing by'}
              </div>
              <span>{lastUpdated ? `Updated ${formatRelativeDate(lastUpdated)}` : 'Awaiting first prompt'}</span>
            </div>
          </div>

          <div className="flex min-h-0 flex-1 flex-col">
            <div className="flex-1 space-y-4 overflow-y-auto px-3 py-5" aria-live="polite" aria-label="Thread transcript">
              {turns.length === 0 ? (
                <div className="flex h-full flex-col items-center justify-center gap-4 text-center text-[color:var(--text-muted)]">
                  <Sparkles className="h-10 w-10 text-[color:var(--accent)]" aria-hidden="true" />
                  <div className="space-y-1">
                    <p className="text-base font-semibold text-[color:var(--text-primary)]">
                      {isLoadingAgents ? 'Loading agents' : hasAgents ? 'Start the thread' : 'Create your first agent'}
                    </p>
                    <p className="text-sm text-[color:var(--text-muted)]">
                      {isLoadingAgents
                        ? 'Fetching your agent roster for this workspace.'
                        : hasAgents
                          ? 'Pick an agent and send a prompt to generate responses, visuals, and summaries.'
                          : 'Agents you create will appear here for new conversations.'}
                    </p>
                  </div>
                  {!hasAgents && !isLoadingAgents ? (
                    <Button type="button" size="sm" onClick={() => router.push(`${agentsBasePath}/definitions`)}>
                      Create an agent
                    </Button>
                  ) : null}
                </div>
              ) : (
                <ol className="space-y-8 text-sm">
                  {turns.map((turn) => {
                    const liveEvents = turn.events ?? [];
                    const internalEvents = turn.internalEvents ?? [];
                    const thinkingPayload = asRecord(turn.thinkingBreakdown);
                    const sqlAudit = Array.isArray(thinkingPayload?.sql_audit)
                      ? (thinkingPayload.sql_audit as Array<Record<string, unknown>>)
                      : [];
                    const recentLiveEvents = liveEvents.slice(-4);
                    const recentInternalEvents = internalEvents.slice(-8);
                    const recentSqlAudit = sqlAudit.slice(-4);

                    return (
                      <li key={turn.id} className="space-y-3">
                        <div className="flex justify-end">
                          <div className="max-w-4xl rounded-2xl bg-[color:var(--accent)]/95 px-5 py-3 text-sm text-white">
                            <p className="whitespace-pre-wrap break-words">{turn.prompt}</p>
                            <p className="mt-2 text-[10px] uppercase tracking-wider text-white/80">
                              {formatRelativeDate(turn.createdAt)}
                            </p>
                          </div>
                        </div>

                        <div className="flex justify-start">
                          <div className="max-w-4xl rounded-2xl border border-[color:var(--panel-border)]/60 bg-[color:var(--panel-bg)]/50 px-4 py-3">
                            <div className="flex items-start justify-between gap-3 text-xs text-[color:var(--text-muted)]">
                              <span>
                                Agent:{' '}
                                {turn.agentLabel ??
                                  (turn.agentId ? agentLabelById.get(turn.agentId) : undefined) ??
                                  'Unknown agent'}
                              </span>
                              <div className="flex items-center gap-2">
                                {turn.status === 'ready' &&
                                (turn.hasInternalEvents || internalEvents.length > 0 || Boolean(turn.thinkingBreakdown)) ? (
                                  <Button
                                    type="button"
                                    variant="ghost"
                                    size="sm"
                                    className="h-7 px-2 text-[10px]"
                                    onClick={() => {
                                      void toggleThinkingBreakdown(turn.id);
                                    }}
                                  >
                                    {turn.showThinking ? (
                                      <>
                                        <ChevronUp className="mr-1 h-3.5 w-3.5" aria-hidden="true" />
                                        Hide audit
                                      </>
                                    ) : (
                                      <>
                                        <ChevronDown className="mr-1 h-3.5 w-3.5" aria-hidden="true" />
                                        Show audit
                                      </>
                                    )}
                                  </Button>
                                ) : null}
                                <span className="uppercase tracking-[0.18em]">
                                  {turn.status === 'pending'
                                    ? (turn.jobStatus ?? 'processing')
                                    : turn.status === 'error'
                                      ? 'error'
                                      : 'complete'}
                                </span>
                              </div>
                            </div>

                            {turn.status === 'pending' ? (
                              <div className="mt-3 space-y-3">
                                <div className="flex items-center gap-2 text-xs text-[color:var(--text-muted)]">
                                  <Spinner className="h-3.5 w-3.5 text-[color:var(--accent)]" />
                                  {liveEvents.length > 0
                                    ? liveEvents[liveEvents.length - 1]?.message ?? 'Generating response...'
                                    : 'Generating response...'}
                                </div>
                                {recentLiveEvents.length > 0 ? (
                                  <ol className="space-y-1 rounded-xl bg-[color:var(--panel-alt)]/55 px-3 py-2 text-xs">
                                    {recentLiveEvents.map((event) => (
                                      <li key={event.id}>
                                        <p className="text-[color:var(--text-secondary)]">
                                          {event.message}
                                          {event.createdAt ? (
                                            <span className="ml-2 text-[10px] text-[color:var(--text-muted)]">
                                              {toClockTime(event.createdAt)}
                                            </span>
                                          ) : null}
                                        </p>
                                      </li>
                                    ))}
                                  </ol>
                                ) : null}
                                <Skeleton className="h-4 w-48" />
                                <Skeleton className="h-4 w-64" />
                                <Skeleton className="h-24 w-full" />
                              </div>
                            ) : turn.status === 'error' ? (
                              <div className="mt-3 space-y-2">
                                <p className="text-sm font-semibold text-red-500">We couldn&apos;t complete that request.</p>
                                <p className="text-xs text-[color:var(--text-muted)]">
                                  {turn.errorMessage ?? 'An unexpected error occurred.'}
                                </p>
                              </div>
                            ) : (
                              <div className="mt-3 space-y-4 text-[color:var(--text-secondary)]">
                                <p className="rounded-xl bg-[color:var(--panel-alt)]/45 px-3 py-2 text-sm leading-relaxed text-[color:var(--text-primary)]">
                                  {turn.summary ?? 'No summary was returned.'}
                                </p>
                                {turn.result ? (
                                  <div className="space-y-3">
                                    {hasRenderableVisualization(turn.visualization) ? (
                                      <VisualizationPreview
                                        result={turn.result ?? undefined}
                                        visualization={turn.visualization ?? undefined}
                                      />
                                    ) : null}
                                    <ResultTable result={turn.result} />
                                  </div>
                                ) : (
                                  <p className="text-xs text-[color:var(--text-muted)]">
                                    No tabular output was produced for this question.
                                  </p>
                                )}

                                {turn.showThinking ? (
                                  <div className="space-y-3 rounded-xl border border-[color:var(--panel-border)]/60 bg-[color:var(--panel-bg)]/45 px-3 py-3">
                                    <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-[color:var(--text-muted)]">
                                      Execution audit
                                    </p>
                                    {turn.loadingThinking ? (
                                      <div className="flex items-center gap-2 text-xs text-[color:var(--text-muted)]">
                                        <Spinner className="h-3.5 w-3.5 text-[color:var(--accent)]" />
                                        Loading internal details...
                                      </div>
                                    ) : (
                                      <div className="space-y-3">
                                        {recentSqlAudit.length > 0 ? (
                                          <div className="space-y-2">
                                            <p className="text-xs font-semibold text-[color:var(--text-primary)]">SQL</p>
                                            {recentSqlAudit.map((entry, index) => {
                                              const statement = typeof entry.sql === 'string' ? entry.sql : '';
                                              const source = typeof entry.source === 'string' ? entry.source : 'sql';
                                              const kind = typeof entry.kind === 'string' ? entry.kind : 'statement';
                                              if (!statement) {
                                                return null;
                                              }
                                              return (
                                                <div key={`${source}-${kind}-${index}`} className="space-y-1 rounded-lg bg-[color:var(--panel-alt)]/40 px-2 py-1.5">
                                                  <p className="text-[10px] text-[color:var(--text-muted)]">{source} • {kind}</p>
                                                  <pre className="overflow-x-auto whitespace-pre-wrap rounded-xl bg-[color:var(--panel-alt)] px-3 py-2 text-[11px] leading-relaxed text-[color:var(--text-primary)]">
                                                    {statement}
                                                  </pre>
                                                </div>
                                              );
                                            })}
                                          </div>
                                        ) : null}

                                        {recentInternalEvents.length > 0 ? (
                                          <ol className="space-y-2 rounded-lg bg-[color:var(--panel-alt)]/35 px-3 py-2">
                                            {recentInternalEvents.map((event) => {
                                              const sqlSnippets = extractSqlStatements(event.details ?? {});
                                              return (
                                                <li key={event.id} className="space-y-1">
                                                  <p className="text-[10px] text-[color:var(--text-muted)]">
                                                    {cleanEventType(event.eventType)}
                                                    {event.source ? ` • ${event.source}` : ''}
                                                    {event.createdAt ? ` • ${toClockTime(event.createdAt)}` : ''}
                                                  </p>
                                                  <p className="text-xs text-[color:var(--text-secondary)]">{event.message}</p>
                                                  {sqlSnippets.slice(0, 1).map((sql, sqlIndex) => (
                                                    <pre
                                                      key={`${event.id}-sql-${sqlIndex}`}
                                                      className="overflow-x-auto whitespace-pre-wrap rounded-xl bg-[color:var(--panel-alt)] px-3 py-2 text-[11px] leading-relaxed text-[color:var(--text-primary)]"
                                                    >
                                                      {sql}
                                                    </pre>
                                                  ))}
                                                </li>
                                              );
                                            })}
                                          </ol>
                                        ) : (
                                          <p className="text-xs text-[color:var(--text-muted)]">
                                            No internal events were captured for this run.
                                          </p>
                                        )}

                                        {thinkingPayload ? (
                                          <details className="group rounded-lg border border-[color:var(--panel-border)]/60 px-2 py-2">
                                            <summary className="cursor-pointer list-none text-xs text-[color:var(--text-muted)] transition hover:text-[color:var(--text-primary)]">
                                              Raw diagnostics
                                            </summary>
                                            <pre className="mt-2 overflow-x-auto whitespace-pre-wrap rounded-xl bg-[color:var(--panel-alt)] px-3 py-2 text-[10px] leading-relaxed text-[color:var(--text-secondary)]">
                                              {JSON.stringify(thinkingPayload, null, 2)}
                                            </pre>
                                          </details>
                                        ) : null}
                                      </div>
                                    )}
                                  </div>
                                ) : null}
                              </div>
                            )}
                          </div>
                        </div>
                      </li>
                    );
                  })}
                </ol>
              )}
              <div ref={messagesEndRef} />
            </div>

            <form onSubmit={handleSubmit} className="mt-2 border-t border-[color:var(--panel-border)]/60 bg-[color:var(--panel-bg)]/70 px-3 py-3 backdrop-blur">
              <div className="bg-transparent">
                <Textarea
                  value={composer}
                  onChange={(event) => setComposer(event.target.value)}
                  onKeyDown={handleComposerKeyDown}
                  placeholder="Shift + Enter for a new line. Describe what you need..."
                  rows={2}
                  className="min-h-[88px] resize-none rounded-2xl border border-[color:var(--panel-border)] bg-[color:var(--panel-bg)]/60 px-4 py-3 text-base text-[color:var(--text-primary)] focus-visible:ring-0"
                  aria-label="Message LangBridge assistant"
                />
                <div className="flex items-center justify-end gap-3 px-1 py-3">
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="text-[color:var(--text-secondary)] hover:text-[color:var(--text-primary)]"
                      onClick={handleRegenerate}
                      disabled={turns.length === 0}
                    >
                      <RefreshCw className="mr-2 h-4 w-4" aria-hidden="true" />
                      Regenerate
                    </Button>
                    <Button
                      type="submit"
                      size="sm"
                      className="gap-2"
                      disabled={isSending || !composer.trim() || !selectedAgentId}
                      isLoading={isSending}
                      loadingText="Sending..."
                    >
                      <Send className="h-4 w-4" aria-hidden="true" />
                      Send
                    </Button>
                </div>
              </div>
            </form>
          </div>
        </div>
      </div>
    </section>
  );
}
