
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from langbridge.runtime.models import (
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    LLMConnectionSecret,
    LineageEdge,
    RuntimeAgentDefinition,
    RuntimeConversationMemoryCategory,
    RuntimeConversationMemoryItem,
    RuntimeJobArtifact,
    RuntimeJobEvent,
    RuntimeJobTask,
    RuntimeThread,
    RuntimeThreadMessage,
    SemanticModelMetadata,
)
from langbridge.runtime.models.metadata import DatasetMaterializationMode, DatasetType
from langbridge.runtime.security import SecretProviderRegistry


class _InMemoryDatasetRepository:
    def __init__(self, datasets: dict[uuid.UUID, DatasetMetadata]) -> None:
        self._datasets = dict(datasets)

    def add(self, instance: DatasetMetadata) -> DatasetMetadata:
        self._datasets[instance.id] = instance
        return instance

    async def save(self, instance: DatasetMetadata) -> DatasetMetadata:
        self._datasets[instance.id] = instance
        return instance

    async def get_by_id(self, id_: object) -> DatasetMetadata | None:
        return self._datasets.get(id_)

    async def get_for_workspace(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> DatasetMetadata | None:
        dataset = self._datasets.get(dataset_id)
        if dataset is None or dataset.workspace_id != workspace_id:
            return None
        return dataset

    async def get_for_workspace_by_sql_alias(
        self,
        *,
        workspace_id: uuid.UUID,
        sql_alias: str,
    ) -> DatasetMetadata | None:
        normalized_alias = str(sql_alias or "").strip().lower()
        if not normalized_alias:
            return None
        for dataset in self._datasets.values():
            if dataset.workspace_id == workspace_id and str(dataset.sql_alias or "").strip().lower() == normalized_alias:
                return dataset
        return None

    async def get_by_ids(self, dataset_ids) -> list[DatasetMetadata]:
        return [self._datasets[dataset_id] for dataset_id in dataset_ids if dataset_id in self._datasets]

    async def get_by_ids_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids,
    ) -> list[DatasetMetadata]:
        items: list[DatasetMetadata] = []
        for dataset_id in dataset_ids:
            dataset = self._datasets.get(dataset_id)
            if dataset is not None and dataset.workspace_id == workspace_id:
                items.append(dataset)
        return items

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        search: str | None = None,
        tags=None,
        dataset_types=None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[DatasetMetadata]:
        normalized_search = str(search or "").strip().lower()
        normalized_tags = {
            str(tag).strip().lower()
            for tag in (tags or [])
            if str(tag).strip()
        }
        normalized_types = {
            str(getattr(dataset_type, "value", dataset_type)).strip().upper()
            for dataset_type in (dataset_types or [])
            if str(dataset_type).strip()
        }

        items: list[DatasetMetadata] = []
        for dataset in self._datasets.values():
            if dataset.workspace_id != workspace_id:
                continue
            if normalized_search:
                haystacks = [
                    str(dataset.name or "").lower(),
                    str(dataset.description or "").lower(),
                ]
                if not any(normalized_search in haystack for haystack in haystacks):
                    continue
            if normalized_tags:
                dataset_tags = {str(tag).strip().lower() for tag in (dataset.tags_json or []) if str(tag).strip()}
                if not normalized_tags.issubset(dataset_tags):
                    continue
            if normalized_types and dataset.dataset_type_value not in normalized_types:
                continue
            items.append(dataset)

        items.sort(
            key=lambda dataset: dataset.updated_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return items[start:end]

    async def find_file_dataset_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        table_name: str,
    ) -> DatasetMetadata | None:
        normalized_table = str(table_name or "").strip().lower()
        for dataset in self._datasets.values():
            if dataset.workspace_id != workspace_id:
                continue
            if dataset.connection_id != connection_id:
                continue
            if dataset.dataset_type != DatasetType.FILE:
                continue
            mode = dataset.materialization_mode
            if mode != DatasetMaterializationMode.SYNCED:
                continue
            if str(dataset.table_name or "").strip().lower() == normalized_table:
                return dataset
        return None

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        dataset_types=None,
        limit: int = 500,
    ) -> list[DatasetMetadata]:
        normalized_types = {
            str(getattr(dataset_type, "value", dataset_type)).strip().upper()
            for dataset_type in (dataset_types or [])
            if str(dataset_type).strip()
        }
        items = [
            dataset
            for dataset in self._datasets.values()
            if dataset.workspace_id == workspace_id
            and dataset.connection_id == connection_id
            and (
                not normalized_types
                or dataset.dataset_type_value in normalized_types
            )
        ]
        items.sort(
            key=lambda dataset: dataset.updated_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return items[: max(1, int(limit))]

    async def flush(self) -> None:
        return None


class _InMemoryDatasetColumnRepository:
    def __init__(self, columns_by_dataset: dict[uuid.UUID, list[DatasetColumnMetadata]]) -> None:
        self._columns_by_dataset = {
            dataset_id: list(columns)
            for dataset_id, columns in columns_by_dataset.items()
        }

    async def list_for_dataset(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnMetadata]:
        return list(self._columns_by_dataset.get(dataset_id, []))

    async def delete_for_dataset(self, *, dataset_id: uuid.UUID) -> None:
        self._columns_by_dataset[dataset_id] = []

    def add(self, instance: DatasetColumnMetadata) -> DatasetColumnMetadata:
        self._columns_by_dataset.setdefault(instance.dataset_id, []).append(instance)
        self._columns_by_dataset[instance.dataset_id].sort(
            key=lambda column: int(column.ordinal_position or 0)
        )
        return instance

    async def flush(self) -> None:
        return None


class _InMemoryDatasetPolicyRepository:
    def __init__(self, policies: dict[uuid.UUID, DatasetPolicyMetadata] | None = None) -> None:
        self._policies_by_dataset = dict(policies or {})

    async def get_for_dataset(self, *, dataset_id: uuid.UUID) -> DatasetPolicyMetadata | None:
        return self._policies_by_dataset.get(dataset_id)

    def add(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        self._policies_by_dataset[instance.dataset_id] = instance
        return instance

    async def save(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        self._policies_by_dataset[instance.dataset_id] = instance
        return instance

    async def flush(self) -> None:
        return None


class _InMemoryConnectorSyncStateRepository:
    def __init__(
        self,
        states: dict[tuple[uuid.UUID, uuid.UUID, str], ConnectorSyncState] | None = None,
    ) -> None:
        self._states = dict(states or {})

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
    ) -> list[ConnectorSyncState]:
        items = [
            state
            for (state_workspace_id, state_connection_id, _), state in self._states.items()
            if state_workspace_id == workspace_id and state_connection_id == connection_id
        ]
        items.sort(
            key=lambda state: (
                str(state.source_key or "").lower(),
                state.updated_at or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=False,
        )
        return items

    async def get_for_resource(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        resource_name: str,
    ) -> ConnectorSyncState | None:
        return self._states.get((workspace_id, connection_id, str(resource_name or "").strip()))

    def add(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        key = (
            instance.workspace_id,
            instance.connection_id,
            str(instance.source_key or "").strip(),
        )
        self._states[key] = instance
        return instance

    async def save(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        return self.add(instance)

    async def flush(self) -> None:
        return None


class _InMemoryDatasetRevisionRepository:
    def __init__(self) -> None:
        self._revisions_by_dataset: dict[uuid.UUID, list[DatasetRevision]] = {}

    def add(self, instance: DatasetRevision) -> DatasetRevision:
        self._revisions_by_dataset.setdefault(instance.dataset_id, []).append(instance)
        self._revisions_by_dataset[instance.dataset_id].sort(
            key=lambda revision: int(revision.revision_number)
        )
        return instance

    async def next_revision_number(self, *, dataset_id: uuid.UUID) -> int:
        revisions = self._revisions_by_dataset.get(dataset_id, [])
        if not revisions:
            return 1
        return max(int(revision.revision_number) for revision in revisions) + 1

    async def flush(self) -> None:
        return None


class _InMemoryLineageEdgeRepository:
    def __init__(self) -> None:
        self._edges: list[LineageEdge] = []

    def add(self, instance: LineageEdge) -> LineageEdge:
        self._edges.append(instance)
        return instance

    async def delete_for_target(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> None:
        self._edges = [
            edge
            for edge in self._edges
            if not (
                edge.workspace_id == workspace_id
                and edge.target_type == target_type
                and edge.target_id == target_id
            )
        ]

    async def flush(self) -> None:
        return None


class _InMemoryAgentRepository:
    def __init__(self, agents: dict[uuid.UUID, RuntimeAgentDefinition]) -> None:
        self._agents = dict(agents)

    async def get_by_id(self, id_: object) -> RuntimeAgentDefinition | None:
        return self._agents.get(id_)


class _InMemoryLLMConnectionRepository:
    def __init__(
        self,
        connections: dict[uuid.UUID, Any],
        *,
        registry: SecretProviderRegistry,
    ) -> None:
        self._connections = dict(connections)
        self._registry = registry

    @staticmethod
    def _connection_id(value: Any) -> uuid.UUID:
        if hasattr(value, "connection"):
            return getattr(value.connection, "id")
        return getattr(value, "id")

    def add(self, instance: Any) -> Any:
        self._connections[self._connection_id(instance)] = instance
        return instance

    async def save(self, instance: Any) -> Any:
        self._connections[self._connection_id(instance)] = instance
        return instance

    async def get_by_id(self, id_: object) -> LLMConnectionSecret | None:
        record = self._connections.get(id_)
        return self._to_secret(record)

    async def get_by_name_for_workspace(
        self,
        *,
        name: str,
        workspace_id: uuid.UUID,
    ) -> LLMConnectionSecret | None:
        normalized_name = str(name or "").strip().lower()
        for record in self._connections.values():
            secret = self._to_secret(record)
            if (
                secret is not None
                and secret.workspace_id == workspace_id
                and secret.name.lower() == normalized_name
            ):
                return secret
        return None

    async def list_llm_connections(
        self,
        *,
        workspace_id: uuid.UUID | None = None,
    ) -> list[LLMConnectionSecret]:
        items: list[LLMConnectionSecret] = []
        for record in self._connections.values():
            secret = self._to_secret(record)
            if secret is None:
                continue
            if workspace_id is not None and secret.workspace_id != workspace_id:
                continue
            items.append(secret)
        return items

    async def delete(self, instance: Any) -> None:
        self._connections.pop(self._connection_id(instance), None)

    def _to_secret(self, record: Any) -> LLMConnectionSecret | None:
        if record is None:
            return None
        if isinstance(record, LLMConnectionSecret):
            return record
        if hasattr(record, "connection"):
            if record.api_key_secret is None:
                return record.connection
            return LLMConnectionSecret(
                id=record.connection.id,
                name=record.connection.name,
                description=record.connection.description,
                provider=record.connection.provider,
                api_key=self._registry.resolve(record.api_key_secret),
                model=record.connection.model,
                configuration=dict(record.connection.configuration or {}),
                is_active=record.connection.is_active,
                default=record.connection.default,
                workspace_id=record.connection.workspace_id,
                created_at=record.connection.created_at,
                updated_at=record.connection.updated_at,
            )
        return LLMConnectionSecret(
            id=getattr(record, "id"),
            name=str(getattr(record, "name")),
            description=getattr(record, "description", None),
            provider=str(getattr(record, "provider")),
            api_key=str(getattr(record, "api_key", "") or ""),
            model=str(getattr(record, "model")),
            configuration=dict(getattr(record, "configuration", None) or {}),
            is_active=bool(getattr(record, "is_active", True)),
            default=bool(getattr(record, "default", False)),
            workspace_id=getattr(record, "workspace_id"),
            created_at=getattr(record, "created_at", None),
            updated_at=getattr(record, "updated_at", None),
        )


class _InMemoryThreadRepository:
    def __init__(self) -> None:
        self._threads: dict[uuid.UUID, RuntimeThread] = {}

    def add(self, instance: RuntimeThread) -> RuntimeThread:
        self._threads[instance.id] = instance
        return instance

    async def save(self, instance: RuntimeThread) -> RuntimeThread:
        self._threads[instance.id] = instance
        return instance

    async def get_by_id(self, id_: object) -> RuntimeThread | None:
        return self._threads.get(id_)

    async def delete(self, id_: object) -> None:
        self._threads.pop(id_, None)

    async def list_for_actor(self, actor_id: uuid.UUID | None = None) -> list[RuntimeThread]:
        if actor_id is None:
            return []
        items = [
            thread
            for thread in self._threads.values()
            if thread.created_by == actor_id
        ]
        items.sort(key=lambda thread: thread.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items

    async def list_for_workspace(self, workspace_id: uuid.UUID) -> list[RuntimeThread]:
        items = [
            thread
            for thread in self._threads.values()
            if thread.workspace_id == workspace_id
        ]
        items.sort(key=lambda thread: thread.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items


class _InMemoryThreadMessageRepository:
    def __init__(self) -> None:
        self.items: list[RuntimeThreadMessage] = []

    def add(self, instance: RuntimeThreadMessage) -> RuntimeThreadMessage:
        self.items.append(instance)
        return instance

    async def list_for_thread(self, thread_id) -> list[RuntimeThreadMessage]:
        return [item for item in self.items if item.thread_id == thread_id]

    async def delete_for_thread(self, thread_id) -> None:
        self.items = [item for item in self.items if item.thread_id != thread_id]


class _InMemoryConversationMemoryRepository:
    def __init__(self) -> None:
        self._items: list[RuntimeConversationMemoryItem] = []

    async def list_for_thread(
        self,
        thread_id: uuid.UUID,
        *,
        limit: int = 200,
    ) -> list[RuntimeConversationMemoryItem]:
        items = [item for item in self._items if item.thread_id == thread_id]
        items.sort(key=lambda item: item.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items[: max(1, int(limit))]

    def create_item(
        self,
        *,
        thread_id: uuid.UUID,
        category: str,
        content: str,
        metadata_json: dict[str, Any] | None = None,
        actor_id: uuid.UUID | None = None,
    ) -> RuntimeConversationMemoryItem | None:
        clean_content = str(content or "").strip()
        if not clean_content:
            return None

        try:
            category_enum = RuntimeConversationMemoryCategory(str(category))
        except ValueError:
            category_enum = RuntimeConversationMemoryCategory.fact

        timestamp = datetime.now(timezone.utc)
        item = RuntimeConversationMemoryItem(
            id=uuid.uuid4(),
            thread_id=thread_id,
            actor_id=actor_id,
            category=category_enum,
            content=clean_content,
            metadata=dict(metadata_json or {}),
            created_at=timestamp,
            updated_at=timestamp,
            last_accessed_at=None,
        )
        self._items.append(item)
        return item

    async def touch_items(self, item_ids) -> None:
        timestamp = datetime.now(timezone.utc)
        target_ids = {item_id for item_id in item_ids if isinstance(item_id, uuid.UUID)}
        for item in self._items:
            if item.id in target_ids:
                item.last_accessed_at = timestamp

    async def flush(self) -> None:
        return None


class _InMemorySemanticModelStore:
    def __init__(self, models: dict[uuid.UUID, SemanticModelMetadata]) -> None:
        self._models = dict(models)

    async def get_by_id(self, model_id: uuid.UUID) -> SemanticModelMetadata | None:
        return self._models.get(model_id)

    async def get_by_ids(self, model_ids: list[uuid.UUID]) -> list[SemanticModelMetadata]:
        return [self._models[model_id] for model_id in model_ids if model_id in self._models]

    def upsert(self, model: SemanticModelMetadata) -> None:
        self._models[model.id] = model


class _InMemoryJobRepository:
    def __init__(self) -> None:
        self._jobs: dict[uuid.UUID, Any] = {}
        self._tasks_by_job: dict[uuid.UUID, dict[str, RuntimeJobTask]] = {}
        self._events_by_job: dict[uuid.UUID, list[RuntimeJobEvent]] = {}
        self._artifacts_by_job: dict[uuid.UUID, dict[str, RuntimeJobArtifact]] = {}

    def create_job(
        self,
        *,
        job_id: uuid.UUID,
        workspace_id: uuid.UUID,
        job_type: str,
        actor_id: uuid.UUID | None,
        subject_type: str | None,
        subject_id: uuid.UUID | None,
        queue_name: str,
        priority: str,
        required_capabilities: list[str],
        runtime_pool_id: str | None,
        affinity_key: str | None,
        concurrency_key: str | None,
        idempotency_key: str | None,
        max_attempts: int,
        scheduled_at: datetime | None,
        payload: dict[str, Any],
    ) -> Any:
        from langbridge.runtime.models import RuntimeJob

        now = datetime.now(timezone.utc)
        job = RuntimeJob(
            id=job_id,
            workspace_id=workspace_id,
            job_type=job_type,
            actor_id=actor_id,
            subject_type=subject_type,
            subject_id=subject_id,
            queue_name=queue_name,
            priority=priority,
            required_capabilities=list(required_capabilities),
            runtime_pool_id=runtime_pool_id,
            affinity_key=affinity_key,
            concurrency_key=concurrency_key,
            idempotency_key=idempotency_key,
            max_attempts=max_attempts,
            scheduled_at=scheduled_at,
            payload=dict(payload),
            queued_at=now,
            created_at=now,
            updated_at=now,
        )
        self._jobs[job.id] = job
        return job

    def add(self, instance: Any) -> Any:
        self._jobs[instance.id] = instance
        return instance

    async def save(self, instance: Any) -> Any:
        self._jobs[instance.id] = instance
        return instance

    async def save_job(self, job: Any) -> Any:
        return await self.save(job)

    async def get_by_id(self, id_: object) -> Any | None:
        job = self._jobs.get(id_)
        if job is None:
            return None
        return self._hydrate_job(job)

    async def get_by_id_for_workspace(self, *, job_id: uuid.UUID, workspace_id: uuid.UUID) -> Any | None:
        job = self._jobs.get(job_id)
        if job is None or job.workspace_id != workspace_id:
            return None
        return self._hydrate_job(job)

    async def get_by_idempotency_key(self, *, workspace_id: uuid.UUID, idempotency_key: str) -> Any | None:
        normalized = str(idempotency_key or "").strip()
        if not normalized:
            return None
        for job in self._jobs.values():
            if job.workspace_id == workspace_id and getattr(job, "idempotency_key", None) == normalized:
                return self._hydrate_job(job)
        return None

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        job_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[Any]:
        items = [
            self._hydrate_job(job)
            for job in self._jobs.values()
            if job.workspace_id == workspace_id
            and (not job_type or job.job_type == job_type)
            and (not status or self._status_value(getattr(job, "status", "")) == status)
        ]
        items.sort(key=lambda job: job.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items[: max(1, int(limit))]

    async def claim_next(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        job_types: set[str] | None = None,
        queue_name: str | None = None,
    ) -> Any | None:
        now = datetime.now(timezone.utc)
        candidates = []
        for job in self._jobs.values():
            status = self._status_value(getattr(job, "status", ""))
            locked_until = getattr(job, "locked_until", None)
            is_runnable = status == "queued" or (
                status == "running"
                and locked_until is not None
                and locked_until < now
            )
            if not is_runnable:
                continue
            scheduled_at = getattr(job, "scheduled_at", None)
            if scheduled_at is not None and scheduled_at > now:
                continue
            if job_types and job.job_type not in job_types:
                continue
            if queue_name and getattr(job, "queue_name", None) != queue_name:
                continue
            candidates.append(job)
        if not candidates:
            return None
        candidates.sort(
            key=lambda job: (
                self._priority_rank(getattr(job, "priority", "normal")),
                job.created_at or datetime.min.replace(tzinfo=timezone.utc),
            )
        )
        job = candidates[0]
        job.status = "running"
        job.lock_owner = worker_id
        job.locked_until = now + self._lease_delta(lease_seconds)
        job.heartbeat_at = now
        job.attempt = int(getattr(job, "attempt", 0) or 0) + 1
        job.started_at = getattr(job, "started_at", None) or now
        job.updated_at = now
        return self._hydrate_job(job)

    async def heartbeat_job(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        job = self._jobs.get(job_id)
        if job is None:
            return False
        if self._status_value(getattr(job, "status", "")) != "running":
            return False
        if getattr(job, "lock_owner", None) != worker_id:
            return False
        now = datetime.now(timezone.utc)
        job.locked_until = now + self._lease_delta(lease_seconds)
        job.heartbeat_at = now
        job.updated_at = now
        return True

    async def append_event(
        self,
        *,
        job_id: uuid.UUID,
        task_id: uuid.UUID | None,
        event_type: str,
        status: str,
        stage: str,
        message: str,
        visibility: str,
        terminal: bool,
        source: str | None,
        raw_event_type: str | None,
        details: dict[str, Any] | None,
    ) -> RuntimeJobEvent:
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(str(job_id))
        next_sequence = int(getattr(job, "last_sequence", 0) or 0) + 1
        event = RuntimeJobEvent(
            id=uuid.uuid4(),
            job_id=job_id,
            task_id=task_id,
            sequence=next_sequence,
            event_type=event_type,
            status=status,
            stage=stage,
            message=message,
            visibility=visibility,
            terminal=terminal,
            source=source,
            raw_event_type=raw_event_type,
            details=dict(details or {}),
            created_at=datetime.now(timezone.utc),
        )
        self._events_by_job.setdefault(job_id, []).append(event)
        job.last_sequence = next_sequence
        if terminal:
            job.terminal_sequence = next_sequence
        job.updated_at = datetime.now(timezone.utc)
        return event

    async def list_events_after(self, *, job_id: uuid.UUID, after_sequence: int = 0) -> list[RuntimeJobEvent]:
        cursor = max(0, int(after_sequence or 0))
        return [
            event
            for event in self._events_by_job.get(job_id, [])
            if int(event.sequence or 0) > cursor
        ]

    async def upsert_task(
        self,
        *,
        job_id: uuid.UUID,
        task_key: str,
        task_type: str,
        status: str,
        attempt: int | None = None,
        max_attempts: int | None = None,
        resume_policy: str | None = None,
        reuse_policy: str | None = None,
        input: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        diagnostics: dict[str, Any] | None = None,
        started_sequence: int | None = None,
        last_sequence: int | None = None,
        terminal_sequence: int | None = None,
    ) -> RuntimeJobTask:
        tasks = self._tasks_by_job.setdefault(job_id, {})
        task = tasks.get(task_key)
        now = datetime.now(timezone.utc)
        if task is None:
            task = RuntimeJobTask(
                id=uuid.uuid4(),
                job_id=job_id,
                task_key=task_key,
                task_type=task_type,
                status=status,
                started_at=now if status == "running" else None,
                updated_at=now,
            )
            tasks[task_key] = task
        task.task_type = task_type
        task.status = status
        if attempt is not None:
            task.attempt = attempt
        if max_attempts is not None:
            task.max_attempts = max_attempts
        if resume_policy is not None:
            task.resume_policy = resume_policy
        if reuse_policy is not None:
            task.reuse_policy = reuse_policy
        if input is not None:
            task.input = dict(input)
        if state is not None:
            task.state = dict(state)
        if result is not None:
            task.result = dict(result)
            task.error = None
        if error is not None:
            task.error = dict(error)
        if diagnostics is not None:
            task.diagnostics = dict(diagnostics)
        if started_sequence is not None:
            task.started_sequence = started_sequence
        if last_sequence is not None:
            task.last_sequence = last_sequence
        if terminal_sequence is not None:
            task.terminal_sequence = terminal_sequence
        if status == "running" and task.started_at is None:
            task.started_at = now
        if status == "succeeded":
            task.completed_at = now
            task.failed_at = None
        if status == "failed":
            task.failed_at = now
        task.updated_at = now
        return task

    async def get_task_by_key(self, *, job_id: uuid.UUID, task_key: str) -> RuntimeJobTask | None:
        return self._tasks_by_job.get(job_id, {}).get(task_key)

    async def add_artifact(
        self,
        *,
        job_id: uuid.UUID,
        task_id: uuid.UUID | None,
        artifact_key: str,
        artifact_type: str,
        title: str | None,
        storage_kind: str,
        storage_uri: str | None,
        data: Any | None,
        schema: dict[str, Any] | None,
        formatting: dict[str, Any] | None,
        metadata: dict[str, Any] | None,
    ) -> RuntimeJobArtifact:
        artifacts = self._artifacts_by_job.setdefault(job_id, {})
        now = datetime.now(timezone.utc)
        artifact = artifacts.get(artifact_key)
        if artifact is None:
            artifact = RuntimeJobArtifact(
                id=uuid.uuid4(),
                job_id=job_id,
                task_id=task_id,
                artifact_key=artifact_key,
                artifact_type=artifact_type,
                created_at=now,
                updated_at=now,
            )
            artifacts[artifact_key] = artifact
        artifact.task_id = task_id
        artifact.artifact_type = artifact_type
        artifact.title = title
        artifact.storage_kind = storage_kind
        artifact.storage_uri = storage_uri
        artifact.data = data
        artifact.artifact_schema = dict(schema or {})
        artifact.formatting = dict(formatting or {})
        artifact.metadata = dict(metadata or {})
        artifact.updated_at = now
        return artifact

    async def get_artifact_by_key(self, *, job_id: uuid.UUID, artifact_key: str) -> RuntimeJobArtifact | None:
        return self._artifacts_by_job.get(job_id, {}).get(artifact_key)

    async def flush(self) -> None:
        return None

    def _hydrate_job(self, job: Any) -> Any:
        job.tasks = list(self._tasks_by_job.get(job.id, {}).values())
        job.events = list(self._events_by_job.get(job.id, []))
        job.artifacts = list(self._artifacts_by_job.get(job.id, {}).values())
        return job

    def _lease_delta(self, lease_seconds: int) -> timedelta:
        return timedelta(seconds=max(1, int(lease_seconds)))

    def _priority_rank(self, priority: Any) -> int:
        value = str(getattr(priority, "value", priority) or "normal").strip().lower()
        if value == "high":
            return 0
        if value == "normal":
            return 1
        return 2

    def _status_value(self, status: Any) -> str:
        return str(getattr(status, "value", status) or "").strip().lower()


__all__ = [
    "_InMemoryAgentRepository",
    "_InMemoryConnectorSyncStateRepository",
    "_InMemoryConversationMemoryRepository",
    "_InMemoryDatasetColumnRepository",
    "_InMemoryDatasetPolicyRepository",
    "_InMemoryDatasetRepository",
    "_InMemoryDatasetRevisionRepository",
    "_InMemoryJobRepository",
    "_InMemoryLLMConnectionRepository",
    "_InMemoryLineageEdgeRepository",
    "_InMemorySemanticModelStore",
    "_InMemoryThreadMessageRepository",
    "_InMemoryThreadRepository",
]
