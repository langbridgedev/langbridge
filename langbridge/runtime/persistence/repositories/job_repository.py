import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, case, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from langbridge.runtime.persistence.db.job import (
    JobArtifactRecord,
    JobEventRecord,
    JobRecord,
    JobTaskRecord,
)

from .base import AsyncBaseRepository


class JobRepository(AsyncBaseRepository[JobRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, JobRecord)

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
    ) -> JobRecord:
        now = datetime.now(timezone.utc)
        job = JobRecord(
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
            status="queued",
            queued_at=now,
            created_at=now,
            updated_at=now,
        )
        self._session.add(job)
        return job

    async def get_by_id(self, id_: object) -> JobRecord | None:
        result = await self._session.scalars(
            self._job_query().where(JobRecord.id == id_)
        )
        return result.one_or_none()

    async def get_by_id_for_workspace(
        self,
        *,
        job_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> JobRecord | None:
        result = await self._session.scalars(
            self._job_query().where(
                JobRecord.id == job_id,
                JobRecord.workspace_id == workspace_id,
            )
        )
        return result.one_or_none()

    async def get_by_idempotency_key(
        self,
        *,
        workspace_id: uuid.UUID,
        idempotency_key: str,
    ) -> JobRecord | None:
        result = await self._session.scalars(
            self._job_query().where(
                JobRecord.workspace_id == workspace_id,
                JobRecord.idempotency_key == idempotency_key,
            )
        )
        return result.one_or_none()

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        job_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[JobRecord]:
        query = self._job_query().where(JobRecord.workspace_id == workspace_id)
        if job_type:
            query = query.where(JobRecord.job_type == job_type)
        if status:
            query = query.where(JobRecord.status == status)
        result = await self._session.scalars(
            query.order_by(JobRecord.created_at.desc()).limit(max(1, int(limit)))
        )
        return list(result.all())

    async def claim_next(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        job_types: set[str] | None = None,
        queue_name: str | None = None,
    ) -> JobRecord | None:
        now = datetime.now(timezone.utc)
        conditions = [
            self._runnable_condition(now),
            or_(JobRecord.scheduled_at.is_(None), JobRecord.scheduled_at <= now),
        ]
        if job_types:
            conditions.append(JobRecord.job_type.in_(sorted(job_types)))
        if queue_name:
            conditions.append(JobRecord.queue_name == queue_name)
        query = select(JobRecord.id).where(*conditions)
        candidate_id = query.order_by(
            case(
                (JobRecord.priority == "high", 0),
                (JobRecord.priority == "normal", 1),
                else_=2,
            ),
            JobRecord.created_at.asc(),
        ).limit(1).scalar_subquery()
        result = await self._session.execute(
            update(JobRecord)
            .where(JobRecord.id == candidate_id, *conditions)
            .values(
                status="running",
                lock_owner=worker_id,
                locked_until=now + timedelta(seconds=max(1, int(lease_seconds))),
                heartbeat_at=now,
                attempt=JobRecord.attempt + 1,
                started_at=case(
                    (JobRecord.started_at.is_(None), now),
                    else_=JobRecord.started_at,
                ),
                updated_at=now,
            )
            .returning(JobRecord.id)
        )
        job_id = result.scalar_one_or_none()
        if job_id is None:
            return None
        await self.flush()
        return await self.get_by_id(job_id)

    async def save_job(self, job: JobRecord) -> JobRecord:
        return await self.save(job)

    async def heartbeat_job(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        now = datetime.now(timezone.utc)
        result = await self._session.execute(
            update(JobRecord)
            .where(
                JobRecord.id == job_id,
                JobRecord.status == "running",
                JobRecord.lock_owner == worker_id,
            )
            .values(
                locked_until=now + timedelta(seconds=max(1, int(lease_seconds))),
                heartbeat_at=now,
                updated_at=now,
            )
            .returning(JobRecord.id)
        )
        renewed_job_id = result.scalar_one_or_none()
        if renewed_job_id is None:
            return False
        await self.flush()
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
    ) -> JobEventRecord:
        await self.flush()
        now = datetime.now(timezone.utc)
        next_sequence_expression = func.coalesce(JobRecord.last_sequence, 0) + 1
        values: dict[Any, Any] = {
            JobRecord.last_sequence: next_sequence_expression,
            JobRecord.updated_at: now,
        }
        if terminal:
            values[JobRecord.terminal_sequence] = next_sequence_expression
        result = await self._session.execute(
            update(JobRecord)
            .where(JobRecord.id == job_id)
            .values(values)
            .returning(JobRecord.last_sequence)
        )
        next_sequence = result.scalar_one_or_none()
        if next_sequence is None:
            raise KeyError(str(job_id))
        event = JobEventRecord(
            id=uuid.uuid4(),
            job_id=job_id,
            task_id=task_id,
            sequence=int(next_sequence),
            event_type=event_type,
            status=status,
            stage=stage,
            message=message,
            visibility=visibility,
            terminal=terminal,
            source=source,
            raw_event_type=raw_event_type,
            details=dict(details or {}),
            created_at=now,
        )
        self._session.add(event)
        await self.flush()
        return event

    @staticmethod
    def _runnable_condition(now: datetime) -> Any:
        return or_(
            JobRecord.status == "queued",
            and_(
                JobRecord.status == "running",
                JobRecord.locked_until.is_not(None),
                JobRecord.locked_until < now,
            ),
        )

    async def list_events_after(
        self,
        *,
        job_id: uuid.UUID,
        after_sequence: int = 0,
    ) -> list[JobEventRecord]:
        result = await self._session.scalars(
            select(JobEventRecord)
            .where(
                JobEventRecord.job_id == job_id,
                JobEventRecord.sequence > max(0, int(after_sequence or 0)),
            )
            .order_by(JobEventRecord.sequence.asc())
        )
        return list(result.all())

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
    ) -> JobTaskRecord:
        task = await self.get_task_by_key(job_id=job_id, task_key=task_key)
        now = datetime.now(timezone.utc)
        if task is None:
            task = JobTaskRecord(
                id=uuid.uuid4(),
                job_id=job_id,
                task_key=task_key,
                task_type=task_type,
                status=status,
                started_at=now if status == "running" else None,
                updated_at=now,
            )
            self._session.add(task)

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
        await self.flush()
        return task

    async def get_task_by_key(
        self,
        *,
        job_id: uuid.UUID,
        task_key: str,
    ) -> JobTaskRecord | None:
        result = await self._session.scalars(
            select(JobTaskRecord).where(
                JobTaskRecord.job_id == job_id,
                JobTaskRecord.task_key == task_key,
            )
        )
        return result.one_or_none()

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
    ) -> JobArtifactRecord:
        existing = await self.get_artifact_by_key(job_id=job_id, artifact_key=artifact_key)
        now = datetime.now(timezone.utc)
        if existing is None:
            artifact = JobArtifactRecord(
                id=uuid.uuid4(),
                job_id=job_id,
                task_id=task_id,
                artifact_key=artifact_key,
                artifact_type=artifact_type,
                created_at=now,
                updated_at=now,
            )
            self._session.add(artifact)
        else:
            artifact = existing

        artifact.task_id = task_id
        artifact.artifact_type = artifact_type
        artifact.title = title
        artifact.storage_kind = storage_kind
        artifact.storage_uri = storage_uri
        artifact.data = data
        artifact.schema = dict(schema or {})
        artifact.formatting = dict(formatting or {})
        artifact.metadata_json = dict(metadata or {})
        artifact.updated_at = now
        await self.flush()
        return artifact

    async def get_artifact_by_key(
        self,
        *,
        job_id: uuid.UUID,
        artifact_key: str,
    ) -> JobArtifactRecord | None:
        result = await self._session.scalars(
            select(JobArtifactRecord).where(
                JobArtifactRecord.job_id == job_id,
                JobArtifactRecord.artifact_key == artifact_key,
            )
        )
        return result.one_or_none()

    def _job_query(self):
        return select(JobRecord).options(
            selectinload(JobRecord.job_tasks),
            selectinload(JobRecord.job_events),
            selectinload(JobRecord.job_artifacts),
        )
