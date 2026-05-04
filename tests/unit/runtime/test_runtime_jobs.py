from __future__ import annotations

import uuid
from typing import Any

import pytest

from langbridge.runtime.application.job_handlers import (
    AGENT_RUN_JOB_TYPE,
    AgentRunJobHandler,
    DATASET_SYNC_JOB_TYPE,
    DatasetSyncJobHandler,
    SQL_QUERY_JOB_TYPE,
    SqlQueryJobHandler,
)
from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.models import CreateRuntimeJobRequest, RuntimeJobStatus
from langbridge.runtime.persistence.in_memory import _InMemoryJobRepository
from langbridge.runtime.services.jobs import (
    RuntimeJobHandlerRegistry,
    RuntimeJobProcessor,
    RuntimeJobService,
)
from langbridge.runtime.services.jobs.context import JobExecutionContext


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class EchoJobHandler:
    @property
    def job_type(self) -> str:
        return "test.echo"

    async def handle(self, context: JobExecutionContext) -> dict[str, Any]:
        task = await context.upsert_task(
            task_key="echo",
            task_type="test.echo",
            status=RuntimeJobStatus.running.value,
            input=dict(context.job.payload),
        )
        await context.emit(
            event_type="test.echo.progress",
            status="running",
            stage="echo",
            message="Echo task running.",
            task_id=task.id,
            visibility="public",
            details={"seen": context.job.payload.get("value")},
        )
        await context.upsert_task(
            task_key="echo",
            task_type="test.echo",
            status=RuntimeJobStatus.succeeded.value,
            result={"echo": context.job.payload.get("value")},
        )
        await context.add_artifact(
            artifact_key="echo-result",
            artifact_type="json",
            title="Echo result",
            data={"echo": context.job.payload.get("value")},
            schema={"type": "object"},
            metadata={"source": "unit-test"},
        )
        return {"echo": context.job.payload.get("value")}


class FailingJobHandler:
    @property
    def job_type(self) -> str:
        return "test.fail"

    async def handle(self, context: JobExecutionContext) -> dict[str, Any]:
        _ = context
        raise RuntimeError("handler exploded")


class RecordingDatasetSyncRuntimeHost:
    def __init__(self, *, workspace_id: uuid.UUID, actor_id: uuid.UUID) -> None:
        self.context = RuntimeContext.build(
            workspace_id=workspace_id,
            actor_id=actor_id,
            roles=["runtime:operator"],
            request_id="unit-test",
        )
        self.calls: list[dict[str, Any]] = []
        self.contexts: list[RuntimeContext] = []

    def with_context(self, context: RuntimeContext) -> "RecordingDatasetSyncRuntimeHost":
        self.contexts.append(context)
        return self

    async def execute_dataset_sync(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(dict(kwargs))
        return {
            "status": "succeeded",
            "dataset_id": uuid.uuid4(),
            "dataset_name": "billing_customers",
            "resources": [
                {
                    "source_key": "resource:customers",
                    "dataset_names": ["billing_customers"],
                    "records_synced": 2,
                }
            ],
            "summary": "Dataset sync completed for 'billing_customers'.",
        }


class RecordingSqlQueryRuntimeHost:
    def __init__(self, *, workspace_id: uuid.UUID, actor_id: uuid.UUID) -> None:
        self.context = RuntimeContext.build(
            workspace_id=workspace_id,
            actor_id=actor_id,
            roles=["runtime:operator"],
            request_id="unit-test",
        )
        self.calls: list[dict[str, Any]] = []
        self.contexts: list[RuntimeContext] = []

    def with_context(self, context: RuntimeContext) -> "RecordingSqlQueryRuntimeHost":
        self.contexts.append(context)
        return self

    async def execute_sql_query(self, **kwargs: Any) -> dict[str, Any]:
        request = kwargs["request"]
        self.calls.append(
            {
                "query_scope": request.query_scope.value,
                "query": request.query,
                "connection_name": request.connection_name,
            }
        )
        return {
            "status": "succeeded",
            "query_scope": request.query_scope.value,
            "sql_job_id": uuid.uuid4(),
            "query": request.query,
            "generated_sql": request.query,
            "columns": [{"name": "answer", "type": None}],
            "rows": [{"answer": 42}],
            "row_count_preview": 1,
            "duration_ms": 12,
            "redaction_applied": False,
        }


class RecordingUnitOfWork:
    def __init__(self) -> None:
        self.commit_count = 0

    async def commit(self) -> None:
        self.commit_count += 1


class RecordingPersistenceController:
    def __init__(self) -> None:
        self.unit_of_work = RecordingUnitOfWork()

    def current_uow(self) -> RecordingUnitOfWork:
        return self.unit_of_work


class RecordingAgentRunRuntimeHost:
    def __init__(self, *, workspace_id: uuid.UUID, actor_id: uuid.UUID) -> None:
        self.context = RuntimeContext.build(
            workspace_id=workspace_id,
            actor_id=actor_id,
            roles=["runtime:operator"],
            request_id="unit-test",
        )
        self.calls: list[dict[str, Any]] = []
        self.contexts: list[RuntimeContext] = []
        self.persistence_controller = RecordingPersistenceController()
        self.commit_counts_after_events: list[int] = []

    def with_context(self, context: RuntimeContext) -> "RecordingAgentRunRuntimeHost":
        self.contexts.append(context)
        return self

    async def execute_agent_run(self, **kwargs: Any) -> dict[str, Any]:
        event_emitter = kwargs["event_emitter"]
        self.calls.append(
            {
                "job_id": kwargs["job_id"],
                "payload": dict(kwargs["payload"]),
            }
        )
        for event_type, message in [
            ("MetaControllerStarted", "Reading agent specifications."),
            ("SqlGenerationStarted", "Generating governed SQL."),
            ("SqlExecutionStarted", "Running query through Langbridge runtime."),
            ("AnalystEvidenceReviewStarted", "Reviewing governed evidence sufficiency."),
            ("PresentationStarted", "Preparing final response."),
            ("SqlExecutionCompleted", "Retrieved governed evidence."),
        ]:
            await event_emitter.emit(
                event_type=event_type,
                message=message,
                source="unit-agent",
                details={"query_scope": "semantic"},
            )
            self.commit_counts_after_events.append(
                self.persistence_controller.unit_of_work.commit_count
            )
        return {
            "thread_id": kwargs["payload"]["thread_id"],
            "job_id": kwargs["job_id"],
            "message_id": uuid.uuid4(),
            "answer_markdown": "Paid Social led Q3 revenue.",
            "artifacts": [
                {
                    "id": "primary_result",
                    "type": "table",
                    "role": "primary_result",
                    "title": "Primary result",
                    "payload": {
                        "columns": ["order_channel"],
                        "rows": [["Paid Social"]],
                        "row_count": 1,
                    },
                    "provenance": {"source": "unit-agent"},
                }
            ],
            "diagnostics": {"query_scope": "semantic"},
            "metadata": {"contract_version": "markdown_artifacts.v1"},
        }


class FailingAgentRunRuntimeHost(RecordingAgentRunRuntimeHost):
    async def execute_agent_run(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(
            {
                "job_id": kwargs["job_id"],
                "payload": dict(kwargs["payload"]),
            }
        )
        raise RuntimeError("agent execution failed")


@pytest.mark.anyio
async def test_runtime_job_lifecycle_persists_events_tasks_and_artifacts() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    repository = _InMemoryJobRepository()
    service = RuntimeJobService(repository=repository)
    handlers = RuntimeJobHandlerRegistry()
    handlers.register(EchoJobHandler())
    processor = RuntimeJobProcessor(
        job_service=service,
        handlers=handlers,
        worker_id="unit-worker",
    )

    job = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type="test.echo",
            payload={"value": "hello"},
            idempotency_key="echo-hello",
        ),
    )

    duplicate = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type="test.echo",
            payload={"value": "ignored"},
            idempotency_key="echo-hello",
        ),
    )
    assert duplicate.id == job.id

    processed = await processor.process_once()
    assert processed is True

    completed = await service.get_job(job_id=job.id)
    assert completed.status == RuntimeJobStatus.succeeded.value
    assert completed.result == {"echo": "hello"}
    assert len(completed.tasks) == 1
    assert completed.tasks[0].status == RuntimeJobStatus.succeeded.value
    assert len(completed.artifacts) == 1
    assert completed.artifacts[0].artifact_key == "echo-result"
    assert [event.event_type for event in completed.events] == [
        "job.created",
        "job.started",
        "test.echo.progress",
        "job.succeeded",
    ]


@pytest.mark.anyio
async def test_runtime_job_stream_replays_public_events_only_until_terminal() -> None:
    service = RuntimeJobService(repository=_InMemoryJobRepository())
    job = await service.create_job(
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        request=CreateRuntimeJobRequest(job_type="test.manual"),
    )
    await service.start_job(job_id=job.id, worker_id="unit-worker")
    await service.append_event(
        job_id=job.id,
        task_id=None,
        event_type="test.internal",
        status="running",
        stage="internal",
        message="Internal detail.",
        visibility="internal",
    )
    await service.complete_job(job_id=job.id, result={"ok": True})

    events = []
    async for event in await service.stream_events(job_id=job.id):
        if event is not None:
            events.append(event)

    assert [event.event for event in events] == ["job.started", "job.succeeded"]
    assert events[-1].terminal is True
    assert events[-1].details == {"result": {"ok": True}}


@pytest.mark.anyio
async def test_runtime_job_processor_marks_handler_failures_failed() -> None:
    service = RuntimeJobService(repository=_InMemoryJobRepository())
    handlers = RuntimeJobHandlerRegistry()
    handlers.register(FailingJobHandler())
    processor = RuntimeJobProcessor(
        job_service=service,
        handlers=handlers,
        worker_id="unit-worker",
    )
    job = await service.create_job(
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        request=CreateRuntimeJobRequest(job_type="test.fail"),
    )

    processed = await processor.process_once()

    failed = await service.get_job(job_id=job.id)
    assert processed is True
    assert failed.status == RuntimeJobStatus.failed.value
    assert failed.error is not None
    assert failed.error["message"] == "handler exploded"
    assert failed.events[-1].event_type == "job.failed"


@pytest.mark.anyio
async def test_runtime_job_processor_claims_default_queue_only() -> None:
    service = RuntimeJobService(repository=_InMemoryJobRepository())
    handlers = RuntimeJobHandlerRegistry()
    handlers.register(EchoJobHandler())
    processor = RuntimeJobProcessor(
        job_service=service,
        handlers=handlers,
        worker_id="unit-worker",
    )
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    inline_job = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type="test.echo",
            queue_name="inline",
            payload={"value": "inline"},
        ),
    )
    default_job = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type="test.echo",
            payload={"value": "default"},
        ),
    )

    processed = await processor.process_once()

    still_inline = await service.get_job(job_id=inline_job.id)
    completed_default = await service.get_job(job_id=default_job.id)
    assert processed is True
    assert still_inline.status == RuntimeJobStatus.queued.value
    assert completed_default.status == RuntimeJobStatus.succeeded.value
    assert completed_default.result == {"echo": "default"}


@pytest.mark.anyio
async def test_dataset_sync_job_handler_executes_sync_and_records_artifacts() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    service = RuntimeJobService(repository=_InMemoryJobRepository())
    handlers = RuntimeJobHandlerRegistry()
    runtime_host = RecordingDatasetSyncRuntimeHost(
        workspace_id=workspace_id,
        actor_id=actor_id,
    )
    handlers.register(DatasetSyncJobHandler(host=runtime_host))  # type: ignore[arg-type]
    processor = RuntimeJobProcessor(
        job_service=service,
        handlers=handlers,
        worker_id="unit-worker",
    )
    job = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type=DATASET_SYNC_JOB_TYPE,
            payload={
                "dataset_ref": "billing_customers",
                "sync_mode": "INCREMENTAL",
            },
        ),
    )

    processed = await processor.process_once()

    completed = await service.get_job(job_id=job.id)
    assert processed is True
    assert runtime_host.calls == [
        {
            "dataset_ref": "billing_customers",
            "sync_mode": "INCREMENTAL",
            "force_full_refresh": False,
        }
    ]
    assert runtime_host.contexts[0].workspace_id == workspace_id
    assert runtime_host.contexts[0].actor_id == actor_id
    assert completed.status == RuntimeJobStatus.succeeded.value
    assert completed.result is not None
    assert completed.result["dataset_name"] == "billing_customers"
    assert completed.tasks[0].status == RuntimeJobStatus.succeeded.value
    assert completed.artifacts[0].artifact_key == "sync_result"
    assert "dataset.sync.started" in [event.event_type for event in completed.events]
    assert "dataset.sync.succeeded" in [event.event_type for event in completed.events]


@pytest.mark.anyio
async def test_sql_query_job_handler_executes_query_and_records_artifacts() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    service = RuntimeJobService(repository=_InMemoryJobRepository())
    handlers = RuntimeJobHandlerRegistry()
    runtime_host = RecordingSqlQueryRuntimeHost(
        workspace_id=workspace_id,
        actor_id=actor_id,
    )
    handlers.register(SqlQueryJobHandler(host=runtime_host))  # type: ignore[arg-type]
    processor = RuntimeJobProcessor(
        job_service=service,
        handlers=handlers,
        worker_id="unit-worker",
    )
    job = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type=SQL_QUERY_JOB_TYPE,
            payload={
                "query_scope": "source",
                "query": "SELECT 42 AS answer",
                "connection_name": "demo",
            },
        ),
    )

    processed = await processor.process_once()

    completed = await service.get_job(job_id=job.id)
    assert processed is True
    assert runtime_host.calls == [
        {
            "query_scope": "source",
            "query": "SELECT 42 AS answer",
            "connection_name": "demo",
        }
    ]
    assert runtime_host.contexts[0].workspace_id == workspace_id
    assert runtime_host.contexts[0].actor_id == actor_id
    assert completed.status == RuntimeJobStatus.succeeded.value
    assert completed.result is not None
    assert completed.result["rows"] == [{"answer": 42}]
    assert {artifact.artifact_key for artifact in completed.artifacts} == {
        "result_table",
        "sql_diagnostics",
    }
    assert completed.tasks[0].status == RuntimeJobStatus.succeeded.value
    assert "sql.query.started" in [event.event_type for event in completed.events]
    assert "sql.query.succeeded" in [event.event_type for event in completed.events]


@pytest.mark.anyio
async def test_agent_run_job_handler_executes_agent_run_and_records_artifacts() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    thread_id = uuid.uuid4()
    user_message_id = uuid.uuid4()
    service = RuntimeJobService(repository=_InMemoryJobRepository())
    handlers = RuntimeJobHandlerRegistry()
    runtime_host = RecordingAgentRunRuntimeHost(
        workspace_id=workspace_id,
        actor_id=actor_id,
    )
    handlers.register(AgentRunJobHandler(host=runtime_host))  # type: ignore[arg-type]
    processor = RuntimeJobProcessor(
        job_service=service,
        handlers=handlers,
        worker_id="unit-worker",
    )
    job = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type=AGENT_RUN_JOB_TYPE,
            subject_type="thread",
            subject_id=thread_id,
            payload={
                "agent_definition_id": str(uuid.uuid4()),
                "agent_name": "growth_analyst",
                "thread_id": str(thread_id),
                "user_message_id": str(user_message_id),
                "agent_mode": "research",
                "mcp": False,
            },
        ),
    )

    processed = await processor.process_once()

    completed = await service.get_job(job_id=job.id)
    event_types = [event.event_type for event in completed.events]
    stream_messages = []
    async for event in await service.stream_events(job_id=job.id):
        if event is not None:
            stream_messages.append(event.message)
    public_messages = [
        event.message
        for event in completed.events
        if str(getattr(event.visibility, "value", event.visibility)) == "public"
    ]
    raw_ai_events = [
        event
        for event in completed.events
        if event.event_type == "run.progress" and event.source == "unit-agent"
    ]
    artifact_keys = {artifact.artifact_key for artifact in completed.artifacts}
    assert processed is True
    assert runtime_host.calls[0]["job_id"] == job.id
    assert runtime_host.calls[0]["payload"]["agent_name"] == "growth_analyst"
    assert runtime_host.contexts[0].workspace_id == workspace_id
    assert runtime_host.contexts[0].actor_id == actor_id
    assert runtime_host.commit_counts_after_events == [1, 2, 3, 4, 5, 6]
    assert completed.status == RuntimeJobStatus.succeeded.value
    assert completed.result is not None
    assert completed.result["answer_markdown"] == "Paid Social led Q3 revenue."
    assert "answer" not in completed.result
    assert "summary" not in completed.result
    assert "result" not in completed.result
    assert "visualization" not in completed.result
    assert completed.tasks[0].status == RuntimeJobStatus.succeeded.value
    assert artifact_keys == {"agent_response", "agent_diagnostics", "agent_artifacts"}
    assert public_messages[:5] == [
        "Job execution started.",
        "Agent run started.",
        "Selecting analyst.",
        "Generating SQL.",
        "Running governed query.",
    ]
    assert "Reviewing evidence." in public_messages
    assert "Composing final answer." in public_messages
    assert "Generating governed SQL." not in stream_messages
    assert "Generating SQL." in stream_messages
    assert raw_ai_events
    assert {
        str(getattr(event.visibility, "value", event.visibility))
        for event in raw_ai_events
    } == {"internal"}
    assert "run.started" in event_types
    assert "run.progress" in event_types
    assert "run.completed" in event_types
    assert event_types[-1] == "job.succeeded"


@pytest.mark.anyio
async def test_agent_run_job_handler_records_run_failure_before_job_failure() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    service = RuntimeJobService(repository=_InMemoryJobRepository())
    handlers = RuntimeJobHandlerRegistry()
    runtime_host = FailingAgentRunRuntimeHost(
        workspace_id=workspace_id,
        actor_id=actor_id,
    )
    handlers.register(AgentRunJobHandler(host=runtime_host))  # type: ignore[arg-type]
    processor = RuntimeJobProcessor(
        job_service=service,
        handlers=handlers,
        worker_id="unit-worker",
    )
    job = await service.create_job(
        workspace_id=workspace_id,
        actor_id=actor_id,
        request=CreateRuntimeJobRequest(
            job_type=AGENT_RUN_JOB_TYPE,
            subject_type="thread",
            subject_id=uuid.uuid4(),
            payload={
                "agent_name": "growth_analyst",
                "thread_id": str(uuid.uuid4()),
                "user_message_id": str(uuid.uuid4()),
                "agent_mode": "auto",
            },
        ),
    )

    processed = await processor.process_once()

    failed = await service.get_job(job_id=job.id)
    event_types = [event.event_type for event in failed.events]
    assert processed is True
    assert runtime_host.calls[0]["payload"]["agent_name"] == "growth_analyst"
    assert failed.status == RuntimeJobStatus.failed.value
    assert failed.tasks[0].status == RuntimeJobStatus.failed.value
    assert failed.tasks[0].error == {
        "type": "RuntimeError",
        "message": "agent execution failed",
    }
    assert "run.failed" in event_types
    assert event_types[-1] == "job.failed"
