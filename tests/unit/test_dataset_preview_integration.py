from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from langbridge.apps.api.langbridge_api.services.dataset_service import DatasetService
from langbridge.apps.worker.langbridge_worker.handlers.query.dataset_job_request_handler import (
    DatasetJobRequestHandler,
)
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.datasets import (
    DatasetColumnRequest,
    DatasetCreateRequest,
    DatasetPolicyRequest,
    DatasetPreviewRequest,
    DatasetType,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.dataset_job import (
    CreateDatasetPreviewJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.dataset_job import (
    DatasetJobRequestMessage,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@dataclass
class _OrgRef:
    id: uuid.UUID


@dataclass
class _FakeConnector:
    id: uuid.UUID
    connector_type: str
    organizations: list[_OrgRef]


@dataclass
class _WorkspacePolicy:
    max_preview_rows: int


class _InMemoryDatasetRepository:
    def __init__(self) -> None:
        self.items: dict[uuid.UUID, DatasetRecord] = {}

    def add(self, dataset: DatasetRecord) -> None:
        self.items[dataset.id] = dataset

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None = None,
        search: str | None = None,
        tags: list[str] | None = None,
        dataset_types: list[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[DatasetRecord]:
        rows = [row for row in self.items.values() if row.workspace_id == workspace_id]
        rows.sort(key=lambda item: item.updated_at, reverse=True)
        return rows[offset : offset + limit]

    async def get_for_workspace(self, *, dataset_id: uuid.UUID, workspace_id: uuid.UUID):
        row = self.items.get(dataset_id)
        if row is None or row.workspace_id != workspace_id:
            return None
        return row

    async def delete(self, dataset: DatasetRecord) -> None:
        self.items.pop(dataset.id, None)


class _InMemoryDatasetColumnRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, list[DatasetColumnRecord]] = {}

    def add(self, column: DatasetColumnRecord) -> None:
        self.by_dataset.setdefault(column.dataset_id, []).append(column)

    async def list_for_dataset(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnRecord]:
        return sorted(
            self.by_dataset.get(dataset_id, []),
            key=lambda item: (item.ordinal_position, item.name),
        )

    async def delete_for_dataset(self, *, dataset_id: uuid.UUID) -> None:
        self.by_dataset[dataset_id] = []


class _InMemoryDatasetPolicyRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, DatasetPolicyRecord] = {}

    def add(self, policy: DatasetPolicyRecord) -> None:
        self.by_dataset[policy.dataset_id] = policy

    async def get_for_dataset(self, *, dataset_id: uuid.UUID):
        return self.by_dataset.get(dataset_id)


class _InMemoryDatasetRevisionRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, list[DatasetRevisionRecord]] = {}

    def add(self, revision: DatasetRevisionRecord) -> None:
        self.by_dataset.setdefault(revision.dataset_id, []).append(revision)

    async def next_revision_number(self, *, dataset_id: uuid.UUID) -> int:
        rows = self.by_dataset.get(dataset_id, [])
        if not rows:
            return 1
        return max(item.revision_number for item in rows) + 1


class _InMemoryJobRepository:
    def __init__(self) -> None:
        self.jobs: dict[uuid.UUID, JobRecord] = {}

    def add(self, job: JobRecord) -> None:
        self.jobs[job.id] = job

    async def get_by_id(self, job_id: uuid.UUID):
        return self.jobs.get(job_id)


class _FakeConnectorRepository:
    def __init__(self, connector: _FakeConnector) -> None:
        self._connector = connector

    async def get_by_id(self, connector_id: uuid.UUID):
        if connector_id == self._connector.id:
            return self._connector
        return None


class _FakeSemanticModelRepository:
    async def list_for_scope(self, organization_id: uuid.UUID, project_id: uuid.UUID | None = None):
        return []


class _FakeSqlWorkspacePolicyRepository:
    def __init__(self, max_preview_rows: int) -> None:
        self._policy = _WorkspacePolicy(max_preview_rows=max_preview_rows)

    async def get_by_workspace_id(self, *, workspace_id: uuid.UUID):
        return self._policy


class _FakeOrganizationRepository:
    def __init__(self, workspace_id: uuid.UUID) -> None:
        self._workspace_id = workspace_id

    async def get_by_id(self, workspace_id: uuid.UUID):
        if workspace_id == self._workspace_id:
            return object()
        return None

    async def get_member_role(self, organization: object, user: object):
        return "admin"


class _FakeUserRepository:
    async def get_by_id(self, user_id: uuid.UUID):
        return object()


class _FakeConnectorService:
    async def get_connector(self, connector_id: uuid.UUID):
        raise RuntimeError("Not expected in this test.")

    async def async_create_sql_connector(self, runtime_type, config):
        raise RuntimeError("Not expected in this test.")


class _FakeFederatedQueryTool:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute_federated_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        return {
            "rows": [{"order_id": 1, "amount": 10.5}],
            "execution": {"total_runtime_ms": 11, "stage_metrics": [{"bytes_written": 64}]},
        }


@dataclass
class _FakeRequestContextProvider:
    correlation_id: str | None = "corr-dataset-integration"


class _InlineDatasetJobRequestService:
    def __init__(
        self,
        *,
        job_repository: _InMemoryJobRepository,
        worker_handler: DatasetJobRequestHandler,
    ) -> None:
        self._job_repository = job_repository
        self._worker_handler = worker_handler

    async def create_preview_job(self, request: CreateDatasetPreviewJobRequest) -> JobRecord:
        now = datetime.now(timezone.utc)
        job = JobRecord(
            id=uuid.uuid4(),
            organisation_id=str(request.workspace_id),
            job_type=request.job_type.value,
            payload=request.model_dump(mode="json"),
            headers={},
            status=JobStatus.queued,
            progress=0,
            status_message="Dataset preview queued.",
            created_at=now,
            queued_at=now,
            updated_at=now,
        )
        self._job_repository.add(job)
        message = DatasetJobRequestMessage(
            job_id=job.id,
            job_type=JobType.DATASET_PREVIEW,
            job_request=request.model_dump(mode="json"),
        )
        await self._worker_handler.handle(message)
        return job

    async def create_profile_job(self, request):
        raise RuntimeError("Profile dispatch not expected in this test.")


@pytest.mark.anyio
async def test_dataset_table_create_then_preview_through_worker_pipeline() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    current_user = UserResponse(
        id=user_id,
        username="integration-user",
        email="integration@example.com",
        is_active=True,
        available_organizations=[workspace_id],
    )

    connector = _FakeConnector(
        id=connector_id,
        connector_type="POSTGRES",
        organizations=[_OrgRef(id=workspace_id)],
    )
    dataset_repository = _InMemoryDatasetRepository()
    dataset_column_repository = _InMemoryDatasetColumnRepository()
    dataset_policy_repository = _InMemoryDatasetPolicyRepository()
    dataset_revision_repository = _InMemoryDatasetRevisionRepository()
    job_repository = _InMemoryJobRepository()
    connector_repository = _FakeConnectorRepository(connector)
    federated_tool = _FakeFederatedQueryTool()

    worker_handler = DatasetJobRequestHandler(
        job_repository=job_repository,
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        federated_query_tool=federated_tool,
    )
    dataset_job_service = _InlineDatasetJobRequestService(
        job_repository=job_repository,
        worker_handler=worker_handler,
    )

    service = DatasetService(
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        connector_repository=connector_repository,
        semantic_model_repository=_FakeSemanticModelRepository(),
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=500),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=dataset_job_service,
        job_repository=job_repository,
        request_context_provider=_FakeRequestContextProvider(),
    )

    dataset = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_preview_dataset",
            dataset_type=DatasetType.TABLE,
            connection_id=connector_id,
            schema_name="public",
            table_name="orders",
            columns=[
                DatasetColumnRequest(
                    name="order_id",
                    data_type="integer",
                    nullable=False,
                    is_allowed=True,
                ),
                DatasetColumnRequest(
                    name="amount",
                    data_type="decimal",
                    nullable=False,
                    is_allowed=True,
                ),
            ],
            policy=DatasetPolicyRequest(max_rows_preview=200),
        ),
        current_user=current_user,
    )

    queued_preview = await service.preview_dataset(
        dataset_id=dataset.id,
        request=DatasetPreviewRequest(
            workspace_id=workspace_id,
            limit=100,
        ),
        current_user=current_user,
    )

    preview = await service.get_preview_job_result(
        dataset_id=dataset.id,
        job_id=queued_preview.job_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert preview.status == JobStatus.succeeded.value
    assert preview.rows == [{"order_id": 1, "amount": 10.5}]
    assert [column.name for column in preview.columns] == ["order_id", "amount"]
    assert preview.effective_limit == 100
    assert len(federated_tool.calls) == 1
