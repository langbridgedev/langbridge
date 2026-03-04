from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from langbridge.apps.api.langbridge_api.services.dataset_service import DatasetService
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.datasets import (
    DatasetBulkCreateRequest,
    DatasetColumnRequest,
    DatasetCreateRequest,
    DatasetEnsureRequest,
    DatasetPolicyRequest,
    DatasetPreviewRequest,
    DatasetSelectionColumnRequest,
    DatasetSelectionRequest,
    DatasetType,
    DatasetUpdateRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.dataset_job import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetPreviewJobRequest,
)
from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus


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


class _FakeDatasetRepository:
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
        if project_id is not None:
            rows = [
                row
                for row in rows
                if row.project_id is None or row.project_id == project_id
            ]
        if search:
            token = search.lower().strip()
            rows = [
                row
                for row in rows
                if token in row.name.lower()
                or token in (row.description or "").lower()
            ]
        if tags:
            required = {tag.lower() for tag in tags if tag.strip()}
            if required:
                rows = [
                    row
                    for row in rows
                    if required.issubset({tag.lower() for tag in row.tags_json or []})
                ]
        if dataset_types:
            allowed_types = {item.upper() for item in dataset_types}
            rows = [row for row in rows if row.dataset_type.upper() in allowed_types]
        rows = sorted(rows, key=lambda item: item.updated_at, reverse=True)
        return rows[offset : offset + limit]

    async def get_for_workspace(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> DatasetRecord | None:
        row = self.items.get(dataset_id)
        if row is None or row.workspace_id != workspace_id:
            return None
        return row

    async def delete(self, dataset: DatasetRecord) -> None:
        self.items.pop(dataset.id, None)


class _FakeDatasetColumnRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, list[DatasetColumnRecord]] = {}

    def add(self, column: DatasetColumnRecord) -> None:
        bucket = self.by_dataset.setdefault(column.dataset_id, [])
        bucket.append(column)

    async def list_for_dataset(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnRecord]:
        columns = list(self.by_dataset.get(dataset_id, []))
        columns.sort(key=lambda item: (item.ordinal_position, item.name))
        return columns

    async def delete_for_dataset(self, *, dataset_id: uuid.UUID) -> None:
        self.by_dataset[dataset_id] = []


class _FakeDatasetPolicyRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, DatasetPolicyRecord] = {}

    def add(self, policy: DatasetPolicyRecord) -> None:
        self.by_dataset[policy.dataset_id] = policy

    async def get_for_dataset(self, *, dataset_id: uuid.UUID) -> DatasetPolicyRecord | None:
        return self.by_dataset.get(dataset_id)


class _BlindDatasetPolicyRepository(_FakeDatasetPolicyRepository):
    """Simulates a repository that cannot read unflushed inserts yet."""

    def __init__(self) -> None:
        super().__init__()
        self.add_count = 0

    def add(self, policy: DatasetPolicyRecord) -> None:
        self.add_count += 1
        super().add(policy)

    async def get_for_dataset(self, *, dataset_id: uuid.UUID) -> DatasetPolicyRecord | None:
        return None


class _FakeDatasetRevisionRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, list[DatasetRevisionRecord]] = {}

    def add(self, revision: DatasetRevisionRecord) -> None:
        bucket = self.by_dataset.setdefault(revision.dataset_id, [])
        bucket.append(revision)

    async def next_revision_number(self, *, dataset_id: uuid.UUID) -> int:
        rows = self.by_dataset.get(dataset_id) or []
        if not rows:
            return 1
        return max(item.revision_number for item in rows) + 1


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


class _FakeJobRepository:
    def __init__(self) -> None:
        self.jobs: dict[uuid.UUID, JobRecord] = {}

    def add(self, job: JobRecord) -> None:
        self.jobs[job.id] = job

    async def get_by_id(self, job_id: uuid.UUID) -> JobRecord | None:
        return self.jobs.get(job_id)


class _FakeDatasetJobRequestService:
    def __init__(self, *, job_repository: _FakeJobRepository) -> None:
        self._job_repository = job_repository
        self.preview_requests: list[CreateDatasetPreviewJobRequest] = []
        self.bulk_requests: list[CreateDatasetBulkCreateJobRequest] = []

    async def create_preview_job(self, request: CreateDatasetPreviewJobRequest) -> JobRecord:
        self.preview_requests.append(request)
        now = datetime.now(timezone.utc)
        job = JobRecord(
            id=uuid.uuid4(),
            organisation_id=str(request.workspace_id),
            job_type=request.job_type.value,
            payload=request.model_dump(mode="json"),
            headers={},
            status=JobStatus.succeeded,
            progress=100,
            status_message="Dataset preview completed.",
            result={
                "result": {
                    "dataset_id": str(request.dataset_id),
                    "columns": [{"name": "order_id", "type": "integer"}],
                    "rows": [{"order_id": 101}],
                    "row_count_preview": 1,
                    "effective_limit": request.enforced_limit,
                    "redaction_applied": False,
                    "duration_ms": 7,
                    "bytes_scanned": 128,
                }
            },
            created_at=now,
            queued_at=now,
            started_at=now,
            finished_at=now,
            updated_at=now,
        )
        self._job_repository.add(job)
        return job

    async def create_profile_job(self, request):
        raise RuntimeError("Profile dispatch not expected in this test.")

    async def create_bulk_create_job(self, request: CreateDatasetBulkCreateJobRequest) -> JobRecord:
        self.bulk_requests.append(request)
        now = datetime.now(timezone.utc)
        job = JobRecord(
            id=uuid.uuid4(),
            organisation_id=str(request.workspace_id),
            job_type=request.job_type.value,
            payload=request.model_dump(mode="json"),
            headers={},
            status=JobStatus.queued,
            progress=0,
            status_message="Bulk dataset creation queued.",
            created_at=now,
            queued_at=now,
            updated_at=now,
        )
        self._job_repository.add(job)
        return job


@dataclass
class _FakeRequestContextProvider:
    correlation_id: str | None = "corr-dataset-tests"


@pytest.mark.anyio
async def test_dataset_service_crud_and_preview_dispatch() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    current_user = UserResponse(
        id=user_id,
        username="dataset-user",
        email="dataset@example.com",
        is_active=True,
        available_organizations=[workspace_id],
    )

    connector = _FakeConnector(
        id=connector_id,
        connector_type="POSTGRES",
        organizations=[_OrgRef(id=workspace_id)],
    )
    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _FakeDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    connector_repository = _FakeConnectorRepository(connector)
    job_repository = _FakeJobRepository()
    dataset_job_request_service = _FakeDatasetJobRequestService(job_repository=job_repository)

    service = DatasetService(
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        connector_repository=connector_repository,
        semantic_model_repository=_FakeSemanticModelRepository(),
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=120),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=dataset_job_request_service,
        job_repository=job_repository,
        request_context_provider=_FakeRequestContextProvider(),
    )

    created = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_dataset",
            dataset_type=DatasetType.TABLE,
            connection_id=connector_id,
            schema_name="public",
            table_name="orders",
            tags=["sales", "governed"],
            columns=[
                DatasetColumnRequest(
                    name="order_id",
                    data_type="integer",
                    nullable=False,
                    is_allowed=True,
                ),
                DatasetColumnRequest(
                    name="customer_email",
                    data_type="text",
                    nullable=True,
                    is_allowed=True,
                ),
            ],
            policy=DatasetPolicyRequest(
                max_rows_preview=200,
                max_export_rows=1000,
            ),
        ),
        current_user=current_user,
    )
    assert created.name == "orders_dataset"
    assert created.dataset_type == DatasetType.TABLE
    assert created.policy.max_rows_preview == 200
    assert len(created.columns) == 2

    listed = await service.list_datasets(
        workspace_id=workspace_id,
        project_id=None,
        search="orders",
        tags=["sales"],
        dataset_types=["TABLE"],
        current_user=current_user,
    )
    assert listed.total == 1
    assert listed.items[0].id == created.id

    fetched = await service.get_dataset(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert fetched.id == created.id
    assert fetched.columns[0].name == "order_id"

    updated = await service.update_dataset(
        dataset_id=created.id,
        request=DatasetUpdateRequest(
            workspace_id=workspace_id,
            name="orders_dataset_v2",
            policy=DatasetPolicyRequest(
                max_rows_preview=75,
                redaction_rules={"customer_email": "hash"},
            ),
        ),
        current_user=current_user,
    )
    assert updated.name == "orders_dataset_v2"
    assert updated.policy.max_rows_preview == 75
    assert updated.policy.redaction_rules["customer_email"] == "hash"

    queued_preview = await service.preview_dataset(
        dataset_id=created.id,
        request=DatasetPreviewRequest(
            workspace_id=workspace_id,
            limit=500,
        ),
        current_user=current_user,
    )
    assert queued_preview.job_id is not None
    assert queued_preview.effective_limit == 75

    preview = await service.get_preview_job_result(
        dataset_id=created.id,
        job_id=queued_preview.job_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert preview.status == JobStatus.succeeded.value
    assert preview.rows == [{"order_id": 101}]
    assert len(dataset_job_request_service.preview_requests) == 1
    assert dataset_job_request_service.preview_requests[0].enforced_limit == 75


@pytest.mark.anyio
async def test_dataset_create_does_not_insert_policy_twice_when_repo_cannot_read_pending() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    current_user = UserResponse(
        id=user_id,
        username="dataset-user",
        email="dataset@example.com",
        is_active=True,
        available_organizations=[workspace_id],
    )
    connector = _FakeConnector(
        id=connector_id,
        connector_type="POSTGRES",
        organizations=[_OrgRef(id=workspace_id)],
    )

    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _BlindDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    job_repository = _FakeJobRepository()
    service = DatasetService(
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        connector_repository=_FakeConnectorRepository(connector),
        semantic_model_repository=_FakeSemanticModelRepository(),
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=120),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=_FakeDatasetJobRequestService(job_repository=job_repository),
        job_repository=job_repository,
        request_context_provider=_FakeRequestContextProvider(),
    )

    created = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_policy_once",
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
            ],
            policy=DatasetPolicyRequest(max_rows_preview=1000),
        ),
        current_user=current_user,
    )

    assert created.policy.max_rows_preview == 1000
    assert dataset_policy_repository.add_count == 1


@pytest.mark.anyio
async def test_ensure_dataset_is_idempotent_for_same_selection_signature() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    current_user = UserResponse(
        id=user_id,
        username="dataset-user",
        email="dataset@example.com",
        is_active=True,
        available_organizations=[workspace_id],
    )
    connector = _FakeConnector(
        id=connector_id,
        connector_type="POSTGRES",
        organizations=[_OrgRef(id=workspace_id)],
    )
    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _FakeDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    job_repository = _FakeJobRepository()
    service = DatasetService(
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        connector_repository=_FakeConnectorRepository(connector),
        semantic_model_repository=_FakeSemanticModelRepository(),
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=120),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=_FakeDatasetJobRequestService(job_repository=job_repository),
        job_repository=job_repository,
        request_context_provider=_FakeRequestContextProvider(),
    )

    payload = DatasetEnsureRequest(
        workspace_id=workspace_id,
        connection_id=connector_id,
        schema="public",
        table="orders",
        columns=[
            DatasetSelectionColumnRequest(name="order_id", data_type="integer"),
            DatasetSelectionColumnRequest(name="amount", data_type="decimal"),
        ],
        tags=["auto-generated"],
    )
    first = await service.ensure_dataset(request=payload, current_user=current_user)
    second = await service.ensure_dataset(request=payload, current_user=current_user)

    assert first.created is True
    assert second.created is False
    assert first.dataset_id == second.dataset_id


@pytest.mark.anyio
async def test_start_bulk_create_dispatches_job_with_deduped_selections() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    current_user = UserResponse(
        id=user_id,
        username="dataset-user",
        email="dataset@example.com",
        is_active=True,
        available_organizations=[workspace_id],
    )
    connector = _FakeConnector(
        id=connector_id,
        connector_type="POSTGRES",
        organizations=[_OrgRef(id=workspace_id)],
    )
    dataset_job_service = _FakeDatasetJobRequestService(job_repository=_FakeJobRepository())
    service = DatasetService(
        dataset_repository=_FakeDatasetRepository(),
        dataset_column_repository=_FakeDatasetColumnRepository(),
        dataset_policy_repository=_FakeDatasetPolicyRepository(),
        dataset_revision_repository=_FakeDatasetRevisionRepository(),
        connector_repository=_FakeConnectorRepository(connector),
        semantic_model_repository=_FakeSemanticModelRepository(),
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=120),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=dataset_job_service,
        job_repository=_FakeJobRepository(),
        request_context_provider=_FakeRequestContextProvider(),
    )

    response = await service.start_bulk_create(
        request=DatasetBulkCreateRequest(
            workspace_id=workspace_id,
            connection_id=connector_id,
            selections=[
                DatasetSelectionRequest(
                    schema="public",
                    table="orders",
                    columns=[DatasetSelectionColumnRequest(name="order_id", data_type="integer")],
                ),
                DatasetSelectionRequest(
                    schema="public",
                    table="orders",
                    columns=[DatasetSelectionColumnRequest(name="order_id", data_type="integer")],
                ),
            ],
            tags=["auto-generated"],
        ),
        current_user=current_user,
    )

    assert response.job_status == JobStatus.queued.value
    assert len(dataset_job_service.bulk_requests) == 1
    assert len(dataset_job_service.bulk_requests[0].selections) == 1
