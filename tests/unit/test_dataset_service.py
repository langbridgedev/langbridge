from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
import types
from typing import Any

import pytest

if "jose" not in sys.modules:
    jose_module = types.ModuleType("jose")

    class _JWTError(Exception):
        pass

    class _JwtFacade:
        @staticmethod
        def encode(*args, **kwargs):
            return "token"

        @staticmethod
        def decode(*args, **kwargs):
            return {}

    jose_module.JWTError = _JWTError
    jose_module.jwt = _JwtFacade()
    sys.modules["jose"] = jose_module

from langbridge.apps.api.langbridge_api.services.dataset_service import DatasetService
from langbridge.apps.api.langbridge_api.services.lineage_service import LineageService
from langbridge.apps.api.langbridge_api.services.semantic.semantic_model_service import (
    SemanticModelService,
)
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.datasets import (
    DatasetBulkCreateRequest,
    DatasetColumnRequest,
    DatasetCreateRequest,
    DatasetEnsureRequest,
    DatasetPolicyRequest,
    DatasetPreviewRequest,
    DatasetRestoreRequest,
    DatasetSelectionColumnRequest,
    DatasetSelectionRequest,
    DatasetType,
    DatasetUpdateRequest,
)
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelCreateRequest,
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
from langbridge.packages.common.langbridge_common.db.semantic import SemanticModelEntry
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.common.langbridge_common.db.lineage import LineageEdgeRecord
from langbridge.packages.common.langbridge_common.utils.lineage import (
    LineageEdgeType,
    LineageNodeType,
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
    name: str = "warehouse"


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

    async def get_by_id(self, dataset_id: uuid.UUID) -> DatasetRecord | None:
        return self.items.get(dataset_id)

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

    async def list_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        limit: int = 50,
    ) -> list[DatasetRevisionRecord]:
        rows = sorted(
            self.by_dataset.get(dataset_id, []),
            key=lambda item: item.revision_number,
            reverse=True,
        )
        return rows[:limit]

    async def get_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        revision_id: uuid.UUID,
    ) -> DatasetRevisionRecord | None:
        for revision in self.by_dataset.get(dataset_id, []):
            if revision.id == revision_id:
                return revision
        return None

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
    def __init__(self) -> None:
        self.items: dict[uuid.UUID, SemanticModelEntry] = {}

    def add(self, model: SemanticModelEntry) -> None:
        self.items[model.id] = model

    async def list_for_scope(self, organization_id: uuid.UUID, project_id: uuid.UUID | None = None):
        rows = [item for item in self.items.values() if item.organization_id == organization_id]
        if project_id is not None:
            rows = [item for item in rows if item.project_id == project_id]
        return rows

    async def get_by_id(self, model_id: uuid.UUID) -> SemanticModelEntry | None:
        return self.items.get(model_id)


class _FakeLineageEdgeRepository:
    def __init__(self) -> None:
        self.items: list[LineageEdgeRecord] = []

    def add(self, edge: LineageEdgeRecord) -> None:
        self.items.append(edge)

    async def delete_for_target(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> None:
        self.items = [
            edge
            for edge in self.items
            if not (
                edge.workspace_id == workspace_id
                and edge.target_type == target_type
                and edge.target_id == target_id
            )
        ]

    async def delete_for_node(
        self,
        *,
        workspace_id: uuid.UUID,
        node_type: str,
        node_id: str,
    ) -> None:
        self.items = [
            edge
            for edge in self.items
            if not (
                edge.workspace_id == workspace_id
                and (
                    (edge.source_type == node_type and edge.source_id == node_id)
                    or (edge.target_type == node_type and edge.target_id == node_id)
                )
            )
        ]

    async def list_inbound(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> list[LineageEdgeRecord]:
        return [
            edge
            for edge in self.items
            if edge.workspace_id == workspace_id
            and edge.target_type == target_type
            and edge.target_id == target_id
        ]

    async def list_outbound(
        self,
        *,
        workspace_id: uuid.UUID,
        source_type: str,
        source_id: str,
    ) -> list[LineageEdgeRecord]:
        return [
            edge
            for edge in self.items
            if edge.workspace_id == workspace_id
            and edge.source_type == source_type
            and edge.source_id == source_id
        ]


class _FakeSqlSavedQueryRepository:
    async def get_by_id(self, query_id: uuid.UUID):
        return None


class _FakeDashboardRepository:
    async def get_by_id(self, dashboard_id: uuid.UUID):
        return None


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


class _SemanticConnectorService(_FakeConnectorService):
    def __init__(self, connector: _FakeConnector) -> None:
        self._connector = connector

    async def get_connector(self, connector_id: uuid.UUID):
        if connector_id == self._connector.id:
            return self._connector
        return None


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
        self.csv_ingest_requests: list[Any] = []

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

    async def create_csv_ingest_job(self, request) -> JobRecord:
        self.csv_ingest_requests.append(request)
        now = datetime.now(timezone.utc)
        job = JobRecord(
            id=uuid.uuid4(),
            organisation_id=str(request.workspace_id),
            job_type=request.job_type.value,
            payload=request.model_dump(mode="json"),
            headers={},
            status=JobStatus.queued,
            progress=0,
            status_message="CSV ingest queued.",
            created_at=now,
            queued_at=now,
            updated_at=now,
        )
        self._job_repository.add(job)
        return job


@dataclass
class _FakeRequestContextProvider:
    correlation_id: str | None = "corr-dataset-tests"


def _build_lineage_service(
    *,
    dataset_repository: _FakeDatasetRepository,
    semantic_model_repository: _FakeSemanticModelRepository,
    connector_repository: _FakeConnectorRepository,
    lineage_edge_repository: _FakeLineageEdgeRepository,
) -> LineageService:
    return LineageService(
        lineage_edge_repository=lineage_edge_repository,
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        sql_saved_query_repository=_FakeSqlSavedQueryRepository(),
        dashboard_repository=_FakeDashboardRepository(),
        connector_repository=connector_repository,
    )


def _build_dataset_service(
    *,
    workspace_id: uuid.UUID,
    connector: _FakeConnector,
    dataset_repository: _FakeDatasetRepository | None = None,
    dataset_column_repository: _FakeDatasetColumnRepository | None = None,
    dataset_policy_repository: _FakeDatasetPolicyRepository | None = None,
    dataset_revision_repository: _FakeDatasetRevisionRepository | None = None,
    semantic_model_repository: _FakeSemanticModelRepository | None = None,
    job_repository: _FakeJobRepository | None = None,
    dataset_job_request_service: _FakeDatasetJobRequestService | None = None,
    lineage_service: LineageService | None = None,
) -> DatasetService:
    job_repo = job_repository or _FakeJobRepository()
    return DatasetService(
        dataset_repository=dataset_repository or _FakeDatasetRepository(),
        dataset_column_repository=dataset_column_repository or _FakeDatasetColumnRepository(),
        dataset_policy_repository=dataset_policy_repository or _FakeDatasetPolicyRepository(),
        dataset_revision_repository=dataset_revision_repository or _FakeDatasetRevisionRepository(),
        connector_repository=_FakeConnectorRepository(connector),
        semantic_model_repository=semantic_model_repository or _FakeSemanticModelRepository(),
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=120),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=dataset_job_request_service or _FakeDatasetJobRequestService(job_repository=job_repo),
        job_repository=job_repo,
        request_context_provider=_FakeRequestContextProvider(),
        lineage_service=lineage_service,
    )


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
async def test_dataset_service_resolves_structured_file_capabilities_for_shopify_parquet() -> None:
    workspace_id = uuid.uuid4()
    current_user = UserResponse(
        id=uuid.uuid4(),
        username="dataset-user",
        email="dataset@example.com",
        is_active=True,
        available_organizations=[workspace_id],
    )
    connector = _FakeConnector(
        id=uuid.uuid4(),
        connector_type="POSTGRES",
        organizations=[_OrgRef(id=workspace_id)],
    )
    service = _build_dataset_service(
        workspace_id=workspace_id,
        connector=connector,
    )

    created = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="shopify_orders",
            dataset_type=DatasetType.FILE,
            dialect="duckdb",
            storage_uri="file:///tmp/shopify_orders.parquet",
            file_config={
                "format": "parquet",
                "connector_sync": {
                    "connector_type": "shopify",
                    "resource_name": "orders",
                },
            },
        ),
        current_user=current_user,
    )
    catalog = await service.get_catalog(
        workspace_id=workspace_id,
        project_id=None,
        current_user=current_user,
    )

    assert created.source_kind.value == "saas"
    assert created.connector_kind == "shopify"
    assert created.storage_kind.value == "parquet"
    assert created.execution_capabilities.supports_sql_federation is True
    assert created.relation_identity.storage_uri == "file:///tmp/shopify_orders.parquet"
    assert catalog.items[0].execution_capabilities.supports_structured_scan is True


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
async def test_upload_csv_dataset_creates_file_dataset_and_ingest_job(tmp_path, monkeypatch) -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    current_user = UserResponse(
        id=user_id,
        username="dataset-user",
        email="dataset@example.com",
        is_active=True,
        available_organizations=[workspace_id],
    )
    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _FakeDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    job_repository = _FakeJobRepository()
    dataset_job_request_service = _FakeDatasetJobRequestService(job_repository=job_repository)
    monkeypatch.setattr(settings, "DATASET_FILE_LOCAL_DIR", str(tmp_path / "datasets"))

    service = DatasetService(
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        connector_repository=_FakeConnectorRepository(
            _FakeConnector(
                id=uuid.uuid4(),
                connector_type="POSTGRES",
                organizations=[_OrgRef(id=workspace_id)],
            )
        ),
        semantic_model_repository=_FakeSemanticModelRepository(),
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=120),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=dataset_job_request_service,
        job_repository=job_repository,
        request_context_provider=_FakeRequestContextProvider(),
    )

    response = await service.upload_csv_dataset(
        workspace_id=workspace_id,
        project_id=None,
        name="marketing_upload",
        filename="../marketing.csv",
        content=b"campaign,spend\nspring,100\n",
        description="Uploaded campaign spend",
        tags=["marketing"],
        current_user=current_user,
    )

    dataset = await dataset_repository.get_for_workspace(
        dataset_id=response.dataset_id,
        workspace_id=workspace_id,
    )
    assert dataset is not None
    assert dataset.dataset_type == DatasetType.FILE.value
    assert dataset.storage_uri == response.storage_uri
    assert dataset.status == "draft"
    assert dataset.file_config_json["format"] == "csv"
    assert dataset.file_config_json["filename"] == "marketing.csv"
    assert Path(settings.DATASET_FILE_LOCAL_DIR, "uploads", str(workspace_id)).exists()
    assert len(dataset_job_request_service.csv_ingest_requests) == 1
    assert dataset_job_request_service.csv_ingest_requests[0].dataset_id == dataset.id
    assert dataset_job_request_service.csv_ingest_requests[0].storage_uri == response.storage_uri
    assert response.job_status == JobStatus.queued.value


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


@pytest.mark.anyio
async def test_dataset_versions_diff_and_restore_flow() -> None:
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
    service = _build_dataset_service(workspace_id=workspace_id, connector=connector)

    created = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_dataset",
            change_summary="Create governed orders dataset.",
            dataset_type=DatasetType.TABLE,
            connection_id=connector_id,
            schema_name="public",
            table_name="orders",
            columns=[
                DatasetColumnRequest(name="order_id", data_type="integer", nullable=False),
                DatasetColumnRequest(name="amount", data_type="decimal", nullable=True),
            ],
            policy=DatasetPolicyRequest(max_rows_preview=100, max_export_rows=500),
        ),
        current_user=current_user,
    )

    updated = await service.update_dataset(
        dataset_id=created.id,
        request=DatasetUpdateRequest(
            workspace_id=workspace_id,
            name="orders_dataset_v2",
            change_summary="Add gross_revenue and tighten preview policy.",
            columns=[
                DatasetColumnRequest(name="order_id", data_type="integer", nullable=False),
                DatasetColumnRequest(name="amount", data_type="decimal", nullable=True),
                DatasetColumnRequest(name="gross_revenue", data_type="decimal", nullable=True),
            ],
            policy=DatasetPolicyRequest(
                max_rows_preview=25,
                max_export_rows=500,
                redaction_rules={"amount": "mask"},
            ),
        ),
        current_user=current_user,
    )

    assert updated.name == "orders_dataset_v2"
    versions = await service.list_dataset_versions(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert [item.revision_number for item in versions.items] == [2, 1]
    assert versions.items[0].is_current is True
    assert versions.items[0].change_summary == "Add gross_revenue and tighten preview policy."

    diff = await service.diff_dataset_versions(
        dataset_id=created.id,
        workspace_id=workspace_id,
        from_revision_id=versions.items[1].id,
        to_revision_id=versions.items[0].id,
        current_user=current_user,
    )
    assert any(change.field == "name" for change in diff.definition_changes)
    assert any(change.field == "max_rows_preview" for change in diff.policy_changes)
    assert any(
        change.column_name == "gross_revenue" and change.change_type == "added"
        for change in diff.schema_changes
    )

    restored = await service.restore_dataset(
        dataset_id=created.id,
        request=DatasetRestoreRequest(
            workspace_id=workspace_id,
            revision_id=versions.items[1].id,
            change_summary="Rollback to stable revision.",
        ),
        current_user=current_user,
    )
    assert restored.name == "orders_dataset"
    assert all(column.name != "gross_revenue" for column in restored.columns)
    assert restored.policy.max_rows_preview == 100

    restored_versions = await service.list_dataset_versions(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert len(restored_versions.items) == 3
    assert restored_versions.items[0].is_current is True
    assert restored_versions.items[0].change_summary == "Rollback to stable revision."


@pytest.mark.anyio
async def test_dataset_lineage_and_impact_cover_table_sql_and_federated_dependencies() -> None:
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
        name="warehouse",
    )
    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _FakeDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    semantic_model_repository = _FakeSemanticModelRepository()
    lineage_edge_repository = _FakeLineageEdgeRepository()
    lineage_service = _build_lineage_service(
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        connector_repository=_FakeConnectorRepository(connector),
        lineage_edge_repository=lineage_edge_repository,
    )
    service = _build_dataset_service(
        workspace_id=workspace_id,
        connector=connector,
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        semantic_model_repository=semantic_model_repository,
        lineage_service=lineage_service,
    )

    base_dataset = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_dataset",
            dataset_type=DatasetType.TABLE,
            connection_id=connector_id,
            schema_name="public",
            table_name="orders",
            columns=[DatasetColumnRequest(name="order_id", data_type="integer", nullable=False)],
        ),
        current_user=current_user,
    )
    sql_dataset = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_sql_dataset",
            dataset_type=DatasetType.SQL,
            connection_id=connector_id,
            sql_text="select order_id from orders_dataset",
            columns=[DatasetColumnRequest(name="order_id", data_type="integer", nullable=False)],
        ),
        current_user=current_user,
    )
    federated_dataset = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_federated_dataset",
            dataset_type=DatasetType.FEDERATED,
            referenced_dataset_ids=[sql_dataset.id],
        ),
        current_user=current_user,
    )

    assert any(
        edge.source_type == LineageNodeType.CONNECTION.value
        and edge.target_id == str(base_dataset.id)
        and edge.edge_type == LineageEdgeType.FEEDS.value
        for edge in lineage_edge_repository.items
    )
    assert any(
        edge.source_type == LineageNodeType.SOURCE_TABLE.value
        and edge.target_id == str(base_dataset.id)
        and edge.edge_type == LineageEdgeType.MATERIALIZES_FROM.value
        for edge in lineage_edge_repository.items
    )
    assert any(
        edge.source_id == str(base_dataset.id)
        and edge.target_id == str(sql_dataset.id)
        and edge.edge_type == LineageEdgeType.DERIVES_FROM.value
        for edge in lineage_edge_repository.items
    )
    assert any(
        edge.source_id == str(sql_dataset.id)
        and edge.target_id == str(federated_dataset.id)
        and edge.edge_type == LineageEdgeType.DERIVES_FROM.value
        for edge in lineage_edge_repository.items
    )

    lineage = await service.get_lineage(
        dataset_id=federated_dataset.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert {node.node_id for node in lineage.nodes if node.node_type == LineageNodeType.DATASET.value} >= {
        str(base_dataset.id),
        str(sql_dataset.id),
        str(federated_dataset.id),
    }
    assert lineage.upstream_count >= 2

    impact = await service.get_impact(
        dataset_id=base_dataset.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    dependent_ids = {item.node_id for item in impact.dependent_datasets}
    assert str(sql_dataset.id) in dependent_ids
    assert str(federated_dataset.id) in dependent_ids
    assert impact.total_downstream_assets >= 2
    assert {item.node_id for item in impact.direct_dependents} == {str(sql_dataset.id)}


@pytest.mark.anyio
async def test_semantic_model_save_registers_dataset_lineage_and_dataset_updates_preserve_impact() -> None:
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
        name="warehouse",
    )
    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _FakeDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    semantic_model_repository = _FakeSemanticModelRepository()
    lineage_edge_repository = _FakeLineageEdgeRepository()
    lineage_service = _build_lineage_service(
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        connector_repository=_FakeConnectorRepository(connector),
        lineage_edge_repository=lineage_edge_repository,
    )
    dataset_service = _build_dataset_service(
        workspace_id=workspace_id,
        connector=connector,
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        semantic_model_repository=semantic_model_repository,
        lineage_service=lineage_service,
    )
    semantic_model_service = SemanticModelService(
        repository=semantic_model_repository,
        builder=types.SimpleNamespace(),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        project_repository=types.SimpleNamespace(get_by_id=None),
        connector_service=_SemanticConnectorService(connector),
        agent_service=types.SimpleNamespace(),
        semantic_search_service=types.SimpleNamespace(),
        emvironment_service=types.SimpleNamespace(),
        lineage_service=lineage_service,
    )

    dataset = await dataset_service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="semantic_orders_dataset",
            dataset_type=DatasetType.TABLE,
            connection_id=connector_id,
            schema_name="public",
            table_name="orders",
            columns=[DatasetColumnRequest(name="order_id", data_type="integer", nullable=False)],
        ),
        current_user=current_user,
    )

    model_yaml = f"""
version: "1.0"
name: "Orders semantic model"
tables:
  orders:
    dataset_id: "{dataset.id}"
    schema: "public"
    name: "orders"
    dimensions:
      - name: "order_id"
        type: "integer"
        primary_key: true
"""
    semantic_model = await semantic_model_service.create_model(
        SemanticModelCreateRequest(
            connector_id=connector_id,
            organization_id=workspace_id,
            name="Orders semantic model",
            description="Model for governed orders.",
            model_yaml=model_yaml,
        )
    )

    assert any(
        edge.source_type == LineageNodeType.DATASET.value
        and edge.source_id == str(dataset.id)
        and edge.target_type == LineageNodeType.SEMANTIC_MODEL.value
        and edge.target_id == str(semantic_model.id)
        and edge.edge_type == LineageEdgeType.FEEDS.value
        for edge in lineage_edge_repository.items
    )

    await dataset_service.update_dataset(
        dataset_id=dataset.id,
        request=DatasetUpdateRequest(
            workspace_id=workspace_id,
            change_summary="Add status column for semantic consumers.",
            columns=[
                DatasetColumnRequest(name="order_id", data_type="integer", nullable=False),
                DatasetColumnRequest(name="status", data_type="text", nullable=True),
            ],
        ),
        current_user=current_user,
    )

    impact = await dataset_service.get_impact(
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert {item.node_id for item in impact.semantic_models} == {str(semantic_model.id)}

    lineage = await dataset_service.get_lineage(
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert any(
        node.node_type == LineageNodeType.SEMANTIC_MODEL.value and node.node_id == str(semantic_model.id)
        for node in lineage.nodes
    )


@pytest.mark.anyio
async def test_dataset_versioning_restore_diff_and_lineage_impact_flow() -> None:
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
        name="analytics",
    )

    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _FakeDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    semantic_model_repository = _FakeSemanticModelRepository()
    lineage_edge_repository = _FakeLineageEdgeRepository()
    job_repository = _FakeJobRepository()

    lineage_service = LineageService(
        lineage_edge_repository=lineage_edge_repository,
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        sql_saved_query_repository=_FakeSqlSavedQueryRepository(),
        dashboard_repository=_FakeDashboardRepository(),
        connector_repository=_FakeConnectorRepository(connector),
    )
    service = DatasetService(
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        connector_repository=_FakeConnectorRepository(connector),
        semantic_model_repository=semantic_model_repository,
        sql_workspace_policy_repository=_FakeSqlWorkspacePolicyRepository(max_preview_rows=120),
        organization_repository=_FakeOrganizationRepository(workspace_id=workspace_id),
        user_repository=_FakeUserRepository(),
        connector_service=_FakeConnectorService(),
        dataset_job_request_service=_FakeDatasetJobRequestService(job_repository=job_repository),
        job_repository=job_repository,
        request_context_provider=_FakeRequestContextProvider(),
        lineage_service=lineage_service,
    )

    created = await service.create_dataset(
        request=DatasetCreateRequest(
            workspace_id=workspace_id,
            name="orders_dataset",
            dataset_type=DatasetType.TABLE,
            connection_id=connector_id,
            schema_name="public",
            table_name="orders",
            change_summary="Initial version",
            columns=[
                DatasetColumnRequest(name="order_id", data_type="integer", nullable=False, is_allowed=True),
                DatasetColumnRequest(name="amount", data_type="decimal", nullable=False, is_allowed=True),
            ],
            policy=DatasetPolicyRequest(max_rows_preview=50, max_export_rows=500),
        ),
        current_user=current_user,
    )

    versions_after_create = await service.list_dataset_versions(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert len(versions_after_create.items) == 1
    assert versions_after_create.items[0].revision_number == 1
    assert versions_after_create.items[0].change_summary == "Initial version"
    assert versions_after_create.items[0].is_current is True

    first_revision = await service.get_dataset_version(
        dataset_id=created.id,
        revision_id=versions_after_create.items[0].id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert first_revision.definition_snapshot["table_name"] == "orders"
    assert first_revision.policy_snapshot["max_rows_preview"] == 50
    assert len(first_revision.source_bindings_snapshot) == 3
    assert first_revision.source_bindings_snapshot[0]["source_type"] == "dataset_contract"

    model = SemanticModelEntry(
        id=uuid.uuid4(),
        connector_id=connector_id,
        organization_id=workspace_id,
        project_id=None,
        name="Orders semantic model",
        description=None,
        content_yaml="name: Orders semantic model",
        content_json=json.dumps(
            {
                "name": "Orders semantic model",
                "tables": {
                    "orders": {
                        "dataset_id": str(created.id),
                    }
                },
            }
        ),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    semantic_model_repository.add(model)
    await lineage_service.register_semantic_model_lineage(model=model)

    updated = await service.update_dataset(
        dataset_id=created.id,
        request=DatasetUpdateRequest(
            workspace_id=workspace_id,
            name="orders_dataset_v2",
            change_summary="Add status column",
            columns=[
                DatasetColumnRequest(name="order_id", data_type="integer", nullable=False, is_allowed=True),
                DatasetColumnRequest(name="amount", data_type="decimal", nullable=False, is_allowed=True),
                DatasetColumnRequest(name="status", data_type="text", nullable=True, is_allowed=True),
            ],
            policy=DatasetPolicyRequest(
                max_rows_preview=75,
                max_export_rows=750,
                redaction_rules={"status": "mask"},
            ),
        ),
        current_user=current_user,
    )

    versions_after_update = await service.list_dataset_versions(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert [item.revision_number for item in versions_after_update.items] == [2, 1]
    assert versions_after_update.items[0].id == updated.revision_id
    assert versions_after_update.items[0].is_current is True

    diff = await service.diff_dataset_versions(
        dataset_id=created.id,
        workspace_id=workspace_id,
        from_revision_id=versions_after_update.items[1].id,
        to_revision_id=versions_after_update.items[0].id,
        current_user=current_user,
    )
    assert any(change.column_name == "status" and change.change_type == "added" for change in diff.schema_changes)
    assert any(change.field == "name" for change in diff.definition_changes)
    assert any(change.field == "max_rows_preview" for change in diff.policy_changes)

    impact = await service.get_impact(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert impact.total_downstream_assets == 1
    assert [item.label for item in impact.semantic_models] == ["Orders semantic model"]
    assert impact.direct_dependents[0].label == "Orders semantic model"

    lineage = await service.get_lineage(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert any(node.node_type.value == "source_table" for node in lineage.nodes)
    assert any(node.node_type.value == "semantic_model" for node in lineage.nodes)
    assert lineage.upstream_count >= 2
    assert lineage.downstream_count >= 1

    restored = await service.restore_dataset(
        dataset_id=created.id,
        request=DatasetRestoreRequest(
            workspace_id=workspace_id,
            revision_id=versions_after_update.items[1].id,
            change_summary="Rollback to baseline",
        ),
        current_user=current_user,
    )
    assert restored.name == "orders_dataset"
    assert [column.name for column in restored.columns] == ["order_id", "amount"]

    versions_after_restore = await service.list_dataset_versions(
        dataset_id=created.id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
    assert [item.revision_number for item in versions_after_restore.items] == [3, 2, 1]
    assert versions_after_restore.items[0].change_summary == "Rollback to baseline"
    assert versions_after_restore.items[0].is_current is True
