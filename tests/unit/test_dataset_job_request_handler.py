from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from apps.runtime_worker.handlers.query.dataset_job_request_handler import (
    DatasetJobRequestHandler,
)
from apps.runtime_worker.messaging.contracts.jobs.dataset_job import (
    DatasetJobRequestMessage,
)
from langbridge.config import settings
from langbridge.runtime.models import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetPreviewJobRequest,
)
from langbridge.contracts.jobs.type import JobType
from langbridge.runtime.persistence.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
)
from langbridge.runtime.persistence.db.job import JobRecord, JobStatus


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeJobRepository:
    def __init__(self, job_record: JobRecord) -> None:
        self._job_record = job_record

    async def get_by_id(self, job_id: uuid.UUID) -> JobRecord | None:
        if job_id == self._job_record.id:
            return self._job_record
        return None


class _FakeDatasetRepository:
    def __init__(self, dataset: DatasetRecord | None = None) -> None:
        self._datasets: dict[uuid.UUID, Any] = {}
        if dataset is not None:
            self._datasets[dataset.id] = dataset

    async def get_for_workspace(self, *, dataset_id: uuid.UUID, workspace_id: uuid.UUID):
        row = self._datasets.get(dataset_id)
        if row is not None and row.workspace_id == workspace_id:
            return row
        return None

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_types: list[str] | None = None,
        limit: int = 5000,
    ) -> list[DatasetRecord]:
        rows = [item for item in self._datasets.values() if item.workspace_id == workspace_id]
        if dataset_types:
            allowed = {value.upper() for value in dataset_types}
            rows = [item for item in rows if str(item.dataset_type).upper() in allowed]
        return rows[:limit]

    def add(self, dataset: DatasetRecord) -> None:
        self._datasets[dataset.id] = dataset

    async def save(self, dataset: Any) -> Any:
        self._datasets[dataset.id] = dataset
        return dataset


class _FakeDatasetColumnRepository:
    def __init__(self, columns: list[DatasetColumnRecord] | None = None) -> None:
        self._columns = list(columns or [])

    async def list_for_dataset(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnRecord]:
        return [column for column in self._columns if column.dataset_id == dataset_id]

    async def delete_for_dataset(self, *, dataset_id: uuid.UUID) -> None:
        self._columns = [column for column in self._columns if column.dataset_id != dataset_id]

    def add(self, column: DatasetColumnRecord) -> None:
        self._columns.append(column)


class _FakeDatasetPolicyRepository:
    def __init__(self, policy: DatasetPolicyRecord | None) -> None:
        self._policies: dict[uuid.UUID, DatasetPolicyRecord] = {}
        if policy is not None:
            self._policies[policy.dataset_id] = policy

    async def get_for_dataset(self, *, dataset_id: uuid.UUID):
        return self._policies.get(dataset_id)

    def add(self, policy: DatasetPolicyRecord) -> None:
        self._policies[policy.dataset_id] = policy

    async def save(self, policy: Any) -> Any:
        self._policies[policy.dataset_id] = policy
        return policy


class _FakeFederatedQueryTool:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def execute_federated_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        if self._responses:
            return self._responses.pop(0)
        return {"rows": []}


def _build_job_record(*, workspace_id: uuid.UUID, job_type: JobType) -> JobRecord:
    now = datetime.now(timezone.utc)
    return JobRecord(
        id=uuid.uuid4(),
        workspace_id=str(workspace_id),
        job_type=job_type.value,
        payload={},
        headers={},
        status=JobStatus.queued,
        progress=0,
        status_message="queued",
        created_at=now,
        queued_at=now,
        updated_at=now,
    )


@pytest.mark.anyio
async def test_dataset_preview_enforces_limit_and_applies_redaction_and_rls() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=connection_id,
        created_by=actor_id,
        updated_by=actor_id,
        name="orders_dataset",
        description=None,
        tags_json=["sales"],
        dataset_type="TABLE",
        dialect="tsql",
        catalog_name=None,
        schema_name="dbo",
        table_name="orders",
        sql_text=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    columns = [
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            name="customer_id",
            data_type="integer",
            nullable=False,
            ordinal_position=0,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        ),
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            name="secret",
            data_type="text",
            nullable=True,
            ordinal_position=1,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        ),
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            name="region",
            data_type="text",
            nullable=True,
            ordinal_position=2,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        ),
    ]
    policy = DatasetPolicyRecord(
        id=uuid.uuid4(),
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        max_rows_preview=10,
        max_export_rows=5000,
        redaction_rules_json={"secret": "hash"},
        row_filters_json=["region = {{region}}"],
        allow_dml=False,
        created_at=now,
        updated_at=now,
    )

    federated_tool = _FakeFederatedQueryTool(
        responses=[
            {
                "rows": [{"customer_id": 42, "secret": "cleartext", "region": "EMEA"}],
                "execution": {"total_runtime_ms": 13, "stage_metrics": [{"bytes_written": 256}]},
            }
        ]
    )
    job_record = _build_job_record(workspace_id=workspace_id, job_type=JobType.DATASET_PREVIEW)
    handler = DatasetJobRequestHandler(
        job_repository=_FakeJobRepository(job_record),
        dataset_repository=_FakeDatasetRepository(dataset),
        dataset_column_repository=_FakeDatasetColumnRepository(columns),
        dataset_policy_repository=_FakeDatasetPolicyRepository(policy),
        federated_query_tool=federated_tool,
    )
    request = CreateDatasetPreviewJobRequest(
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        actor_id=actor_id,
        requested_limit=50,
        enforced_limit=25,
        filters={"customer_id": {"operator": "eq", "value": 42}},
        sort=[{"column": "customer_id", "direction": "desc"}],
        user_context={"region": "EMEA"},
    )
    message = DatasetJobRequestMessage(
        job_id=job_record.id,
        job_type=JobType.DATASET_PREVIEW,
        job_request=request.model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job_record.status == JobStatus.succeeded
    result = (job_record.result or {}).get("result") or {}
    assert result["effective_limit"] == 10
    assert result["redaction_applied"] is True
    assert result["rows"][0]["secret"] != "cleartext"
    assert len(federated_tool.calls) == 1

    query_sql = str(result["query_sql"])
    query_upper = query_sql.upper()
    assert "REGION" in query_upper
    assert "EMEA" in query_sql
    assert ("LIMIT 10" in query_upper) or ("TOP 10" in query_upper)


@pytest.mark.anyio
async def test_dataset_sql_preview_blocks_dml_statements() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=connection_id,
        created_by=actor_id,
        updated_by=actor_id,
        name="unsafe_sql_dataset",
        description=None,
        tags_json=[],
        dataset_type="SQL",
        dialect="tsql",
        catalog_name=None,
        schema_name=None,
        table_name=None,
        sql_text="DELETE FROM dbo.orders",
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    columns = [
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            name="order_id",
            data_type="integer",
            nullable=False,
            ordinal_position=0,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        )
    ]
    policy = DatasetPolicyRecord(
        id=uuid.uuid4(),
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        max_rows_preview=25,
        max_export_rows=5000,
        redaction_rules_json={},
        row_filters_json=[],
        allow_dml=False,
        created_at=now,
        updated_at=now,
    )
    federated_tool = _FakeFederatedQueryTool(responses=[])
    job_record = _build_job_record(workspace_id=workspace_id, job_type=JobType.DATASET_PREVIEW)
    handler = DatasetJobRequestHandler(
        job_repository=_FakeJobRepository(job_record),
        dataset_repository=_FakeDatasetRepository(dataset),
        dataset_column_repository=_FakeDatasetColumnRepository(columns),
        dataset_policy_repository=_FakeDatasetPolicyRepository(policy),
        federated_query_tool=federated_tool,
    )
    request = CreateDatasetPreviewJobRequest(
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        actor_id=actor_id,
        enforced_limit=25,
    )
    message = DatasetJobRequestMessage(
        job_id=job_record.id,
        job_type=JobType.DATASET_PREVIEW,
        job_request=request.model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job_record.status == JobStatus.failed
    error_message = str((job_record.error or {}).get("message") or "")
    assert "SELECT" in error_message.upper()
    assert federated_tool.calls == []


@pytest.mark.anyio
async def test_file_dataset_preview_builds_file_backed_workflow() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=None,
        created_by=actor_id,
        updated_by=actor_id,
        name="orders_file",
        description=None,
        tags_json=[],
        dataset_type="FILE",
        dialect="duckdb",
        catalog_name=None,
        schema_name=None,
        table_name="orders_file",
        storage_uri="file:///tmp/orders.parquet",
        sql_text=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json={"format": "parquet"},
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    columns = [
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            name="order_id",
            data_type="integer",
            nullable=False,
            ordinal_position=0,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        )
    ]
    policy = DatasetPolicyRecord(
        id=uuid.uuid4(),
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        max_rows_preview=10,
        max_export_rows=5000,
        redaction_rules_json={},
        row_filters_json=[],
        allow_dml=False,
        created_at=now,
        updated_at=now,
    )
    federated_tool = _FakeFederatedQueryTool(
        responses=[{"rows": [{"order_id": 1}], "execution": {"total_runtime_ms": 7, "stage_metrics": []}}]
    )
    job_record = _build_job_record(workspace_id=workspace_id, job_type=JobType.DATASET_PREVIEW)
    handler = DatasetJobRequestHandler(
        job_repository=_FakeJobRepository(job_record),
        dataset_repository=_FakeDatasetRepository(dataset),
        dataset_column_repository=_FakeDatasetColumnRepository(columns),
        dataset_policy_repository=_FakeDatasetPolicyRepository(policy),
        federated_query_tool=federated_tool,
    )

    message = DatasetJobRequestMessage(
        job_id=job_record.id,
        job_type=JobType.DATASET_PREVIEW,
        job_request=CreateDatasetPreviewJobRequest(
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            actor_id=actor_id,
            enforced_limit=10,
        ).model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job_record.status == JobStatus.succeeded
    workflow = federated_tool.calls[0]["workflow"]
    binding = workflow["dataset"]["tables"]["orders_file"]
    assert binding["metadata"]["source_kind"] == "file"
    assert binding["metadata"]["storage_uri"] == dataset.storage_uri


@pytest.mark.anyio
async def test_api_connector_file_dataset_preview_ignores_synthetic_schema() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=None,
        created_by=actor_id,
        updated_by=actor_id,
        name="hubspot_deals",
        description=None,
        tags_json=[],
        dataset_type="FILE",
        dialect="duckdb",
        catalog_name=None,
        schema_name="api_connector",
        table_name="hubspot_27d35c77_deals",
        storage_uri="file:///tmp/hubspot_deals.parquet",
        sql_text=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json={"format": "parquet", "connector_sync": {"connector_type": "hubspot", "resource_name": "deals"}},
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    columns = [
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            name="object_id",
            data_type="text",
            nullable=False,
            ordinal_position=0,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        )
    ]
    policy = DatasetPolicyRecord(
        id=uuid.uuid4(),
        dataset_id=dataset.id,
        workspace_id=workspace_id,
        max_rows_preview=10,
        max_export_rows=5000,
        redaction_rules_json={},
        row_filters_json=[],
        allow_dml=False,
        created_at=now,
        updated_at=now,
    )
    federated_tool = _FakeFederatedQueryTool(
        responses=[{"rows": [{"object_id": "1"}], "execution": {"total_runtime_ms": 7, "stage_metrics": []}}]
    )
    job_record = _build_job_record(workspace_id=workspace_id, job_type=JobType.DATASET_PREVIEW)
    handler = DatasetJobRequestHandler(
        job_repository=_FakeJobRepository(job_record),
        dataset_repository=_FakeDatasetRepository(dataset),
        dataset_column_repository=_FakeDatasetColumnRepository(columns),
        dataset_policy_repository=_FakeDatasetPolicyRepository(policy),
        federated_query_tool=federated_tool,
    )

    message = DatasetJobRequestMessage(
        job_id=job_record.id,
        job_type=JobType.DATASET_PREVIEW,
        job_request=CreateDatasetPreviewJobRequest(
            dataset_id=dataset.id,
            workspace_id=workspace_id,
            actor_id=actor_id,
            enforced_limit=10,
        ).model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job_record.status == JobStatus.succeeded
    query_sql = federated_tool.calls[0]["query"]
    assert "api_connector." not in query_sql
    assert 'FROM hubspot_27d35c77_deals' in query_sql


@pytest.mark.anyio
async def test_csv_ingest_job_converts_file_dataset_to_parquet(tmp_path, monkeypatch) -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    source_file = tmp_path / "orders.csv"
    source_file.write_text("order_id,amount\n1,12.5\n2,18.0\n", encoding="utf-8")
    monkeypatch.setattr(settings, "DATASET_FILE_LOCAL_DIR", str(tmp_path / "datasets"))

    dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=None,
        created_by=actor_id,
        updated_by=actor_id,
        name="orders_csv",
        description=None,
        tags_json=[],
        dataset_type="FILE",
        dialect="duckdb",
        catalog_name=None,
        schema_name=None,
        table_name="orders_csv",
        storage_uri=source_file.resolve().as_uri(),
        sql_text=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json={"format": "csv"},
        status="draft",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )

    dataset_repository = _FakeDatasetRepository(dataset)
    column_repository = _FakeDatasetColumnRepository([])
    job_record = _build_job_record(workspace_id=workspace_id, job_type=JobType.DATASET_CSV_INGEST)
    handler = DatasetJobRequestHandler(
        job_repository=_FakeJobRepository(job_record),
        dataset_repository=dataset_repository,
        dataset_column_repository=column_repository,
        dataset_policy_repository=_FakeDatasetPolicyRepository(policy=None),
        federated_query_tool=_FakeFederatedQueryTool(responses=[]),
    )
    message = DatasetJobRequestMessage(
        job_id=job_record.id,
        job_type=JobType.DATASET_CSV_INGEST,
        job_request={
            "dataset_id": str(dataset.id),
            "workspace_id": str(workspace_id),
            "actor_id": str(actor_id),
            "storage_uri": source_file.resolve().as_uri(),
        },
    )

    await handler.handle(message)

    assert job_record.status == JobStatus.succeeded
    assert dataset.status == "published"
    assert dataset.storage_uri is not None and dataset.storage_uri.endswith(".parquet")
    columns = await column_repository.list_for_dataset(dataset_id=dataset.id)
    assert [column.name for column in columns] == ["order_id", "amount"]


@pytest.mark.anyio
async def test_dataset_bulk_create_reuses_existing_and_creates_missing() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    existing_dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=connection_id,
        created_by=actor_id,
        updated_by=actor_id,
        name="public.orders",
        description=None,
        tags_json=["auto-generated"],
        dataset_type="TABLE",
        dialect=None,
        catalog_name=None,
        schema_name="public",
        table_name="orders",
        sql_text=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    existing_columns = [
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=existing_dataset.id,
            workspace_id=workspace_id,
            name="order_id",
            data_type="integer",
            nullable=False,
            ordinal_position=0,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        ),
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=existing_dataset.id,
            workspace_id=workspace_id,
            name="amount",
            data_type="decimal",
            nullable=False,
            ordinal_position=1,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=now,
            updated_at=now,
        ),
    ]

    dataset_repository = _FakeDatasetRepository(existing_dataset)
    dataset_column_repository = _FakeDatasetColumnRepository(existing_columns)
    dataset_policy_repository = _FakeDatasetPolicyRepository(policy=None)

    job_record = _build_job_record(workspace_id=workspace_id, job_type=JobType.DATASET_BULK_CREATE)
    handler = DatasetJobRequestHandler(
        job_repository=_FakeJobRepository(job_record),
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        federated_query_tool=_FakeFederatedQueryTool(responses=[]),
    )

    request = CreateDatasetBulkCreateJobRequest(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        selections=[
            {
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "order_id", "data_type": "integer"},
                    {"name": "amount", "data_type": "decimal"},
                ],
            },
            {
                "schema": "public",
                "table": "customers",
                "columns": [
                    {"name": "customer_id", "data_type": "integer"},
                    {"name": "customer_name", "data_type": "text"},
                ],
            },
        ],
        naming_template="{schema}.{table}",
        tags=["auto-generated", "connection-bootstrap"],
    )
    message = DatasetJobRequestMessage(
        job_id=job_record.id,
        job_type=JobType.DATASET_BULK_CREATE,
        job_request=request.model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job_record.status == JobStatus.succeeded
    result = (job_record.result or {}).get("result") or {}
    assert result["created_count"] == 1
    assert result["reused_count"] == 1
    assert len(result["items"]) == 2

    all_datasets = await dataset_repository.list_for_workspace(workspace_id=workspace_id, limit=10)
    assert len(all_datasets) == 2
