from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from apps.runtime_worker.handlers.query.sql_job_request_handler import (
    SqlJobRequestHandler,
)
from apps.runtime_worker.messaging.contracts.jobs.sql_job import (
    SqlJobRequestMessage,
)
from langbridge.contracts.connectors import (
    ConnectionPolicy,
)
from langbridge.runtime.models import (
    CreateSqlJobRequest,
)
from langbridge.contracts.jobs.type import JobType
from langbridge.runtime.persistence.db.dataset import DatasetRecord
from langbridge.runtime.persistence.db.sql import SqlJobRecord
from langbridge.connectors.base.connector import QueryResult


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeSqlJobRepository:
    def __init__(self, job: SqlJobRecord) -> None:
        self._job = job

    async def get_by_id_for_workspace(self, *, sql_job_id, workspace_id):
        if self._job.id == sql_job_id and self._job.workspace_id == workspace_id:
            return self._job
        return None

    async def save(self, job: SqlJobRecord) -> SqlJobRecord:
        for column in job.__table__.columns:
            setattr(self._job, column.name, getattr(job, column.name))
        return self._job


class _FakeSqlArtifactRepository:
    def __init__(self) -> None:
        self.added = []

    def add(self, artifact) -> None:
        self.added.append(artifact)


class _FakeConnectorRepository:
    def __init__(self, *connector_ids: uuid.UUID, workspace_id: uuid.UUID | None = None) -> None:
        resolved_ids = connector_ids or (uuid.uuid4(),)
        resolved_workspace_id = workspace_id or uuid.uuid4()
        self._connectors = {
            connector_id: SimpleNamespace(
                id=connector_id,
                workspace_id=resolved_workspace_id,
                name=f"test-connector-{connector_id}",
                description=None,
                version=None,
                label="test-connector",
                icon=None,
                connector_type="POSTGRES",
                config={"config": {}},
                connection_policy=ConnectionPolicy(redaction_rules={"secret": "hash"}),
                is_managed=False,
            )
            for connector_id in resolved_ids
        }

    async def get_by_id(self, connector_id):
        if connector_id in self._connectors:
            return self._connectors[connector_id]
        return next(iter(self._connectors.values()))

    async def get_by_id_for_workspace(self, *, connector_id, workspace_id):
        connector = await self.get_by_id(connector_id)
        if connector is None or connector.workspace_id != workspace_id:
            return None
        return connector


class _FakeDatasetRepository:
    def __init__(self, datasets: list[DatasetRecord]) -> None:
        self._datasets = {dataset.id: dataset for dataset in datasets}

    async def get_by_ids_for_workspace(self, *, workspace_id, dataset_ids):
        return [
            dataset
            for dataset_id in dataset_ids
            if (dataset := self._datasets.get(dataset_id)) is not None
            and dataset.workspace_id == workspace_id
        ]


class _FakeSqlConnector:
    def __init__(self) -> None:
        self.executed_sql: str | None = None

    async def execute(self, sql: str, params=None, *, max_rows=None, timeout_s=None):
        self.executed_sql = sql
        return QueryResult(
            columns=["id", "secret"],
            rows=[[1, "sensitive"]],
            rowcount=1,
            elapsed_ms=13,
            sql=sql,
        )


class _FakeFederatedQueryTool:
    def __init__(self) -> None:
        self.execute_payloads: list[dict] = []
        self.explain_payloads: list[dict] = []

    async def execute_federated_query(self, payload: dict):
        self.execute_payloads.append(payload)
        return {
            "columns": ["id", "secret"],
            "rows": [{"id": 1, "secret": "sensitive"}],
            "execution": {
                "total_runtime_ms": 25,
                "stage_metrics": [{"stage_id": "s1", "bytes_written": 512}],
            },
        }

    async def explain_federated_query(self, payload: dict):
        self.explain_payloads.append(payload)
        return {
            "logical_plan": {"tables": {"a": {}}, "joins": []},
            "physical_plan": {"stages": [{"stage_id": "s1"}]},
        }


@pytest.mark.anyio
async def test_sql_job_request_handler_executes_and_redacts(monkeypatch) -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    job = SqlJobRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        execution_mode="single",
        status="queued",
        query_text="SELECT secret FROM dbo.users",
        query_hash="abc",
        query_params_json={},
        requested_limit=None,
        enforced_limit=100,
        requested_timeout_seconds=None,
        enforced_timeout_seconds=30,
        is_explain=False,
        is_federated=False,
        correlation_id="corr-1",
        policy_snapshot_json={},
        created_at=now,
        updated_at=now,
    )
    fake_connector = _FakeSqlConnector()

    handler = SqlJobRequestHandler(
        sql_job_repository=_FakeSqlJobRepository(job),
        sql_job_result_artifact_repository=_FakeSqlArtifactRepository(),
        connector_repository=_FakeConnectorRepository(connection_id, workspace_id=workspace_id),
    )

    async def _fake_create_sql_connector(*, connector_type, connector_payload):
        return fake_connector

    monkeypatch.setattr(handler, "_create_sql_connector", _fake_create_sql_connector)
    monkeypatch.setattr(handler, "_resolve_connector_config", lambda connector: {"config": {}})

    request = CreateSqlJobRequest(
        sql_job_id=job.id,
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        execution_mode="single",
        query="SELECT secret FROM dbo.users",
        params={},
        enforced_limit=100,
        enforced_timeout_seconds=30,
        redaction_rules={"secret": "hash"},
    )
    message = SqlJobRequestMessage(
        sql_job_id=job.id,
        job_type=JobType.SQL,
        job_request=request.model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job.status == "succeeded"
    assert job.row_count_preview == 1
    assert isinstance(job.result_rows_json, list)
    assert job.result_rows_json[0]["secret"] != "sensitive"
    assert fake_connector.executed_sql is not None
    normalized_sql = fake_connector.executed_sql.upper()
    assert "LIMIT 100" in normalized_sql or "TOP 100" in normalized_sql


@pytest.mark.anyio
async def test_sql_job_request_handler_executes_dataset_backed_federated_query_and_redacts() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    crm_connector_id = uuid.uuid4()
    billing_connector_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    job = SqlJobRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=None,
        execution_mode="federated",
        status="queued",
        query_text="SELECT a.id, b.secret FROM crm.public.accounts AS a JOIN billing.public.accounts AS b ON a.id = b.id",
        query_hash="federated-hash",
        query_params_json={},
        requested_limit=None,
        enforced_limit=100,
        requested_timeout_seconds=None,
        enforced_timeout_seconds=30,
        is_explain=False,
        is_federated=True,
        correlation_id="corr-fed-1",
        policy_snapshot_json={},
        created_at=now,
        updated_at=now,
    )
    crm_dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=crm_connector_id,
        created_by=None,
        updated_by=None,
        name="crm_accounts",
        sql_alias="dataset_1",
        description=None,
        tags_json=[],
        dataset_type="TABLE",
        dialect="postgres",
        catalog_name=None,
        schema_name="public",
        table_name="accounts",
        storage_uri=None,
        sql_text=None,
        file_config_json=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    billing_dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=billing_connector_id,
        created_by=None,
        updated_by=None,
        name="billing_accounts",
        sql_alias="dataset_2",
        description=None,
        tags_json=[],
        dataset_type="TABLE",
        dialect="postgres",
        catalog_name=None,
        schema_name="public",
        table_name="accounts",
        storage_uri=None,
        sql_text=None,
        file_config_json=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    fake_federated_tool = _FakeFederatedQueryTool()
    artifact_repo = _FakeSqlArtifactRepository()
    handler = SqlJobRequestHandler(
        sql_job_repository=_FakeSqlJobRepository(job),
        sql_job_result_artifact_repository=artifact_repo,
        connector_repository=_FakeConnectorRepository(),
        dataset_repository=_FakeDatasetRepository([crm_dataset, billing_dataset]),
        federated_query_tool=fake_federated_tool,
    )

    request = CreateSqlJobRequest(
        sql_job_id=job.id,
        workspace_id=workspace_id,
        actor_id=actor_id,
        execution_mode="federated",
        query=(
            "SELECT a.id, b.secret "
            "FROM crm.public.accounts AS a "
            "JOIN billing.public.accounts AS b ON a.id = b.id"
        ),
        query_dialect="tsql",
        params={},
        enforced_limit=100,
        enforced_timeout_seconds=30,
        allow_federation=True,
        redaction_rules={"secret": "hash"},
        federated_datasets=[
            {"alias": "crm", "dataset_id": str(crm_dataset.id)},
            {"alias": "billing", "dataset_id": str(billing_dataset.id)},
        ],
    )
    message = SqlJobRequestMessage(
        sql_job_id=job.id,
        job_type=JobType.SQL,
        job_request=request.model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job.status == "succeeded"
    assert job.row_count_preview == 1
    assert job.duration_ms == 25
    assert job.bytes_scanned == 512
    assert isinstance(job.result_rows_json, list)
    assert job.result_rows_json[0]["secret"] != "sensitive"
    assert len(fake_federated_tool.execute_payloads) == 1
    payload = fake_federated_tool.execute_payloads[0]
    workflow = payload.get("workflow") or {}
    dataset = workflow.get("dataset") or {}
    tables = dataset.get("tables") or {}
    assert "dataset_1" in tables
    assert "dataset_2" in tables
    assert "crm.public.accounts" in tables
    assert "billing.public.accounts" in tables
    assert tables["dataset_1"]["metadata"]["dataset_alias"] == "dataset_1"
    assert tables["dataset_1"]["metadata"]["physical_schema"] == "public"
    assert tables["dataset_1"]["metadata"]["physical_table"] == "accounts"
    assert len(artifact_repo.added) == 1


@pytest.mark.anyio
async def test_sql_job_request_handler_executes_dataset_backed_federated_query() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    warehouse_connector_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    job = SqlJobRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=None,
        execution_mode="federated",
        status="queued",
        query_text=(
            "SELECT o.id, c.customer_id "
            "FROM shop.api_connector.shopify_orders AS o "
            "JOIN customers.public.customers AS c ON o.customer_id = c.customer_id"
        ),
        query_hash="dataset-fed-hash",
        query_params_json={},
        requested_limit=None,
        enforced_limit=100,
        requested_timeout_seconds=None,
        enforced_timeout_seconds=30,
        is_explain=False,
        is_federated=True,
        correlation_id="corr-fed-dataset",
        policy_snapshot_json={},
        created_at=now,
        updated_at=now,
    )
    shopify_dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=None,
        created_by=None,
        updated_by=None,
        name="shopify_orders",
        sql_alias="dataset_1",
        description=None,
        tags_json=[],
        dataset_type="FILE",
        dialect="duckdb",
        catalog_name=None,
        schema_name="api_connector",
        table_name="shopify_orders",
        storage_uri="file:///tmp/shopify_orders.parquet",
        sql_text=None,
        file_config_json={
            "format": "parquet",
            "connector_sync": {"connector_type": "shopify", "resource_name": "orders"},
        },
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    customer_dataset = DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=warehouse_connector_id,
        created_by=None,
        updated_by=None,
        name="customers",
        sql_alias="dataset_2",
        description=None,
        tags_json=[],
        dataset_type="TABLE",
        dialect="postgres",
        catalog_name=None,
        schema_name="public",
        table_name="customers",
        storage_uri=None,
        sql_text=None,
        file_config_json=None,
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )

    fake_federated_tool = _FakeFederatedQueryTool()
    handler = SqlJobRequestHandler(
        sql_job_repository=_FakeSqlJobRepository(job),
        sql_job_result_artifact_repository=_FakeSqlArtifactRepository(),
        connector_repository=_FakeConnectorRepository(),
        dataset_repository=_FakeDatasetRepository([shopify_dataset, customer_dataset]),
        federated_query_tool=fake_federated_tool,
    )

    request = CreateSqlJobRequest(
        sql_job_id=job.id,
        workspace_id=workspace_id,
        actor_id=actor_id,
        execution_mode="federated",
        query=job.query_text,
        query_dialect="tsql",
        params={},
        enforced_limit=100,
        enforced_timeout_seconds=30,
        allow_federation=True,
        federated_datasets=[
            {"alias": "shop", "dataset_id": str(shopify_dataset.id)},
            {"alias": "customers", "dataset_id": str(customer_dataset.id)},
        ],
    )
    message = SqlJobRequestMessage(
        sql_job_id=job.id,
        job_type=JobType.SQL,
        job_request=request.model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job.status == "succeeded"
    payload = fake_federated_tool.execute_payloads[0]
    tables = payload["workflow"]["dataset"]["tables"]
    assert "dataset_1" in tables
    assert "dataset_2" in tables
    assert "shop.api_connector.shopify_orders" in tables
    assert "customers.public.customers" in tables
    assert tables["dataset_1"]["dataset_descriptor"]["source_kind"] == "saas"
    assert tables["dataset_1"]["dataset_descriptor"]["storage_kind"] == "parquet"
    assert tables["dataset_2"]["metadata"]["physical_table"] == "customers"


@pytest.mark.anyio
async def test_sql_job_request_handler_federated_mode_requires_tool() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    job = SqlJobRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=None,
        execution_mode="federated",
        status="queued",
        query_text="SELECT * FROM crm.public.accounts",
        query_hash="federated-no-tool",
        query_params_json={},
        requested_limit=None,
        enforced_limit=100,
        requested_timeout_seconds=None,
        enforced_timeout_seconds=30,
        is_explain=False,
        is_federated=True,
        correlation_id="corr-fed-2",
        policy_snapshot_json={},
        created_at=now,
        updated_at=now,
    )
    handler = SqlJobRequestHandler(
        sql_job_repository=_FakeSqlJobRepository(job),
        sql_job_result_artifact_repository=_FakeSqlArtifactRepository(),
        connector_repository=_FakeConnectorRepository(),
    )
    request = CreateSqlJobRequest(
        sql_job_id=job.id,
        workspace_id=workspace_id,
        actor_id=actor_id,
        execution_mode="federated",
        query="SELECT * FROM crm.public.accounts",
        query_dialect="tsql",
        params={},
        enforced_limit=100,
        enforced_timeout_seconds=30,
        allow_federation=True,
        federated_datasets=[{"alias": "crm", "dataset_id": uuid.uuid4()}],
    )
    message = SqlJobRequestMessage(
        sql_job_id=job.id,
        job_type=JobType.SQL,
        job_request=request.model_dump(mode="json"),
    )

    await handler.handle(message)

    assert job.status == "failed"
    assert isinstance(job.error_json, dict)
    assert "Federated query tool is not configured" in str(job.error_json.get("message"))
