from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from langbridge.apps.worker.langbridge_worker.handlers.query.sql_job_request_handler import (
    SqlJobRequestHandler,
)
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectionPolicy,
    ConnectorResponse,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.sql_job import (
    CreateSqlJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.sql import SqlJobRecord
from langbridge.packages.connectors.langbridge_connectors.api.connector import QueryResult
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.sql_job import (
    SqlJobRequestMessage,
)


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


class _FakeSqlArtifactRepository:
    def __init__(self) -> None:
        self.added = []

    def add(self, artifact) -> None:
        self.added.append(artifact)


class _FakeConnectorRepository:
    async def get_by_id(self, _connector_id):
        return object()


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


@pytest.mark.anyio
async def test_sql_job_request_handler_executes_and_redacts(monkeypatch) -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    job = SqlJobRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        project_id=None,
        user_id=user_id,
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

    def _fake_from_connector(connector, organization_id, project_id=None):
        return ConnectorResponse(
            id=connection_id,
            name="test-connector",
            connector_type="POSTGRES",
            organization_id=organization_id,
            project_id=project_id,
            config={"config": {}},
            connection_policy=ConnectionPolicy(redaction_rules={"secret": "hash"}),
        )

    monkeypatch.setattr(
        ConnectorResponse,
        "from_connector",
        staticmethod(_fake_from_connector),
    )

    handler = SqlJobRequestHandler(
        sql_job_repository=_FakeSqlJobRepository(job),
        sql_job_result_artifact_repository=_FakeSqlArtifactRepository(),
        connector_repository=_FakeConnectorRepository(),
    )

    async def _fake_create_sql_connector(*, connector_type, connector_payload):
        return fake_connector

    monkeypatch.setattr(handler, "_create_sql_connector", _fake_create_sql_connector)
    monkeypatch.setattr(handler, "_resolve_connector_config", lambda connector: {"config": {}})

    request = CreateSqlJobRequest(
        sql_job_id=job.id,
        workspace_id=workspace_id,
        user_id=user_id,
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
