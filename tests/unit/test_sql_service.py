from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from langbridge.apps.api.langbridge_api.services.sql_service import SqlService
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.sql import SqlExecuteRequest
from langbridge.packages.common.langbridge_common.db.sql import (
    SqlJobRecord,
    SqlWorkspacePolicyRecord,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _internal_user() -> UserResponse:
    return UserResponse(
        id=uuid.UUID(int=0),
        username="internal",
        email=None,
        is_active=True,
        available_organizations=[],
        available_projects=[],
    )


def _policy(workspace_id: uuid.UUID) -> SqlWorkspacePolicyRecord:
    now = datetime.now(timezone.utc)
    return SqlWorkspacePolicyRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        max_preview_rows=500,
        max_export_rows=5_000,
        max_runtime_seconds=45,
        max_concurrency=4,
        allow_dml=False,
        allow_federation=False,
        allowed_schemas_json=[],
        allowed_tables_json=[],
        default_datasource_id=None,
        budget_limit_bytes=None,
        updated_by_user_id=None,
        created_at=now,
        updated_at=now,
    )


def _build_service(
    *,
    policy: SqlWorkspacePolicyRecord,
    connector: object | None = None,
    job_record: SqlJobRecord | None = None,
    dispatch_side_effect: Exception | None = None,
) -> tuple[SqlService, SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    sql_job_repository = SimpleNamespace(
        add=MagicMock(),
        count_active_for_workspace=AsyncMock(return_value=0),
        get_by_id_for_workspace=AsyncMock(return_value=job_record),
    )
    sql_job_result_artifact_repository = SimpleNamespace(
        add=MagicMock(),
        list_for_job=AsyncMock(return_value=[]),
    )
    sql_saved_query_repository = SimpleNamespace(
        add=MagicMock(),
        list_for_workspace=AsyncMock(return_value=[]),
        get_for_workspace=AsyncMock(return_value=None),
        delete=AsyncMock(),
    )
    sql_workspace_policy_repository = SimpleNamespace(
        get_by_workspace_id=AsyncMock(return_value=policy),
        add=MagicMock(),
    )
    connector_repository = SimpleNamespace(get_by_id=AsyncMock(return_value=connector))
    organization_repository = SimpleNamespace(
        get_by_id=AsyncMock(return_value=object()),
        get_member_role=AsyncMock(return_value="owner"),
    )
    user_repository = SimpleNamespace(get_by_id=AsyncMock(return_value=object()))
    dispatch_mock = AsyncMock()
    if dispatch_side_effect is not None:
        dispatch_mock.side_effect = dispatch_side_effect
    sql_job_request_service = SimpleNamespace(dispatch_sql_job=dispatch_mock)
    request_context_provider = SimpleNamespace(correlation_id="corr-unit-sql")

    service = SqlService(
        sql_job_repository=sql_job_repository,
        sql_job_result_artifact_repository=sql_job_result_artifact_repository,
        sql_saved_query_repository=sql_saved_query_repository,
        sql_workspace_policy_repository=sql_workspace_policy_repository,
        connector_repository=connector_repository,
        organization_repository=organization_repository,
        user_repository=user_repository,
        sql_job_request_service=sql_job_request_service,
        request_context_provider=request_context_provider,
    )
    return (
        service,
        sql_job_repository,
        sql_job_request_service,
        sql_job_result_artifact_repository,
    )


@pytest.mark.anyio
async def test_execute_sql_applies_policy_limits_and_dispatches_job() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    user = _internal_user()
    policy = _policy(workspace_id)

    connector = SimpleNamespace(
        id=connection_id,
        name="Warehouse",
        description=None,
        connector_type="mssql",
        config_json={},
        organizations=[SimpleNamespace(id=workspace_id)],
        access_policy_json=None,
    )

    service, sql_job_repository, sql_job_request_service, _ = _build_service(
        policy=policy,
        connector=connector,
    )

    response = await service.execute_sql(
        request=SqlExecuteRequest(
            workspace_id=workspace_id,
            connection_id=connection_id,
            query="SELECT * FROM dbo.users",
            requested_limit=5_000,
            requested_timeout_seconds=300,
        ),
        current_user=user,
    )

    assert response.sql_job_id is not None
    assert sql_job_repository.add.call_count == 1
    created_record = sql_job_repository.add.call_args.args[0]
    assert created_record.workspace_id == workspace_id
    assert created_record.connection_id == connection_id
    assert created_record.enforced_limit == policy.max_preview_rows
    assert created_record.enforced_timeout_seconds == policy.max_runtime_seconds
    assert created_record.query_hash

    assert sql_job_request_service.dispatch_sql_job.call_count == 1
    dispatched_request = sql_job_request_service.dispatch_sql_job.call_args.args[0]
    assert dispatched_request.enforced_limit == policy.max_preview_rows
    assert dispatched_request.enforced_timeout_seconds == policy.max_runtime_seconds
    assert dispatched_request.query_dialect == "tsql"


@pytest.mark.anyio
async def test_execute_sql_dispatches_federated_datasets() -> None:
    workspace_id = uuid.uuid4()
    user = _internal_user()
    policy = _policy(workspace_id)
    policy.allow_federation = True
    dataset_id = uuid.uuid4()

    service, _sql_job_repository, sql_job_request_service, _ = _build_service(
        policy=policy,
        connector=None,
    )

    await service.execute_sql(
        request=SqlExecuteRequest(
            workspace_id=workspace_id,
            federated=True,
            query="SELECT * FROM shop.orders",
            federated_datasets=[{"alias": "shop", "dataset_id": dataset_id}],
        ),
        current_user=user,
    )

    dispatched_request = sql_job_request_service.dispatch_sql_job.call_args.args[0]
    assert dispatched_request.execution_mode == "federated"
    assert dispatched_request.federated_datasets == [
        {"alias": "shop", "dataset_id": str(dataset_id)}
    ]
    assert "federated_aliases" not in dispatched_request.model_dump(mode="json")


@pytest.mark.anyio
async def test_get_sql_job_results_returns_page_and_next_cursor() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    policy = _policy(workspace_id)

    job = SqlJobRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        project_id=None,
        user_id=user_id,
        connection_id=uuid.uuid4(),
        execution_mode="single",
        status="succeeded",
        query_text="SELECT id FROM dbo.users",
        query_hash="hash",
        query_params_json={},
        requested_limit=100,
        enforced_limit=100,
        requested_timeout_seconds=30,
        enforced_timeout_seconds=30,
        is_explain=False,
        is_federated=False,
        correlation_id="corr-results",
        policy_snapshot_json={},
        result_columns_json=[{"name": "id", "type": "int"}],
        result_rows_json=[{"id": 1}, {"id": 2}, {"id": 3}],
        row_count_preview=3,
        total_rows_estimate=3,
        bytes_scanned=123,
        duration_ms=20,
        result_cursor=None,
        redaction_applied=False,
        error_json=None,
        warning_json=None,
        stats_json=None,
        created_at=now,
        updated_at=now,
    )

    service, _, _, _ = _build_service(policy=policy, job_record=job)

    page = await service.get_sql_job_results(
        sql_job_id=job.id,
        workspace_id=workspace_id,
        current_user=_internal_user(),
        cursor="1",
        page_size=1,
    )

    assert page.sql_job_id == job.id
    assert page.status.value == "succeeded"
    assert len(page.rows) == 1
    assert page.rows[0]["id"] == 2
    assert page.next_cursor == "2"


@pytest.mark.anyio
async def test_get_sql_job_results_rejects_invalid_cursor() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    policy = _policy(workspace_id)
    job = SqlJobRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        project_id=None,
        user_id=user_id,
        connection_id=uuid.uuid4(),
        execution_mode="single",
        status="succeeded",
        query_text="SELECT 1",
        query_hash="hash",
        query_params_json={},
        requested_limit=100,
        enforced_limit=100,
        requested_timeout_seconds=30,
        enforced_timeout_seconds=30,
        is_explain=False,
        is_federated=False,
        correlation_id=None,
        policy_snapshot_json={},
        result_columns_json=[],
        result_rows_json=[],
        row_count_preview=0,
        total_rows_estimate=None,
        bytes_scanned=None,
        duration_ms=None,
        result_cursor=None,
        redaction_applied=False,
        error_json=None,
        warning_json=None,
        stats_json=None,
        created_at=now,
        updated_at=now,
    )

    service, _, _, _ = _build_service(policy=policy, job_record=job)

    with pytest.raises(BusinessValidationError, match="Invalid cursor"):
        await service.get_sql_job_results(
            sql_job_id=job.id,
            workspace_id=workspace_id,
            current_user=_internal_user(),
            cursor="not-a-number",
            page_size=100,
        )


@pytest.mark.anyio
async def test_execute_sql_marks_job_failed_when_dispatch_fails() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    user = _internal_user()
    policy = _policy(workspace_id)

    connector = SimpleNamespace(
        id=connection_id,
        name="Warehouse",
        description=None,
        connector_type="mssql",
        config_json={},
        organizations=[SimpleNamespace(id=workspace_id)],
        access_policy_json=None,
    )

    service, sql_job_repository, sql_job_request_service, _ = _build_service(
        policy=policy,
        connector=connector,
        dispatch_side_effect=RuntimeError("redis unavailable"),
    )

    with pytest.raises(BusinessValidationError, match="Unable to enqueue SQL job for execution"):
        await service.execute_sql(
            request=SqlExecuteRequest(
                workspace_id=workspace_id,
                connection_id=connection_id,
                query="SELECT TOP 5 * FROM dbo.users",
                requested_limit=10,
                requested_timeout_seconds=10,
            ),
            current_user=user,
        )

    assert sql_job_repository.add.call_count == 1
    created_record = sql_job_repository.add.call_args.args[0]
    assert created_record.status == "failed"
    assert created_record.error_json is not None
    assert sql_job_request_service.dispatch_sql_job.call_count == 1
