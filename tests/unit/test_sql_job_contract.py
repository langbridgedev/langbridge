import uuid

import pytest
from pydantic import ValidationError

from langbridge.runtime.hosting.api_models import RuntimeSqlQueryRequest
from langbridge.runtime.models.jobs import (
    CreateSqlJobRequest,
    SqlWorkbenchMode,
)


def test_sql_job_contract_requires_connection_for_single_mode() -> None:
    with pytest.raises(ValidationError):
        CreateSqlJobRequest(
            sql_job_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            actor_id=uuid.uuid4(),
            execution_mode="single",
            query="SELECT 1",
            enforced_limit=100,
            enforced_timeout_seconds=30,
        )


def test_sql_job_contract_allows_federated_execution_without_selected_datasets() -> None:
    payload = CreateSqlJobRequest(
        sql_job_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        execution_mode="federated",
        query="SELECT * FROM sales_orders",
        enforced_limit=1000,
        enforced_timeout_seconds=30,
        allow_federation=True,
    )

    assert payload.execution_mode == "federated"
    assert payload.workbench_mode.value == "dataset"
    assert payload.selected_datasets == []


def test_sql_job_contract_accepts_dataset_backed_federated_execution() -> None:
    dataset_id = uuid.uuid4()
    payload = CreateSqlJobRequest(
        sql_job_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        execution_mode="federated",
        query="SELECT * FROM shop.public.orders",
        enforced_limit=1000,
        enforced_timeout_seconds=30,
        allow_federation=True,
        selected_datasets=[dataset_id],
    )

    assert payload.execution_mode == "federated"
    assert payload.workbench_mode.value == "dataset"
    assert payload.connection_id is None
    assert payload.query_dialect == "tsql"
    assert payload.selected_datasets == [dataset_id]


def test_runtime_sql_query_request_accepts_direct_sql_payload() -> None:
    payload = RuntimeSqlQueryRequest(
        query="SELECT 1",
        connection_id=uuid.uuid4(),
        query_dialect="postgres",
    )

    assert payload.query == "SELECT 1"
    assert payload.query_dialect == "postgres"


def test_runtime_sql_job_defaults_to_direct_sql_workbench_mode() -> None:
    payload = CreateSqlJobRequest(
        sql_job_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        execution_mode="single",
        connection_id=uuid.uuid4(),
        query="SELECT 1",
        enforced_limit=100,
        enforced_timeout_seconds=30,
    )

    assert payload.workbench_mode == SqlWorkbenchMode.direct_sql


def test_runtime_sql_query_request_rejects_selected_datasets_for_direct_sql() -> None:
    with pytest.raises(ValidationError):
        RuntimeSqlQueryRequest(
            query="SELECT 1",
            connection_name="commerce_demo",
            selected_datasets=[uuid.uuid4()],
        )


def test_runtime_sql_job_rejects_blank_query() -> None:
    with pytest.raises(ValidationError):
        CreateSqlJobRequest(
            sql_job_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            actor_id=uuid.uuid4(),
            execution_mode="single",
            connection_id=uuid.uuid4(),
            query="   ",
            enforced_limit=100,
            enforced_timeout_seconds=30,
        )
