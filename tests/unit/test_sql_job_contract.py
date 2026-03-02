import uuid

import pytest
from pydantic import ValidationError

from langbridge.packages.common.langbridge_common.contracts.jobs.sql_job import (
    CreateSqlJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.sql import SqlExecuteRequest


def test_sql_job_contract_requires_connection_for_single_mode() -> None:
    with pytest.raises(ValidationError):
        CreateSqlJobRequest(
            sql_job_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            execution_mode="single",
            query="SELECT 1",
            enforced_limit=100,
            enforced_timeout_seconds=30,
        )


def test_sql_job_contract_accepts_federated_when_allowed() -> None:
    payload = CreateSqlJobRequest(
        sql_job_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        execution_mode="federated",
        query="SELECT * FROM sales.orders",
        enforced_limit=1000,
        enforced_timeout_seconds=30,
        allow_federation=True,
    )

    assert payload.execution_mode == "federated"
    assert payload.connection_id is None
    assert payload.query_dialect == "tsql"


def test_sql_execute_request_requires_connection_when_not_federated() -> None:
    with pytest.raises(ValidationError):
        SqlExecuteRequest(
            workspace_id=uuid.uuid4(),
            query="SELECT 1",
            federated=False,
        )


def test_sql_execute_request_rejects_unsupported_query_dialect() -> None:
    with pytest.raises(ValidationError):
        SqlExecuteRequest(
            workspace_id=uuid.uuid4(),
            connection_id=uuid.uuid4(),
            query="SELECT 1",
            query_dialect="unsupported",
            federated=False,
        )
