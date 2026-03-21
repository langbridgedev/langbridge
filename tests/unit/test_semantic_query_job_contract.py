import uuid

import pytest
from pydantic import ValidationError

from langbridge.contracts.jobs.semantic_query_job import (
    CreateSemanticQueryJobRequest,
)


def test_semantic_query_job_contract_accepts_single_model_scope() -> None:
    payload = CreateSemanticQueryJobRequest(
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        query_scope="semantic_model",
        semantic_model_id=uuid.uuid4(),
        query={"measures": ["orders.total"]},
    )

    assert payload.query_scope == "semantic_model"
    assert payload.semantic_model_id is not None


def test_semantic_query_job_contract_accepts_unified_scope() -> None:
    payload = CreateSemanticQueryJobRequest(
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        query_scope="unified",
        semantic_model_ids=[uuid.uuid4(), uuid.uuid4()],
        query={"dimensions": ["orders.id"], "limit": 10},
    )

    assert payload.query_scope == "unified"
    assert payload.connector_id is None
    assert payload.semantic_model_ids is not None
    assert len(payload.semantic_model_ids) == 2


def test_semantic_query_job_contract_requires_unified_model_ids() -> None:
    with pytest.raises(ValidationError):
        CreateSemanticQueryJobRequest(
            workspace_id=uuid.uuid4(),
            actor_id=uuid.uuid4(),
            query_scope="unified",
            query={"dimensions": ["orders.id"]},
        )
