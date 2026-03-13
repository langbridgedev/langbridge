from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from langbridge.apps.api.langbridge_api.services.semantic.semantic_query_service import (
    SemanticQueryService,
    _normalize_unified_relationship_payload,
)
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    UnifiedSemanticRelationshipRequest,
    UnifiedSemanticQueryRequest,
    UnifiedSemanticQueryResponse,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_query_unified_request_delegates_to_execution_service() -> None:
    organization_id = uuid.uuid4()
    semantic_model_id = uuid.uuid4()
    response = UnifiedSemanticQueryResponse(
        id=uuid.uuid4(),
        organization_id=organization_id,
        project_id=None,
        connector_id=uuid.uuid4(),
        semantic_model_ids=[semantic_model_id],
        data=[{"orders__id": 1}],
        annotations=[],
        metadata=[],
    )
    execution_service = SimpleNamespace(
        execute_unified_query=AsyncMock(return_value=SimpleNamespace(response=response))
    )
    service = SemanticQueryService(
        semantic_model_service=SimpleNamespace(),
        connector_service=SimpleNamespace(),
        semantic_query_execution_service=execution_service,
    )

    result = await service.query_unified_request(
        UnifiedSemanticQueryRequest(
            organization_id=organization_id,
            semantic_model_ids=[semantic_model_id],
            query={"dimensions": ["orders.id"], "limit": 1},
        )
    )

    assert result == response
    execution_service.execute_unified_query.assert_awaited_once()
    call = execution_service.execute_unified_query.await_args
    assert call.kwargs["organization_id"] == organization_id
    assert call.kwargs["semantic_model_ids"] == [semantic_model_id]
    assert call.kwargs["semantic_query"].dimensions == ["orders.id"]


@pytest.mark.anyio
async def test_query_unified_request_requires_execution_service() -> None:
    service = SemanticQueryService(
        semantic_model_service=SimpleNamespace(),
        connector_service=SimpleNamespace(),
        semantic_query_execution_service=None,
    )

    with pytest.raises(BusinessValidationError, match="not configured"):
        await service.query_unified_request(
            UnifiedSemanticQueryRequest(
                organization_id=uuid.uuid4(),
                semantic_model_ids=[uuid.uuid4()],
                query={"dimensions": ["orders.id"], "limit": 1},
            )
        )


def test_normalize_unified_relationship_payload_uses_snake_case_field_names() -> None:
    relationship = UnifiedSemanticRelationshipRequest(
        source_semantic_model_id=uuid.uuid4(),
        source_field="sales.customer_id",
        target_semantic_model_id=uuid.uuid4(),
        target_field="marketing.customer_id",
        relationship_type="inner",
    )

    normalized = _normalize_unified_relationship_payload(relationship)

    assert "source_semantic_model_id" in normalized
    assert "sourceSemanticModelId" not in normalized
    assert normalized["relationship_type"] == "inner"
