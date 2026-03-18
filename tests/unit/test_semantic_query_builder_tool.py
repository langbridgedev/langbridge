from __future__ import annotations

import uuid

import pytest

from langbridge.packages.orchestrator.langbridge_orchestrator.tools.semantic_query_builder import (
    QueryBuilderCopilotRequest,
    SemanticQueryBuilderCopilotTool,
)
from langbridge.packages.runtime.models import (
    SemanticQueryMetaResponse,
    SemanticQueryRequest,
    SemanticQueryResponse,
)
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeLlm:
    def __init__(self, response: str) -> None:
        self.response = response
        self.prompts: list[str] = []

    async def acomplete(
        self,
        prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        self.prompts.append(prompt)
        return self.response


class _FakeSemanticQueryService:
    def __init__(self) -> None:
        self.meta = SemanticQueryMetaResponse(
            id=uuid.uuid4(),
            name="commerce_performance",
            organization_id=uuid.uuid4(),
            project_id=uuid.uuid4(),
            connector_id=uuid.uuid4(),
            semantic_model={"measures": [{"name": "orders.count"}]},
        )
        self.preview_response = SemanticQueryResponse(
            id=uuid.uuid4(),
            organization_id=self.meta.organization_id,
            project_id=self.meta.project_id,
            semantic_model_id=self.meta.id,
            data=[{"orders__count": 42}],
            annotations=[],
        )
        self.requests: list[SemanticQueryRequest] = []

    async def get_meta(
        self,
        *,
        semantic_model_id: uuid.UUID,
        organization_id: uuid.UUID,
    ) -> SemanticQueryMetaResponse:
        assert semantic_model_id == self.meta.id
        assert organization_id == self.meta.organization_id
        return self.meta

    async def query_request(
        self,
        request: SemanticQueryRequest,
    ) -> SemanticQueryResponse:
        self.requests.append(request)
        return self.preview_response


@pytest.mark.anyio
async def test_semantic_query_builder_tool_uses_runtime_semantic_models() -> None:
    semantic_service = _FakeSemanticQueryService()
    tool = SemanticQueryBuilderCopilotTool(
        llm=_FakeLlm(
            '{"actions":["Added a measure"],"semanticQuery":{"measures":["orders.count"],"dimensions":[]}}'
        ),
        semantic_query_service=semantic_service,
    )

    request = QueryBuilderCopilotRequest(
        organization_id=semantic_service.meta.organization_id,
        project_id=semantic_service.meta.project_id,
        semantic_model_id=semantic_service.meta.id,
        instructions="Show total orders",
        builder_state=SemanticQuery(),
        generate_preview=True,
    )

    response = await tool.arun(request)

    assert response.updated_query.measures == ["orders.count"]
    assert response.preview is not None
    assert isinstance(response.preview, SemanticQueryResponse)
    assert len(semantic_service.requests) == 1
    preview_request = semantic_service.requests[0]
    assert isinstance(preview_request, SemanticQueryRequest)
    assert preview_request.semantic_model_id == semantic_service.meta.id
    assert preview_request.query["measures"] == ["orders.count"]
