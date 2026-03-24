import pathlib
import sys
import uuid
from typing import Any

sys.path.append(str(pathlib.Path(__file__).resolve().parents[5] / "langbridge" / "langbridge"))

from langbridge.orchestrator.tools.sql_analyst.interfaces import (
    AnalyticalColumn,
    AnalyticalContext,
    AnalyticalDatasetBinding,
    AnalystQueryRequest,
    QueryResult,
)
from langbridge.orchestrator.tools.sql_analyst.tool import SqlAnalystTool
from langbridge.runtime.services.semantic_vector_search_service import (
    SemanticVectorSearchHit,
)


class DummyLLM:
    def __init__(self, sql: str) -> None:
        self._sql = sql
        self.prompts: list[str] = []

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        _ = (prompt, temperature, max_tokens)
        self.prompts.append(prompt)
        return self._sql


class RecordingFederatedExecutor:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._rows = rows or [(42,)]

    async def execute_sql(
        self,
        *,
        sql: str,
        dialect: str,
        max_rows: int | None = None,
    ) -> QueryResult:
        self.calls.append({"sql": sql, "dialect": dialect, "max_rows": max_rows})
        return QueryResult(
            columns=["value"],
            rows=self._rows,
            rowcount=len(self._rows),
            elapsed_ms=5,
            source_sql=sql,
        )


def _dataset_context() -> AnalyticalContext:
    return AnalyticalContext(
        asset_type="dataset",
        asset_id="dataset-1",
        asset_name="orders_dataset",
        description="Orders dataset",
        datasets=[
            AnalyticalDatasetBinding(
                dataset_id="dataset-1",
                dataset_name="orders_dataset",
                sql_alias="orders",
                source_kind="connector",
                storage_kind="table",
                columns=[
                    AnalyticalColumn(name="order_id", data_type="integer"),
                    AnalyticalColumn(name="amount", data_type="decimal"),
                ],
            )
        ],
        tables=["orders"],
    )


def _semantic_context() -> AnalyticalContext:
    return AnalyticalContext(
        asset_type="semantic_model",
        asset_id="semantic-model-1",
        asset_name="orders_semantic",
        description="Orders semantic model",
        datasets=[
            AnalyticalDatasetBinding(
                dataset_id="dataset-1",
                dataset_name="orders_dataset",
                sql_alias="orders",
                source_kind="connector",
                storage_kind="table",
                columns=[
                    AnalyticalColumn(name="country", data_type="text"),
                    AnalyticalColumn(name="amount", data_type="decimal"),
                ],
            )
        ],
        tables=["shopify_orders"],
    )


class DummyEmbedder:
    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]


class RecordingSemanticVectorSearchService:
    def __init__(self, hits: list[SemanticVectorSearchHit]) -> None:
        self._hits = hits
        self.calls: list[dict[str, Any]] = []

    async def search(
        self,
        *,
        workspace_id,
        semantic_model_id,
        queries,
        embedding_provider=None,
        top_k=5,
    ) -> list[SemanticVectorSearchHit]:
        self.calls.append(
            {
                "workspace_id": workspace_id,
                "semantic_model_id": semantic_model_id,
                "queries": list(queries),
                "embedding_provider": embedding_provider,
                "top_k": top_k,
            }
        )
        return list(self._hits)


def test_sql_analyst_tool_executes_dataset_context_through_federation() -> None:
    llm = DummyLLM("SELECT COUNT(*) AS order_count FROM orders")
    executor = RecordingFederatedExecutor(rows=[(2,)])
    tool = SqlAnalystTool(
        llm=llm,
        context=_dataset_context(),
        federated_sql_executor=executor,
    )

    response = tool.run(AnalystQueryRequest(question="How many orders?"))

    assert response.error is None
    assert response.analysis_path == "dataset"
    assert response.execution_mode == "federated"
    assert response.asset_name == "orders_dataset"
    assert response.sql_canonical == "SELECT COUNT(*) AS order_count FROM orders"
    assert response.sql_executable == "SELECT COUNT(*) AS order_count FROM orders LIMIT 1000"
    assert executor.calls == [
        {
            "sql": "SELECT COUNT(*) AS order_count FROM orders LIMIT 1000",
            "dialect": "postgres",
            "max_rows": 1000,
        }
    ]
    assert response.result == QueryResult(
        columns=["value"],
        rows=[(2,)],
        rowcount=1,
        elapsed_ms=5,
        source_sql="SELECT COUNT(*) AS order_count FROM orders LIMIT 1000",
    )


def test_sql_analyst_tool_applies_limit_before_federated_execution() -> None:
    llm = DummyLLM("SELECT order_id FROM orders")
    executor = RecordingFederatedExecutor()
    tool = SqlAnalystTool(
        llm=llm,
        context=_dataset_context(),
        federated_sql_executor=executor,
    )

    response = tool.run(AnalystQueryRequest(question="List orders", limit=10))

    assert response.error is None
    assert response.sql_executable == "SELECT order_id FROM orders LIMIT 10"
    assert executor.calls[0]["sql"] == "SELECT order_id FROM orders LIMIT 10"
    assert executor.calls[0]["max_rows"] == 10


def test_sql_analyst_tool_uses_runtime_semantic_vector_search_hints() -> None:
    llm = DummyLLM("SELECT COUNT(*) AS order_count FROM orders WHERE orders.country = 'France'")
    executor = RecordingFederatedExecutor(rows=[(12,)])
    workspace_id = uuid.uuid4()
    semantic_model_id = uuid.uuid4()
    semantic_search = RecordingSemanticVectorSearchService(
        [
            SemanticVectorSearchHit(
                index_id=uuid.uuid4(),
                semantic_model_id=semantic_model_id,
                dataset_key="shopify_orders",
                dimension_name="country",
                matched_value="France",
                score=0.97,
                source_text="French market",
            )
        ]
    )
    tool = SqlAnalystTool(
        llm=llm,
        context=_semantic_context(),
        federated_sql_executor=executor,
        embedder=DummyEmbedder(),
        semantic_vector_search_service=semantic_search,
        semantic_vector_search_workspace_id=workspace_id,
        semantic_vector_search_model_id=semantic_model_id,
    )

    response = tool.run(AnalystQueryRequest(question="How many orders came from the French market?"))

    assert response.error is None
    assert semantic_search.calls == [
        {
            "workspace_id": workspace_id,
            "semantic_model_id": semantic_model_id,
            "queries": [
                "How many orders came from the French market?",
                "the French market",
            ],
            "embedding_provider": tool.embedder,
            "top_k": 3,
        }
    ]
    assert "Filters to apply: shopify_orders.country = 'France'" in llm.prompts[0]
    assert "shopify_orders.country ~= 'France'" in llm.prompts[0]
    assert executor.calls[0]["sql"] == (
        "SELECT COUNT(*) AS order_count FROM orders WHERE orders.country = 'France' LIMIT 1000"
    )
