import asyncio
from typing import Any

from langbridge.ai.llm import LLMInvocation, LLMResponse
from langbridge.ai.tools.sql.interfaces import (
    AnalyticalContext,
    AnalyticalQueryExecutionResult,
    AnalystQueryRequest,
    QueryResult,
    SqlQueryScope,
)
from langbridge.ai.tools.sql.tool import SqlAnalysisTool


class _RecordingLLM:
    def __init__(self, text: str) -> None:
        self.text = text
        self.requests: list[Any] = []

    async def ainvoke(self, request: Any) -> LLMInvocation:
        self.requests.append(request)
        return LLMInvocation(
            request=request,
            response=LLMResponse(raw_response={"text": self.text}, text=self.text, extract_mode="text"),
        )


class _RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute_query(
        self,
        *,
        query: str,
        query_dialect: str,
        requested_limit: int | None = None,
    ) -> AnalyticalQueryExecutionResult:
        self.calls.append(
            {
                "query": query,
                "query_dialect": query_dialect,
                "requested_limit": requested_limit,
            }
        )
        return AnalyticalQueryExecutionResult(
            executable_query=query,
            result=QueryResult(columns=["answer"], rows=[[1]], rowcount=1, source_sql=query),
        )


def test_sql_analysis_tool_uses_request_based_llm_provider() -> None:
    llm = _RecordingLLM("```sql\nSELECT 1 AS answer\n```")
    executor = _RecordingExecutor()
    tool = SqlAnalysisTool(
        llm_provider=llm,  # type: ignore[arg-type]
        context=AnalyticalContext(
            query_scope=SqlQueryScope.dataset,
            asset_type="dataset",
            asset_id="dataset-1",
            asset_name="orders",
            tables=["orders"],
            dialect="postgres",
        ),
        query_executor=executor,
    )

    response = asyncio.run(tool.arun(AnalystQueryRequest(question="Show the answer", limit=50)))

    assert response.sql_canonical == "SELECT 1 AS answer"
    assert response.result is not None
    assert response.result.rows == [[1]]
    assert executor.calls == [
        {
            "query": "SELECT 1 AS answer",
            "query_dialect": "postgres",
            "requested_limit": 50,
        }
    ]

    llm_request = llm.requests[0]
    assert llm_request.purpose == "sql_analysis.generate_sql"
    assert llm_request.temperature == 0.0
    assert llm_request.max_tokens == 1200
    assert llm_request.messages[0].role == "user"
    assert "Show the answer" in llm_request.messages[0].content
