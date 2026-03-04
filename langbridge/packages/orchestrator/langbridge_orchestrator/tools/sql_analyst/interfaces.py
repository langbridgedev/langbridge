"""
Protocol and data model definitions for the SQL analyst tooling.
"""


from typing import Any, List, Protocol, Sequence

from pydantic import BaseModel, Field

from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel


class ConnectorQueryResult(Protocol):
    """
    Runtime type returned by existing connectors.
    """

    columns: Sequence[str]
    rows: Sequence[Sequence[Any]]
    elapsed_ms: int | None
    rowcount: int | None
    sql: str | None


class QueryResult(BaseModel):
    """
    Normalised query result returned by the SQL analyst tool.
    """

    columns: list[str]
    rows: list[Sequence[Any]]
    rowcount: int | None = Field(default=None)
    elapsed_ms: int | None = Field(default=None)
    source_sql: str | None = Field(default=None, description="SQL text the connector executed.")

    @classmethod
    def from_connector(cls, result: ConnectorQueryResult) -> "QueryResult":
        return cls(
            columns=list(result.columns),
            rows=[tuple(row) for row in result.rows],
            rowcount=getattr(result, "rowcount", None),
            elapsed_ms=getattr(result, "elapsed_ms", None),
            source_sql=getattr(result, "sql", None),
        )


class AnalystQueryRequest(BaseModel):
    """
    Request payload for the SQL analyst tool.
    """

    question: str = Field(..., min_length=1)
    conversation_context: str | None = Field(
        default=None,
        description="Optional conversation history to help interpret follow-up questions.",
    )
    filters: dict[str, Any] | None = None
    limit: int | None = Field(default=1000, ge=1)
    semantic_search_result_prompts: List[str] | None = Field(
        default=None,
        description="Optional list of formatted semantic search results to include in the prompt.",
    )

class AnalystQueryResponse(BaseModel):
    """
    Response payload emitted by the SQL analyst tool.
    """

    sql_canonical: str
    sql_executable: str
    dialect: str
    model_name: str
    result: QueryResult | None = None
    error: str | None = None
    execution_time_ms: int | None = None


class FederatedSqlExecutor(Protocol):
    async def execute_sql(
        self,
        *,
        sql: str,
        dialect: str,
        max_rows: int | None = None,
    ) -> QueryResult:
        ...
