import pathlib
import sys
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from langbridge.orchestrator.agents.analyst.agent import AnalystAgent  # noqa: E402
from langbridge.orchestrator.runtime.access_policy import (  # noqa: E402
    AnalyticalAccessScope,
    AnalyticalDeniedAsset,
)
from langbridge.orchestrator.tools.sql_analyst.interfaces import (  # noqa: E402
    AnalyticalContext,
    AnalyticalDatasetBinding,
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryRequest,
    AnalystQueryResponse,
    QueryResult,
)


class _RewriteLLM:
    def __init__(self, rewritten_question: str | None = None) -> None:
        self.rewritten_question = rewritten_question
        self.prompts: list[str] = []

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        _ = (temperature, max_tokens)
        self.prompts.append(prompt)
        if self.rewritten_question is None:
            return "{}"
        return (
            "{"
            f"\"rewritten_question\": \"{self.rewritten_question}\", "
            "\"rationale\": \"Normalize the analytical request.\""
            "}"
        )


class _StubTool:
    def __init__(self, responses: list[AnalystQueryResponse]) -> None:
        self.context = AnalyticalContext(
            asset_type="dataset",
            asset_id="dataset-1",
            asset_name="sales_dataset",
            datasets=[
                AnalyticalDatasetBinding(
                    dataset_id="dataset-1",
                    dataset_name="sales_dataset",
                    sql_alias="sales",
                )
            ],
            tables=["sales"],
        )
        self.priority = 0
        self._responses = list(responses)
        self.calls: list[AnalystQueryRequest] = []

    @property
    def name(self) -> str:
        return self.context.asset_name

    @property
    def asset_type(self) -> str:
        return self.context.asset_type

    def describe_for_selection(self, *, tool_id: str) -> dict[str, Any]:
        return {
            "id": tool_id,
            "asset_type": self.context.asset_type,
            "asset_name": self.context.asset_name,
            "datasets": [{"dataset_name": "sales_dataset", "sql_alias": "sales"}],
            "tables": ["sales"],
            "dimensions": [{"name": "sales.region"}],
            "measures": [{"name": "sales.revenue"}],
            "metrics": [],
        }

    def selection_keywords(self) -> set[str]:
        return {"sales", "revenue", "region"}

    async def arun(self, request: AnalystQueryRequest) -> AnalystQueryResponse:
        self.calls.append(request.model_copy(deep=True))
        return self._responses.pop(0)


def _response(
    *,
    status: AnalystOutcomeStatus,
    message: str | None = None,
    rows: list[tuple[Any, ...]] | None = None,
    recoverable: bool = False,
    terminal: bool = True,
) -> AnalystQueryResponse:
    result = None
    if rows is not None:
        result = QueryResult(
            columns=["region", "revenue"],
            rows=rows,
            rowcount=len(rows),
            elapsed_ms=12,
            source_sql="select * from sales",
        )
    return AnalystQueryResponse(
        analysis_path="dataset",
        execution_mode="federated",
        asset_type="dataset",
        asset_id="dataset-1",
        asset_name="sales_dataset",
        sql_canonical="select * from sales",
        sql_executable="select * from sales",
        dialect="postgres",
        result=result,
        error=message,
        outcome=AnalystExecutionOutcome(
            status=status,
            stage=AnalystOutcomeStage.query if status == AnalystOutcomeStatus.query_error else AnalystOutcomeStage.result,
            message=message,
            original_error=message,
            recoverable=recoverable,
            terminal=terminal,
        ),
    )


def test_analyst_agent_returns_structured_success_outcome() -> None:
    tool = _StubTool([_response(status=AnalystOutcomeStatus.success, rows=[("US", 10)])])
    agent = AnalystAgent(_RewriteLLM(), [tool])

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.success
    assert response.outcome.selected_tool_name == "sales_dataset"
    assert response.outcome.retry_count == 0
    assert response.error is None


def test_analyst_agent_returns_empty_result_outcome_without_retry_when_not_justified() -> None:
    tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.empty_result,
                message="No rows matched the query.",
                rows=[],
                recoverable=True,
                terminal=False,
            )
        ]
    )
    agent = AnalystAgent(_RewriteLLM(), [tool])

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.empty_result
    assert response.outcome.terminal is True
    assert response.outcome.recoverable is False
    assert response.outcome.retry_attempted is False
    assert len(tool.calls) == 1


def test_analyst_agent_classifies_invalid_request_shape() -> None:
    tool = _StubTool([_response(status=AnalystOutcomeStatus.success, rows=[("US", 10)])])
    agent = AnalystAgent(_RewriteLLM(), [tool])

    response = agent.answer("???")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.invalid_request
    assert response.outcome.stage == AnalystOutcomeStage.request
    assert tool.calls == []


def test_analyst_agent_retries_recoverable_query_error_once() -> None:
    tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.query_error,
                message="Canonical SQL failed to parse.",
                recoverable=True,
                terminal=False,
            ),
            _response(status=AnalystOutcomeStatus.success, rows=[("US", 10)]),
        ]
    )
    llm = _RewriteLLM("Total revenue by region")
    agent = AnalystAgent(llm, [tool], max_retries=1)

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.success
    assert response.outcome.retry_attempted is True
    assert response.outcome.retry_count == 1
    assert response.outcome.rewrite_attempted is True
    assert len(tool.calls) == 2
    assert tool.calls[1].question == "Total revenue by region"
    assert tool.calls[1].error_history == ["Canonical SQL failed to parse."]


def test_analyst_agent_stops_on_terminal_query_error_after_bounded_retry() -> None:
    tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.query_error,
                message="Canonical SQL failed to parse.",
                recoverable=True,
                terminal=False,
            ),
            _response(
                status=AnalystOutcomeStatus.query_error,
                message="Canonical SQL failed to parse.",
                recoverable=True,
                terminal=False,
            ),
        ]
    )
    agent = AnalystAgent(_RewriteLLM("Total revenue by region"), [tool], max_retries=1)

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.query_error
    assert response.outcome.terminal is True
    assert response.outcome.recoverable is False
    assert response.outcome.retry_attempted is True
    assert response.outcome.retry_count == 1
    assert len(tool.calls) == 2


def test_analyst_agent_returns_access_denied_for_explicit_denied_asset_request() -> None:
    tool = _StubTool([_response(status=AnalystOutcomeStatus.success, rows=[("US", 10)])])
    access_scope = AnalyticalAccessScope(
        policy_enforced=True,
        authorized_asset_count=1,
        denied_assets=(
            AnalyticalDeniedAsset(
                asset_type="dataset",
                asset_id="dataset-blocked",
                asset_name="payroll_dataset",
                dataset_names=("payroll_dataset",),
                sql_aliases=("payroll",),
                policy_rule="denied_connectors",
                policy_rationale="Connector is explicitly denied.",
            ),
        ),
    )
    agent = AnalystAgent(_RewriteLLM(), [tool], access_scope=access_scope)

    response = agent.answer("Show payroll_dataset revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.access_denied
    assert response.outcome.stage == AnalystOutcomeStage.authorization
    assert response.outcome.terminal is True
    assert response.outcome.recoverable is False
    assert response.outcome.selected_asset_name == "payroll_dataset"
    assert response.outcome.metadata["policy_rule"] == "denied_connectors"
    assert tool.calls == []
