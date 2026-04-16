import pathlib
import sys
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from langbridge.orchestrator.agents.analyst.agent import AnalystAgent  # noqa: E402
from langbridge.orchestrator.definitions import AnalystQueryScopePolicy  # noqa: E402
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
from langbridge.orchestrator.tools.semantic_search.interfaces import (  # noqa: E402
    SemanticSearchResult,
    SemanticSearchResultCollection,
)
from langbridge.runtime.models import SqlQueryScope  # noqa: E402


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


def _dataset_binding(dataset_id: str = "dataset-1", dataset_name: str = "sales_dataset") -> AnalyticalDatasetBinding:
    return AnalyticalDatasetBinding(
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        sql_alias="sales",
    )


class _StubTool:
    def __init__(
        self,
        responses: list[AnalystQueryResponse],
        *,
        asset_type: str = "dataset",
        asset_id: str = "dataset-1",
        asset_name: str = "sales_dataset",
        binding_name: str = "sales",
        query_scope: SqlQueryScope = SqlQueryScope.dataset,
        query_scope_policy: AnalystQueryScopePolicy = AnalystQueryScopePolicy.semantic_preferred,
        priority: int = 0,
        keywords: set[str] | None = None,
    ) -> None:
        self.context = AnalyticalContext(
            query_scope=query_scope,
            asset_type=asset_type,
            asset_id=asset_id,
            asset_name=asset_name,
            datasets=[_dataset_binding()],
            tables=["sales"],
        )
        self.binding_name = binding_name
        self.binding_description = None
        self.query_scope_policy = query_scope_policy
        self.priority = priority
        self._keywords = keywords or {"sales", "revenue", "region"}
        self._responses = list(responses)
        self.calls: list[AnalystQueryRequest] = []

    @property
    def name(self) -> str:
        return self.context.asset_name

    @property
    def asset_type(self) -> str:
        return self.context.asset_type

    @property
    def query_scope(self) -> SqlQueryScope:
        return self.context.query_scope

    def describe_for_selection(self, *, tool_id: str) -> dict[str, Any]:
        return {
            "id": tool_id,
            "binding_name": self.binding_name,
            "query_scope_policy": self.query_scope_policy.value,
            "query_scope": self.query_scope.value,
            "asset_type": self.context.asset_type,
            "asset_name": self.context.asset_name,
            "datasets": [{"dataset_name": "sales_dataset", "sql_alias": "sales"}],
            "tables": ["sales"],
            "dimensions": [{"name": "sales.region"}],
            "measures": [{"name": "sales.revenue"}],
            "metrics": [],
        }

    def selection_keywords(self) -> set[str]:
        return set(self._keywords)

    async def arun(self, request: AnalystQueryRequest) -> AnalystQueryResponse:
        self.calls.append(request.model_copy(deep=True))
        return self._responses.pop(0)


class _StubSemanticSearchTool:
    def __init__(self, name: str, results: list[SemanticSearchResult]) -> None:
        self._name = name
        self._results = list(results)
        self.calls: list[dict[str, Any]] = []

    @property
    def name(self) -> str:
        return self._name

    async def search(self, query: str, top_k: int = 5) -> SemanticSearchResultCollection:
        self.calls.append({"query": query, "top_k": top_k})
        return SemanticSearchResultCollection(results=list(self._results))


def _response(
    *,
    status: AnalystOutcomeStatus,
    query_scope: SqlQueryScope,
    asset_type: str = "dataset",
    asset_id: str = "dataset-1",
    asset_name: str = "sales_dataset",
    message: str | None = None,
    rows: list[tuple[Any, ...]] | None = None,
    recoverable: bool = False,
    terminal: bool = True,
    metadata: dict[str, Any] | None = None,
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
        analysis_path="semantic_model" if asset_type == "semantic_model" else "dataset",
        query_scope=query_scope,
        execution_mode="federated",
        asset_type=asset_type,
        asset_id=asset_id,
        asset_name=asset_name,
        selected_semantic_model_id=asset_id if asset_type == "semantic_model" else None,
        sql_canonical="select * from sales",
        sql_executable="select * from sales",
        dialect="postgres",
        selected_datasets=[_dataset_binding()],
        result=result,
        error=message,
        outcome=AnalystExecutionOutcome(
            status=status,
            stage=AnalystOutcomeStage.query if status == AnalystOutcomeStatus.query_error else AnalystOutcomeStage.result,
            message=message,
            original_error=message,
            recoverable=recoverable,
            terminal=terminal,
            metadata=dict(metadata or {}),
        ),
    )


def test_analyst_agent_returns_structured_success_outcome() -> None:
    tool = _StubTool([_response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.dataset, rows=[("US", 10)])])
    agent = AnalystAgent(_RewriteLLM(), [tool])

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.success
    assert response.outcome.selected_tool_name == "sales_dataset"
    assert response.outcome.retry_count == 0
    assert response.outcome.attempted_query_scope == SqlQueryScope.dataset
    assert response.outcome.final_query_scope == SqlQueryScope.dataset
    assert response.error is None


def test_analyst_agent_retries_recoverable_query_error_once() -> None:
    tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.query_error,
                query_scope=SqlQueryScope.dataset,
                message="Canonical SQL failed to parse.",
                recoverable=True,
                terminal=False,
            ),
            _response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.dataset, rows=[("US", 10)]),
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


def test_analyst_agent_falls_back_from_semantic_to_dataset_when_policy_allows() -> None:
    semantic_tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.query_error,
                query_scope=SqlQueryScope.semantic,
                asset_type="semantic_model",
                asset_id="semantic-model-1",
                asset_name="sales_governed",
                message="Semantic SQL scope does not support the requested join.",
                metadata={"scope_fallback_eligible": True},
            )
        ],
        asset_type="semantic_model",
        asset_id="semantic-model-1",
        asset_name="sales_governed",
        binding_name="sales",
        query_scope=SqlQueryScope.semantic,
        query_scope_policy=AnalystQueryScopePolicy.semantic_preferred,
        keywords={"sales", "revenue", "region"},
    )
    dataset_tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.success,
                query_scope=SqlQueryScope.dataset,
                asset_type="dataset",
                asset_id="dataset-1",
                asset_name="sales_dataset",
                rows=[("US", 10)],
            )
        ],
        binding_name="sales",
        query_scope=SqlQueryScope.dataset,
        query_scope_policy=AnalystQueryScopePolicy.semantic_preferred,
        keywords={"sales", "revenue", "region"},
    )
    agent = AnalystAgent(_RewriteLLM(), [semantic_tool, dataset_tool], max_retries=1)

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.success
    assert response.outcome.attempted_query_scope == SqlQueryScope.semantic
    assert response.outcome.final_query_scope == SqlQueryScope.dataset
    assert response.outcome.fallback_from_query_scope == SqlQueryScope.semantic
    assert response.outcome.fallback_to_query_scope == SqlQueryScope.dataset
    assert response.outcome.fallback_reason == "Semantic SQL scope does not support the requested join."
    assert response.outcome.selected_asset_name == "sales_dataset"
    assert response.outcome.selected_dataset_ids == ["dataset-1"]
    assert any(action.action == "fallback_query_scope" for action in response.outcome.recovery_actions)
    assert len(semantic_tool.calls) == 1
    assert len(dataset_tool.calls) == 1
    assert dataset_tool.calls[0].error_history == ["Semantic SQL scope does not support the requested join."]


def test_analyst_agent_rewrites_unsupported_semantic_shape_before_dataset_fallback() -> None:
    semantic_failure_metadata = {
        "scope_fallback_eligible": True,
        "semantic_failure_kind": "unsupported_semantic_sql_shape",
    }
    semantic_tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.query_error,
                query_scope=SqlQueryScope.semantic,
                asset_type="semantic_model",
                asset_id="semantic-model-1",
                asset_name="sales_governed",
                message="Semantic SQL filters only support literal values.",
                metadata=semantic_failure_metadata,
            ),
            _response(
                status=AnalystOutcomeStatus.query_error,
                query_scope=SqlQueryScope.semantic,
                asset_type="semantic_model",
                asset_id="semantic-model-1",
                asset_name="sales_governed",
                message="Semantic SQL filters only support literal values.",
                metadata=semantic_failure_metadata,
            ),
        ],
        asset_type="semantic_model",
        asset_id="semantic-model-1",
        asset_name="sales_governed",
        binding_name="sales",
        query_scope=SqlQueryScope.semantic,
        query_scope_policy=AnalystQueryScopePolicy.semantic_preferred,
        keywords={"sales", "revenue", "region"},
    )
    dataset_tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.success,
                query_scope=SqlQueryScope.dataset,
                asset_type="dataset",
                asset_id="dataset-1",
                asset_name="sales_dataset",
                rows=[("US", 10)],
            )
        ],
        binding_name="sales",
        query_scope=SqlQueryScope.dataset,
        query_scope_policy=AnalystQueryScopePolicy.semantic_preferred,
        keywords={"sales", "revenue", "region"},
    )
    llm = _RewriteLLM("Revenue by region from 2021-01-01 to 2025-12-31")
    agent = AnalystAgent(llm, [semantic_tool, dataset_tool], max_retries=1)

    response = agent.answer("Revenue by region over the last five years")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.success
    assert response.outcome.attempted_query_scope == SqlQueryScope.semantic
    assert response.outcome.final_query_scope == SqlQueryScope.dataset
    assert response.outcome.fallback_from_query_scope == SqlQueryScope.semantic
    assert response.outcome.fallback_to_query_scope == SqlQueryScope.dataset
    assert response.outcome.retry_attempted is True
    assert response.outcome.rewrite_attempted is True
    assert len(semantic_tool.calls) == 2
    assert semantic_tool.calls[1].question == "Revenue by region from 2021-01-01 to 2025-12-31"
    assert len(dataset_tool.calls) == 1
    assert dataset_tool.calls[0].error_history == [
        "Semantic SQL filters only support literal values.",
        "Semantic SQL filters only support literal values.",
    ]
    assert [action.action for action in response.outcome.recovery_actions] == [
        "retry_query",
        "rewrite_question",
        "fallback_query_scope",
    ]


def test_analyst_agent_does_not_fallback_when_policy_forbids_it() -> None:
    semantic_tool = _StubTool(
        [
            _response(
                status=AnalystOutcomeStatus.query_error,
                query_scope=SqlQueryScope.semantic,
                asset_type="semantic_model",
                asset_id="semantic-model-1",
                asset_name="sales_governed",
                message="Semantic SQL scope does not support the requested join.",
                metadata={"scope_fallback_eligible": True},
            )
        ],
        asset_type="semantic_model",
        asset_id="semantic-model-1",
        asset_name="sales_governed",
        binding_name="sales",
        query_scope=SqlQueryScope.semantic,
        query_scope_policy=AnalystQueryScopePolicy.semantic_only,
        keywords={"sales", "revenue", "region"},
    )
    dataset_tool = _StubTool(
        [_response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.dataset, rows=[("US", 10)])],
        binding_name="sales",
        query_scope=SqlQueryScope.dataset,
        query_scope_policy=AnalystQueryScopePolicy.semantic_only,
        keywords={"sales", "revenue", "region"},
    )
    agent = AnalystAgent(_RewriteLLM(), [semantic_tool, dataset_tool], max_retries=0)

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.query_error
    assert response.outcome.attempted_query_scope == SqlQueryScope.semantic
    assert response.outcome.final_query_scope == SqlQueryScope.semantic
    assert response.outcome.fallback_from_query_scope is None
    assert response.outcome.fallback_to_query_scope is None
    assert len(semantic_tool.calls) == 1
    assert dataset_tool.calls == []


def test_analyst_agent_respects_dataset_only_policy() -> None:
    semantic_tool = _StubTool(
        [_response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.semantic, rows=[("US", 99)])],
        asset_type="semantic_model",
        asset_id="semantic-model-1",
        asset_name="sales_governed",
        binding_name="sales",
        query_scope=SqlQueryScope.semantic,
        query_scope_policy=AnalystQueryScopePolicy.dataset_only,
        keywords={"sales", "revenue", "region"},
    )
    dataset_tool = _StubTool(
        [_response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.dataset, rows=[("US", 10)])],
        binding_name="sales",
        query_scope=SqlQueryScope.dataset,
        query_scope_policy=AnalystQueryScopePolicy.dataset_only,
        keywords={"sales", "revenue", "region"},
    )
    agent = AnalystAgent(_RewriteLLM(), [semantic_tool, dataset_tool])

    response = agent.answer("Revenue by region")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.success
    assert response.outcome.attempted_query_scope == SqlQueryScope.dataset
    assert response.outcome.final_query_scope == SqlQueryScope.dataset
    assert len(dataset_tool.calls) == 1
    assert semantic_tool.calls == []


def test_analyst_agent_classifies_invalid_request_shape() -> None:
    tool = _StubTool([_response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.dataset, rows=[("US", 10)])])
    agent = AnalystAgent(_RewriteLLM(), [tool])

    response = agent.answer("???")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.invalid_request
    assert response.outcome.stage == AnalystOutcomeStage.request
    assert tool.calls == []


def test_analyst_agent_returns_access_denied_for_explicit_denied_asset_request() -> None:
    tool = _StubTool([_response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.dataset, rows=[("US", 10)])])
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


def test_analyst_agent_runs_semantic_search_tools_for_selected_semantic_asset() -> None:
    semantic_tool = _StubTool(
        [_response(status=AnalystOutcomeStatus.success, query_scope=SqlQueryScope.semantic, rows=[("West", 10)])],
        asset_type="semantic_model",
        asset_id="semantic-model-1",
        asset_name="sales_governed",
        binding_name="sales",
        query_scope=SqlQueryScope.semantic,
        keywords={"sales", "west", "revenue"},
    )
    semantic_search_tool = _StubSemanticSearchTool(
        "sales_governed:sales.region",
        [
            SemanticSearchResult(
                identifier=1,
                score=0.98,
                metadata={
                    "column": "sales.region",
                    "value": "West",
                },
            )
        ],
    )
    agent = AnalystAgent(
        _RewriteLLM(),
        [semantic_tool],
        semantic_search_tools_by_asset={"semantic-model-1": [semantic_search_tool]},
    )

    response = agent.answer("Revenue for the west territory")

    assert response.outcome is not None
    assert response.outcome.status == AnalystOutcomeStatus.success
    assert semantic_search_tool.calls == [
        {"query": "Revenue for the west territory", "top_k": 10}
    ]
    assert semantic_tool.calls[0].semantic_search_result_prompts == [
        "Column: sales.region, Value: West (Score: 0.9800)"
    ]
