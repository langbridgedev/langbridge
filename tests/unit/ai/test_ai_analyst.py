import asyncio

from langbridge.ai import AgentTask, AgentTaskKind, AnalystAgentConfig
from langbridge.ai.agents import AnalystAgent
from langbridge.ai.tools.sql.interfaces import (
    AnalyticalColumn,
    AnalyticalDatasetBinding,
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryResponse,
    QueryResult,
    SqlQueryScope,
)
from langbridge.ai.tools.web_search import WebSearchPolicy, WebSearchResultItem, WebSearchTool


def _run(coro):
    return asyncio.run(coro)


class _FakeLLMProvider:
    async def acomplete(self, prompt: str, **kwargs):
        if "Choose the single best SQL analysis tool" in prompt:
            if '"name": "dataset-orders"' in prompt and '"name": "semantic-orders"' not in prompt:
                return '{"tool_name":"dataset-orders","reason":"Fallback to dataset scope."}'
            return '{"tool_name":"semantic-orders","reason":"Prefer governed semantic scope first."}'
        if "Review governed SQL evidence for a Langbridge analyst workflow" in prompt:
            if "Show current order trend sources" in prompt:
                return (
                    '{"decision":"augment_with_web","reason":"Need current external context.",'
                    '"sufficiency":"partial"}'
                )
            if "Show order trend" in prompt:
                return (
                    '{"decision":"clarify","reason":"Need a tighter filter.",'
                    '"sufficiency":"insufficient",'
                    '"clarification_question":"Which filters, entity, or time period should I use to refine the analysis?"}'
                )
            return '{"decision":"answer","reason":"Governed SQL is sufficient.","sufficiency":"sufficient"}'
        if "Synthesize a final analytical answer for a Langbridge user from governed SQL analysis" in prompt:
            return (
                '{"analysis":"Final answer using governed data and external sources.",'
                '"findings":[{"insight":"Current external context found.","source":"https://example.test/orders"}],'
                '"follow_ups":[]}'
            )
        if "Synthesize source-backed research" in prompt:
            return (
                '{"synthesis":"Evidence synthesis using governed and source evidence.",'
                '"findings":[{"insight":"Governed orders evidence used.","source":"governed_result"}],'
                '"follow_ups":[]}'
            )
        if "Summarize verified SQL analysis" in prompt:
            return '{"analysis":"Fallback answer from dataset-native SQL."}'
        raise AssertionError(prompt)


class _AlternativeGovernedLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "Choose the single best SQL analysis tool" in prompt:
            if '"name": "dataset-orders"' in prompt and '"name": "semantic-orders"' in prompt:
                return '{"tool_name":"dataset-orders","reason":"Try dataset scope first."}'
            if '"name": "semantic-orders"' in prompt:
                return '{"tool_name":"semantic-orders","reason":"Retry with semantic scope."}'
        if "Review governed SQL evidence for a Langbridge analyst workflow" in prompt:
            if '"rows": []' in prompt:
                return (
                    '{"decision":"clarify","reason":"Dataset scope returned no matching rows.",'
                    '"sufficiency":"insufficient",'
                    '"clarification_question":"Which filters, entity, or time period should I use to refine the analysis?"}'
                )
            return '{"decision":"answer","reason":"Semantic scope answered the question.","sufficiency":"sufficient"}'
        return await super().acomplete(prompt, **kwargs)


class _DetailedAnswerLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "Summarize verified SQL analysis" in prompt and "Detail expectation:\ndetailed" in prompt:
            return (
                '{"analysis":"Detailed governed answer: the returned result shows 12 orders for 2026-01-01 in the month '
                'grouping, so the answer should reference that governed evidence directly and note that only the returned '
                'period is visible."}'
            )
        return await super().acomplete(prompt, **kwargs)


class _AutoModeLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "You are the Langbridge analyst agent controller." in prompt:
            if "Do regions with higher support load also underperform on marketing efficiency?" in prompt:
                return '{"agent_mode":"research","reason":"This is a diagnostic relationship question that needs iterative evidence building."}'
            return '{"agent_mode":"sql","reason":"This is a straightforward governed analytical request."}'
        if "Plan the internal evidence path for a Langbridge analyst research workflow." in prompt:
            return (
                '{"objective":"Determine whether higher regional support load is associated with weaker marketing efficiency.",'
                '"question_type":"relationship",'
                '"timeframe":"last 12 months if available",'
                '"required_metrics":["support load","marketing efficiency"],'
                '"required_dimensions":["region"],'
                '"steps":['
                '{"step_id":"e1","action":"query_governed",'
                '"question":"Measure support load by region over the last 12 months.",'
                '"evidence_goal":"Establish the regional support-load signal.",'
                '"expected_signal":"Support load by region.",'
                '"success_criteria":"At least one regional support-load row is returned.","depends_on":[]},'
                '{"step_id":"e2","action":"query_governed",'
                '"question":"Measure marketing efficiency by region over the last 12 months.",'
                '"evidence_goal":"Establish the regional marketing-efficiency signal.",'
                '"expected_signal":"Marketing efficiency by region.",'
                '"success_criteria":"At least one regional efficiency row is returned.","depends_on":["e1"]},'
                '{"step_id":"e3","action":"synthesize",'
                '"evidence_goal":"Compare support load and marketing efficiency by region.",'
                '"success_criteria":"The answer gives a direct verdict with caveats.","depends_on":["e1","e2"]}'
                '],'
                '"synthesis_requirements":["State the verdict first.","Compare regions at a shared grain."],'
                '"external_context_needed":false,'
                '"visualization_recommendation":{"recommendation":"helpful","chart_type":"scatter",'
                '"rationale":"A scatter plot is useful for comparing load and efficiency."}}'
            )
        if "You are orchestrating a bounded Langbridge analyst research workflow." in prompt:
            if '"governed_round_count": 0' in prompt:
                return (
                    '{"action":"query_governed","rationale":"Start by measuring the relevant governed signals.",'
                    '"governed_question":"Measure support load by region over the last 12 months.",'
                    '"visualization_recommendation":"none","recommended_chart_type":null}'
                )
            if '"governed_round_count": 1' in prompt:
                return (
                    '{"action":"query_governed","rationale":"Gather the matching efficiency metric before concluding.",'
                    '"governed_question":"Measure marketing efficiency by region over the last 12 months.",'
                    '"visualization_recommendation":"none","recommended_chart_type":null}'
                )
            return (
                '{"action":"synthesize","rationale":"The governed evidence is sufficient to answer.",'
                '"visualization_recommendation":"helpful","recommended_chart_type":"scatter"}'
            )
        if "Synthesize source-backed research" in prompt:
            return (
                '{"synthesis":"Higher support load does not clearly map to worse marketing efficiency across the returned regions.",'
                '"verdict":"No clear underperformance pattern is visible across the returned regions.",'
                '"key_comparisons":["Region support load and efficiency were compared across two governed slices."],'
                '"limitations":["Only governed evidence from the configured slices was used."],'
                '"findings":[{"insight":"Governed evidence compared support load and efficiency by region.","source":"governed_result"}],'
                '"follow_ups":[]}'
            )
        return await super().acomplete(prompt, **kwargs)


class _OverClarifyingResearchLLMProvider(_AutoModeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "You are orchestrating a bounded Langbridge analyst research workflow." in prompt:
            if '"governed_round_count": 0' in prompt:
                return (
                    '{"action":"clarify","rationale":"Need the exact efficiency metric and timeframe.",'
                    '"clarification_question":"Which marketing efficiency metric and time period should I use?",'
                    '"visualization_recommendation":"none","recommended_chart_type":null}'
                )
            return (
                '{"action":"synthesize","rationale":"The governed evidence is sufficient to answer with assumptions.",'
                '"visualization_recommendation":"helpful","recommended_chart_type":"scatter"}'
            )
        return await super().acomplete(prompt, **kwargs)


class _OverClarifyingModeSelectionLLMProvider(_AutoModeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "You are the Langbridge analyst agent controller." in prompt:
            return (
                '{"agent_mode":"clarify",'
                '"reason":"Need support_month format and incomplete-period comparison rule.",'
                '"clarification_question":"What is the exact VARCHAR format of '
                "customer_month_support.support_month, and should all regions have all 12 parsed months?\"}"
            )
        return await super().acomplete(prompt, **kwargs)


class _FakeWebSearchProvider:
    name = "fake-web"

    async def search_async(self, query: str, **kwargs):
        _ = (query, kwargs)
        return [
            WebSearchResultItem(
                title="Orders source",
                url="https://example.test/orders",
                snippet="Current orders context.",
                source=self.name,
                rank=1,
            )
        ]


class _FakeSqlTool:
    def __init__(self, *, name: str, asset_type: str, query_scope: SqlQueryScope, response: AnalystQueryResponse):
        self.name = name
        self.description = f"{name} description"
        self.asset_type = asset_type
        self.query_scope = query_scope
        self._response = response
        self.calls = 0

    def describe(self) -> dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "asset_type": self.asset_type,
            "asset_id": self._response.asset_id,
            "asset_name": self._response.asset_name,
            "query_scope": self.query_scope.value,
            "datasets": [],
            "tables": [],
            "dimensions": [],
            "measures": [],
            "metrics": [],
        }

    async def arun(self, request):
        _ = request
        self.calls += 1
        return self._response


class _SequentialSqlTool(_FakeSqlTool):
    def __init__(self, *, name: str, asset_type: str, query_scope: SqlQueryScope, responses: list[AnalystQueryResponse]):
        super().__init__(name=name, asset_type=asset_type, query_scope=query_scope, response=responses[0])
        self._responses = list(responses)

    async def arun(self, request):
        _ = request
        self.calls += 1
        index = min(self.calls - 1, len(self._responses) - 1)
        return self._responses[index]


def _dataset_binding() -> AnalyticalDatasetBinding:
    return AnalyticalDatasetBinding(
        dataset_id="orders",
        dataset_name="Orders",
        sql_alias="orders",
        columns=[AnalyticalColumn(name="month", data_type="date"), AnalyticalColumn(name="orders", data_type="integer")],
    )


def _semantic_failure_response() -> AnalystQueryResponse:
    return AnalystQueryResponse(
        analysis_path="semantic_model",
        query_scope=SqlQueryScope.semantic,
        execution_mode="federated",
        asset_type="semantic_model",
        asset_id="commerce",
        asset_name="commerce",
        selected_semantic_model_id="commerce",
        sql_canonical="SELECT unsupported_shape FROM commerce",
        sql_executable="SELECT unsupported_shape FROM commerce",
        dialect="postgres",
        selected_datasets=[_dataset_binding()],
        result=None,
        error="Semantic SQL scope does not support requested query shape.",
        outcome=AnalystExecutionOutcome(
            status=AnalystOutcomeStatus.query_error,
            stage=AnalystOutcomeStage.query,
            message="Semantic SQL scope does not support requested query shape.",
            original_error="unsupported semantic query shape",
            recoverable=False,
            terminal=False,
            metadata={
                "scope_fallback_eligible": True,
                "semantic_failure_kind": "unsupported_semantic_sql_shape",
            },
        ),
    )


def _semantic_literal_filter_failure_response() -> AnalystQueryResponse:
    message = (
        "Semantic SQL filters only support literal values such as strings, numbers, booleans, NULL, "
        "or literal lists. Raw SQL expressions are not supported in semantic filters."
    )
    return AnalystQueryResponse(
        analysis_path="semantic_model",
        query_scope=SqlQueryScope.semantic,
        execution_mode="federated",
        asset_type="semantic_model",
        asset_id="commerce",
        asset_name="commerce",
        selected_semantic_model_id="commerce",
        sql_canonical="SELECT month, signups FROM commerce WHERE signup_date >= CURRENT_DATE - INTERVAL '12 months'",
        sql_executable="SELECT month, signups FROM commerce WHERE signup_date >= CURRENT_DATE - INTERVAL '12 months'",
        dialect="postgres",
        selected_datasets=[_dataset_binding()],
        result=None,
        error=message,
        outcome=AnalystExecutionOutcome(
            status=AnalystOutcomeStatus.query_error,
            stage=AnalystOutcomeStage.query,
            message=message,
            original_error="invalid semantic filter expression",
            recoverable=False,
            terminal=False,
            metadata={},
        ),
    )


def _dataset_success_response() -> AnalystQueryResponse:
    return AnalystQueryResponse(
        analysis_path="dataset",
        query_scope=SqlQueryScope.dataset,
        execution_mode="federated",
        asset_type="dataset",
        asset_id="orders",
        asset_name="Orders",
        sql_canonical="SELECT month, COUNT(*) AS orders FROM orders GROUP BY month",
        sql_executable="SELECT month, COUNT(*) AS orders FROM orders GROUP BY month",
        dialect="postgres",
        selected_datasets=[_dataset_binding()],
        result=QueryResult(columns=["month", "orders"], rows=[("2026-01-01", 12)], rowcount=1),
        outcome=AnalystExecutionOutcome(
            status=AnalystOutcomeStatus.success,
            stage=AnalystOutcomeStage.result,
            recoverable=False,
            terminal=True,
        ),
    )


def _semantic_success_response() -> AnalystQueryResponse:
    return AnalystQueryResponse(
        analysis_path="semantic_model",
        query_scope=SqlQueryScope.semantic,
        execution_mode="federated",
        asset_type="semantic_model",
        asset_id="commerce",
        asset_name="commerce",
        selected_semantic_model_id="commerce",
        sql_canonical="SELECT month, COUNT(*) AS orders FROM commerce GROUP BY month",
        sql_executable="SELECT month, COUNT(*) AS orders FROM commerce GROUP BY month",
        dialect="postgres",
        selected_datasets=[_dataset_binding()],
        result=QueryResult(columns=["month", "orders"], rows=[("2026-01-01", 12)], rowcount=1),
        outcome=AnalystExecutionOutcome(
            status=AnalystOutcomeStatus.success,
            stage=AnalystOutcomeStage.result,
            recoverable=False,
            terminal=True,
            attempted_query_scope=SqlQueryScope.semantic,
            final_query_scope=SqlQueryScope.semantic,
            selected_semantic_model_id="commerce",
            selected_dataset_ids=["orders"],
        ),
    )


def _empty_dataset_response() -> AnalystQueryResponse:
    return AnalystQueryResponse(
        analysis_path="dataset",
        query_scope=SqlQueryScope.dataset,
        execution_mode="federated",
        asset_type="dataset",
        asset_id="orders",
        asset_name="Orders",
        sql_canonical="SELECT month, COUNT(*) AS orders FROM orders GROUP BY month",
        sql_executable="SELECT month, COUNT(*) AS orders FROM orders GROUP BY month",
        dialect="postgres",
        selected_datasets=[_dataset_binding()],
        result=QueryResult(columns=["month", "orders"], rows=[], rowcount=0),
        outcome=AnalystExecutionOutcome(
            status=AnalystOutcomeStatus.empty_result,
            stage=AnalystOutcomeStage.result,
            message="No rows matched the query.",
            recoverable=False,
            terminal=True,
        ),
    )


def test_analyst_falls_back_from_semantic_to_dataset_scope() -> None:
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "semantic_preferred",
                },
            }
        ),
        sql_analysis_tools=[
            _FakeSqlTool(
                name="semantic-orders",
                asset_type="semantic_model",
                query_scope=SqlQueryScope.semantic,
                response=_semantic_failure_response(),
            ),
            _FakeSqlTool(
                name="dataset-orders",
                asset_type="dataset",
                query_scope=SqlQueryScope.dataset,
                response=_dataset_success_response(),
            ),
        ],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-fallback",
                task_kind=AgentTaskKind.analyst,
                question="Show first order date by month",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.diagnostics["fallback"]["from_scope"] == "semantic"
    assert result.diagnostics["fallback"]["to_scope"] == "dataset"
    assert result.output["query_scope"] == "dataset"
    assert result.output["error_taxonomy"]["kind"] == "unsupported_semantic_sql_shape"
    assert result.output["outcome"]["attempted_query_scope"] == "semantic"
    assert result.output["outcome"]["final_query_scope"] == "dataset"
    assert result.output["outcome"]["fallback_to_query_scope"] == "dataset"
    assert result.output["outcome"]["recovery_actions"][-1]["action"] == "fallback_query_scope"


def test_analyst_falls_back_from_semantic_to_dataset_scope_when_literal_filter_error_is_only_in_text() -> None:
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "semantic_preferred",
                },
            }
        ),
        sql_analysis_tools=[
            _FakeSqlTool(
                name="semantic-orders",
                asset_type="semantic_model",
                query_scope=SqlQueryScope.semantic,
                response=_semantic_literal_filter_failure_response(),
            ),
            _FakeSqlTool(
                name="dataset-orders",
                asset_type="dataset",
                query_scope=SqlQueryScope.dataset,
                response=_dataset_success_response(),
            ),
        ],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-fallback-literal-filter-text",
                task_kind=AgentTaskKind.analyst,
                question="Analyze cost per signup over the last 12 months by region",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.diagnostics["fallback"]["from_scope"] == "semantic"
    assert result.diagnostics["fallback"]["to_scope"] == "dataset"
    assert result.output["query_scope"] == "dataset"
    assert result.output["error_taxonomy"]["fallback_eligible"] is True
    assert "literal values" in result.output["error_taxonomy"]["message"]


def test_analyst_sql_can_augment_with_web_sources() -> None:
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True},
                "web_search_scope": {"enabled": True},
            }
        ),
        sql_analysis_tools=[
            _FakeSqlTool(
                name="dataset-orders",
                asset_type="dataset",
                query_scope=SqlQueryScope.dataset,
                response=_dataset_success_response(),
            )
        ],
        web_search_tool=WebSearchTool(
            provider=_FakeWebSearchProvider(),
            policy=WebSearchPolicy(allowed_domains=[], denied_domains=[]),
        ),
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-augment-web",
                task_kind=AgentTaskKind.analyst,
                question="Show current order trend sources",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.output["analysis"] == "Final answer using governed data and external sources."
    assert result.output["sources"][0]["url"] == "https://example.test/orders"
    assert result.output["evidence"]["assessment"]["decision"] == "augment_with_web"
    assert result.output["evidence"]["external"]["used"] is True
    assert result.output["review_hints"]["evidence_review_decision"] == "augment_with_web"
    assert result.output["review_hints"]["external_augmentation_used"] is True
    assert result.diagnostics["web_search"]["provider"] == "fake-web"


def test_analyst_sql_respects_external_augmentation_budget() -> None:
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True},
                "web_search_scope": {"enabled": True},
                "execution": {"max_external_augmentations": 0},
            }
        ),
        sql_analysis_tools=[
            _FakeSqlTool(
                name="dataset-orders",
                asset_type="dataset",
                query_scope=SqlQueryScope.dataset,
                response=_dataset_success_response(),
            )
        ],
        web_search_tool=WebSearchTool(
            provider=_FakeWebSearchProvider(),
            policy=WebSearchPolicy(allowed_domains=[], denied_domains=[]),
        ),
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-augment-budget",
                task_kind=AgentTaskKind.analyst,
                question="Show current order trend sources",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.output["analysis"] == "Fallback answer from dataset-native SQL."
    assert result.output["sources"] == []
    assert result.output["evidence"]["external"]["used"] is False
    assert result.output["review_hints"]["external_augmentation_used"] is False
    assert result.output["review_hints"]["evidence_review_decision"] == "augment_with_web"
    assert result.diagnostics["web_search"] is None


def test_analyst_sql_empty_result_marks_weak_evidence() -> None:
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
            }
        ),
        sql_analysis_tools=[
            _FakeSqlTool(
                name="dataset-orders",
                asset_type="dataset",
                query_scope=SqlQueryScope.dataset,
                response=_empty_dataset_response(),
            )
        ],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-empty-result",
                task_kind=AgentTaskKind.analyst,
                question="Show order trend",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "needs_clarification"
    assert result.error == "Which filters, entity, or time period should I use to refine the analysis?"
    assert result.diagnostics["weak_evidence"] is True
    assert result.diagnostics["evidence_review"]["decision"] == "clarify"
    assert result.output["evidence"]["governed"]["answered_question"] is False
    assert result.output["evidence"]["assessment"]["decision"] == "clarify"
    assert result.output["review_hints"]["governed_empty_result"] is True


def test_analyst_sql_retries_with_alternative_governed_tool_before_clarifying() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_empty_dataset_response(),
    )
    semantic_tool = _FakeSqlTool(
        name="semantic-orders",
        asset_type="semantic_model",
        query_scope=SqlQueryScope.semantic,
        response=_semantic_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_AlternativeGovernedLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "execution": {
                    "max_governed_attempts": 2,
                    "max_evidence_rounds": 2,
                },
            }
        ),
        sql_analysis_tools=[dataset_tool, semantic_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-governed-retry",
                task_kind=AgentTaskKind.analyst,
                question="Show order trend with governed retry",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert dataset_tool.calls == 1
    assert semantic_tool.calls == 1
    assert result.output["query_scope"] == "semantic"
    assert result.output["evidence"]["governed"]["attempt_count"] == 2
    assert result.output["evidence"]["governed"]["tools_tried"] == ["dataset-orders", "semantic-orders"]
    assert result.diagnostics["governed_attempt_count"] == 2
    assert result.diagnostics["governed_tools_tried"] == ["dataset-orders", "semantic-orders"]


def test_analyst_sql_respects_governed_attempt_budget_before_clarifying() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_empty_dataset_response(),
    )
    semantic_tool = _FakeSqlTool(
        name="semantic-orders",
        asset_type="semantic_model",
        query_scope=SqlQueryScope.semantic,
        response=_semantic_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_AlternativeGovernedLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "execution": {
                    "max_governed_attempts": 2,
                    "max_evidence_rounds": 1,
                },
            }
        ),
        sql_analysis_tools=[dataset_tool, semantic_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-governed-budget",
                task_kind=AgentTaskKind.analyst,
                question="Show order trend with governed retry",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "needs_clarification"
    assert dataset_tool.calls == 1
    assert semantic_tool.calls == 0
    assert result.output["evidence"]["governed"]["attempt_count"] == 1
    assert result.diagnostics["governed_attempt_count"] == 1


def test_analyst_research_mode_uses_governed_seed_before_external_sources() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_dataset_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True, "max_sources": 3},
                "web_search_scope": {"enabled": True},
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-research-governed-seed",
                task_kind=AgentTaskKind.analyst,
                question="Research order trend evidence",
                input={"mode": "research"},
                context={
                    "sources": [
                        {
                            "title": "Orders source",
                            "url": "https://example.test/orders",
                            "snippet": "Current orders context.",
                        }
                    ]
                },
            )
        )
    )

    assert result.status.value == "succeeded"
    assert dataset_tool.calls == 1
    assert result.output["synthesis"] == "Evidence synthesis using governed and source evidence."
    assert result.output["result"]["rows"] == [["2026-01-01", 12]]
    assert result.output["sources"][0]["url"] == "https://example.test/orders"
    assert result.output["evidence"]["governed"]["attempted"] is True
    assert result.output["evidence"]["governed"]["attempt_count"] == 1
    assert result.output["findings"][0]["source"] == "governed_result"
    assert result.diagnostics["governed_seeded"] is True
    assert result.diagnostics["governed_attempt_count"] == 1


def test_analyst_research_mode_allows_governed_only_evidence_when_sources_required() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_dataset_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True, "require_sources": True},
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-research-governed-only",
                task_kind=AgentTaskKind.analyst,
                question="Research order trend evidence",
                input={"mode": "research"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert dataset_tool.calls == 1
    assert result.output["sources"] == []
    assert result.output["synthesis"] == "Evidence synthesis using governed and source evidence."
    assert result.output["result"]["rows"] == [["2026-01-01", 12]]
    assert result.output["evidence"]["governed"]["attempted"] is True
    assert result.output["evidence"]["governed"]["attempt_count"] == 1
    assert result.output["evidence"]["external"]["used"] is False
    assert result.diagnostics["weak_evidence"] is False
    assert result.diagnostics["governed_seeded"] is True


def test_analyst_research_mode_still_seeds_governed_sql_when_context_result_is_empty_dict() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_dataset_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True},
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-research-empty-context-result",
                task_kind=AgentTaskKind.analyst,
                question="Research order trend evidence",
                input={"mode": "research"},
                context={"result": {}},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert dataset_tool.calls == 1
    assert result.output["result"]["rows"] == [["2026-01-01", 12]]
    assert result.output["evidence"]["governed"]["attempted"] is True
    assert result.diagnostics["governed_seeded"] is True


def test_analyst_sql_uses_detailed_prompt_for_evidence_heavy_questions() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_dataset_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_DetailedAnswerLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-detailed-answer",
                task_kind=AgentTaskKind.analyst,
                question="Provide detailed evidence for the order trend",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.output["analysis"].startswith("Detailed governed answer:")
    assert "12 orders for 2026-01-01" in result.output["analysis"]


def test_analyst_treats_relationship_question_as_detailed() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_dataset_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_DetailedAnswerLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-relationship-detailed-answer",
                task_kind=AgentTaskKind.analyst,
                question="Do regions with higher support load also underperform on marketing efficiency?",
                input={"mode": "sql"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.output["analysis"].startswith("Detailed governed answer:")


def test_analyst_auto_mode_chooses_sql_for_straightforward_governed_question() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_dataset_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_AutoModeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "commerce",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True},
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-auto-sql",
                task_kind=AgentTaskKind.analyst,
                question="Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                input={"mode": "auto"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.diagnostics["agent_mode"] == "sql"
    assert result.diagnostics["mode_decision"]["agent_mode"] == "sql"
    assert dataset_tool.calls == 1


def test_analyst_auto_mode_research_runs_multiple_governed_rounds_before_synthesis() -> None:
    dataset_tool = _SequentialSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        responses=[_dataset_success_response(), _dataset_success_response()],
    )
    agent = AnalystAgent(
        llm_provider=_AutoModeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "growth",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True},
                "execution": {
                    "max_governed_attempts": 3,
                    "max_evidence_rounds": 3,
                },
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-auto-research",
                task_kind=AgentTaskKind.analyst,
                question="Do regions with higher support load also underperform on marketing efficiency?",
                input={"mode": "auto"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert result.diagnostics["agent_mode"] == "research"
    assert result.diagnostics["mode_decision"]["agent_mode"] == "research"
    assert dataset_tool.calls == 2
    assert result.diagnostics["research_state"]["governed_round_count"] == 2
    assert len(result.diagnostics["research_steps"]) == 3
    assert result.output["evidence_plan"]["question_type"] == "relationship"
    assert result.output["evidence_plan"]["steps"][0]["question"] == (
        "Measure support load by region over the last 12 months."
    )
    assert result.output["evidence_bundle"]["assessment"]["governed_round_count"] == 2
    assert result.output["evidence_bundle"]["governed_rounds"][0]["output"]["result"]["rows"] == [
        ["2026-01-01", 12]
    ]
    assert result.output["evidence"]["bundle"]["assessment"]["governed_round_count"] == 2
    assert result.diagnostics["evidence_bundle_assessment"]["answered_by_governed"] is True
    assert result.output["visualization_recommendation"]["recommendation"] == "helpful"
    assert result.output["visualization_recommendation"]["chart_type"] == "scatter"
    assert result.output["recommended_chart_type"] == "scatter"


def test_analyst_research_defers_metric_timeframe_clarification_until_after_governed_evidence() -> None:
    dataset_tool = _FakeSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        response=_dataset_success_response(),
    )
    agent = AnalystAgent(
        llm_provider=_OverClarifyingResearchLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "growth",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True},
                "execution": {
                    "max_governed_attempts": 2,
                    "max_evidence_rounds": 2,
                },
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-research-assumption-first",
                task_kind=AgentTaskKind.analyst,
                question="Do regions with higher support load also underperform on marketing efficiency?",
                input={"mode": "auto"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert dataset_tool.calls == 1
    assert result.diagnostics["research_steps"][0]["action"] == "query_governed"
    assert "Deferring clarification" in result.diagnostics["research_steps"][0]["rationale"]
    assert result.diagnostics["research_state"]["governed_round_count"] == 1


def test_analyst_auto_mode_defers_internal_format_clarification_until_after_governed_evidence() -> None:
    dataset_tool = _SequentialSqlTool(
        name="dataset-orders",
        asset_type="dataset",
        query_scope=SqlQueryScope.dataset,
        responses=[_dataset_success_response(), _dataset_success_response()],
    )
    agent = AnalystAgent(
        llm_provider=_OverClarifyingModeSelectionLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "growth",
                "analyst_scope": {
                    "semantic_models": ["commerce"],
                    "datasets": ["orders"],
                    "query_policy": "dataset_preferred",
                },
                "research_scope": {"enabled": True},
                "execution": {
                    "max_governed_attempts": 2,
                    "max_evidence_rounds": 2,
                },
            }
        ),
        sql_analysis_tools=[dataset_tool],
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="analyst-auto-internal-format-clarification",
                task_kind=AgentTaskKind.analyst,
                question="Do regions with higher support load also underperform on marketing efficiency?",
                input={"mode": "auto"},
            )
        )
    )

    assert result.status.value == "succeeded"
    assert dataset_tool.calls == 2
    assert result.diagnostics["agent_mode"] == "research"
    assert result.diagnostics["mode_decision"]["agent_mode"] == "research"
    assert "support_month" in result.diagnostics["mode_decision"]["deferred_clarification_question"]
    assert result.diagnostics["research_state"]["governed_round_count"] == 2
