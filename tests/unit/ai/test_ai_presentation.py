import asyncio

from langbridge.ai.agents.presentation import PresentationAgent
from langbridge.ai.tools.charting import ChartingTool


def _run(coro):
    return asyncio.run(coro)


class _PromptCheckingLLMProvider:
    async def acomplete(self, prompt: str, **kwargs):
        assert "Compose the final Langbridge response" in prompt
        assert "Decide the answer depth from the question and evidence" in prompt
        assert "Use a detailed answer when the user asks for explanation, evidence, comparisons, drivers, caveats, or source-backed reasoning." in prompt
        assert "If a structured visualization recommendation is provided, honor it when it is compatible with the verified data" in prompt
        assert "answer_markdown" in prompt
        assert "Available artifacts:" in prompt
        assert '"role": "primary_result"' in prompt
        assert '"type": "diagnostics"' in prompt
        assert '"analysis": "Detailed governed answer with evidence."' in prompt
        assert kwargs["max_tokens"] == 2400
        return (
            '{"summary":"Detailed answer ready.",'
            '"result":{},"visualization":null,"research":{},"diagnostics":{"mode":"final"}}'
        )


class _ClarificationPresentationLLMProvider:
    async def acomplete(self, prompt: str, **kwargs):
        assert "Compose the final Langbridge response" in prompt
        return (
            '{"summary":"I need one clarification before I can answer.",'
            '"result":{},"visualization":null,"research":{},'
            '"answer":"Which time period should I use?",'
            '"diagnostics":{"mode":"clarification"}}'
        )


class _AutoVisualPresentationLLMProvider:
    async def acomplete(self, prompt: str, **kwargs):
        if "Create a chart specification for verified tabular data." in prompt:
            assert '"chart_type": "scatter"' in prompt
            assert "Support load vs CAC by region" in prompt
            return (
                '{"chart_type":"scatter","title":"Support load vs CAC by region",'
                '"x":"support_load","y":"cac","series":null,"encoding":{},'
                '"rationale":"A scatter chart best shows whether higher support load tracks with CAC by region."}'
            )
        assert "Compose the final Langbridge response" in prompt
        return (
            '{"summary":"Higher support load does not clearly map to worse CAC.",'
            '"result":{},"visualization":null,"research":{},'
            '"answer":"Higher support load does not clearly map to worse CAC; the regional comparison is easier to read in the attached visual.",'
            '"diagnostics":{"mode":"final"}}'
        )


def test_presentation_falls_back_to_detailed_analysis_when_answer_missing() -> None:
    agent = PresentationAgent(llm_provider=_PromptCheckingLLMProvider())

    response = _run(
        agent.compose(
            question="Explain the detailed evidence for the order trend",
            context={
                "step_results": [
                    {
                        "agent_name": "analyst",
                        "output": {
                            "analysis": "Detailed governed answer with evidence.",
                            "result": {
                                "columns": ["month", "orders"],
                                "rows": [["2026-01-01", 12]],
                            },
                            "evidence": {
                                "governed": {
                                    "attempted": True,
                                    "answered_question": True,
                                }
                            },
                        },
                    }
                ]
            },
        )
    )

    assert response["summary"] == "Detailed answer ready."
    assert response["answer"] == "Detailed governed answer with evidence."
    assert response["result"]["rows"] == [["2026-01-01", 12]]


def test_presentation_does_not_emit_empty_result_for_clarification_only_response() -> None:
    agent = PresentationAgent(llm_provider=_ClarificationPresentationLLMProvider())

    response = _run(
        agent.compose(
            question="Show me revenue by region",
            context={"step_results": []},
        )
    )

    assert response["summary"] == "I need one clarification before I can answer."
    assert response["answer"] == "Which time period should I use?"
    assert response["result"] is None


def test_presentation_auto_adds_visual_for_chartable_relationship_question() -> None:
    provider = _AutoVisualPresentationLLMProvider()
    agent = PresentationAgent(llm_provider=provider, charting_tool=ChartingTool(llm_provider=provider))

    response = _run(
        agent.compose(
            question="Do regions with higher support load also underperform on marketing efficiency?",
            context={
                "visualization_recommendation": {
                    "chart_type": "scatter",
                    "title": "Support load vs CAC by region",
                    "x": "support_load",
                    "y": "cac",
                    "rationale": "A scatter chart makes the support-load/CAC relationship easier to read.",
                },
                "step_results": [
                    {
                        "agent_name": "analyst",
                        "output": {
                            "analysis": "Higher support load does not clearly map to worse CAC across the returned regions.",
                            "result": {
                                "columns": ["region", "support_load", "cac"],
                                "rows": [
                                    ["ANZ", 1.1, 0.082],
                                    ["DACH", 1.17, 0.085],
                                    ["North America", 0.9, 0.084],
                                    ["United Kingdom", 0.8, 0.084],
                                ],
                            },
                        },
                    }
                ]
            },
        )
    )

    assert response["summary"] == "Higher support load does not clearly map to worse CAC."
    assert response["answer"].startswith("Higher support load does not clearly map to worse CAC")
    assert response["visualization"]["chart_type"] == "scatter"


def test_presentation_uses_structured_visualization_recommendation_before_charting_tool() -> None:
    class _RecommendationOnlyLLMProvider:
        async def acomplete(self, prompt: str, **kwargs):
            assert "Visualization recommendation:" in prompt
            return (
                '{"summary":"The verified result is best shown as a bar chart.","result":{},'
                '"visualization":null,"research":{},'
                '"answer":"The verified result is best shown as a bar chart.",'
                '"diagnostics":{"mode":"final"}}'
            )

    agent = PresentationAgent(llm_provider=_RecommendationOnlyLLMProvider(), charting_tool=ChartingTool(llm_provider=_AutoVisualPresentationLLMProvider()))

    response = _run(
        agent.compose(
            question="Show me the result as a chart",
            context={
                "visualization_recommendation": {
                    "chart_type": "bar",
                    "title": "Orders by month",
                    "x": "month",
                    "y": "orders",
                    "rationale": "The result is a monthly distribution, so a bar chart is the clearest view.",
                },
                "step_results": [
                    {
                        "agent_name": "analyst",
                        "output": {
                            "analysis": "Monthly order totals are available.",
                            "result": {
                                "columns": ["month", "orders"],
                                "rows": [["2024-01-01", 12], ["2024-02-01", 15]],
                            },
                        },
                    }
                ],
            },
        )
    )

    assert response["visualization"]["chart_type"] == "bar"
    assert response["visualization"]["x"] == "month"
    assert response["visualization"]["y"] == "orders"


def test_charting_preserves_multiple_requested_measures() -> None:
    class _SingleMeasureChartLLMProvider:
        async def acomplete(self, prompt: str, **kwargs):
            assert '"monthly_net_revenue"' in prompt
            assert '"monthly_gross_margin"' in prompt
            return (
                '{"chart_type":"bar","title":"Order channel performance",'
                '"x":"order_channel","y":"monthly_gross_margin","series":null,'
                '"encoding":{},"rationale":"Compare net revenue and gross margin by channel."}'
            )

    chart = _run(
        ChartingTool(llm_provider=_SingleMeasureChartLLMProvider()).build_chart(
            {
                "columns": ["order_channel", "monthly_net_revenue", "monthly_gross_margin"],
                "rows": [
                    ["Paid Social", 9139.54, 4912.33],
                    ["Organic Search", 8237.29, 4651.32],
                    ["Affiliate", 8080.02, 4471.85],
                ],
            },
            question="Which order channels drove the highest net revenue and gross margin in Q3 2025?",
        )
    )

    assert chart is not None
    assert chart.chart_type == "bar"
    assert chart.y == ["monthly_net_revenue", "monthly_gross_margin"]


def test_charting_normalizes_friendly_pie_measure_to_available_column() -> None:
    class _FriendlyPieChartLLMProvider:
        async def acomplete(self, prompt: str, **kwargs):
            return (
                '{"chart_type":"pie","title":"Q3 net revenue share",'
                '"x":"order_channel","y":"net_revenue","series":null,'
                '"encoding":{},"rationale":"Show net revenue share by channel."}'
            )

    chart = _run(
        ChartingTool(llm_provider=_FriendlyPieChartLLMProvider()).build_chart(
            {
                "columns": ["order_channel", "monthly_net_revenue", "monthly_gross_margin"],
                "rows": [
                    ["Paid Social", 9139.54, 4912.33],
                    ["Organic Search", 8237.29, 4651.32],
                    ["Affiliate", 8080.02, 4471.85],
                ],
            },
            question="Show me in a pie chart",
        )
    )

    assert chart is not None
    assert chart.chart_type == "pie"
    assert chart.x == "order_channel"
    assert chart.y == "monthly_net_revenue"


def test_presentation_inlines_visualization_artifact_for_chart_follow_up() -> None:
    class _ChartFollowUpLLMProvider:
        async def acomplete(self, prompt: str, **kwargs):
            assert "primary_visualization" in prompt
            assert "result_table" in prompt
            return (
                '{"summary":"Pie chart ready.","result":{},"visualization":null,"research":{},'
                '"answer_markdown":"Pie chart view using net revenue.\\n\\nHere is the verified result table.\\n\\n{{artifact:result_table}}",'
                '"artifacts":[{"id":"result_table"}],'
                '"diagnostics":{"mode":"final"}}'
            )

    provider = _ChartFollowUpLLMProvider()
    agent = PresentationAgent(
        llm_provider=provider,
        charting_tool=ChartingTool(llm_provider=provider),
    )

    response = _run(
        agent.compose(
            question="Show me in a pie chart",
            context={
                "chart_request": "Show me in a pie chart",
                "visualization_recommendation": {
                    "chart_type": "pie",
                    "title": "Q3 2025 Net Revenue Share by Order Channel",
                    "x": "order_channel",
                    "y": "net_revenue",
                    "rationale": "Use net revenue as the default metric for the pie chart.",
                },
                "step_results": [
                    {
                        "agent_name": "analyst",
                        "output": {
                            "analysis": "Paid Social led both requested metrics.",
                            "result": {
                                "columns": [
                                    "order_channel",
                                    "monthly_net_revenue",
                                    "monthly_gross_margin",
                                ],
                                "rows": [
                                    ["Paid Social", 9139.54, 4912.33],
                                    ["Organic Search", 8237.29, 4651.32],
                                    ["Affiliate", 8080.02, 4471.85],
                                ],
                            },
                        },
                    }
                ],
            },
        )
    )

    assert response["visualization"]["chart_type"] == "pie"
    assert response["visualization"]["y"] == "monthly_net_revenue"
    assert "{{artifact:primary_visualization}}" in response["answer_markdown"]
    assert response["answer_markdown"].index("{{artifact:primary_visualization}}") < response["answer_markdown"].index(
        "{{artifact:result_table}}"
    )
    assert [artifact["id"] for artifact in response["artifacts"]] == ["primary_visualization", "result_table"]


def test_presentation_returns_typed_chart_table_sql_and_diagnostic_artifacts() -> None:
    class _ArtifactMarkdownLLMProvider:
        async def acomplete(self, prompt: str, **kwargs):
            assert "Available artifacts:" in prompt
            assert "primary_visualization" in prompt
            assert "result_table" in prompt
            assert "generated_sql" in prompt
            assert "execution_diagnostics" in prompt
            assert "{{artifact:artifact_id}}" in prompt
            return (
                '{"summary":"Paid Social led Q3 channel performance.",'
                '"result":{},"visualization":null,"research":{},'
                '"answer":"## Paid Social led Q3 channel performance\\nPaid Social had the highest net revenue and gross margin.\\n\\n{{artifact:primary_visualization}}\\n\\nThe underlying result is included below.\\n\\n{{artifact:result_table}}\\n\\nThe generated SQL is available for audit.\\n\\n{{artifact:generated_sql}}\\n\\nExecution diagnostics are attached for troubleshooting.\\n\\n{{artifact:execution_diagnostics}}",'
                '"answer_markdown":"## Paid Social led Q3 channel performance\\nPaid Social had the highest net revenue and gross margin.\\n\\n{{artifact:primary_visualization}}\\n\\nThe underlying result is included below.\\n\\n{{artifact:result_table}}\\n\\nThe generated SQL is available for audit.\\n\\n{{artifact:generated_sql}}\\n\\nExecution diagnostics are attached for troubleshooting.\\n\\n{{artifact:execution_diagnostics}}",'
                '"artifacts":[{"id":"primary_visualization"},{"id":"result_table"},{"id":"generated_sql"},{"id":"execution_diagnostics"}],'
                '"diagnostics":{"mode":"final"}}'
            )

    provider = _ArtifactMarkdownLLMProvider()
    agent = PresentationAgent(
        llm_provider=provider,
        charting_tool=ChartingTool(llm_provider=provider),
    )

    response = _run(
        agent.compose(
            question="Which order channels drove the highest net revenue and gross margin in Q3 2025?",
            context={
                "visualization_recommendation": {
                    "chart_type": "bar",
                    "title": "Q3 2025 channel performance",
                    "x": "order_channel",
                    "y": ["monthly_net_revenue", "monthly_gross_margin"],
                    "rationale": "Grouped bars compare both requested measures by channel.",
                },
                "step_results": [
                    {
                        "agent_name": "analyst",
                        "task_id": "analyst-step",
                        "status": "succeeded",
                        "output": {
                            "analysis": "Paid Social led both requested metrics.",
                            "result": {
                                "columns": [
                                    "order_channel",
                                    "monthly_net_revenue",
                                    "monthly_gross_margin",
                                ],
                                "rows": [
                                    ["Paid Social", 9139.54, 4912.33],
                                    ["Organic Search", 8237.29, 4651.32],
                                    ["Affiliate", 8080.02, 4471.85],
                                ],
                            },
                            "analysis_path": "dataset",
                            "query_scope": "dataset",
                            "sql_canonical": (
                                "SELECT order_channel, monthly_net_revenue, monthly_gross_margin "
                                "FROM channel_performance"
                            ),
                            "sql_executable": (
                                "SELECT order_channel, monthly_net_revenue, monthly_gross_margin "
                                "FROM channel_performance"
                            ),
                            "dialect": "postgres",
                            "selected_datasets": ["channel_performance"],
                            "outcome": {
                                "status": "success",
                                "stage": "result",
                            },
                            "evidence": {
                                "governed": {
                                    "attempted": True,
                                    "answered_question": True,
                                    "query_scope": "dataset",
                                }
                            },
                        },
                        "diagnostics": {
                            "agent_mode": "sql",
                            "selected_tool": "dataset-channel-performance",
                        },
                    }
                ],
            },
        )
    )

    assert response["summary"] == "Paid Social led Q3 channel performance."
    assert "{{artifact:primary_visualization}}" in response["answer_markdown"]
    assert "{{artifact:result_table}}" in response["answer_markdown"]
    assert "{{artifact:generated_sql}}" in response["answer_markdown"]
    assert "{{artifact:execution_diagnostics}}" in response["answer_markdown"]
    assert response["answer"] == response["answer_markdown"]
    assert [artifact["id"] for artifact in response["artifacts"]] == [
        "primary_visualization",
        "result_table",
        "generated_sql",
        "execution_diagnostics",
    ]
    artifacts_by_id = {artifact["id"]: artifact for artifact in response["artifacts"]}
    assert artifacts_by_id["primary_visualization"]["type"] == "chart"
    assert artifacts_by_id["primary_visualization"]["role"] == "primary_result"
    assert artifacts_by_id["primary_visualization"]["payload"]["chart_type"] == "bar"
    assert artifacts_by_id["primary_visualization"]["data_ref"]["artifact_id"] == "result_table"
    assert artifacts_by_id["result_table"]["type"] == "table"
    assert artifacts_by_id["result_table"]["role"] == "supporting_result"
    assert artifacts_by_id["result_table"]["payload"]["rows"][0][0] == "Paid Social"
    assert artifacts_by_id["generated_sql"]["type"] == "sql"
    assert artifacts_by_id["generated_sql"]["role"] == "supporting_result"
    assert "monthly_net_revenue" in artifacts_by_id["generated_sql"]["payload"]["sql_executable"]
    assert artifacts_by_id["generated_sql"]["provenance"]["query_scope"] == "dataset"
    assert artifacts_by_id["execution_diagnostics"]["type"] == "diagnostics"
    assert artifacts_by_id["execution_diagnostics"]["role"] == "diagnostic"
    assert artifacts_by_id["execution_diagnostics"]["payload"]["agent_diagnostics"]["agent_mode"] == "sql"
    assert response["visualization"]["chart_type"] == "bar"
    assert response["result"]["rows"][0][0] == "Paid Social"


def test_presentation_can_arrange_analyst_owned_artifacts() -> None:
    class _AnalystArtifactLLMProvider:
        async def acomplete(self, prompt: str, **kwargs):
            assert "primary_result" in prompt
            assert "primary_sql" in prompt
            assert "governed_attempts" in prompt
            return (
                '{"summary":"Regional efficiency evidence arranged.",'
                '"result":{},"visualization":null,"research":{},'
                '"answer_markdown":"## Regional efficiency evidence\\nThe verified result is below.\\n\\n{{artifact:primary_result}}\\n\\nThe generated SQL and attempts are available for audit.\\n\\n{{artifact:primary_sql}}\\n\\n{{artifact:governed_attempts}}",'
                '"artifacts":[{"id":"primary_result"},{"id":"primary_sql"},{"id":"governed_attempts"}],'
                '"diagnostics":{"mode":"final"}}'
            )

    agent = PresentationAgent(llm_provider=_AnalystArtifactLLMProvider())

    response = _run(
        agent.compose(
            question="Do regions with higher support load underperform?",
            context={
                "step_results": [
                    {
                        "agent_name": "analyst",
                        "task_id": "step-1",
                        "status": "succeeded",
                        "output": {
                            "analysis": "ANZ is the clearest weak-efficiency region.",
                            "artifacts": {
                                "primary_result": {
                                    "id": "primary_result",
                                    "type": "table",
                                    "role": "primary_result",
                                    "title": "Verified analyst result",
                                    "payload": {
                                        "columns": ["region", "support_load", "cac"],
                                        "rows": [["ANZ", 1.12, 82.4]],
                                    },
                                    "provenance": {"source": "analyst"},
                                },
                                "primary_sql": {
                                    "id": "primary_sql",
                                    "type": "sql",
                                    "role": "diagnostic",
                                    "title": "Generated SQL",
                                    "payload": {
                                        "sql_executable": (
                                            "SELECT region, support_load, cac "
                                            "FROM regional_metrics"
                                        )
                                    },
                                    "provenance": {"source": "analyst"},
                                },
                                "governed_attempts": {
                                    "id": "governed_attempts",
                                    "type": "diagnostics",
                                    "role": "diagnostic",
                                    "title": "Governed query attempts",
                                    "payload": [{"status": "success"}],
                                    "provenance": {"source": "analyst"},
                                },
                            },
                        },
                        "artifacts": {
                            "primary_result": {
                                "id": "primary_result",
                                "type": "table",
                                "role": "primary_result",
                                "payload": {
                                    "columns": ["region", "support_load", "cac"],
                                    "rows": [["ANZ", 1.12, 82.4]],
                                },
                            }
                        },
                    }
                ]
            },
        )
    )

    assert response["answer"] == response["answer_markdown"]
    assert [artifact["id"] for artifact in response["artifacts"]] == [
        "primary_result",
        "primary_sql",
        "governed_attempts",
    ]
    artifacts_by_id = {artifact["id"]: artifact for artifact in response["artifacts"]}
    assert response["result"]["rows"] == [["ANZ", 1.12, 82.4]]
    assert artifacts_by_id["primary_result"]["payload"]["rows"] == [["ANZ", 1.12, 82.4]]
    assert "support_load" in artifacts_by_id["primary_sql"]["payload"]["sql_executable"]
    assert artifacts_by_id["governed_attempts"]["payload"][0]["status"] == "success"


def test_presentation_filters_invented_artifact_ids_from_markdown_and_artifact_list() -> None:
    class _InventedArtifactLLMProvider:
        async def acomplete(self, prompt: str, **kwargs):
            assert "result_table" in prompt
            assert "made_up_chart" not in prompt
            return (
                '{"summary":"Returned the verified table.",'
                '"result":{},"visualization":null,"research":{},'
                '"answer_markdown":"The verified table is below.\\n\\n{{artifact:result_table}}\\n\\n{{artifact:made_up_chart}}",'
                '"artifacts":[{"id":"result_table"},{"id":"made_up_chart"},{"id":"not_in_registry"}],'
                '"diagnostics":{"mode":"final"}}'
            )

    agent = PresentationAgent(llm_provider=_InventedArtifactLLMProvider())

    response = _run(
        agent.compose(
            question="Show me revenue by region",
            context={
                "step_results": [
                    {
                        "agent_name": "analyst",
                        "output": {
                            "analysis": "Revenue by region is available.",
                            "result": {
                                "columns": ["region", "revenue"],
                                "rows": [["US", 2200]],
                            },
                        },
                    }
                ]
            },
        )
    )

    assert "{{artifact:result_table}}" in response["answer_markdown"]
    assert "{{artifact:made_up_chart}}" not in response["answer_markdown"]
    assert response["answer"] == response["answer_markdown"]
    assert [artifact["id"] for artifact in response["artifacts"]] == ["result_table"]
