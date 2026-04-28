import asyncio

from langbridge.ai import (
    AgentIOContract,
    AgentRegistry,
    AgentResultStatus,
    AgentRoutingSpec,
    AgentSpecification,
    AgentTask,
    AgentTaskKind,
    BaseAgent,
    MetaControllerAgent,
)
from langbridge.ai.agents import PresentationAgent


def _run(coro):
    return asyncio.run(coro)


class _GoldenLLMProvider:
    def __init__(self) -> None:
        self.prompts: list[str] = []

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt:
            if "support load" in prompt:
                return (
                    '{"action":"direct","rationale":"Relationship analysis needs research-mode governed evidence.",'
                    '"agent_name":"analyst.growth","task_kind":"analyst","input":{"agent_mode":"auto"},'
                    '"clarification_question":null,"plan_guidance":null}'
                )
            return (
                '{"action":"direct","rationale":"Single governed analytics question fits one analyst.",'
                '"agent_name":"analyst.growth","task_kind":"analyst","input":{"agent_mode":"auto"},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        if "Compose the final Langbridge response" in prompt:
            if "support load" in prompt:
                assert "primary_result" in prompt
                return (
                    '{"summary":"Higher support load does not clearly predict weaker marketing efficiency.",'
                    '"result":{},"visualization":null,"research":{},'
                    '"answer_markdown":"## Higher support load does not clearly predict weaker marketing efficiency\\nDACH has the highest support load but the strongest efficiency, while ANZ is the clearest weak-efficiency region.\\n\\n{{artifact:primary_result}}\\n\\nThe caveat is that marketing spend is not region-native, so the efficiency comparison depends on attribution.",'
                    '"artifacts":[{"id":"primary_result"}],'
                    '"diagnostics":{"mode":"golden"}}'
                )
            assert "primary_result" in prompt
            assert "primary_sql" in prompt
            return (
                '{"summary":"Paid Social led Q3 2025 channel performance.",'
                '"result":{},"visualization":null,"research":{},'
                '"answer_markdown":"## Paid Social led Q3 2025 channel performance\\nPaid Social drove the highest net revenue and gross margin among returned channels.\\n\\n{{artifact:primary_result}}\\n\\nThe generated SQL is available for audit.\\n\\n{{artifact:primary_sql}}",'
                '"artifacts":[{"id":"primary_result"},{"id":"primary_sql"}],'
                '"diagnostics":{"mode":"golden"}}'
            )
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded and artifact references are valid.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        raise AssertionError(f"Unexpected prompt: {prompt[:160]}")

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _GoldenAnalystAgent(BaseAgent):
    def __init__(self) -> None:
        self.inputs: list[dict[str, object]] = []

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst.growth",
            description="Golden-flow analyst test double.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["revenue", "support"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["analysis", "result", "evidence", "review_hints"]),
        )

    async def execute(self, task: AgentTask):
        self.inputs.append(dict(task.input))
        if "support load" in task.question:
            result = {
                "columns": ["region", "support_load", "revenue_per_marketing_dollar"],
                "rows": [["DACH", 1.17, 0.085], ["ANZ", 1.12, 0.082]],
                "rowcount": 2,
            }
            output = {
                "analysis": "Higher support load does not clearly predict weaker marketing efficiency.",
                "result": result,
                "synthesis": "DACH counters the hypothesis; ANZ supports it.",
                "findings": [{"insight": "ANZ is the weak-efficiency exception.", "source": "governed_result"}],
                "sources": [],
                "evidence": {"governed": {"attempted": True, "answered_question": True}},
                "review_hints": {"answered_question": True},
                "artifacts": {
                    "primary_result": {
                        "id": "primary_result",
                        "type": "table",
                        "role": "primary_result",
                        "title": "Regional support load vs efficiency",
                        "payload": result,
                    }
                },
            }
        else:
            result = {
                "columns": ["order_channel", "monthly_net_revenue", "monthly_gross_margin"],
                "rows": [
                    ["Paid Social", 9139.54, 4912.33],
                    ["Organic Search", 8237.29, 4651.32],
                    ["Affiliate", 8080.02, 4471.85],
                ],
                "rowcount": 3,
            }
            output = {
                "analysis": "Paid Social led both requested Q3 2025 channel metrics.",
                "result": result,
                "analysis_path": "semantic_model",
                "query_scope": "semantic",
                "sql_canonical": "SELECT order_channel, monthly_net_revenue, monthly_gross_margin FROM growth_performance",
                "sql_executable": "SELECT order_channel, SUM(monthly_net_revenue), SUM(monthly_gross_margin) FROM customer_month_revenue GROUP BY order_channel",
                "evidence": {"governed": {"attempted": True, "answered_question": True}},
                "review_hints": {"answered_question": True},
                "artifacts": {
                    "primary_result": {
                        "id": "primary_result",
                        "type": "table",
                        "role": "primary_result",
                        "title": "Q3 2025 channel performance",
                        "payload": result,
                    },
                    "primary_sql": {
                        "id": "primary_sql",
                        "type": "sql",
                        "role": "diagnostic",
                        "title": "Generated SQL",
                        "payload": {"sql_executable": "SELECT order_channel, SUM(monthly_net_revenue) FROM customer_month_revenue"},
                    },
                },
            }
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output=output,
            artifacts=output["artifacts"],
        )


def _controller(llm: _GoldenLLMProvider, analyst: _GoldenAnalystAgent) -> MetaControllerAgent:
    return MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=PresentationAgent(llm_provider=llm),
    )


def test_golden_order_channel_answer_returns_markdown_and_verified_artifacts() -> None:
    llm = _GoldenLLMProvider()
    analyst = _GoldenAnalystAgent()

    run = _run(
        _controller(llm, analyst).handle(
            question="Which order channels drove the highest net revenue and gross margin in Q3 2025?"
        )
    )

    assert run.status == "completed"
    assert run.execution_mode == "direct"
    assert run.plan.route == "direct:analyst.growth"
    assert run.step_results[0]["output"]["evidence"]["governed"]["attempted"] is True
    assert run.final_result["answer_markdown"].startswith("## Paid Social led Q3 2025 channel performance")
    assert "{{artifact:primary_result}}" in run.final_result["answer_markdown"]
    assert "{{artifact:primary_sql}}" in run.final_result["answer_markdown"]
    assert [artifact["id"] for artifact in run.final_result["artifacts"]] == ["primary_result", "primary_sql"]
    assert run.final_result["artifacts"][0]["payload"]["rows"][0][0] == "Paid Social"
    assert run.final_review["action"] == "approve"


def test_golden_support_load_question_uses_research_mode_without_clarification() -> None:
    llm = _GoldenLLMProvider()
    analyst = _GoldenAnalystAgent()

    run = _run(
        _controller(llm, analyst).handle(
            question="Do regions with higher support load also underperform on marketing efficiency?"
        )
    )

    assert run.status == "completed"
    assert run.plan.route == "direct:analyst.growth"
    assert run.status != "clarification_needed"
    assert run.diagnostics["route_decision"]["clarification_question"] is None
    assert "does not clearly predict" in run.final_result["answer_markdown"]
    assert "{{artifact:primary_result}}" in run.final_result["answer_markdown"]
    assert run.final_result["artifacts"][0]["payload"]["rows"][0] == ["DACH", 1.17, 0.085]
