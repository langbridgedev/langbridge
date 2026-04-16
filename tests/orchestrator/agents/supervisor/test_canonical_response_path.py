import asyncio
import pathlib
import sys
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from langbridge.orchestrator.agents.reasoning.agent import ReasoningDecision  # noqa: E402
from langbridge.orchestrator.agents.visual import VisualAgent  # noqa: E402
from langbridge.orchestrator.agents.supervisor.orchestrator import SupervisorOrchestrator  # noqa: E402
from langbridge.orchestrator.agents.supervisor.schemas import (  # noqa: E402
    ClarificationDecision,
    ClarificationState,
    ClassifiedQuestion,
    ResolvedEntities,
)
from langbridge.orchestrator.agents.planner import Plan, PlanStep  # noqa: E402
from langbridge.orchestrator.definitions import (  # noqa: E402
    AgentDefinitionFactory,
    GuardrailConfig,
    OutputFormat,
    OutputSchema,
    PromptContract,
    ResponseMode,
)
from langbridge.orchestrator.runtime.access_policy import (  # noqa: E402
    AnalyticalAccessScope,
    AnalyticalDeniedAsset,
)
from langbridge.orchestrator.runtime.response_formatter import ResponsePresentation  # noqa: E402
from langbridge.orchestrator.tools.sql_analyst.interfaces import (  # noqa: E402
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryResponse,
    QueryResult,
)


class _StubLLM:
    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.calls: list[dict[str, Any]] = []

    async def ainvoke(
        self,
        messages: list[Any],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        self.calls.append({"messages": messages, "temperature": temperature, "max_tokens": max_tokens})
        return self.response_text


class _StubQuestionClassifier:
    async def classify_async(self, *_args: Any, **_kwargs: Any) -> ClassifiedQuestion:
        return ClassifiedQuestion(route_hint="SimpleAnalyst", requires_clarification=False, confidence=0.9)


class _StubEntityResolver:
    async def resolve_async(self, *_args: Any, **_kwargs: Any) -> ResolvedEntities:
        return ResolvedEntities()


class _StubClarificationManager:
    def decide(self, *_args: Any, **_kwargs: Any) -> ClarificationDecision:
        return ClarificationDecision(
            requires_clarification=False,
            updated_state=ClarificationState(),
        )


class _StubPlanningAgent:
    def plan(self, request: Any) -> Plan:
        return Plan(
            route="SimpleAnalyst",
            steps=[PlanStep(id="step_1", agent="Analyst", input={"question": request.question})],
            justification="Use analyst.",
            user_summary="Run analysis.",
        )


class _StubAnalystThenVisualPlanningAgent:
    def plan(self, request: Any) -> Plan:
        return Plan(
            route="AnalystThenVisual",
            steps=[
                PlanStep(id="step_1", agent="Analyst", input={"question": request.question}),
                PlanStep(
                    id="step_2",
                    agent="Visual",
                    input={"rows_ref": "step_1", "user_intent": "insight_visualization"},
                ),
            ],
            justification="Use analyst and visualization.",
            user_summary="Run analysis and render a chart.",
        )


class _StubReasoningAgent:
    max_iterations = 1

    def evaluate(self, **_kwargs: Any) -> ReasoningDecision:
        return ReasoningDecision(continue_planning=False, rationale="Enough signal.")


class _StubAnalystAgent:
    def __init__(self, response: AnalystQueryResponse) -> None:
        self.response = response

    async def answer_async(self, *_args: Any, **_kwargs: Any) -> AnalystQueryResponse:
        return self.response


def _success_response() -> AnalystQueryResponse:
    return AnalystQueryResponse(
        analysis_path="dataset",
        execution_mode="federated",
        asset_type="dataset",
        asset_id="dataset-1",
        asset_name="sales_dataset",
        sql_canonical="select region, revenue from sales",
        sql_executable="select region, revenue from sales",
        dialect="postgres",
        result=QueryResult(
            columns=["region", "revenue"],
            rows=[("US", 2200), ("EMEA", 1200)],
            rowcount=2,
            elapsed_ms=12,
            source_sql="select region, revenue from sales",
        ),
        outcome=AnalystExecutionOutcome(
            status=AnalystOutcomeStatus.success,
            stage=AnalystOutcomeStage.result,
            recoverable=False,
            terminal=True,
        ),
    )


def test_supervisor_uses_canonical_response_formatter_for_analyst_mode() -> None:
    llm = _StubLLM("US leads revenue.")
    supervisor = SupervisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        analyst_agent=_StubAnalystAgent(_success_response()),
        planning_agent=_StubPlanningAgent(),
        reasoning_agent=_StubReasoningAgent(),
        question_classifier=_StubQuestionClassifier(),
        entity_resolver=_StubEntityResolver(),
        clarification_manager=_StubClarificationManager(),
        response_presentation=ResponsePresentation(
            prompt_contract=PromptContract(system_prompt="System prompt"),
            output_schema=OutputSchema(format=OutputFormat.text),
            guardrails=GuardrailConfig(),
            response_mode=ResponseMode.analyst,
        ),
    )

    result = asyncio.run(supervisor.handle("Which region had the highest revenue?"))

    assert result["summary"] == "US leads revenue."
    human_prompt = str(llm.calls[0]["messages"][-1].content)
    assert "Key analytical facts:" in human_prompt
    assert "Analyst outcome:" in human_prompt


def test_supervisor_applies_definition_response_config_on_live_summary_path() -> None:
    definition = AgentDefinitionFactory().create_agent_definition(
        {
            "prompt": {
                "system_prompt": "Executive system prompt",
                "user_instructions": "Keep it board-ready.",
            },
            "memory": {"strategy": "none"},
            "features": {
                "bi_copilot_enabled": False,
                "deep_research_enabled": False,
                "visualization_enabled": False,
                "mcp_enabled": False,
            },
            "execution": {
                "mode": "single_step",
                "response_mode": "executive",
                "max_iterations": 1,
            },
            "output": {
                "format": "markdown",
                "markdown_template": "## Summary",
            },
            "guardrails": {
                "moderation_enabled": True,
                "regex_denylist": ["forbidden"],
                "escalation_message": "Blocked by guardrails.",
            },
            "observability": {
                "log_level": "info",
                "emit_traces": True,
                "capture_prompts": True,
            },
        }
    )
    llm = _StubLLM("This contains forbidden text.")
    supervisor = SupervisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        analyst_agent=_StubAnalystAgent(_success_response()),
        planning_agent=_StubPlanningAgent(),
        reasoning_agent=_StubReasoningAgent(),
        question_classifier=_StubQuestionClassifier(),
        entity_resolver=_StubEntityResolver(),
        clarification_manager=_StubClarificationManager(),
        response_presentation=ResponsePresentation.from_definition(definition),
    )

    result = asyncio.run(supervisor.handle("Which region had the highest revenue?"))

    assert result["summary"] == "Blocked by guardrails."
    system_prompt = str(llm.calls[0]["messages"][0].content)
    human_prompt = str(llm.calls[0]["messages"][-1].content)
    assert "Executive system prompt" in system_prompt
    assert "Keep it board-ready." in system_prompt
    assert "Markdown template:\n## Summary" in human_prompt
    assert "executive briefing assistant" in human_prompt


def test_supervisor_returns_access_denied_summary_when_all_analytical_assets_are_blocked() -> None:
    llm = _StubLLM("Access is blocked by policy.")
    supervisor = SupervisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        question_classifier=_StubQuestionClassifier(),
        entity_resolver=_StubEntityResolver(),
        clarification_manager=_StubClarificationManager(),
        analytical_access_scope=AnalyticalAccessScope(
            policy_enforced=True,
            authorized_asset_count=0,
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
        ),
        response_presentation=ResponsePresentation(
            prompt_contract=PromptContract(system_prompt="System prompt"),
            output_schema=OutputSchema(format=OutputFormat.text),
            guardrails=GuardrailConfig(),
            response_mode=ResponseMode.analyst,
        ),
    )

    result = asyncio.run(supervisor.handle("Revenue by payroll_dataset"))

    assert result["summary"] == "Access is blocked by policy."
    assert result["diagnostics"]["analyst_outcome"]["status"] == "access_denied"
    assert result["diagnostics"]["analytical_access"]["denied_assets_count"] == 1


def test_supervisor_visual_step_uses_generated_chart_title_instead_of_raw_query() -> None:
    llm = _StubLLM("US leads revenue.")
    supervisor = SupervisorOrchestrator(
        llm=llm,  # type: ignore[arg-type]
        analyst_agent=_StubAnalystAgent(_success_response()),
        visual_agent=VisualAgent(),
        planning_agent=_StubAnalystThenVisualPlanningAgent(),
        reasoning_agent=_StubReasoningAgent(),
        question_classifier=_StubQuestionClassifier(),
        entity_resolver=_StubEntityResolver(),
        clarification_manager=_StubClarificationManager(),
        response_presentation=ResponsePresentation(
            prompt_contract=PromptContract(system_prompt="System prompt"),
            output_schema=OutputSchema(format=OutputFormat.text),
            guardrails=GuardrailConfig(),
            response_mode=ResponseMode.analyst,
        ),
    )

    result = asyncio.run(supervisor.handle("Show me a chart of revenue by region"))

    assert result["visualization"]["title"] == "Revenue by Region"
