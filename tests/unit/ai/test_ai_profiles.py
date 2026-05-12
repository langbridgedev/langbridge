import asyncio

from langbridge.ai import (
    AgentIOContract,
    AgentRegistry,
    AgentResultStatus,
    AgentRoutingSpec,
    AgentSpecification,
    AgentTask,
    AgentTaskKind,
    AiAgentProfile,
    AnalystToolBundle,
    AnalystAgentConfig,
    BaseAgent,
    LangbridgeAIFactory,
    MetaControllerAgent,
    PlanReviewAction,
    build_ai_profiles_from_definition,
    resolve_orchestration_from_definition,
)
from langbridge.ai.agents import AnalystAgent
from langbridge.ai.agents.presentation import PresentationAgent
from langbridge.ai.tools.charting import ChartingTool
from langbridge.ai.tools.web_search import WebSearchPolicy, WebSearchResultItem, WebSearchTool
from tests.unit.structured_llm_stub import StructuredTextLLMStub


def _run(coro):
    return asyncio.run(coro)


def _analyst_config(
    *,
    name: str = "analyst",
    semantic_models: list[str] | None = None,
    datasets: list[str] | None = None,
    query_policy: str = "semantic_preferred",
    research_enabled: bool = False,
    web_search_enabled: bool = False,
) -> AnalystAgentConfig:
    return AnalystAgentConfig.model_validate(
        {
            "name": name,
            "data_scope": {
                "semantic_models": semantic_models or ["commerce"],
                "datasets": datasets or ["orders"],
                "query_policy": query_policy,
            },
            "capabilities": {
                "research": {"enabled": research_enabled, "max_sources": 3},
                "web_search": {"enabled": web_search_enabled},
            },
        }
    )


class _FakeLLMProvider(StructuredTextLLMStub):
    async def acomplete(self, prompt: str, **kwargs):
        if "Decide Langbridge agent route" in prompt:
            if "Show factory result" in prompt:
                return (
                    '{"action":"plan","rationale":"Factory runtime test exercises planner path.",'
                    '"agent_name":null,"task_kind":null,"input":{},'
                    '"clarification_question":null,"plan_guidance":"Use factory analyst."}'
                )
            if "analyst.commerce_semantic_sql" in prompt:
                return (
                    '{"action":"direct","rationale":"Scoped commerce analyst can answer.",'
                    '"agent_name":"analyst.commerce_semantic_sql","task_kind":"analyst","input":{},'
                    '"clarification_question":null,"plan_guidance":null}'
                )
            return (
                '{"action":"direct","rationale":"Single analyst can answer.",'
                '"agent_name":"analyst","task_kind":"analyst","input":{},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        if "Build Langbridge execution plan" in prompt:
            return (
                '{"route":"planned:factory","rationale":"Use configured factory analyst.",'
                '"steps":[{"agent_name":"analyst.factory_analyst","task_kind":"analyst",'
                '"question":"Show factory result","input":{},"depends_on":[]}]}'
            )
        if "Choose the next execution mode" in prompt:
            if "Search current docs" in prompt:
                return '{"mode":"research","reason":"web research requested"}'
            return '{"mode":"context_analysis","reason":"structured result available"}'
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Create a chart specification" in prompt:
            return '{"chart_type":"bar","title":"Chart","x":"region","y":"revenue"}'
        if "Synthesize source-backed research" in prompt:
            return (
                '{"synthesis":"Source-backed research synthesis.",'
                '"findings":[{"insight":"Docs found.","source":"https://docs.langbridge.dev/runtime"}],'
                '"follow_ups":[]}'
            )
        if "Compose the final Langbridge response" in prompt:
            if "Recovered answer." in prompt:
                return (
                    '{"answer_markdown":"Recovered answer.",'
                    '"artifact_ids":[],"diagnostics":{"mode":"test"},'
                    '"metadata":{}}'
                )
            return (
                '{"answer_markdown":"Profile runtime answer.",'
                '"artifact_ids":[],"diagnostics":{"mode":"test"},'
                '"metadata":{}}'
            )
        if "Analyze verified Langbridge result data" in prompt:
            return '{"analysis":"Scoped analyst answer.","result":{"columns":[],"rows":[]}}'
        return '{"analysis":"Analysis complete."}'

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _FakeWebSearchProvider:
    name = "fake-web"

    async def search_async(self, query: str, **kwargs):
        return [
            WebSearchResultItem(
                title="Docs",
                url="https://docs.langbridge.dev/runtime",
                snippet="Runtime docs.",
                source=self.name,
                rank=1,
            )
        ]


def _presentation(llm: _FakeLLMProvider) -> PresentationAgent:
    return PresentationAgent(llm_provider=llm, charting_tool=ChartingTool(llm_provider=llm))


def test_build_ai_profiles_from_definition_rejects_legacy_tool_shape() -> None:
    try:
        build_ai_profiles_from_definition(
            name="commerce_analyst",
            description="Commerce analyst",
            definition={
                "features": {"deep_research_enabled": True},
                "tools": [
                    {
                        "name": "commerce_semantic_sql",
                        "tool_type": "sql",
                        "description": "Governed commerce semantic model.",
                        "config": {"semantic_model_ids": ["commerce_performance"]},
                    }
                ],
            },
        )
    except ValueError as exc:
        assert "data_scope" in str(exc) or "extra_forbidden" in str(exc)
    else:
        raise AssertionError("Legacy agent tool definitions must not be accepted.")


def test_factory_profile_runtime_routes_to_scoped_analyst() -> None:
    profile = AiAgentProfile.from_config(
        {
            "name": "commerce_semantic_sql",
            "description": "Commerce revenue and order analytics.",
            "data_scope": {"semantic_models": ["commerce_performance"], "query_policy": "semantic_only"},
            "orchestration": {"policy": "strict_governed"},
        }
    )
    runtime = LangbridgeAIFactory(llm_provider=_FakeLLMProvider()).create_profile_runtime(profile)

    run = _run(
        runtime.meta_controller.handle(
            question="Show commerce revenue by region",
            context={"result": {"columns": [], "rows": []}},
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.route == "direct:analyst.commerce_semantic_sql"
    assert run.step_results[0]["agent_name"] == "analyst.commerce_semantic_sql"
    assert run.step_results[0]["output"]["result"] == {"columns": [], "rows": []}


def test_ai_factory_builds_meta_controller_without_runtime_wiring() -> None:
    llm = _FakeLLMProvider()
    controller = LangbridgeAIFactory(llm_provider=llm).create_meta_controller(
        analysts=[
            AnalystToolBundle(
                config=_analyst_config(name="factory_analyst"),
            )
        ]
    )

    run = _run(
        controller.handle(
            question="Show factory result",
            context={"result": {"columns": [], "rows": []}},
        )
    )

    assert run.execution_mode == "planned"
    assert run.status == "completed"
    assert run.step_results[0]["agent_name"] == "analyst.factory_analyst"
    assert run.final_result["answer_markdown"] == "Profile runtime answer.\n\n{{artifact:primary_result}}"


def test_ai_profile_parses_runtime_schema() -> None:
    profile = AiAgentProfile.from_config(
        {
            "name": "support_analyst",
            "description": "Support ticket analyst.",
            "availability": {"runtime": True, "mcp": True},
            "data_scope": {"datasets": ["support_tickets"], "query_policy": "dataset_only"},
            "capabilities": {
                "research": {"enabled": True, "extended_thinking": True},
                "web_search": {"enabled": True, "provider": "duckduckgo"},
            },
            "instructions": {"system": "You are support analyst.", "presentation": "Be concise."},
        }
    )

    assert profile.available_via_runtime is True
    assert profile.available_via_mcp is True
    assert profile.data_scope.datasets == ["support_tickets"]
    assert profile.capabilities.research.extended_thinking is True
    assert profile.instructions.system == "You are support analyst."
    assert profile.instructions.presentation == "Be concise."


def test_profile_to_analyst_config_resolves_orchestration_budgets() -> None:
    profile = AiAgentProfile.from_config(
        {
            "name": "ops_analyst",
            "data_scope": {"datasets": ["ops_metrics"], "query_policy": "dataset_only"},
            "orchestration": {"policy": "research_heavy"},
        }
    )

    analyst_config = profile.to_analyst_config()

    assert analyst_config.max_evidence_rounds == 4
    assert analyst_config.max_governed_attempts == 3
    assert analyst_config.max_external_augmentations == 3
    assert analyst_config.final_review_enabled is True


def test_resolve_orchestration_from_definition_aggregates_policy_budgets() -> None:
    aggregated = resolve_orchestration_from_definition(
        name="support_runtime",
        description="Support runtime",
        definition={
            "profiles": [
                {
                    "name": "support_primary",
                    "data_scope": {"datasets": ["support_tickets"], "query_policy": "dataset_only"},
                    "orchestration": {"policy": "fast_sql"},
                },
                {
                    "name": "support_secondary",
                    "data_scope": {"datasets": ["support_tickets"], "query_policy": "dataset_only"},
                    "orchestration": {"policy": "research_heavy"},
                },
            ]
        },
    )

    assert aggregated.max_iterations == 6
    assert aggregated.max_replans == 2
    assert aggregated.max_step_retries == 1
    assert aggregated.max_evidence_rounds == 4
    assert aggregated.max_governed_attempts == 3
    assert aggregated.max_external_augmentations == 3
    assert aggregated.final_review_enabled is True


def test_analyst_research_mode_can_use_web_search_tool_provider() -> None:
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=_analyst_config(name="docs_research", research_enabled=True, web_search_enabled=True),
        web_search_tool=WebSearchTool(
            provider=_FakeWebSearchProvider(),
            policy=WebSearchPolicy(
                allowed_domains=[],
                denied_domains=[],
                focus_terms=["langbridge"],
            ),
        ),
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="research",
                task_kind=AgentTaskKind.analyst,
                question="Search current docs",
                input={"mode": "research"},
            )
        )
    )

    assert result.status == AgentResultStatus.succeeded
    assert result.output["sources"][0]["url"] == "https://docs.langbridge.dev/runtime"
    assert result.diagnostics["web_search"]["query"] == "Search current docs langbridge"
    assert result.diagnostics["web_search"]["provider"] == "fake-web"


def test_analyst_research_uses_context_sources() -> None:
    agent = AnalystAgent(
        llm_provider=_FakeLLMProvider(),
        config=AnalystAgentConfig.model_validate(
            {
                "name": "growth_research",
                "data_scope": {},
                "capabilities": {
                    "research": {"enabled": True, "max_sources": 3, "require_sources": True},
                    "web_search": {"enabled": True, "provider": "duckduckgo"},
                },
            }
        ),
    )

    result = _run(
        agent.execute(
            AgentTask(
                task_id="research",
                task_kind=AgentTaskKind.analyst,
                question="Research runtime docs",
                input={"mode": "research"},
                context={
                    "step_results": [
                        {
                            "agent_name": "tool.web.docs_search",
                            "output": {
                                "results": [
                                    {
                                        "title": "Docs",
                                        "url": "https://docs.langbridge.dev/runtime",
                                        "snippet": "Runtime docs.",
                                    }
                                ]
                            },
                        },
                        {
                            "agent_name": "tool.web.general",
                            "output": {
                                "results": [
                                    {
                                        "title": "General",
                                        "url": "https://example.test/general",
                                        "snippet": "General source.",
                                    }
                                ]
                            },
                        },
                    ]
                },
            )
        )
    )

    assert len(result.output["sources"]) == 2
    assert result.output["sources"][0]["url"] == "https://docs.langbridge.dev/runtime"
    assert result.output["sources"][1]["url"] == "https://example.test/general"


class _FlakyAnalystAgent(BaseAgent):
    def __init__(self) -> None:
        self.calls = 0

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Flaky analyst test agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["explain"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["answer"]),
        )

    async def execute(self, task: AgentTask):
        self.calls += 1
        if self.calls == 1:
            return self.build_result(
                task=task,
                status=AgentResultStatus.failed,
                error="temporary failure",
            )
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"answer": "Recovered answer."},
        )


def test_retry_success_reviews_latest_record() -> None:
    llm = _FakeLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([_FlakyAnalystAgent()]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
        max_step_retries=1,
    )

    run = _run(controller.handle(question="Explain this"))

    assert [outcome.passed for outcome in run.verification] == [False, True]
    assert [decision.action for decision in run.review_decisions] == [
        PlanReviewAction.retry_step,
        PlanReviewAction.finalize,
    ]
    assert run.final_result["answer_markdown"] == "Recovered answer."
