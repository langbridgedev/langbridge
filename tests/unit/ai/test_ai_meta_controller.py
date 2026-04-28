import asyncio

from langbridge.ai import (
    AgentIOContract,
    AgentResult,
    AgentRegistry,
    AgentResultStatus,
    AgentRoutingSpec,
    AgentSpecification,
    AgentTask,
    AgentTaskKind,
    AnalystAgentConfig,
    BaseAgent,
    MetaControllerAgent,
    PlanReviewAction,
    FinalReviewAction,
    FinalReviewReasonCode,
    PlanReviewReasonCode,
    VerificationReasonCode,
)
from langbridge.ai.agents import (
    AnalystAgent,
    PresentationAgent,
)
from langbridge.ai.orchestration.execution import PlanExecutionState
from langbridge.ai.orchestration.plan_review import PlanReviewAgent
from langbridge.ai.orchestration.planner import ExecutionPlan, PlannerAgent, PlanStep
from langbridge.ai.orchestration.verification import VerificationOutcome
from langbridge.ai.tools.charting import ChartingTool
from langbridge.ai.tools.web_search import WebSearchResultItem, WebSearchTool


def _run(coro):
    return asyncio.run(coro)


def _analyst_config(*, research_enabled: bool = False, web_search_enabled: bool = False) -> AnalystAgentConfig:
    return AnalystAgentConfig.model_validate(
        {
            "name": "analyst",
            "analyst_scope": {
                "semantic_models": ["commerce"],
                "datasets": ["orders"],
                "query_policy": "semantic_preferred",
            },
            "research_scope": {"enabled": research_enabled},
            "web_search_scope": {"enabled": web_search_enabled},
        }
    )


class _FakeLLMProvider:
    async def acomplete(self, prompt: str, **kwargs):
        if "Decide Langbridge agent route" in prompt:
            if "Dependency ordered request" in prompt:
                return (
                    '{"action":"plan","rationale":"Dependency-aware plan required.",'
                    '"agent_name":null,"task_kind":null,"input":{},'
                    '"clarification_question":null,"plan_guidance":"Honor step dependencies."}'
                )
            if "Show it" in prompt:
                return (
                    '{"action":"clarify","rationale":"Question lacks metric and dataset scope.",'
                    '"agent_name":null,"task_kind":null,"input":{},'
                    '"clarification_question":"Which metric and dataset should I use?",'
                    '"plan_guidance":null}'
                )
            if "Ambiguous planned request" in prompt:
                return (
                    '{"action":"plan","rationale":"Planner should decide whether clarification is needed.",'
                    '"agent_name":null,"task_kind":null,"input":{},'
                    '"clarification_question":null,"plan_guidance":"Resolve ambiguity before execution."}'
                )
            if "Search the web" in prompt:
                return (
                    '{"action":"direct","rationale":"One analyst can perform source-backed research directly.",'
                    '"agent_name":"analyst","task_kind":"analyst","input":{"agent_mode":"research"},'
                    '"clarification_question":null,"plan_guidance":null}'
                )
            if "broken-analyst" in prompt:
                return (
                    '{"action":"direct","rationale":"Test selects weak direct agent first.",'
                    '"agent_name":"broken-analyst","task_kind":"analyst","input":{},'
                    '"clarification_question":null,"plan_guidance":null}'
                )
            return (
                '{"action":"direct","rationale":"Single analyst can answer from provided context.",'
                '"agent_name":"analyst","task_kind":"analyst","input":{},'
                '"clarification_question":null,"plan_guidance":null}'
                )
        if "Build Langbridge execution plan" in prompt:
            if "Ambiguous planned request" in prompt:
                return (
                    '{"route":"planned:clarification",'
                    '"rationale":"Cannot plan safely without metric scope.",'
                    '"clarification_question":"Which metric and dataset should I use?",'
                    '"steps":[]}'
                )
            if "broken-analyst" in prompt:
                return (
                    '{"route":"planned:recovery","rationale":"Avoid failed agent and recover with analyst.",'
                    '"steps":[{"agent_name":"analyst","task_kind":"analyst",'
                    '"question":"Explain this result","input":{},"depends_on":[]}]}'
                )
            return (
                '{"route":"planned:research","rationale":"Use source-backed research mode.",'
                '"steps":[{"agent_name":"analyst","task_kind":"analyst",'
                '"question":"Search the web and then explain latest sources for Langbridge",'
                '"input":{"mode":"research"},"depends_on":[]}]}'
            )
        if "Choose the next execution mode" in prompt:
            if "Search the web" in prompt:
                return '{"mode":"research","reason":"web research requested"}'
            return '{"mode":"context_analysis","reason":"structured result available"}'
        if "Review the final Langbridge answer package" in prompt:
            if "Ambiguous revenue request" in prompt:
                return (
                    '{"action":"ask_clarification","reason_code":"ambiguous_question",'
                    '"rationale":"Need metric scope before finalizing.",'
                    '"issues":["Metric scope is ambiguous."],"updated_context":{"needs_metric_scope":true},'
                    '"clarification_question":"Which revenue metric should I use?"}'
                )
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Create a chart specification" in prompt:
            return '{"chart_type":"bar","title":"Revenue by region","x":"region","y":"revenue"}'
        if "Synthesize source-backed research" in prompt:
            return (
                '{"synthesis":"Source-backed synthesis for Langbridge.",'
                '"findings":[{"insight":"Runtime reference found.","source":"https://example.test/langbridge"}],'
                '"follow_ups":[]}'
            )
        if "Compose the final Langbridge response" in prompt:
            if "Mode: clarification" in prompt:
                if "Which revenue metric should I use?" in prompt:
                    return (
                        '{"summary":"Clarification needed.",'
                        '"result":{},"visualization":null,"research":{},'
                        '"answer":"Which revenue metric should I use?",'
                        '"diagnostics":{"mode":"clarification"}}'
                    )
                return (
                    '{"summary":"Clarification needed.",'
                    '"result":{},"visualization":null,"research":{},'
                    '"answer":"Which metric and dataset should I use?",'
                    '"diagnostics":{"mode":"clarification"}}'
                )
            if "Analyst recovered answer." in prompt:
                return (
                    '{"summary":"Analyst recovered answer.",'
                    '"result":{},"visualization":null,"research":{},'
                    '"answer":"Analyst recovered answer.","diagnostics":{"mode":"test"}}'
                )
            return (
                '{"summary":"Final answer from verified outputs.",'
                '"result":{},"visualization":null,'
                '"research":{"synthesis":"Source-backed synthesis for Langbridge."},'
                '"answer":"Final answer from verified outputs.","diagnostics":{"mode":"test"}}'
            )
        if "Analyze verified Langbridge result data" in prompt:
            return '{"analysis":"Analyzed verified result data.","result":{"columns":[],"rows":[]}}'
        return '{"analysis":"Analysis complete."}'

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _FreshContextAnalysisRouteLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "Decide Langbridge agent route" in prompt and "Fresh context-analysis route" in prompt:
            return (
                '{"action":"direct","rationale":"Fresh question should stay auto unless explicitly requested.",'
                '"agent_name":"analyst","task_kind":"analyst","input":{"agent_mode":"context_analysis"},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        return await super().acomplete(prompt, **kwargs)


class _PrematureClarificationRouteLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "Decide Langbridge agent route" in prompt and "higher support load" in prompt:
            return (
                '{"action":"clarify","rationale":"Need metric and time frame to compare regions reliably.",'
                '"agent_name":null,"task_kind":null,"input":{},'
                '"clarification_question":"Which marketing efficiency metric and time period should I use?",'
                '"plan_guidance":null}'
            )
        return await super().acomplete(prompt, **kwargs)


class _FakeWebSearchProvider:
    name = "fake-web"

    async def search_async(self, query: str, **kwargs):
        return [
            WebSearchResultItem(
                title="Langbridge docs",
                url="https://example.test/langbridge",
                snippet="Langbridge runtime reference.",
                source=self.name,
                rank=1,
            )
        ]


class _AnswerAliasRouteLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "Decide Langbridge agent route" in prompt:
            return (
                '{"action":"direct","rationale":"Alias route bug regression.",'
                '"agent_name":"analyst","task_kind":"analyst",'
                '"input":{"agent_mode":"answer"},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        return await super().acomplete(prompt, **kwargs)


class _ChartFollowUpShouldNotRouteLLMProvider(_FakeLLMProvider):
    def __init__(self) -> None:
        self.prompts: list[str] = []

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt:
            raise AssertionError("Route LLM should be bypassed for deterministic chart follow-ups.")
        if "Analyze verified Langbridge result data" in prompt:
            return (
                '{"analysis":"Analyzed prior order channel result.",'
                '"result":{"columns":["order_channel","net_revenue","gross_margin"],'
                '"rows":[["Online",125000,42000],["Retail",98000,31500]]}}'
            )
        if "Create a chart specification" in prompt:
            return (
                '{"chart_type":"pie","title":"Q3 2025 order channel mix","x":"order_channel",'
                '"y":"net_revenue"}'
            )
        return await super().acomplete(prompt, **kwargs)


class _NonChartFollowUpShouldNotRouteLLMProvider(_FakeLLMProvider):
    def __init__(self) -> None:
        self.prompts: list[str] = []

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt:
            raise AssertionError("Route LLM should be bypassed for deterministic follow-ups.")
        if "Analyze verified Langbridge result data" in prompt:
            return (
                '{"analysis":"Gross margin leaders are visible in the prior verified result.",'
                '"result":{"columns":["order_channel","net_revenue","gross_margin"],'
                '"rows":[["Online",125000,42000],["Retail",98000,31500]]}}'
            )
        return await super().acomplete(prompt, **kwargs)


class _ReviseThenApproveLLMProvider(_FakeLLMProvider):
    def __init__(self) -> None:
        self.final_review_calls = 0

    async def acomplete(self, prompt: str, **kwargs):
        if "Review the final Langbridge answer package" in prompt:
            self.final_review_calls += 1
            if self.final_review_calls == 1:
                return (
                    '{"action":"revise_answer","reason_code":"missing_caveat_or_framing",'
                    '"rationale":"Needs tighter caveats.",'
                    '"issues":["Add the missing caveat."],"updated_context":{"needs_caveat":true},'
                    '"clarification_question":null}'
                )
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Revised answer is grounded.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt and "Presentation revision request:" in prompt:
            return (
                '{"summary":"Revised answer.",'
                '"result":{},"visualization":null,"research":{},'
                '"answer":"Revised answer.","diagnostics":{"mode":"test"}}'
            )
        return await super().acomplete(prompt, **kwargs)


class _ChartReviseThenApproveLLMProvider(_ChartFollowUpShouldNotRouteLLMProvider):
    def __init__(self) -> None:
        super().__init__()
        self.final_review_calls = 0

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Review the final Langbridge answer package" in prompt:
            self.final_review_calls += 1
            if '"visualization"' in prompt and '"chart_type": "pie"' in prompt:
                return (
                    '{"action":"approve","reason_code":"grounded_complete",'
                    '"rationale":"Chart-ready response now directly fulfills the request.",'
                    '"issues":[],"updated_context":{},"clarification_question":null}'
                )
            return (
                '{"action":"revise_answer","reason_code":"missing_caveat_or_framing",'
                '"rationale":"Grounded in the result, but it does not directly fulfill the pie-chart request.",'
                '"issues":["Return a direct chart-ready representation and note that a pie chart should show one metric at a time."],'
                '"updated_context":{"follow_up_hint":"Return labels and values for one metric and mention that a pie chart should show one metric at a time."},'
                '"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt and "Presentation revision request:" in prompt:
            return (
                '{"summary":"Pie chart ready for net revenue by order channel.",'
                '"result":{"columns":["order_channel","net_revenue","gross_margin"],'
                '"rows":[["Online",125000,42000],["Retail",98000,31500]]},'
                '"visualization":null,"research":{},'
                '"answer":"Here is a pie-chart-ready split for net revenue by order channel. A pie chart should show one metric at a time, so this version uses net revenue.",'
                '"diagnostics":{"mode":"test"}}'
            )
        return await super().acomplete(prompt, **kwargs)


def _presentation(llm: _FakeLLMProvider) -> PresentationAgent:
    return PresentationAgent(llm_provider=llm, charting_tool=ChartingTool(llm_provider=llm))


def _controller() -> MetaControllerAgent:
    llm = _FakeLLMProvider()
    registry = AgentRegistry(
        [
            AnalystAgent(
                llm_provider=llm,
                config=_analyst_config(research_enabled=True, web_search_enabled=True),
                web_search_tool=WebSearchTool(provider=_FakeWebSearchProvider()),
            ),
        ]
    )
    return MetaControllerAgent(registry=registry, llm_provider=llm, presentation_agent=_presentation(llm))


def test_meta_controller_routes_simple_analyst_question_directly() -> None:
    run = _run(
        _controller().handle(
            question="Show revenue by region",
            context={"semantic_model_id": "commerce", "result": {"columns": [], "rows": []}},
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.route == "direct:analyst"
    assert [step.agent_name for step in run.plan.steps] == ["analyst"]
    assert run.plan.steps[0].expected_output.required_keys == ["analysis", "result", "evidence", "review_hints"]
    assert all(item.passed for item in run.verification)
    assert all(item.reason_code == VerificationReasonCode.passed for item in run.verification)
    assert run.review_decisions[-1].action == PlanReviewAction.finalize
    assert run.review_decisions[-1].reason_code == PlanReviewReasonCode.all_steps_completed
    assert run.final_review["action"] == FinalReviewAction.approve.value
    assert run.final_review["reason_code"] == FinalReviewReasonCode.grounded_complete.value
    assert run.final_result["summary"] == "Final answer from verified outputs."
    assert run.final_result["visualization"] is None


def test_meta_controller_routes_single_analyst_web_research_directly() -> None:
    analyst = _ResearchRecordingAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=_FakeLLMProvider(),
        presentation_agent=_presentation(_FakeLLMProvider()),
    )
    run = _run(
        controller.handle(
            question="Search the web and then explain latest sources for Langbridge"
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.route == "direct:analyst"
    assert [step.agent_name for step in run.plan.steps] == ["analyst"]
    assert run.plan.steps[0].task_kind == AgentTaskKind.analyst
    assert run.plan.steps[0].input["agent_mode"] == "research"
    assert run.plan.steps[0].expected_output.required_keys == [
        "analysis",
        "result",
        "synthesis",
        "sources",
        "findings",
    ]
    assert "mode" not in run.plan.steps[0].input
    assert all(item.passed for item in run.verification)
    assert run.review_decisions[-1].reason_code == PlanReviewReasonCode.all_steps_completed
    assert analyst.calls == 1
    assert analyst.inputs[0]["agent_mode"] == "research"
    assert run.final_result["summary"] == "Final answer from verified outputs."


def test_meta_controller_redirects_premature_metric_timeframe_clarification_to_research() -> None:
    llm = _PrematureClarificationRouteLLMProvider()
    analyst = _ResearchRecordingAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Do regions with higher support load also underperform on marketing efficiency?"
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.route == "direct:analyst"
    assert run.plan.steps[0].input["agent_mode"] == "research"
    assert run.diagnostics["route_decision"]["action"] == "direct"
    assert run.diagnostics["route_decision"]["clarification_question"] is None
    assert "Original clarification" in run.diagnostics["route_decision"]["rationale"]
    assert analyst.calls == 1
    assert analyst.inputs[0]["agent_mode"] == "research"


def test_meta_controller_upgrades_diagnostic_direct_auto_route_to_research() -> None:
    llm = _FakeLLMProvider()
    analyst = _ResearchRecordingAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Do regions with higher support load also underperform on marketing efficiency?"
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.route == "direct:analyst"
    assert run.plan.steps[0].input["agent_mode"] == "research"
    assert run.plan.steps[0].expected_output.required_keys == [
        "analysis",
        "result",
        "synthesis",
        "sources",
        "findings",
    ]
    assert run.diagnostics["route_decision"]["input"]["agent_mode"] == "research"
    assert analyst.calls == 1
    assert analyst.inputs[0]["agent_mode"] == "research"


def test_meta_controller_preserves_explicit_requested_research_mode_for_direct_analyst() -> None:
    analyst = _ResearchRecordingAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=_FakeLLMProvider(),
        presentation_agent=_presentation(_FakeLLMProvider()),
    )
    run = _run(
        controller.handle(
            question="Explain the current context result",
            context={
                "agent_mode": "research",
                "result": {"columns": ["region", "revenue"], "rows": [["US", 2200]]},
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "research"
    assert analyst.calls == 1
    assert analyst.inputs[0]["agent_mode"] == "research"


def test_meta_controller_can_ask_clarification_before_execution() -> None:
    run = _run(_controller().handle(question="Show it"))

    assert run.execution_mode is None
    assert run.status == "clarification_needed"
    assert run.plan.route == "clarification"
    assert run.verification == []
    assert run.final_result["answer"] == "Which metric and dataset should I use?"
    assert run.diagnostics["stop_reason"] == "clarification"


def test_final_review_can_request_clarification_before_presentation() -> None:
    llm = _FakeLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([_RecoveringAnalystAgent()]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(controller.handle(question="Ambiguous revenue request"))

    assert run.execution_mode == "direct"
    assert run.status == "clarification_needed"
    assert run.final_review["action"] == FinalReviewAction.ask_clarification.value
    assert run.final_review["reason_code"] == FinalReviewReasonCode.ambiguous_question.value
    assert run.final_review["clarification_question"] == "Which revenue metric should I use?"
    assert run.final_result["answer"] == "Which revenue metric should I use?"
    assert run.diagnostics["stop_reason"] == "final_review_clarification"


def test_final_review_revise_answer_revises_presentation_without_planner_replan() -> None:
    llm = _ReviseThenApproveLLMProvider()
    agent = _RevisableAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([agent]),
        llm_provider=llm,
        planner=_PlannerShouldNotReplan(),
        presentation_agent=_presentation(llm),
        max_replans=1,
    )

    run = _run(controller.handle(question="Revise this answer"))

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert agent.calls == 1
    assert llm.final_review_calls == 2
    assert run.final_review["action"] == FinalReviewAction.approve.value
    assert run.final_review["reason_code"] == FinalReviewReasonCode.grounded_complete.value
    assert run.final_result["answer"] == "Revised answer."
    assert run.diagnostics["replan_count"] == 0
    assert run.diagnostics["stop_reason"] == "final_review_presentation_revision"


def test_post_presentation_final_review_revises_invalid_artifact_contract() -> None:
    llm = _FakeLLMProvider()
    presentation = _ArtifactContractPresentationAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([_RecoveringAnalystAgent()]),
        llm_provider=llm,
        planner=_PlannerShouldNotReplan(),
        presentation_agent=presentation,
        max_replans=1,
    )

    run = _run(controller.handle(question="Review artifact contract"))

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert presentation.calls == 2
    assert run.final_review["action"] == FinalReviewAction.approve.value
    assert run.final_result["answer_markdown"] == "Revised artifact-safe answer."
    assert run.final_result["artifacts"] == []
    assert run.diagnostics["replan_count"] == 0
    assert run.diagnostics["stop_reason"] == "final_review_presentation_revision"


def test_planner_can_return_clarification_instead_of_empty_plan_failure() -> None:
    run = _run(_controller().handle(question="Ambiguous planned request"))

    assert run.execution_mode == "planned"
    assert run.status == "clarification_needed"
    assert run.plan.route == "planned:clarification"
    assert run.final_result["answer"] == "Which metric and dataset should I use?"
    assert run.diagnostics["clarification_source"] == "planner"


def test_planner_replan_keeps_only_analyst_available_when_avoid_list_would_empty_candidates() -> None:
    llm = _FakeLLMProvider()
    planner = PlannerAgent(llm_provider=llm)
    analyst = AnalystAgent(llm_provider=llm, config=_analyst_config())
    failed_step = PlanStep(
        step_id="step-1",
        agent_name=analyst.specification.name,
        task_kind=AgentTaskKind.analyst,
        question="Analyze commerce performance",
        input={},
        expected_output=analyst.specification.output_contract,
    )
    state = PlanExecutionState(
        original_question="Analyze commerce performance",
        current_plan=ExecutionPlan(
            route="direct:analyst",
            steps=[failed_step],
            rationale="Use the only analyst.",
            requires_pev=True,
        ),
        replan_count=1,
        context={"failed_agent": analyst.specification.name},
    )
    state.record(
        step=failed_step,
        result=analyst.build_result(
            task=AgentTask(task_id="step-1", task_kind=AgentTaskKind.analyst, question="Analyze commerce performance"),
            status=AgentResultStatus.failed,
            output={},
            error="Execution failed: Binder Error: Cannot compare VARCHAR and DATE.",
        ),
        verification=VerificationOutcome(
            passed=False,
            step_id="step-1",
            agent_name=analyst.specification.name,
            message="Execution failed: Binder Error: Cannot compare VARCHAR and DATE.",
            reason_code=VerificationReasonCode.non_succeeded_status,
        ),
    )

    plan = _run(
        planner.replan(
            state=state,
            context_updates={"retry_hint": "Cast the date column before filtering."},
            specifications=[analyst.specification],
        )
    )

    assert [step.agent_name for step in plan.steps] == [analyst.specification.name]


def test_planner_does_not_emit_stale_avoid_agents_when_only_candidate_remains() -> None:
    class _RecordingPlannerLLM(_FakeLLMProvider):
        def __init__(self) -> None:
            self.prompts: list[str] = []

        async def acomplete(self, prompt: str, **kwargs):
            self.prompts.append(prompt)
            return (
                '{"route":"planned","rationale":"Retry with the only analyst.",'
                '"steps":[{"agent_name":"analyst","task_kind":"analyst","question":"Retry analysis","input":{},"depends_on":[]}]}'
            )

    llm = _RecordingPlannerLLM()
    planner = PlannerAgent(llm_provider=llm)
    analyst = AnalystAgent(llm_provider=llm, config=_analyst_config())
    state = PlanExecutionState(
        original_question="Retry analysis",
        current_plan=ExecutionPlan(
            route="planned",
            steps=[],
            rationale="initial",
            requires_pev=True,
        ),
        replan_count=1,
        context={"failed_agent": analyst.specification.name},
    )

    _run(
        planner.replan(
            state=state,
            context_updates={"failed_agent": analyst.specification.name},
            specifications=[analyst.specification],
        )
    )

    assert llm.prompts
    assert "Avoid agents: []" in llm.prompts[-1]


def test_registry_rejects_duplicate_agent_names() -> None:
    llm = _FakeLLMProvider()
    registry = AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())])

    try:
        registry.register(AnalystAgent(llm_provider=llm, config=_analyst_config()))
    except ValueError as exc:
        assert "already registered" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("duplicate agent registration should fail")


class _BrokenAnalystAgent(BaseAgent):
    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="broken-analyst",
            description="Broken test agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["explain"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["answer"]),
        )

    async def execute(self, task: AgentTask):
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={},
        )


class _RecoveringAnalystAgent(BaseAgent):
    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Recovering analyst test agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["explain"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["answer"]),
        )

    async def execute(self, task: AgentTask):
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"answer": "Analyst recovered answer."},
        )


class _RevisableAnalystAgent(BaseAgent):
    def __init__(self) -> None:
        self.calls = 0
        self.contexts: list[dict[str, object]] = []

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Revisable analyst test agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["revise"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["answer"]),
        )

    async def execute(self, task: AgentTask):
        self.calls += 1
        self.contexts.append(dict(task.context))
        answer = "Initial answer."
        if task.context.get("final_review_rationale"):
            answer = "Revised answer."
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"answer": answer},
        )


class _ArtifactContractPresentationAgent(BaseAgent):
    def __init__(self) -> None:
        self.calls = 0

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="presentation",
            description="Presentation test agent with an intentionally broken artifact contract.",
            task_kinds=[AgentTaskKind.presentation],
            output_contract=AgentIOContract(required_keys=["response"]),
        )

    async def execute(self, task: AgentTask):
        self.calls += 1
        if task.context.get("presentation_revision_request"):
            response = {
                "summary": "Revised artifact-safe answer.",
                "result": {},
                "visualization": None,
                "research": {},
                "answer": "Revised artifact-safe answer.",
                "answer_markdown": "Revised artifact-safe answer.",
                "artifacts": [],
                "diagnostics": {"mode": "test"},
            }
        else:
            response = {
                "summary": "Broken artifact answer.",
                "result": {},
                "visualization": None,
                "research": {},
                "answer": "Broken artifact answer.\n\n{{artifact:bad_table}}",
                "answer_markdown": "Broken artifact answer.\n\n{{artifact:bad_table}}",
                "artifacts": [
                    {
                        "id": "bad_table",
                        "type": "table",
                        "role": "primary_result",
                        "title": "Broken table",
                        "payload": {"columns": ["region", "revenue"]},
                    }
                ],
                "diagnostics": {"mode": "test"},
            }
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"response": response},
        )


class _RecordingFollowUpAnalystAgent(BaseAgent):
    def __init__(self) -> None:
        self.calls = 0
        self.questions: list[str] = []
        self.inputs: list[dict[str, object]] = []
        self.contexts: list[dict[str, object]] = []

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Records follow-up execution details.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["same", "break", "quarter"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["answer"]),
        )

    async def execute(self, task: AgentTask):
        self.calls += 1
        self.questions.append(task.question)
        self.inputs.append(dict(task.input))
        self.contexts.append(dict(task.context))
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"answer": f"Executed follow-up question: {task.question}"},
        )


class _ResearchRecordingAnalystAgent(BaseAgent):
    def __init__(self) -> None:
        self.calls = 0
        self.questions: list[str] = []
        self.inputs: list[dict[str, object]] = []

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Records research-route execution details.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["search", "web"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["analysis", "result", "synthesis", "sources", "findings"]),
            metadata={"supported_modes": ["auto", "sql", "context_analysis", "research"]},
        )

    async def execute(self, task: AgentTask):
        self.calls += 1
        self.questions.append(task.question)
        self.inputs.append(dict(task.input))
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={
                "analysis": f"Recorded research analysis for: {task.question}",
                "result": {"columns": ["source"], "rows": [["stub"]] },
                "synthesis": "Source-backed synthesis.",
                "sources": ["stub-source"],
                "findings": ["stub-finding"],
            },
        )


class _RetryableFailureAnalystAgent(BaseAgent):
    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Retryable failing analyst test agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["explain"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["analysis", "result"]),
        )

    async def execute(self, task: AgentTask):
        return self.build_result(
            task=task,
            status=AgentResultStatus.failed,
            output={
                "analysis": "",
                "result": {},
                "outcome": {"recoverable": True},
            },
            error="temporary failure",
        )


class _NonRecoverableFailureAnalystAgent(BaseAgent):
    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Non-recoverable failing analyst test agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["explain"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["analysis", "result"]),
        )

    async def execute(self, task: AgentTask):
        return self.build_result(
            task=task,
            status=AgentResultStatus.failed,
            output={
                "analysis": "",
                "result": {},
                "outcome": {"recoverable": False},
            },
            error="Execution failed: Binder Error: Cannot compare VARCHAR and DATE.",
        )


class _RetryContextAnalystAgent(BaseAgent):
    def __init__(self) -> None:
        self.calls = 0
        self.contexts: list[dict[str, object]] = []

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="analyst",
            description="Retry context test agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["explain"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["answer"]),
        )

    async def execute(self, task: AgentTask):
        self.calls += 1
        self.contexts.append(dict(task.context))
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


class _OrderedAnalystAgent(BaseAgent):
    def __init__(self, *, name: str, answer: str, call_order: list[str]) -> None:
        self._name = name
        self._answer = answer
        self._call_order = call_order

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name=self._name,
            description=f"{self._name} ordered step agent.",
            task_kinds=[AgentTaskKind.analyst],
            routing=AgentRoutingSpec(keywords=["dependency"], direct_threshold=1),
            output_contract=AgentIOContract(required_keys=["answer"]),
        )

    async def execute(self, task: AgentTask):
        self._call_order.append(self._name)
        return self.build_result(
            task=task,
            status=AgentResultStatus.succeeded,
            output={"answer": self._answer},
        )


class _StaticPlanner:
    def __init__(self, *, plan: ExecutionPlan) -> None:
        self._plan = plan

    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="planner",
            description="Static planner for dependency tests.",
            task_kinds=[AgentTaskKind.orchestration],
            routing=AgentRoutingSpec(keywords=["plan"], direct_threshold=99),
            output_contract=AgentIOContract(required_keys=["plan"]),
            can_execute_direct=False,
        )

    async def build_plan(
        self,
        *,
        question: str,
        context: dict[str, object],
        specifications: list[AgentSpecification],
    ) -> ExecutionPlan:
        _ = (question, context, specifications)
        return self._plan

    async def replan(self, *, state, context_updates=None, specifications=None) -> ExecutionPlan:
        _ = (state, context_updates, specifications)
        return self._plan


class _PlannerShouldNotReplan:
    @property
    def specification(self) -> AgentSpecification:
        return AgentSpecification(
            name="planner",
            description="Planner that should not be called.",
            task_kinds=[AgentTaskKind.orchestration],
            routing=AgentRoutingSpec(keywords=["plan"], direct_threshold=99),
            output_contract=AgentIOContract(required_keys=["plan"]),
            can_execute_direct=False,
        )

    async def build_plan(self, *, question: str, context: dict[str, object], specifications: list[AgentSpecification]):
        raise AssertionError("planner.build_plan should not be called")

    async def replan(self, *, state, context_updates=None, specifications=None) -> ExecutionPlan:
        raise AssertionError("planner.replan should not be called for revise_answer")


def test_pev_replans_after_missing_required_output_key() -> None:
    llm = _FakeLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([_BrokenAnalystAgent(), _RecoveringAnalystAgent()]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
        max_replans=1,
    )

    run = _run(controller.handle(question="Explain this result"))

    assert run.execution_mode == "planned"
    assert run.status == "completed"
    assert run.verification[0].passed is False
    assert run.verification[0].reason_code == VerificationReasonCode.missing_output_keys
    assert run.verification[0].missing_output_keys == ["answer"]
    assert run.review_decisions[0].action == PlanReviewAction.revise_plan
    assert run.review_decisions[0].reason_code == PlanReviewReasonCode.deterministic_verification_failed
    assert run.review_decisions[-1].action == PlanReviewAction.finalize
    assert run.diagnostics["replan_count"] == 1
    assert run.final_result["answer"] == "Analyst recovered answer."


def test_meta_controller_preserves_requested_agent_mode_for_direct_analyst() -> None:
    run = _run(
        _controller().handle(
            question="Explain the current context result",
            context={
                "agent_mode": "context_analysis",
                "result": {"columns": ["region", "revenue"], "rows": [["US", 2200]]},
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.plan.steps[0].input["agent_mode"] == "context_analysis"
    assert run.status == "completed"


def test_meta_controller_normalizes_fresh_context_analysis_choice_to_auto() -> None:
    llm = _FreshContextAnalysisRouteLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Fresh context-analysis route",
            context={"result": {"columns": ["region", "revenue"], "rows": [["US", 2200]]}},
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert "agent_mode" not in run.plan.steps[0].input
    assert run.final_result["summary"] == "Final answer from verified outputs."


def test_meta_controller_normalizes_legacy_answer_mode_alias() -> None:
    llm = _AnswerAliasRouteLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Explain the verified result",
            context={"result": {"columns": ["region", "revenue"], "rows": [["US", 2200]]}},
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert "agent_mode" not in run.plan.steps[0].input
    assert run.final_result["summary"] == "Final answer from verified outputs."


def test_meta_controller_reuses_continuation_state_for_chart_follow_up_without_route_llm() -> None:
    llm = _ChartFollowUpShouldNotRouteLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Show me in a pie chart",
            context={
                "continuation_state": {
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [["Online", 125000, 42000], ["Retail", 98000, 31500]],
                    },
                    "chartable": True,
                    "selected_agent": "analyst",
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.route == "direct:analyst"
    assert run.plan.steps[0].input["agent_mode"] == "context_analysis"
    assert run.plan.steps[0].input["reuse_last_result"] is True
    assert run.plan.steps[0].input["follow_up_intent"] == "visualize_prior_result"
    assert run.diagnostics["route_decision"]["agent_name"] == "analyst"
    assert run.final_result["visualization"]["chart_type"] == "pie"
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_carries_requested_chart_type_from_follow_up_resolution() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Make that a bar chart",
            context={
                "continuation_state": {
                    "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [["Online", 125000, 42000], ["Retail", 98000, 31500]],
                    },
                    "chartable": True,
                    "selected_agent": "analyst",
                    "visualization_state": {
                        "chart_type": "pie",
                        "title": "Q3 2025 order channel mix",
                        "x": "order_channel",
                        "y": "net_revenue",
                    },
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "context_analysis"
    assert run.plan.steps[0].input["follow_up_intent"] == "visualize_prior_result"
    assert run.plan.steps[0].input["follow_up_chart_type"] == "bar"
    assert run.final_result["visualization"]["chart_type"] == "bar"
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_chart_follow_up_can_complete_after_final_review_requests_presentation_revision() -> None:
    llm = _ChartReviseThenApproveLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
        max_replans=0,
    )

    run = _run(
        controller.handle(
            question="Put this in a pie chart",
            context={
                "continuation_state": {
                    "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [["Online", 125000, 42000], ["Retail", 98000, 31500]],
                    },
                    "chartable": True,
                    "selected_agent": "analyst",
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.final_review["action"] == FinalReviewAction.approve.value
    assert run.diagnostics["stop_reason"] == "final_review_presentation_revision"
    assert run.diagnostics["replan_count"] == 0
    assert run.final_result["visualization"]["chart_type"] == "pie"
    assert "pie-chart-ready split" in run.final_result["answer"]


def test_meta_controller_requeries_for_chart_follow_up_with_structured_multi_filters() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    analyst = _RecordingFollowUpAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Same chart, but only online and EU",
            context={
                "continuation_state": {
                    "resolved_question": (
                        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
                        "Filter the analysis to only EU for region. Present the result as a pie chart."
                    ),
                    "summary": "Q3 2025 EU order channel performance.",
                    "result": {
                        "columns": ["order_channel", "region", "net_revenue", "gross_margin"],
                        "rows": [["Online", "EU", 125000, 42000], ["Retail", "EU", 98000, 31500]],
                    },
                    "chartable": True,
                    "selected_agent": "analyst",
                    "visualization_state": {
                        "chart_type": "pie",
                        "title": "Q3 2025 EU order channel mix",
                        "x": "order_channel",
                        "y": "net_revenue",
                    },
                    "analysis_state": {
                        "available_fields": ["order channel", "region", "net revenue", "gross margin"],
                        "metrics": ["net revenue", "gross margin"],
                        "dimensions": ["order channel", "region"],
                        "primary_dimension": "order channel",
                        "dimension_value_samples": {
                            "order channel": ["Online", "Retail", "Wholesale"],
                            "region": ["EU", "US"],
                        },
                        "active_filters": [
                            {"field": "region", "operator": "include", "values": ["EU"]},
                        ],
                    },
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "sql"
    assert run.plan.steps[0].input["follow_up_chart_type"] == "pie"
    assert run.plan.steps[0].input["follow_up_filters"] == [
        {"field": "order channel", "operator": "include", "values": ["Online"]},
        {"field": "region", "operator": "include", "values": ["EU"]},
    ]
    assert run.plan.steps[0].input["active_filters"] == [
        {"field": "order channel", "operator": "include", "values": ["Online"]},
        {"field": "region", "operator": "include", "values": ["EU"]},
    ]
    assert run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Filter the analysis to only EU for region. Present the result as a pie chart. "
        "Filter the analysis to only Online for order channel. "
        "Filter the analysis to only EU for region. Present the result as a pie chart."
    )
    assert analyst.questions[-1] == run.plan.steps[0].question
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_reuses_prior_result_for_same_but_metric_follow_up() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Same but gross margin",
            context={
                "continuation_state": {
                    "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [["Online", 125000, 42000], ["Retail", 98000, 31500]],
                    },
                    "chartable": True,
                    "selected_agent": "analyst",
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "context_analysis"
    assert run.plan.steps[0].input["reuse_last_result"] is True
    assert run.plan.steps[0].input["follow_up_intent"] == "analyze_prior_result"
    assert run.plan.steps[0].input["follow_up_focus_field"] == "gross margin"
    assert "focus on gross margin" in run.plan.steps[0].question.casefold()
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_rewrites_exclude_filter_follow_up_from_structured_state() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    analyst = _RecordingFollowUpAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Exclude retail",
            context={
                "continuation_state": {
                    "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [["Online", 125000, 42000], ["Retail", 98000, 31500]],
                    },
                    "selected_agent": "analyst",
                    "analysis_state": {
                        "available_fields": ["order channel", "net revenue", "gross margin"],
                        "metrics": ["net revenue", "gross margin"],
                        "dimensions": ["order channel"],
                        "primary_dimension": "order channel",
                        "dimension_value_samples": {"order channel": ["Online", "Retail"]},
                    },
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "sql"
    assert run.plan.steps[0].input["follow_up_intent"] == "requery_prior_analysis"
    assert run.plan.steps[0].input["follow_up_filter"] == {
        "field": "order channel",
        "operator": "exclude",
        "value": "Retail",
    }
    assert run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Exclude Retail from order channel."
    )
    assert analyst.questions[-1] == run.plan.steps[0].question
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_clarifies_ambiguous_filter_follow_up() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Exclude retail",
            context={
                "continuation_state": {
                    "question": "Which group drove the highest net revenue in Q3 2025?",
                    "selected_agent": "analyst",
                    "analysis_state": {
                        "available_fields": ["order channel", "segment", "net revenue"],
                        "metrics": ["net revenue"],
                        "dimensions": ["order channel", "segment"],
                        "primary_dimension": "order channel",
                        "dimension_value_samples": {
                            "order channel": ["Retail", "Online"],
                            "segment": ["Retail", "Enterprise"],
                        },
                    },
                }
            },
        )
    )

    assert run.status == "clarification_needed"
    assert run.plan.route == "clarification"
    assert run.final_result["answer"] == (
        "I found 'retail' in multiple fields: order channel and segment. Which field should I use?"
    )
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_rewrites_multi_value_filter_follow_up_from_structured_state() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    analyst = _RecordingFollowUpAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Exclude retail and wholesale",
            context={
                "continuation_state": {
                    "resolved_question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [
                            ["Online", 125000, 42000],
                            ["Retail", 98000, 31500],
                            ["Wholesale", 87000, 29000],
                        ],
                    },
                    "selected_agent": "analyst",
                    "analysis_state": {
                        "available_fields": ["order channel", "net revenue", "gross margin"],
                        "metrics": ["net revenue", "gross margin"],
                        "dimensions": ["order channel"],
                        "primary_dimension": "order channel",
                        "dimension_value_samples": {"order channel": ["Online", "Retail", "Wholesale"]},
                    },
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "sql"
    assert run.plan.steps[0].input["follow_up_filter"] == {
        "field": "order channel",
        "operator": "exclude",
        "value": "Retail",
    }
    assert run.plan.steps[0].input["follow_up_filters"] == [
        {"field": "order channel", "operator": "exclude", "values": ["Retail", "Wholesale"]},
    ]
    assert run.plan.steps[0].input["active_filters"] == [
        {"field": "order channel", "operator": "exclude", "values": ["Retail", "Wholesale"]},
    ]
    assert run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Exclude Retail and Wholesale from order channel."
    )
    assert analyst.questions[-1] == run.plan.steps[0].question
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_resolves_metric_alias_from_structured_analysis_state() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([AnalystAgent(llm_provider=llm, config=_analyst_config())]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Same but margin",
            context={
                "continuation_state": {
                    "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [["Online", 125000, 42000], ["Retail", 98000, 31500]],
                    },
                    "chartable": True,
                    "selected_agent": "analyst",
                    "analysis_state": {
                        "available_fields": ["order channel", "net revenue", "gross margin"],
                        "metrics": ["net revenue", "gross margin"],
                        "dimensions": ["order channel"],
                        "primary_dimension": "order channel",
                        "period": {"kind": "quarter", "quarter": "Q3", "year": "2025", "label": "Q3 2025"},
                    },
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "context_analysis"
    assert run.plan.steps[0].input["follow_up_focus_field"] == "gross margin"
    assert "focus on gross margin" in run.plan.steps[0].question.casefold()
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_rewrites_breakdown_follow_up_to_sql_from_prior_question() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    analyst = _RecordingFollowUpAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Break that down by region",
            context={
                "continuation_state": {
                    "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "result": {
                        "columns": ["order_channel", "net_revenue", "gross_margin"],
                        "rows": [["Online", 125000, 42000], ["Retail", 98000, 31500]],
                    },
                    "selected_agent": "analyst",
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "sql"
    assert run.plan.steps[0].input["follow_up_intent"] == "requery_prior_analysis"
    assert run.plan.steps[0].input["follow_up_dimension"] == "region"
    assert run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Break the analysis down by region."
    )
    assert analyst.questions[-1] == run.plan.steps[0].question
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_rewrites_q4_follow_up_to_sql_from_prior_question() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    analyst = _RecordingFollowUpAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Make that Q4",
            context={
                "continuation_state": {
                    "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
                    "summary": "Q3 2025 order channel performance.",
                    "analysis_state": {
                        "period": {"kind": "quarter", "quarter": "Q3", "year": "2025", "label": "Q3 2025"},
                    },
                    "selected_agent": "analyst",
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "sql"
    assert run.plan.steps[0].input["follow_up_intent"] == "requery_prior_analysis"
    assert run.plan.steps[0].input["follow_up_period"]["label"] == "Q4 2025"
    assert run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q4 2025?"
    )
    assert analyst.questions[-1] == run.plan.steps[0].question
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_rewrites_last_12_months_follow_up_to_sql_from_prior_question() -> None:
    llm = _NonChartFollowUpShouldNotRouteLLMProvider()
    analyst = _RecordingFollowUpAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([analyst]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
    )

    run = _run(
        controller.handle(
            question="Make that last 12 months",
            context={
                "continuation_state": {
                    "question": "Which regions had the highest cost per signup in 2025?",
                    "summary": "2025 regional cost per signup.",
                    "analysis_state": {
                        "period": {"kind": "year", "year": "2025", "label": "2025"},
                    },
                    "selected_agent": "analyst",
                }
            },
        )
    )

    assert run.execution_mode == "direct"
    assert run.status == "completed"
    assert run.plan.steps[0].input["agent_mode"] == "sql"
    assert run.plan.steps[0].input["follow_up_intent"] == "requery_prior_analysis"
    assert run.plan.steps[0].input["follow_up_period"]["kind"] == "rolling_window"
    assert run.plan.steps[0].input["follow_up_period"]["label"] == "last 12 months"
    assert run.plan.steps[0].question == "Which regions had the highest cost per signup in last 12 months?"
    assert analyst.questions[-1] == run.plan.steps[0].question
    assert all("Decide Langbridge agent route" not in prompt for prompt in llm.prompts)


def test_meta_controller_surfaces_last_error_when_iteration_budget_exhausts() -> None:
    llm = _FakeLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([_RetryableFailureAnalystAgent()]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
        max_iterations=1,
        max_step_retries=1,
    )

    run = _run(controller.handle(question="Explain this result"))

    assert run.review_decisions[0].action == PlanReviewAction.retry_step
    assert run.review_decisions[0].reason_code == PlanReviewReasonCode.retryable_step_failure
    assert run.diagnostics["stop_reason"] == "max_iterations"
    assert run.diagnostics["terminal_error"] == "temporary failure"


def test_plan_review_does_not_retry_non_recoverable_execution_failure() -> None:
    llm = _FakeLLMProvider()
    controller = MetaControllerAgent(
        registry=AgentRegistry([_NonRecoverableFailureAnalystAgent()]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
        max_step_retries=1,
        max_replans=0,
    )

    run = _run(controller.handle(question="Explain this result"))

    assert run.review_decisions[0].action == PlanReviewAction.abort
    assert run.review_decisions[0].reason_code == PlanReviewReasonCode.verification_failed_after_replans
    assert run.diagnostics["stop_reason"] == "abort"


def test_retry_step_propagates_updated_context_to_future_execution() -> None:
    llm = _FakeLLMProvider()
    agent = _RetryContextAnalystAgent()
    controller = MetaControllerAgent(
        registry=AgentRegistry([agent]),
        llm_provider=llm,
        presentation_agent=_presentation(llm),
        max_step_retries=1,
    )

    run = _run(controller.handle(question="Explain this result"))

    assert run.status == "completed"
    assert agent.calls == 2
    assert agent.contexts[1]["last_error"] == "temporary failure"
    assert run.review_decisions[0].action == PlanReviewAction.retry_step
    assert run.review_decisions[0].reason_code == PlanReviewReasonCode.retryable_step_failure


def test_execution_honors_step_dependencies_when_plan_steps_are_out_of_order() -> None:
    llm = _FakeLLMProvider()
    call_order: list[str] = []
    prepare = _OrderedAnalystAgent(name="prepare-analyst", answer="Prepared answer.", call_order=call_order)
    finalize = _OrderedAnalystAgent(name="final-analyst", answer="Final answer.", call_order=call_order)
    plan = ExecutionPlan(
        route="planned:dependency",
        rationale="Dependency-aware execution order.",
        steps=[
            PlanStep(
                step_id="step-2",
                agent_name="final-analyst",
                task_kind=AgentTaskKind.analyst,
                question="Finalize the answer",
                expected_output=finalize.specification.output_contract,
                depends_on=["step-1"],
            ),
            PlanStep(
                step_id="step-1",
                agent_name="prepare-analyst",
                task_kind=AgentTaskKind.analyst,
                question="Prepare the answer",
                expected_output=prepare.specification.output_contract,
            ),
        ],
    )
    controller = MetaControllerAgent(
        registry=AgentRegistry([prepare, finalize]),
        llm_provider=llm,
        planner=_StaticPlanner(plan=plan),
        presentation_agent=_presentation(llm),
    )

    run = _run(controller.handle(question="Dependency ordered request"))

    assert run.execution_mode == "planned"
    assert run.status == "completed"
    assert call_order == ["prepare-analyst", "final-analyst"]
    assert [item["agent_name"] for item in run.step_results] == ["prepare-analyst", "final-analyst"]


def test_plan_review_flags_nested_empty_tabular_results_as_weak_evidence() -> None:
    step = PlanStep(
        step_id="step-1",
        agent_name="analyst",
        task_kind=AgentTaskKind.analyst,
        question="Show revenue by region",
        expected_output=AgentIOContract(required_keys=["analysis", "result"]),
    )
    result = AgentResult(
        task_id="step-1",
        agent_name="analyst",
        status=AgentResultStatus.succeeded,
        output={
            "analysis": "No rows matched the query.",
            "result": {"columns": ["region", "revenue"], "rows": []},
            "outcome": {"status": "empty_result"},
        },
    )
    verification = VerificationOutcome(
        passed=True,
        step_id="step-1",
        agent_name="analyst",
        message="Step output passed deterministic verification.",
        reason_code=VerificationReasonCode.passed,
    )
    state = PlanExecutionState(
        original_question="Show revenue by region",
        current_plan=ExecutionPlan(route="direct:analyst", steps=[step], rationale="Direct analyst"),
        max_replans=1,
    )
    state.record(step=step, result=result, verification=verification)

    decision = PlanReviewAgent().review(state)

    assert decision.action == PlanReviewAction.revise_plan
    assert decision.reason_code == PlanReviewReasonCode.weak_evidence
    assert decision.updated_context["weak_result_agent"] == "analyst"


def test_presentation_agent_returns_chart_when_tabular_data_is_chartable() -> None:
    llm = _FakeLLMProvider()
    agent = _presentation(llm)
    result = _run(
        agent.execute(
            AgentTask(
                task_id="presentation",
                task_kind=AgentTaskKind.presentation,
                question="Show a bar chart of revenue by region",
                input={"mode": "final"},
                context={
                    "step_results": [
                        {
                            "output": {
                                "result": {
                                    "columns": ["region", "revenue"],
                                    "rows": [["US", 2200], ["EMEA", 1200], ["APAC", 900]],
                                }
                            }
                        }
                    ]
                },
                expected_output=agent.specification.output_contract,
            )
        )
    )

    response = result.output["response"]
    assert response["visualization"] is not None
    assert response["visualization"]["chart_type"] == "bar"


def test_analyst_deep_research_mode_exposes_structured_sources_and_findings() -> None:
    llm = _FakeLLMProvider()
    result = _run(
        AnalystAgent(llm_provider=llm, config=_analyst_config(research_enabled=True)).execute(
            AgentTask(
                task_id="research",
                task_kind=AgentTaskKind.analyst,
                question="Research Langbridge runtime architecture",
                input={"mode": "research"},
                context={
                    "sources": [
                        {
                            "title": "Runtime architecture",
                            "url": "https://example.test/runtime",
                            "snippet": "Runtime owns semantic and federated execution.",
                        }
                    ]
                },
            )
        )
    )

    assert result.output["synthesis"].startswith("Source-backed")
    assert result.output["sources"][0]["url"] == "https://example.test/runtime"
    assert result.output["findings"][0]["source"] == "https://example.test/langbridge"
