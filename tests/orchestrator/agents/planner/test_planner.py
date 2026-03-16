import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

if "pandas" not in sys.modules:
    pandas_stub = SimpleNamespace(
        DataFrame=type("DataFrame", (), {}),
        api=SimpleNamespace(
            types=SimpleNamespace(
                is_numeric_dtype=lambda _series: False,
                is_object_dtype=lambda _series: False,
                is_categorical_dtype=lambda _series: False,
            )
        ),
    )
    sys.modules["pandas"] = pandas_stub

REPO_ROOT = Path(__file__).resolve().parents[4]
PACKAGE_ROOT = REPO_ROOT
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from langbridge.packages.orchestrator.langbridge_orchestrator.agents.planner import (  # noqa: E402  (path adjustment)
    PlanningAgent,
    PlannerRequest,
    PlanningConstraints,
)


@pytest.fixture()
def planner() -> PlanningAgent:
    return PlanningAgent()


def test_simple_analyst_route(planner: PlanningAgent) -> None:
    request = PlannerRequest(
        question="List total expenses for the ACME fund in 2024.",
        constraints=PlanningConstraints(prefer_low_latency=True, require_viz_when_chartable=False),
    )
    plan = planner.plan(request)

    assert plan.route == "SimpleAnalyst"
    assert len(plan.steps) == 1
    assert plan.steps[0].agent == "Analyst"


def test_analyst_then_visual_route(planner: PlanningAgent) -> None:
    request = PlannerRequest(
        question="Show me a chart of monthly revenue by region for 2024.",
    )
    plan = planner.plan(request)

    assert plan.route == "AnalystThenVisual"
    assert len(plan.steps) == 2
    assert plan.steps[0].agent == "Analyst"
    assert plan.steps[1].agent == "Visual"
    assert plan.steps[1].input["rows_ref"] == plan.steps[0].id


def test_deep_research_route(planner: PlanningAgent) -> None:
    request = PlannerRequest(
        question="Summarize the latest private markets outlook from PDFs and verify any performance claims.",
    )
    plan = planner.plan(request)

    assert plan.route == "DeepResearch"
    assert plan.steps[0].agent == "DocRetrieval"
    assert len(plan.steps) >= 1


def test_clarify_route_for_ambiguous_request(planner: PlanningAgent) -> None:
    request = PlannerRequest(question="Show me performance.")
    plan = planner.plan(request)

    assert plan.route == "Clarify"
    assert plan.steps[0].agent == "Clarify"
    assert "clarifying_question" in plan.steps[0].input


def test_short_entity_query_routes_to_analyst(planner: PlanningAgent) -> None:
    request = PlannerRequest(question="Show me my customers")
    plan = planner.plan(request)

    assert plan.route == "SimpleAnalyst"
    assert len(plan.steps) == 1
    assert plan.steps[0].agent == "Analyst"


def test_max_steps_constraint_respected(planner: PlanningAgent) -> None:
    request = PlannerRequest(
        question="Please visualise quarterly revenue by product line.",
        constraints=PlanningConstraints(max_steps=1, require_viz_when_chartable=True),
    )
    plan = planner.plan(request)

    assert plan.route == "SimpleAnalyst"
    assert len(plan.steps) == 1
    assert plan.steps[0].agent == "Analyst"


def test_deep_research_disallowed_by_constraints(planner: PlanningAgent) -> None:
    request = PlannerRequest(
        question="Summarize the latest market outlook reports from PDFs and emails.",
        constraints=PlanningConstraints(allow_deep_research=False),
    )
    plan = planner.plan(request)

    assert plan.route != "DeepResearch"
