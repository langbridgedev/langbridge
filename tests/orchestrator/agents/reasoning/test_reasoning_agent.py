import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from langbridge.orchestrator.agents.models import PlanExecutionArtifacts  # noqa: E402
from langbridge.orchestrator.agents.planner import Plan, PlanStep, RouteName  # noqa: E402
from langbridge.orchestrator.agents.reasoning.agent import ReasoningAgent  # noqa: E402
from langbridge.orchestrator.tools.sql_analyst.interfaces import (  # noqa: E402
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryResponse,
    QueryResult,
)


def _plan() -> Plan:
    return Plan(
        route=RouteName.SIMPLE_ANALYST.value,
        steps=[PlanStep(id="step_1", agent="Analyst", input={"question": "Revenue by store"})],
        justification="Use analyst.",
        user_summary="Analyze data.",
    )


def _response(status: AnalystOutcomeStatus, *, terminal: bool, rows: list[tuple[object, ...]] | None) -> AnalystQueryResponse:
    result = None
    if rows is not None:
        result = QueryResult(
            columns=["store", "revenue"],
            rows=rows,
            rowcount=len(rows),
            elapsed_ms=8,
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
        error="Canonical SQL failed to parse." if status == AnalystOutcomeStatus.query_error else None,
        outcome=AnalystExecutionOutcome(
            status=status,
            stage=AnalystOutcomeStage.query if status == AnalystOutcomeStatus.query_error else AnalystOutcomeStage.result,
            message="Canonical SQL failed to parse." if status == AnalystOutcomeStatus.query_error else "No rows matched the query." if status == AnalystOutcomeStatus.empty_result else None,
            recoverable=False,
            terminal=terminal,
        ),
    )


def test_reasoning_agent_preserves_empty_result_entity_resolution_retry() -> None:
    agent = ReasoningAgent(max_iterations=2)
    decision = agent.evaluate(
        iteration=0,
        plan=_plan(),
        artifacts=PlanExecutionArtifacts(
            analyst_result=_response(AnalystOutcomeStatus.empty_result, terminal=False, rows=[]),
            data_payload={"columns": ["store", "revenue"], "rows": []},
        ),
        diagnostics={"extra_context": {}},
        user_query="Show store Acme revenue",
    )

    assert decision.continue_planning is True
    assert decision.updated_context is not None
    assert decision.updated_context["reasoning"]["entity_resolution"]["entity_type"] == "store"


def test_reasoning_agent_stops_on_terminal_analyst_failure() -> None:
    agent = ReasoningAgent(max_iterations=2)
    decision = agent.evaluate(
        iteration=0,
        plan=_plan(),
        artifacts=PlanExecutionArtifacts(
            analyst_result=_response(AnalystOutcomeStatus.query_error, terminal=True, rows=None)
        ),
        diagnostics={"extra_context": {}},
        user_query="Revenue by region",
    )

    assert decision.continue_planning is False
    assert "terminal outcome" in (decision.rationale or "").lower()


def test_reasoning_agent_stops_on_terminal_access_denied() -> None:
    agent = ReasoningAgent(max_iterations=2)
    decision = agent.evaluate(
        iteration=0,
        plan=_plan(),
        artifacts=PlanExecutionArtifacts(
            analyst_result=_response(AnalystOutcomeStatus.access_denied, terminal=True, rows=None)
        ),
        diagnostics={"extra_context": {}},
        user_query="Revenue by payroll dataset",
    )

    assert decision.continue_planning is False
    assert "access_denied" in (decision.rationale or "")
