from langbridge.ai.base import AgentResult, AgentResultStatus, AgentTaskKind
from langbridge.ai.orchestration.meta_controller import MetaControllerRun
from langbridge.ai.orchestration.planner import ExecutionPlan, PlanStep
from langbridge.ai.orchestration.verification import VerificationOutcome, VerificationReasonCode
from langbridge.runtime.services.agents.response import AgentRunResponseBuilder


def test_execution_diagnostics_include_generated_sql_and_friendly_summary() -> None:
    run = MetaControllerRun(
        execution_mode="direct",
        status="completed",
        plan=ExecutionPlan(
            route="direct:analyst.commerce",
            rationale="Single analyst step.",
            steps=[
                PlanStep(
                    step_id="step-1",
                    agent_name="analyst.commerce",
                    task_kind=AgentTaskKind.analyst,
                    question="Show revenue by region",
                )
            ],
        ),
        step_results=[
            AgentResult(
                task_id="step-1",
                agent_name="analyst.commerce",
                status=AgentResultStatus.succeeded,
                output={
                    "analysis": "US revenue is 2200.",
                    "result": {
                        "columns": ["region", "revenue"],
                        "rows": [["US", 2200]],
                        "rowcount": 1,
                    },
                    "analysis_path": "dataset",
                    "query_scope": "dataset",
                    "sql_canonical": "SELECT region, revenue FROM orders",
                    "sql_executable": "SELECT orders.region, orders.revenue FROM orders",
                    "selected_datasets": ["orders"],
                    "outcome": {
                        "status": "success",
                        "stage": "result",
                        "message": None,
                    },
                    "evidence": {
                        "governed": {
                            "attempted": True,
                            "answered_question": True,
                            "query_scope": "dataset",
                            "used_fallback": False,
                        }
                    },
                },
                diagnostics={
                    "agent_mode": "sql",
                    "selected_tool": "dataset-orders",
                    "governed_attempt_count": 1,
                    "governed_tools_tried": ["dataset-orders"],
                },
            ).model_dump(mode="json")
        ],
        verification=[
            VerificationOutcome(
                passed=True,
                step_id="step-1",
                agent_name="analyst.commerce",
                message="Step output passed deterministic verification.",
                reason_code=VerificationReasonCode.passed,
            )
        ],
        final_result={
            "summary": "US revenue is 2200.",
            "answer": "US revenue is 2200.",
            "result": {"columns": ["region", "revenue"], "rows": [["US", 2200]]},
            "diagnostics": {"mode": "test"},
        },
        diagnostics={
            "selected_agent": "analyst.commerce",
            "stop_reason": "finalize",
            "iterations": 1,
            "replan_count": 0,
        },
    )

    response = AgentRunResponseBuilder().build_response(run)

    execution = response["diagnostics"]["execution"]
    assert execution["summary"] == (
        "Run completed via direct:analyst.commerce. Generated 1 SQL query across dataset. "
        "Latest tabular result returned 1 row."
    )
    assert execution["selected_agent"] == "analyst.commerce"
    assert execution["total_sql_queries"] == 1
    assert execution["rowcount"] == 1
    assert execution["sql"][0]["sql_executable"] == "SELECT orders.region, orders.revenue FROM orders"
    assert execution["sql"][0]["rows_sample"] == [["US", 2200]]
    assert response["diagnostics"]["sql"][0]["query_scope"] == "dataset"
    assert response["diagnostics"]["ai_run"]["step_results"][0]["rowcount"] == 1
