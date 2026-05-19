import uuid

import pytest
from pydantic import ValidationError

from langbridge.ai.base import AgentResult, AgentResultStatus, AgentTaskKind
from langbridge.ai.orchestration.meta_controller import MetaControllerRun
from langbridge.ai.orchestration.planner import ExecutionPlan, PlanStep
from langbridge.ai.orchestration.verification import VerificationOutcome, VerificationReasonCode
from langbridge.runtime.hosting.api_models import RuntimeAgentAskResponse
from langbridge.runtime.models import RuntimeMessageRole, RuntimeThread, RuntimeThreadMessage, RuntimeThreadState
from langbridge.runtime.services.agents.response import AgentRunResponseBuilder
from langbridge.runtime.services.agents.thread_state import AgentThreadStateManager


def _minimal_run() -> MetaControllerRun:
    return MetaControllerRun(
        execution_mode="direct",
        status="completed",
        plan=ExecutionPlan(route="direct:analyst.commerce", rationale="Single analyst step.", steps=[]),
        final_result={
            "answer_markdown": "US revenue is 2200.",
            "artifacts": [],
            "diagnostics": {"mode": "test"},
            "metadata": {},
        },
    )


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
                    "investigation_trace": [
                        {
                            "id": "entity-resolution",
                            "type": "entity_resolution",
                            "title": "Entity resolved",
                            "status": "resolved",
                            "summary": "Resolved governed entity before metric analysis.",
                        }
                    ],
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
            "answer_markdown": "US revenue is 2200.",
            "artifacts": [],
            "diagnostics": {"mode": "test"},
            "metadata": {},
        },
        diagnostics={
            "selected_agent": "analyst.commerce",
            "stop_reason": "finalize",
            "iterations": 1,
            "replan_count": 0,
        },
    )

    response = AgentRunResponseBuilder().build_response(run)

    assert response["answer_markdown"] == "US revenue is 2200."
    assert "summary" not in response
    assert "answer" not in response
    assert "result" not in response
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
    assert execution["step_results"][0]["diagnostics"]["investigation_trace"][0]["title"] == "Entity resolved"
    assert response["diagnostics"]["sql"][0]["query_scope"] == "dataset"
    assert response["diagnostics"]["ai_run"]["step_results"][0]["rowcount"] == 1


def test_public_agent_response_rejects_legacy_top_level_fields() -> None:
    payload = {
        "answer_markdown": "US revenue is 2200.",
        "artifacts": [],
        "diagnostics": {},
        "metadata": {},
        "summary": "Legacy summary",
        "answer": "Legacy answer",
        "result": {"columns": [], "rows": []},
        "visualization": {"chart_type": "bar"},
        "sql": [],
    }

    with pytest.raises(ValidationError):
        AgentRunResponseBuilder().public_response(payload)


def test_runtime_agent_ask_response_rejects_legacy_top_level_fields() -> None:
    with pytest.raises(ValidationError):
        RuntimeAgentAskResponse.model_validate(
            {
                "thread_id": uuid.uuid4(),
                "status": "succeeded",
                "job_id": uuid.uuid4(),
                "message_id": uuid.uuid4(),
                "answer_markdown": "US revenue is 2200.",
                "artifacts": [],
                "diagnostics": {},
                "metadata": {},
                "events": [],
                "summary": "Legacy summary",
            }
        )


class _RecordingThreadMessageStore:
    def __init__(self) -> None:
        self.messages: list[RuntimeThreadMessage] = []

    def add(self, message: RuntimeThreadMessage) -> None:
        self.messages.append(message)


def test_thread_state_records_only_markdown_artifact_assistant_content() -> None:
    workspace_id = uuid.uuid4()
    thread_id = uuid.uuid4()
    user_message_id = uuid.uuid4()
    agent_id = uuid.uuid4()
    store = _RecordingThreadMessageStore()
    manager = AgentThreadStateManager(
        thread_repository=object(),
        thread_message_repository=store,
        memory_writer=object(),
    )
    thread = RuntimeThread(
        id=thread_id,
        workspace_id=workspace_id,
        created_by=uuid.uuid4(),
        last_message_id=user_message_id,
        state=RuntimeThreadState.processing,
    )
    user_message = RuntimeThreadMessage(
        id=user_message_id,
        thread_id=thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "Show revenue by region"},
    )
    response = {
        "answer_markdown": "US revenue is 2200.",
        "artifacts": [],
        "diagnostics": {},
        "metadata": {},
        "summary": "Legacy summary",
        "answer": "Legacy answer",
        "result": {"columns": [], "rows": []},
        "visualization": {"chart_type": "bar"},
        "sql": [],
    }

    message = manager.record_assistant_message(
        thread=thread,
        user_message=user_message,
        response=response,
        agent_id=agent_id,
        ai_run=_minimal_run(),
        continuation_state={"summary": "Allowed internal continuation metadata."},
    )

    assert store.messages == [message]
    assert set(message.content) == {"answer_markdown", "artifacts", "diagnostics", "metadata"}
    assert "summary" not in message.content
    assert "answer" not in message.content
    assert "result" not in message.content
    assert "visualization" not in message.content
    assert "sql" not in message.content
    assert message.content["metadata"]["continuation_state"]["summary"] == "Allowed internal continuation metadata."
