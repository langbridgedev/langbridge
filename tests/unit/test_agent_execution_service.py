from __future__ import annotations
import sys
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from langbridge.packages.runtime.services.agent_execution_service import (  # noqa: E402
    AgentExecutionService,
)
from langbridge.packages.runtime.models import RuntimeThreadState  # noqa: E402


def test_build_planning_context_ignores_inactive_clarification_state() -> None:
    thread = SimpleNamespace(
        metadata_json={
            "clarification_state": {
                "turn_count": 1,
                "asked_questions": [],
                "pending_slots": [],
            }
        }
    )

    context = AgentExecutionService._build_planning_context(
        base_context={},
        thread=thread,
        memory_context=SimpleNamespace(short_term_context="", retrieved_items=[]),
    )

    assert "clarification_state" not in context


def test_persist_supervisor_state_clears_resolved_clarification_state() -> None:
    thread = SimpleNamespace(
        metadata={
            "clarification_state": {
                "turn_count": 1,
                "asked_questions": ["Which customer segment?"],
                "pending_slots": ["segment"],
            }
        }
    )

    AgentExecutionService._persist_supervisor_state(
        thread,
        {
            "diagnostics": {
                "clarification_state": {
                    "turn_count": 1,
                    "asked_questions": [],
                    "pending_slots": [],
                }
            }
        },
    )

    assert thread.metadata == {}

def test_set_thread_awaiting_user_input_sets_runtime_state() -> None:
    thread = SimpleNamespace(state="processing")

    AgentExecutionService._set_thread_awaiting_user_input(thread)

    assert thread.state == RuntimeThreadState.awaiting_user_input
