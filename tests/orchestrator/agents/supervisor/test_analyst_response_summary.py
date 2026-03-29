import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from langbridge.orchestrator.agents.supervisor.orchestrator import SupervisorOrchestrator  # noqa: E402
from langbridge.orchestrator.definitions import ResponseMode  # noqa: E402
from langbridge.orchestrator.runtime import analysis_grounding  # noqa: E402
from langbridge.orchestrator.tools.sql_analyst.interfaces import (  # noqa: E402
    AnalystQueryResponse,
    QueryResult,
)


def _analyst_result(*, columns: list[str], rows: list[tuple[object, ...]]) -> AnalystQueryResponse:
    return AnalystQueryResponse(
        analysis_path="dataset",
        execution_mode="federated",
        asset_type="dataset",
        asset_id="sales_dataset",
        asset_name="sales",
        sql_canonical="select 1",
        sql_executable="select 1",
        dialect="postgres",
        result=QueryResult(
            columns=columns,
            rows=rows,
            rowcount=len(rows),
            elapsed_ms=12,
            source_sql="select 1",
        ),
    )


def test_analyst_summary_answers_direct_question_with_ranked_findings() -> None:
    payload = {
        "columns": ["region", "revenue"],
        "rows": [("US", 2200), ("EMEA", 1200), ("APAC", 800)],
    }
    summary = SupervisorOrchestrator._build_response_summary(
        user_query="Which region had the highest revenue?",
        data_payload=payload,
        visualization=None,
        analyst_result=_analyst_result(columns=payload["columns"], rows=payload["rows"]),
        clarifying_question=None,
        response_mode=ResponseMode.analyst,
    )

    assert "US" in summary
    assert "highest revenue" in summary.lower()
    assert "EMEA" in summary
    assert "APAC" in summary
    assert "Found 3 rows across 2 columns" not in summary


def test_analyst_summary_stays_grounded_without_inventing_trends() -> None:
    payload = {
        "columns": ["region", "revenue"],
        "rows": [("US", 2200), ("EMEA", 1200), ("APAC", 800)],
    }
    summary = SupervisorOrchestrator._build_response_summary(
        user_query="Compare revenue by region",
        data_payload=payload,
        visualization=None,
        analyst_result=_analyst_result(columns=payload["columns"], rows=payload["rows"]),
        clarifying_question=None,
        response_mode=ResponseMode.analyst,
    )

    assert "trend" not in summary.lower()
    assert "increased" not in summary.lower()
    assert "decreased" not in summary.lower()
    assert "caused" not in summary.lower()


def test_analyst_summary_handles_two_row_comparison() -> None:
    payload = {
        "columns": ["segment", "profit"],
        "rows": [("Enterprise", 540), ("SMB", 420)],
    }
    summary = SupervisorOrchestrator._build_response_summary(
        user_query="Compare profit between enterprise and SMB",
        data_payload=payload,
        visualization=None,
        analyst_result=_analyst_result(columns=payload["columns"], rows=payload["rows"]),
        clarifying_question=None,
        response_mode=ResponseMode.analyst,
    )

    assert "Enterprise" in summary
    assert "SMB" in summary
    assert "540" in summary
    assert "420" in summary
    assert "gap" in summary.lower() or "higher" in summary.lower()


def test_analyst_summary_calls_out_sparse_or_empty_results() -> None:
    payload = {
        "columns": ["region", "revenue"],
        "rows": [],
    }
    summary = SupervisorOrchestrator._build_response_summary(
        user_query="Which region had the highest revenue?",
        data_payload=payload,
        visualization=None,
        analyst_result=_analyst_result(columns=payload["columns"], rows=[]),
        clarifying_question=None,
        response_mode=ResponseMode.analyst,
    )

    assert "No rows matched the query" in summary
    assert "grounded analytical" in summary


def test_non_analyst_modes_preserve_existing_summary_shape() -> None:
    payload = {
        "columns": ["region", "revenue"],
        "rows": [("US", 2200), ("EMEA", 1200), ("APAC", 800)],
    }
    summary = SupervisorOrchestrator._build_response_summary(
        user_query="Which region had the highest revenue?",
        data_payload=payload,
        visualization=None,
        analyst_result=_analyst_result(columns=payload["columns"], rows=payload["rows"]),
        clarifying_question=None,
        response_mode=ResponseMode.executive,
    )

    assert summary == "Found 3 rows across 2 columns for 'Which region had the highest revenue?'."


def test_analyst_result_summary_includes_grounding_context() -> None:
    payload = {
        "columns": ["region", "revenue"],
        "rows": [("US", 2200), ("EMEA", 1200), ("APAC", 800)],
    }
    result_summary = SupervisorOrchestrator._summarize_analyst_result(
        _analyst_result(columns=payload["columns"], rows=payload["rows"]),
        payload,
        question="Which region had the highest revenue?",
    )

    grounding = result_summary.get("analyst_grounding")
    assert isinstance(grounding, dict)
    assert grounding.get("primary_measure") == "revenue"
    assert grounding.get("primary_dimension") == "region"
    assert grounding.get("observed_facts")


def test_analyst_grounding_falls_back_instead_of_throwing(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise TypeError("'NoneType' object is not callable")

    monkeypatch.setattr(analysis_grounding, "_build_analyst_grounding", _boom)

    grounding = analysis_grounding.build_analyst_grounding(
        "Which region had the highest revenue?",
        {"columns": ["region", "revenue"], "rows": [("US", 2200)]},
    )

    assert grounding["analysis_type"] == "fallback"
    assert grounding["caveats"]
