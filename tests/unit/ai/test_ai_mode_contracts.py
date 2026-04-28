from pydantic import ValidationError

from langbridge.ai.agents.analyst.contracts import (
    AnalystEvidencePlan,
    AnalystEvidencePlanStep,
    AnalystModeDecision,
    VisualizationRecommendation,
)
from langbridge.ai.modes import (
    AnalystAgentMode,
    analyst_output_contract_for_task_input,
    normalize_analyst_mode_decision,
    normalize_visualization_recommendation,
)


def test_normalize_analyst_mode_decision_supports_legacy_context_alias() -> None:
    decision = normalize_analyst_mode_decision({"agent_mode": "context", "reason": "use prior result"})

    assert decision is not None
    assert decision.agent_mode == "context_analysis"


def test_normalize_analyst_mode_decision_supports_clarify() -> None:
    decision = normalize_analyst_mode_decision(
        {
            "agent_mode": "clarify",
            "reason": "Need a time period.",
            "clarification_question": "Which time period should I use?",
        }
    )

    assert decision is not None
    assert decision.agent_mode == "clarify"
    assert decision.clarification_question == "Which time period should I use?"


def test_analyst_mode_decision_rejects_unknown_mode() -> None:
    try:
        AnalystModeDecision.model_validate({"agent_mode": "dashboard", "reason": "nope"})
    except ValidationError:
        return
    raise AssertionError("Expected ValidationError for unsupported analyst mode decision.")


def test_visualization_recommendation_normalizes_mapping_payload() -> None:
    recommendation = normalize_visualization_recommendation(
        {"recommendation": "helpful", "chart_type": " Scatter ", "rationale": "Useful for the comparison."}
    )

    assert recommendation is not None
    assert recommendation.recommendation == "helpful"
    assert recommendation.chart_type == "scatter"


def test_analyst_output_contract_includes_richer_answer_fields() -> None:
    contract = analyst_output_contract_for_task_input({"agent_mode": AnalystAgentMode.research.value})

    assert "verdict" in contract.optional_keys
    assert "key_comparisons" in contract.optional_keys
    assert "visualization_recommendation" in contract.optional_keys
    assert "recommended_chart_type" in contract.optional_keys


def test_normalize_visualization_recommendation_allows_none() -> None:
    recommendation = normalize_visualization_recommendation(VisualizationRecommendation())

    assert recommendation is not None
    assert recommendation.recommendation == "none"


def test_analyst_evidence_plan_normalizes_steps() -> None:
    plan = AnalystEvidencePlan.model_validate(
        {
            "objective": " Compare support load and marketing efficiency ",
            "question_type": " relationship ",
            "required_metrics": [" support load ", "marketing efficiency"],
            "required_dimensions": [" region "],
            "steps": [
                AnalystEvidencePlanStep(
                    step_id=" e1 ",
                    question=" Measure support load by region ",
                    evidence_goal=" Gather support signal ",
                    depends_on=[""],
                ).model_dump(mode="json")
            ],
        }
    )

    assert plan.objective == "Compare support load and marketing efficiency"
    assert plan.question_type == "relationship"
    assert plan.required_metrics == ["support load", "marketing efficiency"]
    assert plan.required_dimensions == ["region"]
    assert plan.steps[0].step_id == "e1"
    assert plan.steps[0].depends_on == []
