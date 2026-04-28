from langbridge.ai.orchestration.continuation import (
    ContinuationState,
    ContinuationStateBuilder,
    FollowUpResolver,
)
from langbridge.ai.orchestration.timeframes import (
    extract_timeframe_state,
    rewrite_prior_question_for_requested_timeframe,
)


def test_continuation_state_builder_anchors_to_last_passed_step() -> None:
    response = {
        "summary": "Q4 2025 channel answer.",
        "answer": "Q4 2025 channel answer.",
        "result": {
            "columns": ["order_channel", "net_revenue"],
            "rows": [["Online", 125000]],
        },
        "diagnostics": {
            "ai_run": {
                "status": "completed",
                "plan": {
                    "steps": [
                        {
                            "step_id": "step-1",
                            "agent_name": "analyst.commerce_sql",
                            "question": "Which order channels drove the highest net revenue in Q3 2025?",
                            "input": {},
                        },
                        {
                            "step_id": "r1-step-1",
                            "agent_name": "analyst.commerce_sql",
                            "question": "Which order channels drove the highest net revenue in Q4 2025?",
                            "input": {
                                "follow_up_period": {
                                    "kind": "quarter",
                                    "quarter": "Q4",
                                    "year": "2025",
                                    "label": "Q4 2025",
                                }
                            },
                        },
                    ]
                },
                "verification": [
                    {"step_id": "step-1", "passed": False},
                    {"step_id": "r1-step-1", "passed": True},
                ],
                "diagnostics": {"selected_agent": "analyst.commerce_sql"},
            }
        },
    }

    state = ContinuationStateBuilder.from_response(
        response=response,
        user_query="Make that Q4",
    )

    assert state is not None
    assert state.resolved_question == "Which order channels drove the highest net revenue in Q4 2025?"
    assert state.analysis_state is not None
    assert state.analysis_state.period is not None
    assert state.analysis_state.period.label == "Q4 2025"


def test_follow_up_resolver_preserves_literal_ampersand_values() -> None:
    continuation_state = ContinuationState.model_validate(
        {
            "resolved_question": "Show revenue by category",
            "selected_agent": "analyst",
            "analysis_state": {
                "available_fields": ["category", "revenue"],
                "metrics": ["revenue"],
                "dimensions": ["category"],
                "primary_dimension": "category",
                "dimension_value_samples": {
                    "category": ["Health & Beauty", "Home"],
                },
            },
        }
    )

    resolution = FollowUpResolver.resolve(
        question="Exclude Health & Beauty",
        continuation_state=continuation_state,
    )

    assert resolution is not None
    assert resolution.kind == "requery_prior_analysis"
    assert [item.model_dump(mode="json") for item in resolution.filters] == [
        {"field": "category", "operator": "exclude", "values": ["Health & Beauty"]}
    ]


def test_follow_up_resolver_clarifies_ambiguous_filter_value() -> None:
    continuation_state = ContinuationState.model_validate(
        {
            "resolved_question": "Show revenue by retail grouping",
            "selected_agent": "analyst",
            "analysis_state": {
                "available_fields": ["order channel", "segment", "revenue"],
                "metrics": ["revenue"],
                "dimensions": ["order channel", "segment"],
                "primary_dimension": "order channel",
                "dimension_value_samples": {
                    "order channel": ["Retail", "Online"],
                    "segment": ["Retail", "Enterprise"],
                },
            },
        }
    )

    resolution = FollowUpResolver.resolve(
        question="Exclude retail",
        continuation_state=continuation_state,
    )

    assert resolution is not None
    assert resolution.kind == "clarify_follow_up"
    assert resolution.clarification_question == (
        "I found 'retail' in multiple fields: order channel and segment. Which field should I use?"
    )


def test_extract_timeframe_state_parses_last_12_months() -> None:
    timeframe = extract_timeframe_state("Use the last 12 months for cost per signup")

    assert timeframe is not None
    assert timeframe.kind == "rolling_window"
    assert timeframe.label == "last 12 months"
    assert timeframe.quantity == 12
    assert timeframe.unit == "month"
    assert timeframe.relation == "last"


def test_rewrite_prior_question_replaces_year_with_rolling_window() -> None:
    rewritten = rewrite_prior_question_for_requested_timeframe(
        question="Make that last 12 months",
        prior_question="Which regions had the highest cost per signup in 2025?",
    )

    assert rewritten == "Which regions had the highest cost per signup in last 12 months?"


def test_follow_up_resolver_rewrites_last_12_months_from_prior_question() -> None:
    continuation_state = ContinuationState.model_validate(
        {
            "question": "Which regions had the highest cost per signup in 2025?",
            "resolved_question": "Which regions had the highest cost per signup in 2025?",
            "selected_agent": "analyst",
            "analysis_state": {
                "period": {"kind": "year", "year": "2025", "label": "2025"},
            },
        }
    )

    resolution = FollowUpResolver.resolve(
        question="Make that last 12 months",
        continuation_state=continuation_state,
    )

    assert resolution is not None
    assert resolution.kind == "requery_prior_analysis"
    assert resolution.period is not None
    assert resolution.period.kind == "rolling_window"
    assert resolution.period.label == "last 12 months"
    assert resolution.resolved_question == "Which regions had the highest cost per signup in last 12 months?"
