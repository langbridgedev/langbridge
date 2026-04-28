from langbridge.runtime.services.agents.agent_run_tool_factory import _SemanticScopeExecutor
from langbridge.semantic.errors import SemanticSqlInvalidFilterError, SemanticSqlInvalidMemberError


def test_semantic_scope_executor_marks_binder_type_failures_as_scope_fallback_eligible() -> None:
    failure = _SemanticScopeExecutor._semantic_failure(
        RuntimeError(
            "Binder Error: Cannot compare values of type VARCHAR and type DATE - an explicit cast is required"
        )
    )

    assert failure.stage.value == "execution"
    assert failure.recoverable is False
    assert failure.metadata["scope_fallback_eligible"] is True
    assert failure.metadata["semantic_failure_kind"] == "semantic_runtime_type_mismatch"


def test_semantic_scope_executor_marks_invalid_filter_failures_as_unsupported_shape() -> None:
    failure = _SemanticScopeExecutor._semantic_failure(
        SemanticSqlInvalidFilterError(
            "Semantic SQL filters only support literal values such as strings, numbers, booleans, NULL, "
            "or literal lists. Raw SQL expressions are not supported in semantic filters."
        )
    )

    assert failure.stage.value == "query"
    assert failure.recoverable is False
    assert failure.metadata["scope_fallback_eligible"] is True
    assert failure.metadata["semantic_failure_kind"] == "unsupported_semantic_sql_shape"


def test_semantic_scope_executor_marks_invalid_member_failures_as_coverage_gap() -> None:
    failure = _SemanticScopeExecutor._semantic_failure(
        SemanticSqlInvalidMemberError("Unknown semantic member 'orders.first_order_date'.")
    )

    assert failure.stage.value == "query"
    assert failure.recoverable is False
    assert failure.metadata["scope_fallback_eligible"] is True
    assert failure.metadata["semantic_failure_kind"] == "semantic_coverage_gap"
