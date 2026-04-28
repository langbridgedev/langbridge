"""Shared intent guards for analyst-oriented routing decisions."""

class AnalystQuestionIntent:
    """Small deterministic guardrails around LLM routing and mode decisions.

    The LLM remains responsible for the primary decision. These guards prevent a
    known product failure mode: asking business users to resolve analytical
    proxy or SQL implementation details before governed evidence is inspected.
    """

    _ASSUMPTION_FIRST_MARKERS = (
        " do ",
        " does ",
        " why ",
        " underperform",
        " underperforming",
        " efficiency",
        " support load",
        " slowdown",
        " slowing",
        " root cause",
        " associated",
        " association",
        " relationship",
        " correlate",
        " correlation",
        " compare",
    )
    _INTERNAL_EXECUTION_CLARIFICATION_MARKERS = (
        "varchar",
        "sql",
        "schema",
        "column",
        "table",
        "date format",
        "month format",
        "timestamp format",
        "parse",
        "parsed",
        "cast",
        "casting",
        "incomplete-period",
        "incomplete period",
    )

    @classmethod
    def is_assumption_first_question(cls, question: str) -> bool:
        text = cls._normalized(question)
        return any(marker in text for marker in cls._ASSUMPTION_FIRST_MARKERS)

    @classmethod
    def is_internal_execution_clarification(cls, clarification: str | None) -> bool:
        text = cls._normalized(clarification or "")
        return any(marker in text for marker in cls._INTERNAL_EXECUTION_CLARIFICATION_MARKERS)

    @classmethod
    def should_inspect_evidence_before_clarifying(
        cls,
        *,
        question: str,
        clarification: str | None = None,
    ) -> bool:
        return cls.is_assumption_first_question(question) or cls.is_internal_execution_clarification(clarification)

    @staticmethod
    def _normalized(text: str) -> str:
        return f" {str(text or '').casefold()} "


__all__ = ["AnalystQuestionIntent"]
