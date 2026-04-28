class SemanticModelError(Exception):
    """Raised when the semantic model cannot satisfy a query."""


class SemanticQueryError(Exception):
    """Raised when the semantic query is invalid or unsupported."""


class JoinPathError(SemanticQueryError):
    """Raised when the required join path cannot be resolved."""


class SemanticSqlError(ValueError):
    """Raised when semantic SQL is invalid or unsupported."""

    category = "semantic_sql_error"

    def __init__(
        self,
        message: str,
        *,
        construct: str | None = None,
    ) -> None:
        super().__init__(message)
        self.construct = construct


class SemanticSqlParseError(SemanticSqlError):
    category = "parse_error"


class SemanticSqlUnsupportedConstructError(SemanticSqlError):
    category = "unsupported_construct"


class SemanticSqlInvalidMemberError(SemanticSqlError):
    category = "invalid_member"


class SemanticSqlAmbiguousMemberError(SemanticSqlError):
    category = "ambiguous_member"


class SemanticSqlInvalidGroupingError(SemanticSqlError):
    category = "invalid_grouping"


class SemanticSqlInvalidFilterError(SemanticSqlError):
    category = "invalid_filter"


class SemanticSqlUnsupportedExpressionError(SemanticSqlError):
    category = "unsupported_expression"


class SemanticSqlInvalidTimeBucketError(SemanticSqlError):
    category = "invalid_time_bucket"
