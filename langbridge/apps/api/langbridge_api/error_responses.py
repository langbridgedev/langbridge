from __future__ import annotations

from typing import Any

from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse


def _stringify_details(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return str(value)


def _default_code(status_code: int) -> str:
    if status_code == 400:
        return "INVALID_REQUEST"
    if status_code == 401:
        return "AUTHENTICATION_REQUIRED"
    if status_code == 403:
        return "PERMISSION_DENIED"
    if status_code == 404:
        return "RESOURCE_NOT_FOUND"
    if status_code == 409:
        return "RESOURCE_ALREADY_EXISTS"
    if status_code == 422:
        return "VALIDATION_ERROR"
    if status_code >= 500:
        return "INTERNAL_ERROR"
    return "REQUEST_FAILED"


def _default_message(status_code: int) -> str:
    if status_code == 400:
        return "The request could not be completed."
    if status_code == 401:
        return "Authentication is required to continue."
    if status_code == 403:
        return "You do not have permission to perform this action."
    if status_code == 404:
        return "The requested resource could not be found."
    if status_code == 409:
        return "A resource with the same identity already exists."
    if status_code == 422:
        return "Some inputs are invalid."
    if status_code >= 500:
        return "An internal server error occurred."
    return "The request could not be completed."


def _dataset_file_missing(details: str) -> bool:
    lowered = details.lower()
    return (
        "no files found that match the pattern" in lowered
        or "no such file or directory" in lowered
        or "file.parquet" in lowered
    )


def infer_error_metadata(
    *,
    status_code: int,
    message: str | None,
    details: str | None,
    fallback_code: str | None = None,
) -> tuple[str, str, list[str]]:
    raw = details or message or ""
    lowered = raw.lower()

    if _dataset_file_missing(raw):
        return (
            "DATASET_FILE_NOT_FOUND",
            "Dataset file could not be located.",
            [
                "Re-run the dataset sync.",
                "Check the connector configuration.",
                "Restore a previous dataset revision if needed.",
            ],
        )

    if "syntax error" in lowered or "parser error" in lowered:
        return (
            "SQL_INVALID",
            "The SQL statement is invalid.",
            [
                "Review the SQL syntax and referenced objects.",
                "Confirm that the selected source supports the SQL dialect being used.",
            ],
        )

    if status_code == 404:
        return (
            fallback_code or _default_code(status_code),
            message or _default_message(status_code),
            ["Refresh the page and confirm the resource still exists."],
        )

    if status_code in {400, 422}:
        return (
            fallback_code or _default_code(status_code),
            message or _default_message(status_code),
            ["Review the current input and correct any invalid fields."],
        )

    return (
        fallback_code or _default_code(status_code),
        message or _default_message(status_code),
        [],
    )


def build_error_content(
    *,
    status_code: int,
    code: str | None = None,
    message: str | None = None,
    details: Any = None,
    suggestions: list[str] | None = None,
    field_errors: dict[str, str] | None = None,
) -> dict[str, Any]:
    technical_details = _stringify_details(details)
    resolved_code, resolved_message, inferred_suggestions = infer_error_metadata(
      status_code=status_code,
      message=message,
      details=technical_details,
      fallback_code=code,
    )

    return {
        "error": {
            "code": resolved_code,
            "message": resolved_message,
            "details": technical_details,
            "suggestions": suggestions or inferred_suggestions,
            "fieldErrors": field_errors or {},
        }
    }


def build_error_response(
    *,
    status_code: int,
    code: str | None = None,
    message: str | None = None,
    details: Any = None,
    suggestions: list[str] | None = None,
    field_errors: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=build_error_content(
            status_code=status_code,
            code=code,
            message=message,
            details=details,
            suggestions=suggestions,
            field_errors=field_errors,
        ),
    )


def build_validation_error_response(exc: RequestValidationError) -> JSONResponse:
    field_errors: dict[str, str] = {}
    for error in exc.errors():
        location = error.get("loc") or []
        field_name = ".".join(str(part) for part in location if part not in {"body", "query", "path"})
        if field_name:
            field_errors[field_name] = str(error.get("msg") or "Invalid value")

    return build_error_response(
        status_code=422,
        code="VALIDATION_ERROR",
        message="Some inputs are invalid.",
        details=exc.errors(),
        suggestions=["Review the highlighted fields and try again."],
        field_errors=field_errors,
    )
