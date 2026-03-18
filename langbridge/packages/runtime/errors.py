from __future__ import annotations

from typing import Any


class ApplicationError(Exception):
    pass


class BusinessValidationError(ApplicationError):
    def __init__(self, message: str, errors: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.errors = errors or {}

    def __str__(self) -> str:
        return f"{self.message} - {self.errors}"
