"""Markdown-first presentation response contract."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


MARKDOWN_ARTIFACT_RESPONSE_VERSION = "markdown_artifacts.v1"


class PresentationResponseContract(BaseModel):
    """Public assistant response returned by the Langbridge AI runtime."""

    model_config = ConfigDict(extra="forbid")

    answer_markdown: str
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "MARKDOWN_ARTIFACT_RESPONSE_VERSION",
    "PresentationResponseContract",
]
