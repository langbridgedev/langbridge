"""Pydantic models used by the semantic query builder copilot tool."""
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from langbridge.packages.runtime.models import SemanticQueryResponse
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery


class QueryBuilderContext(BaseModel):
    """Optional UI hints sent alongside the builder state."""

    summary: Optional[str] = Field(
        default=None,
        description="High-level description of the dashboard tile or question.",
    )
    focus: Optional[str] = Field(
        default=None,
        description="Identifier for the UI element currently being edited (dimension, measure, etc.).",
    )
    timezone: Optional[str] = Field(
        default=None,
        description="Preferred timezone for rendering results.",
    )


class QueryBuilderCopilotRequest(BaseModel):
    """Incoming payload for the semantic query builder copilot."""

    organization_id: UUID
    project_id: Optional[UUID] = None
    semantic_model_id: UUID
    instructions: str = Field(..., min_length=1, description="What the user asked the copilot to do.")
    builder_state: SemanticQuery = Field(
        default_factory=SemanticQuery,
        description="Current semantic query represented by the UI.",
    )
    conversation_context: Optional[str] = Field(
        default=None,
        description="Optional chat transcript to preserve continuity.",
    )
    generate_preview: bool = Field(
        default=True,
        description="Execute the suggested query and return preview data when true.",
    )
    context: Optional[QueryBuilderContext] = Field(
        default=None,
        description="Additional hints from the dashboard experience.",
    )


class QueryBuilderCopilotResponse(BaseModel):
    """Structured response returned to the UI/agent."""

    updated_query: SemanticQuery = Field(
        ..., description="Semantic query after applying the copilot suggestions."
    )
    actions: List[str] = Field(
        default_factory=list,
        description="Readable list of steps the copilot applied.",
    )
    explanation: Optional[str] = Field(
        default=None,
        description="Optional textual explanation describing the plan.",
    )
    preview: Optional[SemanticQueryResponse] = Field(
        default=None,
        description="Executed query response when generate_preview=True.",
    )
    raw_model_response: Optional[str] = Field(
        default=None,
        description="Raw text returned by the LLM before JSON parsing (useful for debugging).",
    )
