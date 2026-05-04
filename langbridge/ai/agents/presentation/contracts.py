"""Markdown-first presentation response contract."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


MARKDOWN_ARTIFACT_RESPONSE_VERSION = "markdown_artifacts.v1"
ArtifactRole = Literal["primary_result", "supporting_result", "diagnostic"]


class _StrictPresentationModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TableArtifactPayload(_StrictPresentationModel):
    columns: list[str]
    rows: list[Any]
    row_count: int | None = None
    elapsed_ms: int | float | None = None
    source_sql: str | None = None
    formatting: dict[str, Any] | None = None


class ChartArtifactPayload(_StrictPresentationModel):
    chart_type: str
    title: str
    x: str | None = None
    y: str | list[str] | None = None
    series: str | None = None
    encoding: dict[str, Any] = Field(default_factory=dict)
    rationale: str | None = None
    formatting: dict[str, Any] | None = None


class SqlArtifactPayload(_StrictPresentationModel):
    sql_canonical: str | None = None
    sql_executable: str | None = None
    dialect: str | None = None
    query_scope: str | None = None
    analysis_path: str | None = None
    selected_datasets: list[Any] | None = None
    selected_semantic_models: list[Any] | None = None


class PresentationArtifactBase(_StrictPresentationModel):
    id: str
    role: ArtifactRole
    title: str
    provenance: dict[str, Any] = Field(default_factory=dict)
    data_ref: dict[str, Any] | None = None


class TableArtifact(PresentationArtifactBase):
    type: Literal["table"]
    payload: TableArtifactPayload


class ChartArtifact(PresentationArtifactBase):
    type: Literal["chart"]
    payload: ChartArtifactPayload


class SqlArtifact(PresentationArtifactBase):
    type: Literal["sql"]
    payload: SqlArtifactPayload


class DiagnosticsArtifact(PresentationArtifactBase):
    type: Literal["diagnostics"]
    payload: Any = Field(default_factory=dict)


PresentationArtifact = Annotated[
    TableArtifact | ChartArtifact | SqlArtifact | DiagnosticsArtifact,
    Field(discriminator="type"),
]
PresentationArtifactAdapter = TypeAdapter(PresentationArtifact)


class PresentationLLMOutput(_StrictPresentationModel):
    """Strict JSON shape returned by the presentation LLM."""

    answer_markdown: str | None = None
    artifact_ids: list[str] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PresentationResponseContract(BaseModel):
    """Public assistant response returned by the Langbridge AI runtime."""

    model_config = ConfigDict(extra="forbid")

    answer_markdown: str
    artifacts: list[PresentationArtifact] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "ArtifactRole",
    "MARKDOWN_ARTIFACT_RESPONSE_VERSION",
    "ChartArtifact",
    "ChartArtifactPayload",
    "DiagnosticsArtifact",
    "PresentationArtifact",
    "PresentationArtifactAdapter",
    "PresentationArtifactBase",
    "PresentationLLMOutput",
    "PresentationResponseContract",
    "SqlArtifact",
    "SqlArtifactPayload",
    "TableArtifact",
    "TableArtifactPayload",
]
