"""Structured-output provider contracts and schema helpers."""
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, Mapping, TypeVar

from pydantic import BaseModel

from .structured import StructuredOutputError


StructuredModel = TypeVar("StructuredModel", bound=BaseModel)


class StructuredOutputMode(str, Enum):
    """Runtime strategy for satisfying a structured-output request."""

    auto = "auto"
    native = "native"
    prompt = "prompt"


class StructuredOutputUnsupportedError(StructuredOutputError):
    """Raised when a provider cannot satisfy native structured output."""


class StructuredOutputRefusalError(StructuredOutputError):
    """Raised when a provider reports a structured-output refusal."""


class StructuredOutputIncompleteError(StructuredOutputError):
    """Raised when a provider reports an incomplete structured response."""


@dataclass(frozen=True)
class StructuredOutputConfig:
    """Provider-neutral structured-output configuration."""

    mode: StructuredOutputMode = StructuredOutputMode.auto

    @classmethod
    def from_mapping(cls, configuration: Mapping[str, Any] | None) -> "StructuredOutputConfig":
        configuration = configuration or {}
        return cls(mode=resolve_structured_output_mode(configuration.get("structured_outputs", "auto")))


@dataclass(frozen=True)
class StructuredOutputSchema(Generic[StructuredModel]):
    """Schema metadata for a Pydantic structured-output model."""

    response_model: type[StructuredModel]

    @property
    def name(self) -> str:
        return self.response_model.__name__

    def as_dict(self) -> dict[str, Any]:
        return self.response_model.model_json_schema()

    def as_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.as_dict(), indent=indent, default=str)


@dataclass(frozen=True)
class StructuredOutputContract(Generic[StructuredModel]):
    """Prompt-side contract used only when native schema enforcement is unavailable."""

    response_model: type[StructuredModel]

    @property
    def schema(self) -> StructuredOutputSchema[StructuredModel]:
        return StructuredOutputSchema(self.response_model)

    def system_instruction(self) -> str:
        return (
            "Return only valid JSON matching the supplied JSON Schema. "
            "Do not include markdown fences, commentary, or keys outside the schema.\n"
            f"JSON Schema:\n{self.schema.as_json()}"
        )


@dataclass(frozen=True)
class StructuredOutputSchemaIssue:
    path: str
    message: str


@dataclass(frozen=True)
class StrictJsonSchemaCompatibility:
    """Detect schema features unsupported by native strict structured-output APIs."""

    schema: Mapping[str, Any]

    @classmethod
    def for_model(cls, response_model: type[BaseModel]) -> "StrictJsonSchemaCompatibility":
        return cls(response_model.model_json_schema())

    def issues(self) -> list[StructuredOutputSchemaIssue]:
        return list(self._walk(self.schema, path="$"))

    def is_compatible(self) -> bool:
        return not self.issues()

    def _walk(self, node: Any, *, path: str) -> list[StructuredOutputSchemaIssue]:
        if isinstance(node, list):
            issues: list[StructuredOutputSchemaIssue] = []
            for index, item in enumerate(node):
                issues.extend(self._walk(item, path=f"{path}[{index}]"))
            return issues
        if not isinstance(node, Mapping):
            return []

        issues = []
        if node.get("type") == "object":
            additional = node.get("additionalProperties")
            has_properties_schema = isinstance(node.get("properties"), Mapping)
            if additional is not False and not has_properties_schema:
                issues.append(
                    StructuredOutputSchemaIssue(
                        path=path,
                        message=(
                            "Open object/map schemas are not compatible with native strict structured output; "
                            "use prompt JSON fallback or model the object fields explicitly."
                        ),
                    )
                )
            elif additional not in (None, False):
                issues.append(
                    StructuredOutputSchemaIssue(
                        path=path,
                        message=(
                            "Open object/map schemas are not compatible with native strict structured output; "
                            "use prompt JSON fallback or model the object fields explicitly."
                        ),
                    )
                )

        for key, value in node.items():
            if key in {"properties", "$defs", "definitions"} and isinstance(value, Mapping):
                for child_name, child_schema in value.items():
                    issues.extend(self._walk(child_schema, path=f"{path}.{key}.{child_name}"))
                continue
            if key in {"items", "anyOf", "oneOf", "allOf", "not"}:
                issues.extend(self._walk(value, path=f"{path}.{key}"))

        return issues


def resolve_structured_output_mode(value: Any) -> StructuredOutputMode:
    text = str(value or StructuredOutputMode.auto.value).strip().lower()
    if text in {"true", "yes", "on"}:
        return StructuredOutputMode.auto
    if text in {"false", "no", "off", "legacy"}:
        return StructuredOutputMode.prompt
    try:
        return StructuredOutputMode(text)
    except ValueError as exc:
        raise StructuredOutputError(f"Unsupported structured_outputs mode: {value!r}") from exc


def configured_structured_output_mode(configuration: Mapping[str, Any]) -> StructuredOutputMode:
    return StructuredOutputConfig.from_mapping(configuration).mode


def json_schema_for_model(response_model: type[BaseModel]) -> dict[str, Any]:
    return StructuredOutputSchema(response_model).as_dict()


__all__ = [
    "StructuredOutputConfig",
    "StructuredOutputContract",
    "StructuredOutputIncompleteError",
    "StructuredOutputMode",
    "StructuredOutputRefusalError",
    "StructuredOutputSchema",
    "StructuredOutputSchemaIssue",
    "StructuredOutputUnsupportedError",
    "StrictJsonSchemaCompatibility",
    "configured_structured_output_mode",
    "json_schema_for_model",
    "resolve_structured_output_mode",
]
