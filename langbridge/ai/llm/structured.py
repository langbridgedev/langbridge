"""Structured-output primitives shared by LLM providers."""
import json
import re
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError


StructuredModel = TypeVar("StructuredModel", bound=BaseModel)


class StructuredOutputError(RuntimeError):
    """Raised when an LLM structured output cannot be parsed or validated."""


class JsonPayloadExtractor:
    """Extract the first valid JSON payload from model text."""

    _fence_pattern = re.compile(r"```(?:json)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)

    def extract(self, raw: str) -> Any:
        text = str(raw or "").strip()
        if not text:
            raise StructuredOutputError("LLM response was empty.")

        errors: list[str] = []
        for candidate in self._candidate_texts(text):
            try:
                return json.loads(candidate)
            except json.JSONDecodeError as exc:
                errors.append(str(exc))

        detail = f": {errors[-1]}" if errors else "."
        raise StructuredOutputError(f"LLM response did not contain valid JSON{detail}")

    def _candidate_texts(self, text: str) -> list[str]:
        candidates: list[str] = []

        self._append_candidate(candidates, text)
        for match in self._fence_pattern.finditer(text):
            self._append_candidate(candidates, match.group(1))

        for opener, closer in (("{", "}"), ("[", "]")):
            balanced = self._balanced_json_slice(text, opener=opener, closer=closer)
            if balanced is not None:
                self._append_candidate(candidates, balanced)

        return candidates

    @staticmethod
    def _append_candidate(candidates: list[str], value: str) -> None:
        candidate = value.strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    @staticmethod
    def _balanced_json_slice(text: str, *, opener: str, closer: str) -> str | None:
        start = text.find(opener)
        if start == -1:
            return None

        depth = 0
        in_string = False
        escape = False
        for index, char in enumerate(text[start:], start=start):
            if escape:
                escape = False
                continue
            if char == "\\" and in_string:
                escape = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if char == opener:
                depth += 1
            elif char == closer:
                depth -= 1
                if depth == 0:
                    return text[start : index + 1]
        return None


@dataclass(frozen=True)
class StructuredOutputParser(Generic[StructuredModel]):
    """Validate native or extracted JSON payloads into a Pydantic model."""

    response_model: type[StructuredModel]
    extractor: JsonPayloadExtractor = field(default_factory=JsonPayloadExtractor)

    def parse_text(self, raw: str) -> StructuredModel:
        return self.validate_payload(self.extractor.extract(raw))

    def validate_payload(self, payload: Any) -> StructuredModel:
        try:
            if isinstance(payload, self.response_model):
                return payload
            if isinstance(payload, str):
                return self.response_model.model_validate_json(payload)
            return self.response_model.model_validate(payload)
        except ValidationError as exc:
            raise StructuredOutputError(
                f"LLM structured output failed validation for {self.response_model.__name__}: {exc}"
            ) from exc


def validate_structured_payload(
    payload: Any,
    *,
    response_model: type[StructuredModel],
) -> StructuredModel:
    return StructuredOutputParser(response_model).validate_payload(payload)


def parse_json_payload_from_text(raw: str) -> Any:
    return JsonPayloadExtractor().extract(raw)


def parse_json_object_from_text(raw: str) -> dict[str, Any]:
    payload = parse_json_payload_from_text(raw)
    if not isinstance(payload, dict):
        raise StructuredOutputError("LLM structured output JSON must be an object.")
    return payload


def parse_structured_text(
    raw: str,
    *,
    response_model: type[StructuredModel],
) -> StructuredModel:
    return StructuredOutputParser(response_model).parse_text(raw)


__all__ = [
    "JsonPayloadExtractor",
    "StructuredOutputError",
    "StructuredOutputParser",
    "parse_json_object_from_text",
    "parse_json_payload_from_text",
    "parse_structured_text",
    "validate_structured_payload",
]
