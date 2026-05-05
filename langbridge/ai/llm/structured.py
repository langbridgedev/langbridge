"""Shared structured-output helpers for Langbridge LLM providers."""
import json
import re
from enum import Enum
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from .base import LLMMessage, response_text


StructuredModel = TypeVar("StructuredModel", bound=BaseModel)


class StructuredOutputMode(str, Enum):
    auto = "auto"
    native = "native"
    prompt = "prompt"


class StructuredOutputError(RuntimeError):
    """Raised when an LLM structured output cannot be parsed or validated."""


class StructuredOutputUnsupportedError(StructuredOutputError):
    """Raised when a provider cannot satisfy native structured output."""


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


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


def configured_structured_output_mode(configuration: dict[str, Any]) -> StructuredOutputMode:
    return resolve_structured_output_mode(configuration.get("structured_outputs", "auto"))


def json_schema_for_model(response_model: type[BaseModel]) -> dict[str, Any]:
    return response_model.model_json_schema()


def validate_structured_payload(
    payload: Any,
    *,
    response_model: type[StructuredModel],
) -> StructuredModel:
    try:
        if isinstance(payload, response_model):
            return payload
        if isinstance(payload, str):
            return response_model.model_validate_json(payload)
        return response_model.model_validate(payload)
    except ValidationError as exc:
        raise StructuredOutputError(
            f"LLM structured output failed validation for {response_model.__name__}: {exc}"
        ) from exc


def parse_json_object_from_text(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    for match in _JSON_FENCE_RE.finditer(text):
        candidate = match.group(1).strip()
        if candidate:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise StructuredOutputError("LLM response did not contain a JSON object.")

    try:
        parsed = json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise StructuredOutputError(f"LLM response contained invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise StructuredOutputError("LLM structured output JSON must be an object.")
    return parsed


def parse_structured_text(
    raw: str,
    *,
    response_model: type[StructuredModel],
) -> StructuredModel:
    return validate_structured_payload(
        parse_json_object_from_text(raw),
        response_model=response_model,
    )


def structured_prompt(prompt: str, *, response_model: type[BaseModel]) -> str:
    schema = json.dumps(json_schema_for_model(response_model), indent=2, default=str)
    return (
        f"{prompt.rstrip()}\n\n"
        "Return only valid JSON matching this JSON Schema. Do not include markdown fences, "
        "commentary, or any keys outside the schema.\n"
        f"JSON Schema:\n{schema}"
    )


async def acomplete_structured(
    llm: Any,
    prompt: str,
    *,
    response_model: type[StructuredModel],
    temperature: float = 0.0,
    max_tokens: int | None = None,
) -> StructuredModel:
    method = getattr(llm, "acomplete_structured", None)
    if callable(method):
        return await method(
            prompt,
            response_model=response_model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    raw = await llm.acomplete(
        structured_prompt(prompt, response_model=response_model),
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return parse_structured_text(raw, response_model=response_model)


async def ainvoke_structured(
    llm: Any,
    messages: list[LLMMessage],
    *,
    response_model: type[StructuredModel],
    temperature: float = 0.0,
    max_tokens: int | None = None,
) -> StructuredModel:
    method = getattr(llm, "ainvoke_structured", None)
    if callable(method):
        return await method(
            messages,
            response_model=response_model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    if not messages:
        messages = [{"role": "user", "content": ""}]
    response = await llm.ainvoke(
        [
            *messages[:-1],
            {
                **messages[-1],
                "content": structured_prompt(
                    str(messages[-1].get("content") or ""),
                    response_model=response_model,
                ),
            },
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return parse_structured_text(response_text(response), response_model=response_model)


__all__ = [
    "StructuredOutputError",
    "StructuredOutputMode",
    "StructuredOutputUnsupportedError",
    "acomplete_structured",
    "ainvoke_structured",
    "configured_structured_output_mode",
    "json_schema_for_model",
    "parse_json_object_from_text",
    "parse_structured_text",
    "resolve_structured_output_mode",
    "structured_prompt",
    "validate_structured_payload",
]
