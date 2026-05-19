import asyncio
import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from pydantic import BaseModel

from langbridge.ai.llm import (
    LLMMessage,
    LLMRequest,
    StructuredOutputContract,
    StructuredOutputError,
    StructuredOutputIncompleteError,
    StructuredOutputMode,
    StructuredOutputParser,
    create_provider,
)
from langbridge.ai.llm.contracts import resolve_structured_output_mode


def _run(coro):
    return asyncio.run(coro)


class _Pet(BaseModel):
    name: str
    age: int


class _OpenPayload(BaseModel):
    payload: dict[str, Any]


def test_structured_output_parser_extracts_fenced_json() -> None:
    parser = StructuredOutputParser(_Pet)

    result = parser.parse_text('Here is the result:\n```json\n{"name":"Luna","age":4}\n```')
    assert result == _Pet(name="Luna", age=4)


def test_structured_output_parser_raises_for_invalid_payload() -> None:
    parser = StructuredOutputParser(_Pet)

    with pytest.raises(StructuredOutputError):
        parser.parse_text('{"name":"Luna"}')


def test_structured_output_contract_builds_system_instruction() -> None:
    instruction = StructuredOutputContract(_Pet).system_instruction()

    assert "JSON Schema:" in instruction
    assert '"name"' in instruction
    assert "markdown fences" in instruction


def test_structured_output_mode_aliases() -> None:
    assert resolve_structured_output_mode("true") == StructuredOutputMode.auto
    assert resolve_structured_output_mode("off") == StructuredOutputMode.prompt
    assert resolve_structured_output_mode("native") == StructuredOutputMode.native


def test_openai_provider_uses_responses_parse_for_structured_output() -> None:
    class _Responses:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def parse(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(output_parsed=_Pet(name="Luna", age=4))

    class _Client:
        def __init__(self) -> None:
            self.responses = _Responses()

    provider = create_provider(
        {
            "provider": "openai",
            "api_key": "test",
            "model": "gpt-4o-mini",
            "configuration": {"structured_outputs": "native"},
        }
    )
    client = _Client()
    provider.create_client = lambda **_: client  # type: ignore[method-assign]

    invocation = _run(
        provider.ainvoke(
            LLMRequest[_Pet](
                purpose="test.extract_pet",
                messages=[LLMMessage(role="user", content="Extract the pet.")],
                response_model=_Pet,
                max_tokens=100,
            )
        )
    )

    assert invocation.response.parsed == _Pet(name="Luna", age=4)
    assert invocation.response.extract_mode == "native_structured"
    call = client.responses.calls[0]
    assert call["model"] == "gpt-4o-mini"
    assert call["text_format"] is _Pet
    assert call["input"] == [{"role": "user", "content": "Extract the pet."}]
    assert call["max_output_tokens"] == 100


def test_openai_provider_falls_back_to_json_extraction_when_native_is_unavailable() -> None:
    class _Responses:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def create(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                output_text='{"name":"Luna","age":4}',
                model_dump=lambda mode="json": {"text": '{"name":"Luna","age":4}'},
            )

    class _Client:
        def __init__(self) -> None:
            self.responses = _Responses()

    provider = create_provider(
        {
            "provider": "openai",
            "api_key": "test",
            "model": "gpt-4o-mini",
            "configuration": {"structured_outputs": "auto"},
        }
    )
    client = _Client()
    provider.create_client = lambda **_: client  # type: ignore[method-assign]

    invocation = _run(
        provider.ainvoke(
            LLMRequest[_Pet](
                purpose="test.extract_pet",
                messages=[LLMMessage(role="user", content="Extract the pet.")],
                response_model=_Pet,
            )
        )
    )

    assert invocation.response.parsed == _Pet(name="Luna", age=4)
    assert invocation.response.extract_mode == "json_extractor"
    call = client.responses.calls[0]
    assert call["input"][0]["role"] == "system"
    assert "JSON Schema:" in call["input"][0]["content"]
    assert call["input"][1] == {"role": "user", "content": "Extract the pet."}


def test_openai_provider_retries_text_response_without_rejected_temperature() -> None:
    class _UnsupportedTemperatureError(Exception):
        body = {
            "error": {
                "message": "Unsupported parameter: 'temperature' is not supported with this model.",
                "param": "temperature",
            }
        }

    class _Responses:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def create(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                raise _UnsupportedTemperatureError()
            return SimpleNamespace(
                output_text="plain answer",
                model_dump=lambda mode="json": {"text": "plain answer"},
            )

    class _Client:
        def __init__(self) -> None:
            self.responses = _Responses()

    provider = create_provider(
        {
            "provider": "openai",
            "api_key": "test",
            "model": "gpt-5-mini",
        }
    )
    client = _Client()
    provider.create_client = lambda **_: client  # type: ignore[method-assign]

    invocation = _run(
        provider.ainvoke(
            LLMRequest(
                purpose="test.text",
                messages=[LLMMessage(role="user", content="Answer.")],
            )
        )
    )

    assert invocation.response.text == "plain answer"
    assert len(client.responses.calls) == 2
    assert "temperature" in client.responses.calls[0]
    assert "temperature" not in client.responses.calls[1]


def test_openai_provider_retries_native_structured_response_without_rejected_temperature() -> None:
    class _UnsupportedTemperatureError(Exception):
        body = {
            "error": {
                "message": "Unsupported parameter: 'temperature' is not supported with this model.",
                "param": "temperature",
            }
        }

    class _Responses:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def parse(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                raise _UnsupportedTemperatureError()
            return SimpleNamespace(output_parsed=_Pet(name="Luna", age=4))

    class _Client:
        def __init__(self) -> None:
            self.responses = _Responses()

    provider = create_provider(
        {
            "provider": "openai",
            "api_key": "test",
            "model": "gpt-5-mini",
            "configuration": {"structured_outputs": "native"},
        }
    )
    client = _Client()
    provider.create_client = lambda **_: client  # type: ignore[method-assign]

    invocation = _run(
        provider.ainvoke(
            LLMRequest[_Pet](
                purpose="test.extract_pet",
                messages=[LLMMessage(role="user", content="Extract the pet.")],
                response_model=_Pet,
            )
        )
    )

    assert invocation.response.parsed == _Pet(name="Luna", age=4)
    assert len(client.responses.calls) == 2
    assert "temperature" in client.responses.calls[0]
    assert "temperature" not in client.responses.calls[1]


def test_openai_provider_falls_back_when_schema_is_not_native_strict_compatible() -> None:
    class _Responses:
        def __init__(self) -> None:
            self.create_calls: list[dict] = []
            self.parse_calls = 0

        async def parse(self, **kwargs):
            _ = kwargs
            self.parse_calls += 1
            raise AssertionError("Open dict schemas should not be sent to responses.parse.")

        async def create(self, **kwargs):
            self.create_calls.append(kwargs)
            return SimpleNamespace(
                output_text='{"payload":{"agent_mode":"auto"}}',
                model_dump=lambda mode="json": {"text": '{"payload":{"agent_mode":"auto"}}'},
            )

    class _Client:
        def __init__(self) -> None:
            self.responses = _Responses()

    provider = create_provider(
        {
            "provider": "openai",
            "api_key": "test",
            "model": "gpt-4o-mini",
            "configuration": {"structured_outputs": "auto"},
        }
    )
    client = _Client()
    provider.create_client = lambda **_: client  # type: ignore[method-assign]

    invocation = _run(
        provider.ainvoke(
            LLMRequest[_OpenPayload](
                purpose="test.open_payload",
                messages=[LLMMessage(role="user", content="Return payload.")],
                response_model=_OpenPayload,
            )
        )
    )

    assert invocation.response.parsed == _OpenPayload(payload={"agent_mode": "auto"})
    assert invocation.response.extract_mode == "json_extractor"
    assert client.responses.parse_calls == 0
    assert len(client.responses.create_calls) == 1


def test_openai_provider_does_not_fallback_when_native_response_is_incomplete() -> None:
    class _Responses:
        async def parse(self, **kwargs):
            _ = kwargs
            return SimpleNamespace(
                output_parsed=None,
                model_dump=lambda mode="json": {
                    "status": "incomplete",
                    "incomplete_details": {"reason": "max_output_tokens"},
                },
            )

    class _Client:
        def __init__(self) -> None:
            self.responses = _Responses()

    provider = create_provider(
        {
            "provider": "openai",
            "api_key": "test",
            "model": "gpt-4o-mini",
            "configuration": {"structured_outputs": "auto"},
        }
    )
    provider.create_client = lambda **_: _Client()  # type: ignore[method-assign]

    with pytest.raises(StructuredOutputIncompleteError):
        _run(
            provider.ainvoke(
                LLMRequest[_Pet](
                    purpose="test.extract_pet",
                    messages=[LLMMessage(role="user", content="Extract the pet.")],
                    response_model=_Pet,
                    max_tokens=1,
                )
            )
        )


def test_azure_provider_uses_chat_parse_for_structured_output() -> None:
    class _Completions:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def parse(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(parsed={"name": "Luna", "age": 4}),
                    )
                ]
            )

    class _Client:
        def __init__(self) -> None:
            self.chat = SimpleNamespace(completions=_Completions())

    provider = create_provider(
        {
            "provider": "azure",
            "api_key": "test",
            "model": "ignored",
            "configuration": {
                "azure_endpoint": "https://example.openai.azure.com",
                "deployment_name": "gpt-4o",
                "structured_outputs": "native",
            },
        }
    )
    client = _Client()
    provider.create_client = lambda **_: client  # type: ignore[method-assign]

    invocation = _run(
        provider.ainvoke(
            LLMRequest[_Pet](
                purpose="test.extract_pet",
                messages=[LLMMessage(role="user", content="Extract the pet.")],
                response_model=_Pet,
            )
        )
    )

    assert invocation.response.parsed == _Pet(name="Luna", age=4)
    assert invocation.response.extract_mode == "native_structured"
    call = client.chat.completions.calls[0]
    assert call["model"] == "gpt-4o"
    assert call["response_format"] is _Pet
    assert "max_tokens" not in call


def test_ollama_provider_sends_json_schema_format_for_structured_output() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "model": "gpt-oss",
                "message": {"role": "assistant", "content": '{"name":"Luna","age":4}'},
                "done": True,
            },
        )

    provider = create_provider(
        {
            "provider": "ollama",
            "api_key": "",
            "model": "gpt-oss",
            "configuration": {"base_url": "http://ollama.local", "structured_outputs": "native"},
        }
    )
    transport = httpx.MockTransport(handler)
    provider.create_client = lambda **_: httpx.AsyncClient(  # type: ignore[method-assign]
        transport=transport,
        base_url="http://ollama.local",
    )

    invocation = _run(
        provider.ainvoke(
            LLMRequest[_Pet](
                purpose="test.extract_pet",
                messages=[LLMMessage(role="user", content="Extract the pet.")],
                response_model=_Pet,
            )
        )
    )

    assert invocation.response.parsed == _Pet(name="Luna", age=4)
    assert invocation.response.extract_mode == "native_structured"
    payload = json.loads(requests[0].content.decode("utf-8"))
    assert payload["format"]["type"] == "object"
    assert set(payload["format"]["properties"]) >= {"name", "age"}


def test_anthropic_provider_uses_forced_tool_for_structured_output() -> None:
    class _Messages:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def create(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                model_dump=lambda mode="json": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "langbridge_structured_response",
                            "input": {"name": "Luna", "age": 4},
                        }
                    ]
                }
            )

    class _Client:
        def __init__(self) -> None:
            self.messages = _Messages()

    provider = create_provider(
        {
            "provider": "anthropic",
            "api_key": "test",
            "model": "claude-3-5-sonnet-latest",
            "configuration": {"structured_outputs": "native"},
        }
    )
    client = _Client()
    provider.create_client = lambda **_: client  # type: ignore[method-assign]

    invocation = _run(
        provider.ainvoke(
            LLMRequest[_Pet](
                purpose="test.extract_pet",
                messages=[LLMMessage(role="user", content="Extract the pet.")],
                response_model=_Pet,
            )
        )
    )

    assert invocation.response.parsed == _Pet(name="Luna", age=4)
    assert invocation.response.extract_mode == "native_structured"
    call = client.messages.calls[0]
    assert call["tool_choice"] == {"type": "tool", "name": "langbridge_structured_response"}
    assert call["tools"][0]["input_schema"]["type"] == "object"
