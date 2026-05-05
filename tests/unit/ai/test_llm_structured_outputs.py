import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from pydantic import BaseModel

from langbridge.ai.llm import StructuredOutputError, StructuredOutputMode, acomplete_structured, create_provider
from langbridge.ai.llm.structured import resolve_structured_output_mode


def _run(coro):
    return asyncio.run(coro)


class _Pet(BaseModel):
    name: str
    age: int


class _FallbackLLM:
    def __init__(self, response: str) -> None:
        self.response = response
        self.prompts: list[str] = []

    async def acomplete(self, prompt: str, **kwargs):
        _ = kwargs
        self.prompts.append(prompt)
        return self.response


def test_structured_prompt_fallback_extracts_fenced_json() -> None:
    llm = _FallbackLLM('Here is the result:\n```json\n{"name":"Luna","age":4}\n```')

    result = _run(acomplete_structured(llm, "Extract the pet.", response_model=_Pet))

    assert result == _Pet(name="Luna", age=4)
    assert "JSON Schema:" in llm.prompts[0]
    assert '"name"' in llm.prompts[0]


def test_structured_prompt_fallback_raises_for_invalid_payload() -> None:
    llm = _FallbackLLM('{"name":"Luna"}')

    with pytest.raises(StructuredOutputError):
        _run(acomplete_structured(llm, "Extract the pet.", response_model=_Pet))


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
    provider.create_async_client = lambda **_: client  # type: ignore[method-assign]

    result = _run(provider.acomplete_structured("Extract the pet.", response_model=_Pet, max_tokens=100))

    assert result == _Pet(name="Luna", age=4)
    call = client.responses.calls[0]
    assert call["model"] == "gpt-4o-mini"
    assert call["text_format"] is _Pet
    assert call["input"] == [{"role": "user", "content": "Extract the pet."}]
    assert call["max_output_tokens"] == 100


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
    provider.create_async_client = lambda **_: client  # type: ignore[method-assign]

    result = _run(provider.acomplete_structured("Extract the pet.", response_model=_Pet))

    assert result == _Pet(name="Luna", age=4)
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
    provider.create_async_client = lambda **_: httpx.AsyncClient(  # type: ignore[method-assign]
        transport=transport,
        base_url="http://ollama.local",
    )

    result = _run(provider.acomplete_structured("Extract the pet.", response_model=_Pet))

    assert result == _Pet(name="Luna", age=4)
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
    provider.create_async_client = lambda **_: client  # type: ignore[method-assign]

    result = _run(provider.acomplete_structured("Extract the pet.", response_model=_Pet))

    assert result == _Pet(name="Luna", age=4)
    call = client.messages.calls[0]
    assert call["tool_choice"] == {"type": "tool", "name": "langbridge_structured_response"}
    assert call["tools"][0]["input_schema"]["type"] == "object"
