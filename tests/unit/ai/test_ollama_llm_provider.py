import asyncio
from types import SimpleNamespace

import httpx
import pytest

from langbridge.ai.llm import (
    LLMEmbeddingsRequest,
    LLMMessage,
    LLMProviderName,
    LLMRequest,
    OllamaProvider,
    create_provider,
    registered_providers,
)
from langbridge.runtime.config.models import LocalRuntimeConfig
from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.models import LLMProvider as RuntimeLLMProvider


def _run(coro):
    return asyncio.run(coro)


async def _client_info(provider: OllamaProvider, **overrides) -> tuple[str, float]:
    client = provider.create_client(**overrides)
    try:
        return str(client.base_url), client.timeout.read
    finally:
        await client.aclose()


def test_ollama_provider_is_registered() -> None:
    assert LLMProviderName.OLLAMA in registered_providers()

    provider = create_provider(
        SimpleNamespace(
            provider="ollama",
            model="llama3.1",
            api_key="",
            configuration={"base_url": "http://localhost:11434"},
        )
    )

    assert isinstance(provider, OllamaProvider)


def test_ollama_chat_response_is_normalized() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "model": "llama3.1",
                "message": {"role": "assistant", "content": "local answer"},
                "done": True,
            },
        )

    provider = create_provider(
        {
            "provider": "ollama",
            "model": "llama3.1",
            "api_key": "",
            "configuration": {"base_url": "http://ollama.local", "options": {"num_ctx": 4096}},
        }
    )
    transport = httpx.MockTransport(handler)
    provider.create_client = lambda **_: httpx.AsyncClient(  # type: ignore[method-assign]
        transport=transport,
        base_url="http://ollama.local",
    )

    response = _run(
        provider.ainvoke(
            LLMRequest(
                purpose="test.chat",
                messages=[LLMMessage(role="user", content="hello")],
                temperature=0.2,
                max_tokens=64,
            )
        )
    )

    assert response.response.text == "local answer"
    assert requests[0].url.path == "/api/chat"
    payload = _json_payload(requests[0])
    assert payload["model"] == "llama3.1"
    assert payload["stream"] is False
    assert payload["options"] == {"num_ctx": 4096, "temperature": 0.2, "num_predict": 64}


def test_ollama_chat_enables_json_mode_for_json_prompts() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"message": {"role": "assistant", "content": '{"ok":true}'}, "done": True},
        )

    provider = create_provider(
        {
            "provider": "ollama",
            "model": "llama3.1",
            "api_key": "",
            "configuration": {"base_url": "http://ollama.local"},
        }
    )
    transport = httpx.MockTransport(handler)
    provider.create_client = lambda **_: httpx.AsyncClient(  # type: ignore[method-assign]
        transport=transport,
        base_url="http://ollama.local",
    )

    _run(
        provider.ainvoke(
            LLMRequest(
                purpose="test.chat",
                messages=[LLMMessage(role="user", content="Return STRICT JSON only.")],
            )
        )
    )

    assert _json_payload(requests[0])["format"] == "json"


def test_ollama_client_accepts_api_url_configuration_alias() -> None:
    provider = create_provider(
        {
            "provider": "ollama",
            "model": "llama3.1",
            "api_key": "",
            "configuration": {"api_url": "http://ollama.local/"},
        }
    )

    base_url, read_timeout = _run(
        _client_info(provider, transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    )

    assert base_url == "http://ollama.local"
    assert read_timeout == 300.0


def test_ollama_client_uses_configured_timeout() -> None:
    provider = create_provider(
        {
            "provider": "ollama",
            "model": "llama3.1",
            "api_key": "",
            "configuration": {"base_url": "http://ollama.local", "timeout": 45.0},
        }
    )

    _, read_timeout = _run(
        _client_info(provider, transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    )

    assert read_timeout == 45.0


def test_ollama_embedding_response_is_normalized() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"embeddings": [[0.1, 0.2], [0.3, 0.4]]})

    provider = create_provider(
        {
            "provider": "ollama",
            "model": "llama3.1",
            "api_key": "",
            "configuration": {
                "base_url": "http://ollama.local",
                "embedding_model": "nomic-embed-text",
                "keep_alive": "5m",
            },
        }
    )
    transport = httpx.MockTransport(handler)
    provider.create_client = lambda **_: httpx.AsyncClient(  # type: ignore[method-assign]
        transport=transport,
        base_url="http://ollama.local",
    )

    invocation = _run(
        provider.acreate_embeddings(
            LLMEmbeddingsRequest(texts=["one", "two"])
        )
    )

    assert invocation.embeddings == [[0.1, 0.2], [0.3, 0.4]]
    assert requests[0].url.path == "/api/embed"
    assert _json_payload(requests[0]) == {
        "model": "nomic-embed-text",
        "input": ["one", "two"],
        "keep_alive": "5m",
    }


def test_runtime_config_allows_ollama_without_credentials() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "llm_connections": [
                {
                    "name": "local_ollama",
                    "provider": "ollama",
                    "model": "llama3.1",
                    "configuration": {"base_url": "http://localhost:11434"},
                }
            ],
        }
    )

    assert config.llm_connections[0].provider == "ollama"


def test_runtime_config_normalizes_ollama_top_level_api_url() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "llm_connections": [
                {
                    "name": "local_ollama",
                    "provider": "ollama",
                    "model": "llama3.1",
                    "api_url": "http://ollama.local",
                }
            ],
        }
    )

    assert config.llm_connections[0].configuration["base_url"] == "http://ollama.local"


def test_runtime_config_still_requires_credentials_for_remote_provider() -> None:
    with pytest.raises(ValueError, match="LLM connection must define api_key or api_key_secret"):
        LocalRuntimeConfig.model_validate(
            {
                "version": 1,
                "llm_connections": [
                    {
                        "name": "remote_openai",
                        "provider": "openai",
                        "model": "gpt-4o-mini",
                    }
                ],
            }
        )


def test_runtime_embedding_provider_uses_ollama_embed_api(monkeypatch: pytest.MonkeyPatch) -> None:
    posts: list[dict[str, object]] = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def post(self, path, json):
            posts.append({"path": path, "json": json, "kwargs": self.kwargs})
            return httpx.Response(
                200,
                json={"embeddings": [[1.0], [2.0]]},
                request=httpx.Request("POST", f"{self.kwargs['base_url']}{path}"),
            )

    monkeypatch.setattr("langbridge.runtime.embeddings.httpx.Client", FakeClient)
    provider = EmbeddingProvider(
        provider=RuntimeLLMProvider.OLLAMA,
        api_key="",
        model_name="llama3.1",
        configuration={"base_url": "http://ollama.local", "embedding_model": "nomic-embed-text"},
    )

    embeddings = _run(provider.embed(["alpha", "beta"]))

    assert embeddings == [[1.0], [2.0]]
    assert posts == [
        {
            "path": "/api/embed",
            "json": {"model": "nomic-embed-text", "input": ["alpha", "beta"]},
            "kwargs": {"base_url": "http://ollama.local", "timeout": 300.0},
        }
    ]


def test_runtime_embedding_provider_passes_openai_compatible_client_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_clients: list[dict[str, object]] = []

    class FakeOpenAI:
        def __init__(self, **kwargs):
            created_clients.append(kwargs)
            self.embeddings = self

        def create(self, *, model, input):
            return SimpleNamespace(
                data=[SimpleNamespace(embedding=[float(index)]) for index, _ in enumerate(input)]
            )

    monkeypatch.setattr("langbridge.runtime.embeddings.OpenAI", FakeOpenAI)
    provider = EmbeddingProvider(
        provider=RuntimeLLMProvider.OPENAI,
        api_key="lm-studio",
        model_name="local-chat-model",
        configuration={
            "base_url": "http://localhost:1234/v1",
            "embedding_model": "local-embedding-model",
            "timeout": 15.0,
            "max_retries": 0,
        },
    )

    embeddings = _run(provider.embed(["alpha", "beta"]))

    assert embeddings == [[0.0], [1.0]]
    assert created_clients == [
        {
            "base_url": "http://localhost:1234/v1",
            "timeout": 15.0,
            "max_retries": 0,
            "api_key": "lm-studio",
        }
    ]
    assert provider.embedding_model == "local-embedding-model"


def _json_payload(request: httpx.Request) -> dict:
    import json

    return json.loads(request.content.decode("utf-8"))
