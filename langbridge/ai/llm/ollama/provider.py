from typing import Any, Mapping

import httpx

from ..base import LLMMessage, LLMProvider, LLMProviderName, LLMResponse, response_text
from ..factory import register_provider

DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"

_CLIENT_CONFIG_KEYS = {
    "timeout",
    "headers",
}


def _base_url(value: Any) -> str:
    base_url = str(value or DEFAULT_OLLAMA_BASE_URL).strip() or DEFAULT_OLLAMA_BASE_URL
    return base_url.rstrip("/")


def _response_payload(response: httpx.Response) -> dict[str, Any]:
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Ollama response must be a JSON object.")
    return payload


@register_provider
class OllamaProvider(LLMProvider):
    name = LLMProviderName.OLLAMA

    def create_client(self, **overrides: Any) -> httpx.Client:
        params = {key: self.configuration.get(key) for key in _CLIENT_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("base_url", _base_url(self.configuration.get("base_url")))
        return httpx.Client(**params)

    def create_async_client(self, **overrides: Any) -> httpx.AsyncClient:
        params = {key: self.configuration.get(key) for key in _CLIENT_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("base_url", _base_url(self.configuration.get("base_url")))
        return httpx.AsyncClient(**params)

    def complete(
        self,
        prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        return response_text(
            self.invoke(
                [{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
            )
        )

    async def acomplete(
        self,
        prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        return response_text(
            await self.ainvoke(
                [{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
            )
        )

    def invoke(
        self,
        messages: list[LLMMessage],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> LLMResponse:
        with self.create_client() as client:
            response = client.post(
                "/api/chat",
                json=self._chat_payload(messages, temperature=temperature, max_tokens=max_tokens),
            )
        return self._chat_response(_response_payload(response))

    async def ainvoke(
        self,
        messages: list[LLMMessage],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> LLMResponse:
        async with self.create_async_client() as client:
            response = await client.post(
                "/api/chat",
                json=self._chat_payload(messages, temperature=temperature, max_tokens=max_tokens),
            )
        return self._chat_response(_response_payload(response))

    async def create_embeddings(
        self,
        texts: list[str],
        embedding_model: str | None = None,
    ) -> list[list[float]]:
        cleaned = [text for text in texts if isinstance(text, str)]
        if not cleaned:
            return []
        model = self._embedding_model(embedding_model)
        async with self.create_async_client() as client:
            response = await client.post(
                "/api/embed",
                json=self._embedding_payload(model=model, texts=cleaned),
            )
        return self._embedding_response(_response_payload(response))

    def _chat_payload(
        self,
        messages: list[LLMMessage],
        *,
        temperature: float,
        max_tokens: int | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self.model_name,
            "messages": [self._message_payload(message) for message in messages],
            "stream": False,
        }
        if self.configuration.get("format") is not None:
            payload["format"] = self.configuration["format"]
        if self.configuration.get("keep_alive") is not None:
            payload["keep_alive"] = self.configuration["keep_alive"]
        options = self._options(temperature=temperature, max_tokens=max_tokens)
        if options:
            payload["options"] = options
        return payload

    def _embedding_payload(self, *, model: str, texts: list[str]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model,
            "input": texts,
        }
        if self.configuration.get("keep_alive") is not None:
            payload["keep_alive"] = self.configuration["keep_alive"]
        if self.configuration.get("truncate") is not None:
            payload["truncate"] = bool(self.configuration["truncate"])
        options = self.configuration.get("embedding_options") or self.configuration.get("options")
        if isinstance(options, Mapping):
            payload["options"] = dict(options)
        return payload

    def _embedding_model(self, override: str | None) -> str:
        return str(
            override
            or self.configuration.get("embedding_model")
            or self.configuration.get("embedding")
            or self.model_name
        )

    def _options(self, *, temperature: float, max_tokens: int | None) -> dict[str, Any]:
        configured = self.configuration.get("options")
        options = dict(configured) if isinstance(configured, Mapping) else {}
        options.setdefault("temperature", temperature)
        if max_tokens is not None:
            options.setdefault("num_predict", max_tokens)
        return self._clean_kwargs(options)

    @staticmethod
    def _message_payload(message: LLMMessage) -> dict[str, Any]:
        return {
            "role": str(message.get("role") or "user"),
            "content": str(message.get("content") or ""),
        }

    @staticmethod
    def _chat_response(payload: dict[str, Any]) -> LLMResponse:
        message = payload.get("message")
        if isinstance(message, Mapping):
            content = message.get("content")
            if isinstance(content, str):
                payload["text"] = content
        return payload

    @staticmethod
    def _embedding_response(payload: dict[str, Any]) -> list[list[float]]:
        embeddings = payload.get("embeddings")
        if not isinstance(embeddings, list):
            raise RuntimeError("Ollama embedding response missing embeddings.")
        return [list(embedding) for embedding in embeddings]


__all__ = ["OllamaProvider"]
