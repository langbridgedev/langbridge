from typing import Any

from pydantic import BaseModel

from ..base import LLMMessage, LLMProvider, LLMProviderName, LLMResponse, response_text
from ..factory import register_provider
from ..structured import StructuredOutputUnsupportedError, validate_structured_payload

try:  # pragma: no cover - optional dependency
    from openai import AsyncOpenAI, OpenAI
except ImportError as exc:  # pragma: no cover - optional dependency
    AsyncOpenAI = None  # type: ignore[assignment]
    OpenAI = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover - optional dependency
    _IMPORT_ERROR = None

_ALLOWED_CONFIG_KEYS = {
    "timeout",
    "max_retries",
    "base_url",
    "organization",
    "default_headers",
    "http_client",
}


def _to_dict(response: Any) -> LLMResponse:
    if hasattr(response, "model_dump"):
        return response.model_dump(mode="json")
    if isinstance(response, dict):
        return response
    return {"raw": response}


@register_provider
class OpenAIProvider(LLMProvider):
    name = LLMProviderName.OPENAI

    def create_client(self, **overrides: Any) -> Any:
        if OpenAI is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = {key: self.configuration.get(key) for key in _ALLOWED_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("api_key", self.api_key)
        return OpenAI(**params)

    def create_async_client(self, **overrides: Any) -> "AsyncOpenAI": # type: ignore
        if AsyncOpenAI is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = {key: self.configuration.get(key) for key in _ALLOWED_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("api_key", self.api_key)
        return AsyncOpenAI(**params)

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
        client = self.create_client()
        response = client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            temperature=temperature,
            # max_tokens=max_tokens,
        )
        return _to_dict(response)

    async def ainvoke(
        self,
        messages: list[LLMMessage],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> LLMResponse:
        client = self.create_async_client()
        response = await client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            temperature=temperature,
            # max_tokens=max_tokens,
        )
        return _to_dict(response)

    async def _ainvoke_structured_native(
        self,
        messages: list[LLMMessage],
        *,
        response_model: type[BaseModel],
        temperature: float,
        max_tokens: int | None,
    ) -> BaseModel:
        client = self.create_async_client()
        if not hasattr(client, "responses") or not hasattr(client.responses, "parse"):
            raise StructuredOutputUnsupportedError("OpenAI client does not expose responses.parse.")
        params = self._clean_kwargs(
            {
                "model": self.model_name,
                "input": messages,
                "text_format": response_model,
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            }
        )
        response = await client.responses.parse(**params)
        parsed = getattr(response, "output_parsed", None)
        if parsed is None:
            raise StructuredOutputUnsupportedError("OpenAI structured response did not include output_parsed.")
        return validate_structured_payload(parsed, response_model=response_model)

    async def create_embeddings(
        self,
        texts: list[str],
        embedding_model: str | None = None,
    ) -> list[list[float]]:
        if not texts:
            return []
        model = embedding_model or self.configuration.get("embedding_model") or "text-embedding-3-small"
        client = self.create_async_client()
        response = await client.embeddings.create(model=model, input=texts)
        return [list(item.embedding) for item in response.data]


__all__ = ["OpenAIProvider"]
