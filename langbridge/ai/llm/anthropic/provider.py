from typing import Any

from ..base import LLMMessage, LLMProvider, LLMProviderName, LLMResponse, response_text
from ..factory import register_provider

try:  # pragma: no cover - optional dependency
    from anthropic import Anthropic, AsyncAnthropic
except ImportError as exc:  # pragma: no cover - optional dependency
    Anthropic = None  # type: ignore[assignment]
    AsyncAnthropic = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover - optional dependency
    _IMPORT_ERROR = None

_ALLOWED_CONFIG_KEYS = {
    "timeout",
    "max_retries",
    "default_headers",
}


def _to_dict(response: Any) -> LLMResponse:
    if hasattr(response, "model_dump"):
        payload = response.model_dump(mode="json")
    elif isinstance(response, dict):
        payload = response
    else:
        payload = {"raw": response}
    content = payload.get("content")
    if isinstance(content, list):
        text_parts = [
            str(item.get("text"))
            for item in content
            if isinstance(item, dict) and item.get("type") == "text" and item.get("text") is not None
        ]
        if text_parts:
            payload["text"] = "".join(text_parts)
    return payload


def _split_system(messages: list[LLMMessage]) -> tuple[str | None, list[LLMMessage]]:
    system_parts: list[str] = []
    user_messages: list[LLMMessage] = []
    for message in messages:
        if str(message.get("role") or "") == "system":
            content = message.get("content")
            if content is not None:
                system_parts.append(str(content))
            continue
        user_messages.append(message)
    system = "\n\n".join(system_parts) if system_parts else None
    return system, user_messages


@register_provider
class AnthropicProvider(LLMProvider):
    name = LLMProviderName.ANTHROPIC

    def create_client(self, **overrides: Any) -> Any:
        if Anthropic is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = {key: self.configuration.get(key) for key in _ALLOWED_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("api_key", self.api_key)
        return Anthropic(**params)

    def create_async_client(self, **overrides: Any) -> Any:
        if AsyncAnthropic is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = {key: self.configuration.get(key) for key in _ALLOWED_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("api_key", self.api_key)
        return AsyncAnthropic(**params)

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
        system, user_messages = _split_system(messages)
        response = client.messages.create(
            model=self.model_name,
            messages=user_messages,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens or int(self.configuration.get("max_tokens") or 1024),
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
        system, user_messages = _split_system(messages)
        response = await client.messages.create(
            model=self.model_name,
            messages=user_messages,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens or int(self.configuration.get("max_tokens") or 1024),
        )
        return _to_dict(response)

    async def create_embeddings(
        self,
        texts: list[str],
        embedding_model: str | None = None,
    ) -> list[list[float]]:
        raise NotImplementedError("Anthropic provider does not support embeddings.")


__all__ = ["AnthropicProvider"]
