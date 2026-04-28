from typing import Any

from ..base import LLMMessage, LLMProvider, LLMProviderName, LLMResponse, ProviderConfigurationError, response_text
from ..factory import register_provider

try:  # pragma: no cover - optional dependency
    from openai import AsyncAzureOpenAI, AzureOpenAI
except ImportError as exc:  # pragma: no cover - optional dependency
    AsyncAzureOpenAI = None  # type: ignore[assignment]
    AzureOpenAI = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover - optional dependency
    _IMPORT_ERROR = None

_OPTIONAL_CONFIG_KEYS = {
    "timeout",
    "max_retries",
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
class AzureOpenAIProvider(LLMProvider):
    name = LLMProviderName.AZURE

    def _client_params(self, overrides: dict[str, Any]) -> dict[str, Any]:
        deployment = (
            self.configuration.get("deployment_name")
            or self.configuration.get("deployment")
            or self.configuration.get("azure_deployment")
        )
        if not deployment:
            raise ProviderConfigurationError(
                "Azure OpenAI configuration requires 'deployment_name' "
                "(or 'deployment'/'azure_deployment')."
            )

        endpoint = (
            self.configuration.get("azure_endpoint")
            or self.configuration.get("api_base")
            or self.configuration.get("endpoint")
        )
        if not endpoint:
            raise ProviderConfigurationError(
                "Azure OpenAI configuration requires 'azure_endpoint' "
                "(or 'api_base'/'endpoint')."
            )

        api_version = overrides.pop("api_version", None) or self.configuration.get("api_version") or "2024-05-01-preview"
        params = {
            "api_key": self.api_key,
            "azure_endpoint": endpoint,
            "api_version": api_version,
        }
        for key in _OPTIONAL_CONFIG_KEYS:
            if key in self.configuration:
                params[key] = self.configuration[key]
        params.update(overrides)
        params = self._clean_kwargs(params)
        params["_deployment"] = str(deployment)
        return params

    def create_client(self, **overrides: Any) -> Any:
        if AzureOpenAI is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = self._client_params(dict(overrides))
        params.pop("_deployment", None)
        return AzureOpenAI(**params)

    def create_async_client(self, **overrides: Any) -> Any:
        if AsyncAzureOpenAI is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = self._client_params(dict(overrides))
        params.pop("_deployment", None)
        return AsyncAzureOpenAI(**params)

    def _deployment(self) -> str:
        return str(self._client_params({})["_deployment"])

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
            model=self._deployment(),
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
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
            model=self._deployment(),
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return _to_dict(response)

    async def create_embeddings(
        self,
        texts: list[str],
        embedding_model: str | None = None,
    ) -> list[list[float]]:
        if not texts:
            return []
        model = embedding_model or self.configuration.get("embedding_model")
        if not model:
            raise ProviderConfigurationError("Azure embeddings require embedding_model.")
        client = self.create_async_client()
        response = await client.embeddings.create(model=model, input=texts)
        return [list(item.embedding) for item in response.data]


__all__ = ["AzureOpenAIProvider"]
