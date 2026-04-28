
import asyncio
from dataclasses import dataclass
from typing import Any, Iterable, List, Sequence

import httpx
from openai import AzureOpenAI, OpenAI, OpenAIError  # type: ignore[import-untyped]

from langbridge.runtime.models import LLMProvider

DEFAULT_OPENAI_EMBED_MODEL = "text-embedding-3-small"
DEFAULT_AZURE_API_VERSION = "2024-05-01-preview"
DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"
_OPENAI_COMPATIBLE_CLIENT_CONFIG_KEYS = {
    "base_url",
    "timeout",
    "max_retries",
    "organization",
    "default_headers",
    "http_client",
}


class EmbeddingProviderError(RuntimeError):
    pass


@dataclass(slots=True)
class EmbeddingProvider:
    provider: LLMProvider
    api_key: str
    model_name: str
    configuration: dict
    embedding_model: str | None = None
    _client: Any | None = None
    _batch_size: int = 1000

    def __post_init__(self) -> None:
        self.configuration = dict(self.configuration or {})
        self.embedding_model = self._resolve_embedding_model()
        self._client = self._build_client()
        self._batch_size = int(self.configuration.get("embedding_batch_size", 1000))

    @classmethod
    def from_llm_connection(cls, connection: Any) -> "EmbeddingProvider":
        provider = getattr(connection, "provider", None)
        provider_value = getattr(provider, "value", provider)
        return cls(
            provider=LLMProvider(str(provider_value).lower()),
            api_key=str(getattr(connection, "api_key")),
            model_name=str(getattr(connection, "model")),
            configuration=getattr(connection, "configuration", {}) or {},
        )

    async def embed(self, texts: Sequence[str]) -> List[List[float]]:
        cleaned = [text.strip() for text in texts if isinstance(text, str) and text.strip()]
        if not cleaned:
            return []

        embeddings: List[List[float]] = []
        for chunk in _chunk(cleaned, self._batch_size):
            chunk_embeddings = await asyncio.to_thread(self._embed_chunk, chunk)
            embeddings.extend(chunk_embeddings)
        return embeddings

    def _embed_chunk(self, chunk: Sequence[str]) -> List[List[float]]:
        try:
            if self.provider == LLMProvider.OLLAMA:
                return self._embed_ollama_chunk(chunk)
            response = self._client.embeddings.create(
                model=self.embedding_model,
                input=list(chunk),
            )
        except (OpenAIError, httpx.HTTPError) as exc:  # pragma: no cover
            raise EmbeddingProviderError(f"Embedding request failed: {exc}") from exc

        return [list(item.embedding) for item in response.data]

    def _embed_ollama_chunk(self, chunk: Sequence[str]) -> List[List[float]]:
        payload: dict[str, Any] = {
            "model": self.embedding_model,
            "input": list(chunk),
        }
        if self.configuration.get("keep_alive") is not None:
            payload["keep_alive"] = self.configuration["keep_alive"]
        if self.configuration.get("truncate") is not None:
            payload["truncate"] = bool(self.configuration["truncate"])
        options = self.configuration.get("embedding_options") or self.configuration.get("options")
        if isinstance(options, dict):
            payload["options"] = dict(options)
        response = self._client.post("/api/embed", json=payload)
        response.raise_for_status()
        response_payload = response.json()
        embeddings = response_payload.get("embeddings") if isinstance(response_payload, dict) else None
        if not isinstance(embeddings, list):
            raise EmbeddingProviderError("Ollama embedding response missing embeddings.")
        return [list(item) for item in embeddings]

    def _resolve_embedding_model(self) -> str:
        configured = (
            self.configuration.get("embedding_model")
            or self.configuration.get("embedding_deployment")
            or self.configuration.get("embedding")
        )
        if self.provider == LLMProvider.OPENAI:
            return configured or DEFAULT_OPENAI_EMBED_MODEL
        if self.provider == LLMProvider.AZURE:
            if not configured:
                raise EmbeddingProviderError(
                    "Azure OpenAI connections must specify 'embedding_model' or "
                    "'embedding_deployment' in configuration."
                )
            return configured
        if self.provider == LLMProvider.OLLAMA:
            return configured or self.model_name
        raise EmbeddingProviderError(f"Provider '{self.provider}' does not support embeddings.")

    def _build_client(self):
        if self.provider == LLMProvider.OPENAI:
            return OpenAI(**self._openai_client_params())
        if self.provider == LLMProvider.AZURE:
            endpoint = self.configuration.get("api_base") or self.configuration.get("azure_endpoint")
            if not endpoint:
                raise EmbeddingProviderError(
                    "Azure OpenAI connections must include 'api_base' in configuration."
                )
            api_version = self.configuration.get("api_version", DEFAULT_AZURE_API_VERSION)
            return AzureOpenAI(
                api_key=self.api_key,
                azure_endpoint=endpoint,
                api_version=api_version,
            )
        if self.provider == LLMProvider.OLLAMA:
            base_url = str(self.configuration.get("base_url") or DEFAULT_OLLAMA_BASE_URL).strip()
            params: dict[str, Any] = {"base_url": (base_url or DEFAULT_OLLAMA_BASE_URL).rstrip("/")}
            if "timeout" in self.configuration:
                params["timeout"] = self.configuration["timeout"]
            return httpx.Client(**params)
        raise EmbeddingProviderError(f"Provider '{self.provider}' does not support embeddings.")

    def _openai_client_params(self) -> dict[str, Any]:
        params = {
            key: self.configuration.get(key)
            for key in _OPENAI_COMPATIBLE_CLIENT_CONFIG_KEYS
            if key in self.configuration
        }
        params = {key: value for key, value in params.items() if value is not None}
        params.setdefault("api_key", self.api_key)
        return params


def _chunk(values: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
