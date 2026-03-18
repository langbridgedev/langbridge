from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Iterable, List, Sequence

from openai import AzureOpenAI, OpenAI, OpenAIError  # type: ignore[import-untyped]

from langbridge.packages.runtime.models import LLMProvider

DEFAULT_OPENAI_EMBED_MODEL = "text-embedding-3-small"
DEFAULT_AZURE_API_VERSION = "2024-05-01-preview"


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
            response = self._client.embeddings.create(
                model=self.embedding_model,
                input=list(chunk),
            )
        except OpenAIError as exc:  # pragma: no cover
            raise EmbeddingProviderError(f"Embedding request failed: {exc}") from exc

        return [list(item.embedding) for item in response.data]

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
        raise EmbeddingProviderError(f"Provider '{self.provider}' does not support embeddings.")

    def _build_client(self):
        if self.provider == LLMProvider.OPENAI:
            return OpenAI(api_key=self.api_key)
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
        raise EmbeddingProviderError(f"Provider '{self.provider}' does not support embeddings.")


def _chunk(values: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
