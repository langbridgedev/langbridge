"""Semantic search tool for query grounding."""

import json
import logging
import uuid
from typing import Any, Protocol

from pydantic import BaseModel, Field

from langbridge.ai.events import AIEventEmitter, AIEventSource
from langbridge.ai.llm.base import LLMProvider
from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.services.semantic_vector_search import SemanticVectorSearchService


class VectorStoreLike(Protocol):
    async def search(
        self,
        embedding: list[float],
        *,
        top_k: int,
        metadata_filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        ...


class SemanticSearchResult(BaseModel):
    score: float
    identifier: int
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_prompt_string(self) -> str:
        column = str(self.metadata.get("column") or "").strip()
        value = str(self.metadata.get("value") or "").strip()
        if column and value:
            return f"Column: {column}, Value: {value} (Score: {self.score:.4f})"
        return f"ID: {self.identifier}, Score: {self.score:.4f}, Metadata: {self.metadata}"


class SemanticSearchResultCollection(BaseModel):
    results: list[SemanticSearchResult] = Field(default_factory=list)

    def to_prompt_strings(self) -> list[str]:
        return [result.to_prompt_string() for result in self.results]


class SemanticSearchTool(AIEventSource):
    """Runs semantic/vector search for grounding SQL generation."""

    def __init__(
        self,
        *,
        name: str,
        llm_provider: LLMProvider | None = None,
        embedding_model: str | None = None,
        vector_store: VectorStoreLike | None = None,
        entity_recognition: bool = False,
        metadata_filters: dict[str, Any] | None = None,
        logger: logging.Logger | None = None,
        semantic_vector_search_service: SemanticVectorSearchService | None = None,
        semantic_vector_search_workspace_id: uuid.UUID | None = None,
        semantic_vector_search_model_id: uuid.UUID | None = None,
        semantic_vector_search_dataset_key: str | None = None,
        semantic_vector_search_dimension_name: str | None = None,
        embedding_provider: EmbeddingProvider | None = None,
        event_emitter: AIEventEmitter | None = None,
    ) -> None:
        super().__init__(event_emitter=event_emitter)
        self._name = name
        self._llm = llm_provider
        self._embedding_model = embedding_model
        self._vector_store = vector_store
        self._entity_recognition = entity_recognition
        self._metadata_filters = dict(metadata_filters or {})
        self._logger = logger or logging.getLogger(__name__)
        self._semantic_vector_search_service = semantic_vector_search_service
        self._semantic_vector_search_workspace_id = semantic_vector_search_workspace_id
        self._semantic_vector_search_model_id = semantic_vector_search_model_id
        self._semantic_vector_search_dataset_key = str(semantic_vector_search_dataset_key or "").strip() or None
        self._semantic_vector_search_dimension_name = str(semantic_vector_search_dimension_name or "").strip() or None
        self._embedding_provider = embedding_provider
        self._validate_configuration()

    @property
    def name(self) -> str:
        return self._name

    async def search(self, query: str, top_k: int = 5) -> SemanticSearchResultCollection:
        if top_k <= 0:
            raise ValueError("Semantic search top_k must be greater than zero.")
        await self._emit_ai_event(
            event_type="SemanticSearchStarted",
            message=f"Searching semantic index {self.name}.",
            source=self.name,
            details={"tool": self.name, "top_k": top_k},
        )
        if self._semantic_vector_search_service is not None:
            result = await self._search_runtime_vectors(query=query, top_k=top_k)
        else:
            result = await self._search_vector_store(query=query, top_k=top_k)
        await self._emit_ai_event(
            event_type="SemanticSearchCompleted",
            message=f"Semantic search returned {len(result.results)} result(s).",
            source=self.name,
            details={"tool": self.name, "result_count": len(result.results)},
        )
        return result

    async def search_prompts(self, query: str, top_k: int = 5) -> list[str]:
        return (await self.search(query=query, top_k=top_k)).to_prompt_strings()

    def _validate_configuration(self) -> None:
        if self._semantic_vector_search_service is not None:
            missing = [
                name
                for name, value in {
                    "semantic_vector_search_workspace_id": self._semantic_vector_search_workspace_id,
                    "semantic_vector_search_model_id": self._semantic_vector_search_model_id,
                    "semantic_vector_search_dataset_key": self._semantic_vector_search_dataset_key,
                    "semantic_vector_search_dimension_name": self._semantic_vector_search_dimension_name,
                }.items()
                if not value
            ]
            if missing:
                raise ValueError(f"Runtime semantic vector search missing: {', '.join(missing)}")
            return
        if self._vector_store is None or self._llm is None:
            raise ValueError("SemanticSearchTool requires runtime vector search or llm_provider + vector_store.")

    async def _search_runtime_vectors(self, *, query: str, top_k: int) -> SemanticSearchResultCollection:
        assert self._semantic_vector_search_service is not None
        hits = await self._semantic_vector_search_service.search_dimension(
            workspace_id=self._semantic_vector_search_workspace_id,
            semantic_model_id=self._semantic_vector_search_model_id,
            dataset_key=self._semantic_vector_search_dataset_key,
            dimension_name=self._semantic_vector_search_dimension_name,
            queries=await self._search_phrases(query=query, top_k=top_k),
            embedding_provider=self._embedding_provider,
            top_k=top_k,
        )
        return SemanticSearchResultCollection(
            results=[
                SemanticSearchResult(
                    identifier=hit.index_id.int,
                    score=float(hit.score),
                    metadata={
                        "dataset_key": hit.dataset_key,
                        "dimension_name": hit.dimension_name,
                        "column": f"{hit.dataset_key}.{hit.dimension_name}",
                        "value": hit.matched_value,
                        "source_text": hit.source_text,
                    },
                )
                for hit in hits
            ]
        )

    async def _search_vector_store(self, *, query: str, top_k: int) -> SemanticSearchResultCollection:
        if self._llm is None or self._vector_store is None:
            raise ValueError("SemanticSearchTool is not configured for vector store search.")
        phrases = await self._search_phrases(query=query, top_k=top_k)
        embeddings = await self._llm.create_embeddings(phrases, embedding_model=self._embedding_model)
        if len(embeddings) != len(phrases):
            raise RuntimeError("Embedding provider returned a different number of embeddings than input phrases.")

        merged: dict[int, tuple[float, dict[str, Any]]] = {}
        for embedding in embeddings:
            raw_results = await self._vector_store.search(
                embedding,
                top_k=top_k,
                metadata_filters=self._metadata_filters,
            )
            for raw in raw_results:
                identifier = int(raw["id"])
                score = float(raw.get("score", 0.0))
                metadata = dict(raw.get("metadata") or {})
                current = merged.get(identifier)
                if current is None or score > current[0]:
                    merged[identifier] = (score, metadata)
        results = [
            SemanticSearchResult(identifier=identifier, score=score, metadata=metadata)
            for identifier, (score, metadata) in merged.items()
        ]
        results.sort(key=lambda item: item.score, reverse=True)
        return SemanticSearchResultCollection(results=results[:top_k])

    async def _search_phrases(self, *, query: str, top_k: int) -> list[str]:
        if not self._entity_recognition:
            return [query]
        if self._llm is None:
            raise ValueError("Entity recognition requires an LLM provider.")
        prompt = (
            "Extract concrete entity phrases for vector search.\n"
            "Return STRICT JSON only: {\"entities\":[\"...\"]}\n"
            f"Return at most {top_k} entities. Query: {query}\n"
        )
        raw = await self._llm.acomplete(prompt, temperature=0.0, max_tokens=300)
        parsed = self._parse_json_object(raw)
        entities = parsed.get("entities")
        if not isinstance(entities, list):
            raise ValueError("Entity recognition response did not include an entities list.")
        phrases = [str(item).strip() for item in entities if str(item).strip()]
        return phrases[:top_k] or [query]

    @staticmethod
    def _parse_json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("Semantic search LLM response did not contain a JSON object.")
        return json.loads(text[start : end + 1])


__all__ = [
    "SemanticSearchResult",
    "SemanticSearchResultCollection",
    "SemanticSearchTool",
    "VectorStoreLike",
]
