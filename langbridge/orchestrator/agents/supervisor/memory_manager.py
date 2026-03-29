
import logging
import math
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Sequence

from langbridge.runtime.models import (
    RuntimeMessageRole,
    RuntimeThreadMessage,
)
from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.ports import ConversationMemoryStore
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedVectorDB
from langbridge.plugins.connectors import VectorDBConnectorFactory

from .schemas import MemoryItem, MemoryRetrievalResult


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SHORT_TERM_WINDOW = 20
_DEFAULT_TOP_K = 5


class MemoryManager:
    """System-owned memory retrieval + writeback manager."""

    def __init__(
        self,
        *,
        repository: ConversationMemoryStore,
        embedding_provider: Optional[EmbeddingProvider],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._repository = repository
        self._embedding_provider = embedding_provider
        self._logger = logger or logging.getLogger(__name__)
        self._vector_factory = VectorDBConnectorFactory()

    async def retrieve_context(
        self,
        *,
        thread_id: uuid.UUID,
        query: str,
        messages: Sequence[RuntimeThreadMessage],
        top_k: int = _DEFAULT_TOP_K,
    ) -> MemoryRetrievalResult:
        short_term = self._build_short_term_context(messages)
        stored_items = await self._repository.list_for_thread(thread_id, limit=250)
        if not stored_items:
            return MemoryRetrievalResult(short_term_context=short_term, retrieved_items=[])

        embedding_scores: dict[str, float] = await self._search_vector_scores(thread_id, query, top_k=max(top_k * 3, 12))
        ranked = self._rank_items(query=query, items=stored_items, embedding_scores=embedding_scores)
        selected = ranked[: max(1, top_k)]
        selected_ids = [item.id for item in selected if getattr(item, "id", None)]
        if selected_ids:
            await self._repository.touch_items(selected_ids)

        memories = [
            MemoryItem(
                id=str(item.id),
                thread_id=str(item.thread_id),
                actor_id=str(item.actor_id) if item.actor_id else None,
                category=item.category,
                content=item.content,
                metadata=item.metadata_json or {},
                created_at=item.created_at,
                last_accessed_at=item.last_accessed_at,
                score=item.metadata_json.get("retrieval_score") if isinstance(item.metadata_json, dict) else None,
            )
            for item in selected
        ]
        return MemoryRetrievalResult(short_term_context=short_term, retrieved_items=memories)

    async def write_back(
        self,
        *,
        thread_id: uuid.UUID,
        actor_id: Optional[uuid.UUID],
        user_query: str,
        response: dict[str, Any],
    ) -> None:
        entries = self._distill_memory_entries(user_query=user_query, response=response)
        if not entries:
            return

        created_records = []
        timestamp = datetime.now(timezone.utc)
        for entry in entries:
            metadata = dict(entry.get("metadata") or {})
            metadata["captured_at"] = timestamp.isoformat()
            record = self._repository.create_item(
                thread_id=thread_id,
                actor_id=actor_id,
                category=str(entry.get("category") or "fact"),
                content=str(entry.get("content") or "").strip(),
                metadata_json=metadata,
            )
            if record:
                created_records.append(record)

        if not created_records:
            return

        await self._repository.flush()
        await self._upsert_vectors(thread_id=thread_id, records=created_records)

    def _distill_memory_entries(self, *, user_query: str, response: dict[str, Any]) -> list[dict[str, Any]]:
        summary = str(response.get("summary") or "").strip()
        diagnostics = response.get("diagnostics") if isinstance(response.get("diagnostics"), dict) else {}
        assumptions = diagnostics.get("assumptions_applied") if isinstance(diagnostics, dict) else None
        research = diagnostics.get("research") if isinstance(diagnostics, dict) else None
        tool_calls = response.get("tool_calls") if isinstance(response.get("tool_calls"), list) else []

        entries: list[dict[str, Any]] = []

        if summary:
            entries.append(
                {
                    "category": "answer",
                    "content": f"User asked: {user_query}\nAssistant answer: {summary}",
                    "metadata": {"kind": "final_answer"},
                }
            )

        if isinstance(assumptions, list):
            for assumption in assumptions:
                text = str(assumption or "").strip()
                if not text:
                    continue
                entries.append(
                    {
                        "category": "decision",
                        "content": text,
                        "metadata": {"kind": "assumption"},
                    }
                )

        preference = self._extract_preference(user_query)
        if preference:
            entries.append(
                {
                    "category": "preference",
                    "content": preference,
                    "metadata": {"kind": "query_preference"},
                }
            )

        if isinstance(research, dict):
            report = research.get("report")
            if isinstance(report, dict):
                executive_summary = str(report.get("executive_summary") or "").strip()
                if executive_summary:
                    entries.append(
                        {
                            "category": "fact",
                            "content": f"Research summary for '{user_query}': {executive_summary}",
                            "metadata": {"kind": "research_summary"},
                        }
                    )

                key_findings = report.get("key_findings")
                if isinstance(key_findings, list):
                    for finding in key_findings[:6]:
                        if not isinstance(finding, dict):
                            continue
                        claim = str(finding.get("claim") or "").strip()
                        if not claim:
                            continue
                        citations = finding.get("citations")
                        citation_text = ""
                        if isinstance(citations, list):
                            normalized = [str(item).strip() for item in citations if str(item).strip()]
                            if normalized:
                                citation_text = " Sources: " + ", ".join(normalized[:3])
                        entries.append(
                            {
                                "category": "fact",
                                "content": f"{claim}{citation_text}",
                                "metadata": {
                                    "kind": "research_finding",
                                    "confidence": str(finding.get("confidence") or "medium"),
                                },
                            }
                        )

                next_steps = report.get("next_steps")
                if isinstance(next_steps, list):
                    for item in next_steps[:3]:
                        text = str(item or "").strip()
                        if not text:
                            continue
                        entries.append(
                            {
                                "category": "decision",
                                "content": text,
                                "metadata": {"kind": "research_next_step"},
                            }
                        )

        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            tool_name = str(tool_call.get("tool_name") or tool_call.get("tool") or "").strip()
            if not tool_name:
                continue
            error = tool_call.get("error")
            status = "failed" if error else "succeeded"
            entries.append(
                {
                    "category": "tool_outcome",
                    "content": f"Tool {tool_name} {status}.",
                    "metadata": {
                        "tool_name": tool_name,
                        "status": status,
                        "duration_ms": tool_call.get("duration_ms"),
                    },
                }
            )

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for entry in entries:
            key = (entry.get("category", ""), entry.get("content", ""))
            if key in seen:
                continue
            seen.add(key)
            if len(str(entry.get("content") or "").strip()) < 8:
                continue
            deduped.append(entry)
        return deduped

    @staticmethod
    def _extract_preference(query: str) -> Optional[str]:
        lowered = query.lower()
        if " in usd" in lowered or "usd" in lowered:
            return "User preference: report financial values in USD when possible."
        if "exclude benchmark" in lowered or "without benchmark" in lowered:
            return "User preference: exclude benchmark comparisons by default."
        return None

    def _build_short_term_context(self, messages: Sequence[RuntimeThreadMessage]) -> str:
        recent = list(messages)[-_SHORT_TERM_WINDOW:]
        if not recent:
            return ""

        lines: list[str] = []
        for message in recent:
            text = self._extract_message_text(message)
            if not text:
                continue
            role = "User" if self._role_value(message.role) == RuntimeMessageRole.user.value else "Assistant"
            lines.append(f"{role}: {text}")
        return "\n".join(lines)

    @staticmethod
    def _extract_message_text(message: RuntimeThreadMessage) -> str:
        content = message.content
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, dict):
            if MemoryManager._role_value(message.role) == RuntimeMessageRole.user.value:
                for key in ("text", "message", "prompt", "query"):
                    value = content.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
            if MemoryManager._role_value(message.role) == RuntimeMessageRole.assistant.value:
                for key in ("summary", "text"):
                    value = content.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
        return ""

    @staticmethod
    def _role_value(role: Any) -> str:
        return str(getattr(role, "value", role))

    def _rank_items(
        self,
        *,
        query: str,
        items: Sequence[Any],
        embedding_scores: dict[str, float],
    ) -> list[Any]:
        query_tokens = self._tokens(query)
        now = datetime.now(timezone.utc)

        def score_item(item: Any) -> float:
            item_id = str(getattr(item, "id", ""))
            embedding_score = embedding_scores.get(item_id)
            lexical = self._lexical_score(query_tokens, self._tokens(getattr(item, "content", "")))

            created_at = getattr(item, "created_at", None)
            recency = 0.0
            if isinstance(created_at, datetime):
                created = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
                age_hours = max(0.0, (now - created).total_seconds() / 3600.0)
                recency = 1.0 / (1.0 + age_hours / 24.0)

            if embedding_score is not None:
                score = (embedding_score * 0.75) + (lexical * 0.15) + (recency * 0.10)
            else:
                score = (lexical * 0.65) + (recency * 0.35)

            metadata_json = getattr(item, "metadata_json", None)
            if isinstance(metadata_json, dict):
                metadata_json["retrieval_score"] = round(score, 4)
            return score

        return sorted(items, key=score_item, reverse=True)

    async def _search_vector_scores(self, thread_id: uuid.UUID, query: str, top_k: int) -> dict[str, float]:
        if not self._embedding_provider:
            return {}

        vector_store = await self._get_vector_store(thread_id)
        if vector_store is None:
            return {}

        try:
            vectors = await self._embedding_provider.embed([query])
            if not vectors:
                return {}
            results = await vector_store.search(vectors[0], top_k=top_k)
        except Exception as exc:  # pragma: no cover
            self._logger.warning("Memory vector search failed for thread %s: %s", thread_id, exc)
            return {}

        scored: dict[str, float] = {}
        for match in results:
            metadata = match.get("metadata") if isinstance(match, dict) else None
            if not isinstance(metadata, dict):
                continue
            memory_item_id = metadata.get("memory_item_id")
            if not memory_item_id:
                continue
            score = match.get("score")
            try:
                score_value = float(score)
            except (TypeError, ValueError):
                score_value = 0.0
            normalized = 1.0 / (1.0 + math.exp(-score_value))
            scored[str(memory_item_id)] = max(scored.get(str(memory_item_id), 0.0), normalized)
        return scored

    async def _upsert_vectors(self, *, thread_id: uuid.UUID, records: Sequence[Any]) -> None:
        if not self._embedding_provider:
            return

        vector_store = await self._get_vector_store(thread_id)
        if vector_store is None:
            return

        texts = [str(record.content).strip() for record in records if str(record.content).strip()]
        if not texts:
            return

        try:
            embeddings = await self._embedding_provider.embed(texts)
            if not embeddings:
                return
        except Exception as exc:  # pragma: no cover
            self._logger.warning("Memory embedding generation failed: %s", exc)
            return

        metadata_payload = []
        clean_records = [record for record in records if str(record.content).strip()]
        for record in clean_records:
            metadata_payload.append(
                {
                    "memory_item_id": str(record.id),
                    "thread_id": str(record.thread_id),
                    "category": str(record.category),
                }
            )

        try:
            await vector_store.upsert_vectors(embeddings, metadata=metadata_payload)
        except Exception as exc:  # pragma: no cover
            self._logger.warning("Memory vector upsert failed for thread %s: %s", thread_id, exc)

    async def _get_vector_store(self, thread_id: uuid.UUID) -> Optional[ManagedVectorDB]:
        try:
            vector_class = self._vector_factory.get_managed_vector_db_class_reference(
                ConnectorRuntimeType.FAISS
            )
            index_name = f"thread_memory_{thread_id.hex}"
            vector_store = await vector_class.create_managed_instance(
                kwargs={"index_name": index_name},
                logger=self._logger,
            )
            await vector_store.test_connection()
            return vector_store
        except Exception as exc:  # pragma: no cover
            self._logger.warning("Unable to initialize memory vector store for thread %s: %s", thread_id, exc)
            return None

    @staticmethod
    def _tokens(text: str) -> set[str]:
        return set(_TOKEN_RE.findall(str(text or "").lower()))

    @staticmethod
    def _lexical_score(query_tokens: set[str], candidate_tokens: set[str]) -> float:
        if not query_tokens or not candidate_tokens:
            return 0.0
        overlap = query_tokens.intersection(candidate_tokens)
        if not overlap:
            return 0.0
        return len(overlap) / max(len(query_tokens), 1)


__all__ = ["MemoryManager"]
