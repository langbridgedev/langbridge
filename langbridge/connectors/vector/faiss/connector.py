import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
import uuid

import numpy as np

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedVectorDB
from langbridge.connectors.base.connector import run_sync
from langbridge.connectors.base.errors import ConnectorError
from .config import FaissConnectorConfig
from langbridge.runtime.logger import get_root_logger

try: 
    import faiss 
except ImportError as exc:
    faiss = None
    _FAISS_IMPORT_ERROR = exc
else:  
    _FAISS_IMPORT_ERROR = None

class FaissConnector(ManagedVectorDB):
    """Lightweight FAISS connector that stores a persistent local index."""

    RUNTIME_TYPE = ConnectorRuntimeType.FAISS

    def __init__(self, config: FaissConnectorConfig, logger: Optional[Any] = None) -> None:
        super().__init__(config=config, logger=logger)
        base_path = Path(config.location).expanduser()
        if base_path.is_dir() or not base_path.suffix:
            self._index_path = base_path / "index.faiss"
        else:
            self._index_path = base_path
            base_path = self._index_path.parent
        self._storage_dir = base_path
        self._metadata_path = self._index_path.with_name(self._index_path.name + ".meta.json")
        self._index: Any | None = None
        self._dimension: Optional[int] = None
        self._metadata: Dict[int, Any] = {}
        self._next_id: int = 1
        self._loaded: bool = False
        self._lock = asyncio.Lock()

    async def test_connection(self) -> None:
        self._require_faiss()
        async with self._lock:
            await self._ensure_loaded()
            
    @staticmethod
    async def create_managed_instance(
        kwargs: Any,
        logger: Optional[logging.Logger] = None,
    ) -> "FaissConnector":
        index_name: str = kwargs.get("index_name")
        if not index_name:
            raise ConnectorError("index_name is required to create a FAISS managed instance.")
        if logger is None:
            logger = get_root_logger()
        config = FaissConnectorConfig(location=f"~/langbridge/faiss_data/{index_name}")
        return FaissConnector(config=config, logger=logger)
    
    async def create_index(self, dimension, *, metric = "cosine"):
        self._require_faiss()
        async with self._lock:
            await self._ensure_loaded()
            if self._index is not None:
                raise ConnectorError("FAISS index already exists.")
            if metric != "cosine":
                raise ConnectorError("Only 'cosine' metric is supported in this FAISS connector.")
            self._dimension = dimension
            base_index = faiss.IndexFlatIP(self._dimension)
            self._index = faiss.IndexIDMap(base_index)
            await self._persist_state()
            
    async def delete_index(self):
        self._require_faiss()
        async with self._lock:
            await self._ensure_loaded()
            self._index = None
            self._dimension = None
            self._metadata = {}
            self._next_id = 1
            if self._index_path.exists():
                self._index_path.unlink()
            if self._metadata_path.exists():
                self._metadata_path.unlink()

    async def upsert_vectors(
        self,
        vectors: Sequence[Sequence[float]],
        *,
        metadata: Optional[Sequence[Any]] = None,
    ) -> List[int]:
        if not vectors:
            return []
        self._require_faiss()

        try:
            matrix = self._normalize_matrix(self._to_matrix(vectors))
        except ValueError as exc:  # pragma: no cover - defensive parsing guard
            raise ConnectorError(f"Invalid vector payload: {exc}") from exc

        payloads: List[Any]
        if metadata is None:
            payloads = [None] * len(matrix)
        else:
            payloads = list(metadata)
            if len(payloads) != len(matrix):
                raise ConnectorError("Metadata length must match number of vectors.")

        async with self._lock:
            await self._ensure_loaded()
            self._ensure_index_ready(matrix.shape[1])
            if self._index is None or self._dimension is None:
                raise ConnectorError("FAISS index unavailable; call test_connection first.")

            ids = np.arange(self._next_id, self._next_id + len(matrix), dtype="int64")
            self._next_id += len(matrix)
            try:
                self._index.add_with_ids(matrix, ids)
            except Exception as exc:  # pragma: no cover - relies on FAISS internals
                raise ConnectorError(f"Failed to add vectors to FAISS index: {exc}") from exc

            for idx, payload in zip(ids.tolist(), payloads):
                self._metadata[int(idx)] = payload

            await self._persist_state()
            return [int(idx) for idx in ids]

    async def search(
        self,
        vector: Sequence[float],
        *,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if top_k <= 0:
            return []
        self._require_faiss()

        try:
            query = self._normalize_matrix(self._to_matrix([vector]))
        except ValueError as exc:  # pragma: no cover - defensive parsing guard
            raise ConnectorError(f"Invalid query vector: {exc}") from exc
        async with self._lock:
            await self._ensure_loaded()
            if self._index is None or self._dimension is None:
                raise ConnectorError("FAISS index unavailable; call test_connection first.")
            if query.shape[1] != self._dimension:
                raise ConnectorError(
                    f"Query vector dimension {query.shape[1]} does not match index dimension {self._dimension}."
                )
            try:
                distances, indices = self._index.search(query.reshape(1, -1), top_k)
            except Exception as exc:  # pragma: no cover - relies on FAISS internals
                raise ConnectorError(f"FAISS search failed: {exc}") from exc

            results: List[Dict[str, Any]] = []
            for idx, dist in zip(indices[0], distances[0]):
                if idx == -1:
                    continue
                metadata_entry = self._metadata.get(int(idx))
                
                if metadata_filters:
                    if not metadata_entry or not all(
                        item in metadata_entry.items() for item in metadata_filters.items()
                    ):
                        continue
                
                results.append({
                    "id": int(idx),
                    "score": float(dist),
                    "metadata": metadata_entry,
                })
            return results

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        await run_sync(self._load_state)
        self._loaded = True

    async def _persist_state(self) -> None:
        try:
            await run_sync(self._persist_state_sync)
        except Exception as exc:  # pragma: no cover - surfaced to callers
            raise ConnectorError(f"Failed to persist FAISS index: {exc}") from exc

    def _load_state(self) -> None:
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        self._metadata = {}
        self._next_id = 1

        if faiss is None:
            return

        try:
            if self._index_path.exists():
                self._index = faiss.read_index(str(self._index_path))
                self._dimension = int(self._index.d)
            else:
                self._index = None
                self._dimension = None
        except Exception as exc:  # pragma: no cover - relies on FAISS internals
            raise ConnectorError(f"Failed to load FAISS index: {exc}") from exc

        if self._metadata_path.exists():
            try:
                with self._metadata_path.open("r", encoding="utf-8") as handle:
                    state = json.load(handle)
            except Exception as exc:  # pragma: no cover - IO/JSON guard
                raise ConnectorError(f"Failed to load FAISS metadata: {exc}") from exc

            self._next_id = int(state.get("next_id", 1))
            stored_dimension = state.get("dimension")
            if stored_dimension and not self._dimension:
                self._dimension = int(stored_dimension)
            raw_metadata = state.get("metadata") or state.get("items") or {}
            if isinstance(raw_metadata, list):
                for entry in raw_metadata:
                    if not isinstance(entry, dict) or "id" not in entry:
                        continue
                    self._metadata[int(entry.get("id"))] = entry.get("metadata")
            elif isinstance(raw_metadata, dict):
                for key, value in raw_metadata.items():
                    try:
                        self._metadata[int(key)] = value
                    except (TypeError, ValueError):  # pragma: no cover - defensive guard
                        continue

    def _persist_state_sync(self) -> None:
        if faiss is None:
            return
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        if self._index is not None:
            faiss.write_index(self._index, str(self._index_path))
        state = {
            "dimension": self._dimension,
            "next_id": self._next_id,
            "metadata": {str(key): value for key, value in self._metadata.items()},
        }
        with self._metadata_path.open("w", encoding="utf-8") as handle:
            json.dump(state, handle)

    def _ensure_index_ready(self, dimension: int) -> None:
        if self._dimension and self._dimension != dimension:
            raise ConnectorError(
                f"Existing FAISS index dimension {self._dimension} does not match provided dimension {dimension}."
            )
        if self._dimension is None:
            self._dimension = dimension
        if self._index is None:
            if faiss is None:
                raise ConnectorError("FAISS runtime not available.")
            base_index = faiss.IndexFlatIP(self._dimension)
            self._index = faiss.IndexIDMap(base_index)

    @staticmethod
    def _normalize_matrix(matrix: np.ndarray) -> np.ndarray:
        norm = np.linalg.norm(matrix)
        if norm == 0:
            return matrix
        return matrix / norm

    @staticmethod
    def _to_matrix(vectors: Sequence[Sequence[float]]) -> np.ndarray:
        matrix = np.asarray(vectors, dtype="float32")
        if len(matrix.shape) != 2:
            raise ValueError("Vectors must be a 2D array.")
        return matrix

    @staticmethod
    def _require_faiss() -> None:
        if faiss is not None:
            return
        raise ConnectorError(
            "faiss-cpu is not installed; add it to requirements to enable FAISS connectors."
        ) from _FAISS_IMPORT_ERROR
