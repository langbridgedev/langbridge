
import asyncio
import logging
import os
from typing import Any, Dict, List, Optional, Sequence

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedVectorDB, run_sync
from langbridge.connectors.base.errors import ConnectorError
from langbridge.runtime.logger import get_root_logger
from .config import QdrantConnectorConfig, _parse_bool

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qmodels
except ImportError as exc:
    QdrantClient = None
    qmodels = None
    _QDRANT_IMPORT_ERROR = exc
else:
    _QDRANT_IMPORT_ERROR = None


class QdrantConnector(ManagedVectorDB):
    """Managed connector for Qdrant."""

    RUNTIME_TYPE = ConnectorRuntimeType.QDRANT

    def __init__(self, config: QdrantConnectorConfig, logger: Optional[Any] = None) -> None:
        super().__init__(config=config, logger=logger)
        self._require_qdrant()
        self._client = QdrantClient(
            host=config.host,
            port=config.port,
            api_key=config.api_key,
            https=config.https,
        )
        self._collection = config.collection
        self._next_id = 1
        self._lock = asyncio.Lock()

    async def test_connection(self) -> None:
        self._require_qdrant()
        await run_sync(self._client.get_collections)

    @staticmethod
    async def create_managed_instance(
        kwargs: Any,
        logger: Optional[logging.Logger] = None,
    ) -> "QdrantConnector":
        index_name: str = kwargs.get("index_name")
        if not index_name:
            raise ConnectorError("index_name is required to create a Qdrant managed instance.")
        if logger is None:
            logger = get_root_logger()
        host = kwargs.get("host") or os.getenv("QDRANT_HOST", "localhost")
        port = int(kwargs.get("port") or os.getenv("QDRANT_PORT", "6333"))
        api_key = kwargs.get("api_key") or os.getenv("QDRANT_API_KEY")
        https = _parse_bool(kwargs.get("https") or os.getenv("QDRANT_HTTPS", "false"))
        config = QdrantConnectorConfig(
            host=host,
            port=port,
            api_key=api_key,
            https=https,
            collection=index_name,
        )
        return QdrantConnector(config=config, logger=logger)

    async def create_index(self, dimension: int, *, metric: str = "cosine") -> None:
        self._require_qdrant()
        async with self._lock:
            try:
                await run_sync(self._client.get_collection, self._collection)
                raise ConnectorError("Qdrant collection already exists.")
            except Exception:
                pass

            distance = _distance_from_metric(metric)
            params = qmodels.VectorParams(size=dimension, distance=distance)
            await run_sync(
                self._client.create_collection,
                collection_name=self._collection,
                vectors_config=params,
            )

    async def delete_index(self) -> None:
        self._require_qdrant()
        await run_sync(self._client.delete_collection, self._collection)

    async def upsert_vectors(
        self,
        vectors: Sequence[Sequence[float]],
        *,
        metadata: Optional[Sequence[Any]] = None,
    ) -> List[int]:
        if not vectors:
            return []
        self._require_qdrant()

        payloads: List[Any]
        if metadata is None:
            payloads = [None] * len(vectors)
        else:
            payloads = list(metadata)
            if len(payloads) != len(vectors):
                raise ConnectorError("Metadata length must match number of vectors.")

        async with self._lock:
            ids = list(range(self._next_id, self._next_id + len(vectors)))
            self._next_id += len(vectors)
            points = [
                qmodels.PointStruct(id=idx, vector=vector, payload=payload)
                for idx, vector, payload in zip(ids, vectors, payloads)
            ]
            await run_sync(
                self._client.upsert,
                collection_name=self._collection,
                points=points,
            )
            return ids

    async def search(
        self,
        vector: Sequence[float],
        *,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if top_k <= 0:
            return []
        self._require_qdrant()

        query_filter = _build_filter(metadata_filters)
        results = await run_sync(
            self._client.search,
            collection_name=self._collection,
            query_vector=vector,
            limit=top_k,
            with_payload=True,
            query_filter=query_filter,
        )
        output: List[Dict[str, Any]] = []
        for hit in results:
            output.append(
                {
                    "id": hit.id,
                    "score": float(hit.score),
                    "metadata": hit.payload,
                }
            )
        return output

    @staticmethod
    def _require_qdrant() -> None:
        if QdrantClient is not None:
            return
        raise ConnectorError(
            "qdrant-client is not installed; add it to requirements to enable Qdrant connectors."
        ) from _QDRANT_IMPORT_ERROR


def _distance_from_metric(metric: str) -> "qmodels.Distance":
    metric = metric.lower()
    if metric == "cosine":
        return qmodels.Distance.COSINE
    if metric in {"dot", "inner"}:
        return qmodels.Distance.DOT
    if metric in {"l2", "euclidean"}:
        return qmodels.Distance.EUCLID
    raise ConnectorError(f"Unsupported metric '{metric}' for Qdrant.")


def _build_filter(metadata_filters: Optional[Dict[str, Any]]) -> Optional["qmodels.Filter"]:
    if not metadata_filters:
        return None
    conditions = [
        qmodels.FieldCondition(key=key, match=qmodels.MatchValue(value=value))
        for key, value in metadata_filters.items()
    ]
    return qmodels.Filter(must=conditions)
