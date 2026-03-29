import asyncio
import logging
import pathlib
import sys
import uuid

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedVectorDB
from langbridge.connectors.vector.faiss.config import FaissConnectorConfig
from langbridge.connectors.vector.faiss.connector import FaissConnector
from langbridge.runtime.models import (
    ConnectionMetadata,
    ConnectorCapabilities,
    ConnectorMetadata,
    LifecycleState,
    ManagementMode,
    SecretReference,
    SemanticModelMetadata,
    SemanticVectorIndexStatus,
    SemanticVectorStoreTarget,
)
from langbridge.runtime.providers.memory import (
    MemoryConnectorProvider,
    MemorySemanticModelProvider,
    MemorySemanticVectorIndexProvider,
)
from langbridge.runtime.services.semantic_vector_search_service import (
    SemanticVectorSearchService,
)


class RecordingFederatedQueryTool:
    def __init__(self, rows):
        self._rows = list(rows)
        self.calls: list[dict] = []

    async def execute_federated_query(self, payload):
        self.calls.append(dict(payload))
        return {"rows": list(self._rows)}


class StubEmbeddingProvider:
    def __init__(self, vectors_by_text):
        self._vectors_by_text = dict(vectors_by_text)
        self.calls: list[list[str]] = []

    async def embed(self, texts):
        batch = list(texts)
        self.calls.append(batch)
        return [list(self._vectors_by_text[text]) for text in batch]


class DummyWorkflow:
    def model_dump(self, mode="json"):
        _ = mode
        return {"id": "semantic-vector-workflow"}


class StaticCredentialProvider:
    def __init__(self, value: str) -> None:
        self._value = value

    def resolve_secret(self, reference):
        _ = reference
        return self._value


class RecordingManagedVectorDB(ManagedVectorDB):
    RUNTIME_TYPE = ConnectorRuntimeType.QDRANT
    last_instance = None

    def __init__(self, config, logger=None) -> None:
        super().__init__(config=config, logger=logger)
        self.created_dimension = None
        self.deleted = False
        self.upserts = []
        type(self).last_instance = self

    @staticmethod
    async def create_managed_instance(kwargs, logger=None):
        raise AssertionError("Managed instance creation is not expected for explicit connectors.")

    async def test_connection(self) -> None:
        return None

    async def create_index(self, dimension: int, *, metric: str = "cosine") -> None:
        _ = metric
        self.created_dimension = dimension

    async def delete_index(self) -> None:
        self.deleted = True

    async def upsert_vectors(self, vectors, *, metadata=None):
        self.upserts.append((list(vectors), list(metadata or [])))
        return list(range(1, len(vectors) + 1))

    async def search(self, vector, *, top_k=5, metadata_filters=None):
        _ = (vector, top_k, metadata_filters)
        return []


def _semantic_model_yaml(vector_block: str) -> str:
    return (
        'version: "1.0"\n'
        "datasets:\n"
        "  shopify_orders:\n"
        "    relation_name: orders_enriched\n"
        "    dimensions:\n"
        "      - name: country\n"
        "        expression: country\n"
        "        type: string\n"
        f"{vector_block}"
    )


def test_semantic_vector_refresh_builds_default_faiss_index_and_searches_dimension_values(
    tmp_path, monkeypatch
) -> None:
    pytest.importorskip("faiss")

    async def _run() -> None:
        workspace_id = uuid.uuid4()
        semantic_model_id = uuid.uuid4()
        semantic_model = SemanticModelMetadata(
            id=semantic_model_id,
            workspace_id=workspace_id,
            name="orders_semantic",
            content_yaml=_semantic_model_yaml(
                "        vector:\n"
                "          enabled: true\n"
                "          refresh_interval: 1d\n"
                "          store:\n"
                "            type: managed_faiss\n"
            ),
            management_mode=ManagementMode.RUNTIME_MANAGED,
            lifecycle_state=LifecycleState.ACTIVE,
        )
        model_provider = MemorySemanticModelProvider(
            {(workspace_id, semantic_model_id): semantic_model}
        )
        index_store = MemorySemanticVectorIndexProvider({})
        federated_query_tool = RecordingFederatedQueryTool(
            [
                {"value": "France"},
                {"value": "Germany"},
                {"value": "France"},
                {"value": None},
                {"value": "  "},
            ]
        )
        embedder = StubEmbeddingProvider(
            {
                "France": [1.0, 0.0],
                "Germany": [0.0, 1.0],
                "French market": [0.99, 0.01],
            }
        )
        service = SemanticVectorSearchService(
            dataset_repository=None,
            federated_query_tool=federated_query_tool,
            logger=logging.getLogger("semantic-vector-tests"),
            semantic_model_provider=model_provider,
            semantic_vector_index_store=index_store,
        )

        async def _build_semantic_workflow(**kwargs):
            _ = kwargs
            return DummyWorkflow(), "postgres"

        async def _create_managed_instance(kwargs, logger=None):
            _ = logger
            return FaissConnector(
                config=FaissConnectorConfig(location=str(tmp_path / kwargs["index_name"]))
            )

        monkeypatch.setattr(
            service._dataset_execution_resolver,
            "build_semantic_workflow",
            _build_semantic_workflow,
        )
        monkeypatch.setattr(
            FaissConnector,
            "create_managed_instance",
            staticmethod(_create_managed_instance),
        )

        refreshed = await service.refresh_workspace(
            workspace_id=workspace_id,
            embedding_provider=embedder,
        )
        hits = await service.search(
            workspace_id=workspace_id,
            semantic_model_id=semantic_model_id,
            queries=["French market"],
            embedding_provider=embedder,
            top_k=3,
        )
        indexes = await index_store.list_for_workspace(
            workspace_id=workspace_id,
            semantic_model_id=semantic_model_id,
        )

        assert len(refreshed) == 1
        assert len(indexes) == 1
        assert indexes[0].vector_store_target == SemanticVectorStoreTarget.MANAGED_FAISS
        assert indexes[0].refresh_status == SemanticVectorIndexStatus.READY
        assert indexes[0].indexed_value_count == 2
        assert indexes[0].embedding_dimension == 2
        assert federated_query_tool.calls[0]["query"].startswith("SELECT DISTINCT")
        assert "FROM shopify_orders" in federated_query_tool.calls[0]["query"]
        assert embedder.calls[0] == ["France", "Germany"]
        assert hits[0].dataset_key == "shopify_orders"
        assert hits[0].dimension_name == "country"
        assert hits[0].matched_value == "France"
        assert hits[0].source_text == "French market"

    asyncio.run(_run())


def test_semantic_vector_explicit_connector_path_resolves_connector_scoped_store(
    monkeypatch,
) -> None:
    async def _run() -> None:
        workspace_id = uuid.uuid4()
        connector_id = uuid.uuid4()
        connector_provider = MemoryConnectorProvider(
            {
                connector_id: ConnectorMetadata(
                    id=connector_id,
                    workspace_id=workspace_id,
                    name="semantic-qdrant",
                    connector_type="qdrant",
                    connector_family="vector",
                    config={"config": {"host": "qdrant.internal", "port": 6333}},
                    connection_metadata=ConnectionMetadata(extra={"https": False}),
                    secret_references={
                        "api_key": SecretReference(
                            provider_type="env",
                            identifier="QDRANT_API_KEY",
                        )
                    },
                    capabilities=ConnectorCapabilities(),
                    management_mode=ManagementMode.RUNTIME_MANAGED,
                    lifecycle_state=LifecycleState.ACTIVE,
                )
            }
        )
        service = SemanticVectorSearchService(
            dataset_repository=None,
            federated_query_tool=None,
            logger=logging.getLogger("semantic-vector-tests"),
            connector_provider=connector_provider,
            credential_provider=StaticCredentialProvider("secret-qdrant-key"),
        )
        service._vector_factory = type(
            "RecordingVectorFactory",
            (),
            {
                "get_managed_vector_db_class_reference": staticmethod(
                    lambda connector_type: RecordingManagedVectorDB
                    if connector_type == ConnectorRuntimeType.QDRANT
                    else None
                )
            },
        )()

        metadata = await service._build_index_metadata(
            workspace_id=workspace_id,
            semantic_model_id=uuid.uuid4(),
            dataset_key="shopify_orders",
            dimension=service._load_model(
                _semantic_model_yaml(
                    "        vector:\n"
                    "          enabled: true\n"
                    "          refresh_interval: 6h\n"
                    "          store:\n"
                    "            type: connector\n"
                    "            connector_name: semantic-qdrant\n"
                    "            index_name: customer-country-search\n"
                )
            ).datasets["shopify_orders"].dimensions[0],
            prior=None,
        )
        vector_store = await service._resolve_vector_store(
            workspace_id=workspace_id,
            index_metadata=metadata,
        )

        assert metadata.vector_store_target == SemanticVectorStoreTarget.CONNECTOR
        assert metadata.vector_connector_id == connector_id
        assert metadata.refresh_interval_seconds == 21600
        assert metadata.vector_index_name == "customer_country_search"
        assert isinstance(vector_store, RecordingManagedVectorDB)
        assert RecordingManagedVectorDB.last_instance is vector_store
        assert vector_store.config.collection == "customer_country_search"
        assert vector_store.config.host == "qdrant.internal"
        assert vector_store.config.port == 6333
        assert vector_store.config.api_key == "secret-qdrant-key"

    asyncio.run(_run())
