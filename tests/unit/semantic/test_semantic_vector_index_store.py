import asyncio
import pathlib
import sys
import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3]))

from langbridge.runtime.models import (
    LifecycleState,
    ManagementMode,
    SemanticVectorIndexMetadata,
    SemanticVectorIndexStatus,
    SemanticVectorStoreTarget,
)
from langbridge.runtime.persistence.db.base import Base
from langbridge.runtime.persistence.db.semantic import (
    SemanticModelEntry,
    SemanticVectorIndexEntry,
)
from langbridge.runtime.persistence.db.workspace import Workspace
from langbridge.runtime.persistence.repositories.semantic_search_repository import (
    SemanticVectorIndexRepository,
)
from langbridge.runtime.persistence.stores import RepositorySemanticVectorIndexStore


def test_semantic_vector_index_store_round_trips_dimension_metadata() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=[
                        Workspace.__table__,
                        SemanticModelEntry.__table__,
                        SemanticVectorIndexEntry.__table__,
                    ],
                )
            )

        workspace_id = uuid.uuid4()
        semantic_model_id = uuid.uuid4()
        semantic_vector_index_id = uuid.uuid4()

        async with AsyncSession(engine, expire_on_commit=False) as session:
            session.add(Workspace(id=workspace_id, name="semantic-vector-tests"))
            session.add(
                SemanticModelEntry(
                    id=semantic_model_id,
                    workspace_id=workspace_id,
                    name="orders_semantic",
                    content_yaml="version: '1.0'\ndatasets: {}",
                    content_json="{}",
                    management_mode=ManagementMode.RUNTIME_MANAGED.value,
                    lifecycle_state=LifecycleState.ACTIVE.value,
                )
            )
            await session.commit()

            store = RepositorySemanticVectorIndexStore(
                repository=SemanticVectorIndexRepository(session)
            )
            saved = await store.save(
                SemanticVectorIndexMetadata(
                    id=semantic_vector_index_id,
                    workspace_id=workspace_id,
                    semantic_model_id=semantic_model_id,
                    dataset_key="shopify_orders",
                    dimension_name="country",
                    vector_store_target=SemanticVectorStoreTarget.MANAGED_FAISS,
                    vector_index_name="semantic_country_idx",
                    refresh_interval_seconds=86400,
                    refresh_status=SemanticVectorIndexStatus.READY,
                    indexed_value_count=2,
                    embedding_dimension=2,
                    last_refreshed_at=datetime.now(timezone.utc),
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )
            await session.commit()

            loaded = await store.get_for_dimension(
                workspace_id=workspace_id,
                semantic_model_id=semantic_model_id,
                dataset_key="shopify_orders",
                dimension_name="country",
            )
            listed = await store.list_for_workspace(
                workspace_id=workspace_id,
                semantic_model_id=semantic_model_id,
            )

            assert loaded is not None
            assert loaded.id == semantic_vector_index_id
            assert loaded.vector_store_target == SemanticVectorStoreTarget.MANAGED_FAISS
            assert loaded.refresh_status == SemanticVectorIndexStatus.READY
            assert loaded.vector_index_name == "semantic_country_idx"
            assert loaded.last_refreshed_at is not None
            assert loaded.last_refreshed_at.tzinfo is not None
            assert loaded.created_at is not None
            assert loaded.created_at.tzinfo is not None
            assert loaded.updated_at is not None
            assert loaded.updated_at.tzinfo is not None
            assert listed == [loaded]

            await store.delete(
                workspace_id=workspace_id,
                semantic_vector_index_id=semantic_vector_index_id,
            )
            await session.commit()

            assert (
                await store.get_by_id(
                    workspace_id=workspace_id,
                    semantic_vector_index_id=semantic_vector_index_id,
                )
                is None
            )

        await engine.dispose()

        assert saved.refresh_status == SemanticVectorIndexStatus.READY

    asyncio.run(_run())
