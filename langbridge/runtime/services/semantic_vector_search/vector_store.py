import uuid
from pathlib import Path
from typing import Any

from langbridge.connectors.base import ConnectorRuntimeType, get_connector_config_factory
from langbridge.connectors.base.connector import ManagedVectorDB
from langbridge.plugins.connectors import VectorDBConnectorFactory
from langbridge.runtime import settings
from langbridge.runtime.models import SemanticVectorIndexMetadata, SemanticVectorStoreTarget
from langbridge.runtime.ports import ConnectorMetadataProvider, CredentialProvider
from langbridge.runtime.services.errors import ExecutionValidationError


class SemanticVectorStoreResolver:
    """Resolves vector index metadata into managed vector database instances."""

    def __init__(
        self,
        *,
        connector_provider: ConnectorMetadataProvider | None,
        credential_provider: CredentialProvider | None,
        logger: Any,
        vector_factory: VectorDBConnectorFactory | None = None,
    ) -> None:
        self._connector_provider = connector_provider
        self._credential_provider = credential_provider
        self._logger = logger
        self.vector_factory = vector_factory or VectorDBConnectorFactory()

    async def resolve_vector_store(
        self,
        *,
        workspace_id: uuid.UUID,
        index_metadata: SemanticVectorIndexMetadata,
    ) -> ManagedVectorDB:
        target = SemanticVectorStoreTarget(index_metadata.vector_store_target)
        if target == SemanticVectorStoreTarget.MANAGED_FAISS:
            connector_class = self.vector_factory.get_managed_vector_db_class_reference(
                ConnectorRuntimeType.FAISS
            )
            return await connector_class.create_managed_instance(
                {
                    "index_name": index_metadata.vector_index_name,
                    "location": settings.runtime_settings.MANAGED_VECTOR_FAISS_DB_DIR,
                },
                logger=self._logger,
            )

        connector = await self.load_connector_for_index(
            workspace_id=workspace_id,
            index_metadata=index_metadata,
        )
        if connector.connector_type is None:
            raise ValueError(f"Connector '{connector.id}' does not define a connector_type.")
        connector_type = connector.connector_type
        connector_class = self.vector_factory.get_managed_vector_db_class_reference(connector_type)
        connector_payload = self.resolve_connector_config(connector)
        runtime_config = dict(connector_payload.get("config") or {})
        runtime_config = self.apply_index_namespace(
            connector_type=connector_type,
            runtime_config=runtime_config,
            index_name=index_metadata.vector_index_name,
        )
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(runtime_config)
        return connector_class(config=config_instance, logger=self._logger)

    async def load_connector_for_index(
        self,
        *,
        workspace_id: uuid.UUID,
        index_metadata: SemanticVectorIndexMetadata,
    ) -> Any:
        if index_metadata.vector_connector_id is None:
            raise ExecutionValidationError(
                f"Semantic vector index '{index_metadata.id}' is missing a vector connector."
            )
        if self._connector_provider is None:
            raise ExecutionValidationError(
                "Connector metadata provider is required for explicit semantic vector connectors."
            )
        connector = await self._connector_provider.get_connector(
            workspace_id=workspace_id,
            connector_id=index_metadata.vector_connector_id,
        )
        if connector is None:
            raise ExecutionValidationError(
                f"Vector connector '{index_metadata.vector_connector_name or index_metadata.vector_connector_id}' was not found."
            )
        return connector

    async def resolve_connector_id(
        self,
        *,
        workspace_id: uuid.UUID,
        connector_name: str | None,
    ) -> uuid.UUID | None:
        normalized_name = str(connector_name or "").strip()
        if not normalized_name:
            return None
        if self._connector_provider is None:
            raise ExecutionValidationError(
                "Connector metadata provider is required for explicit semantic vector connectors."
            )
        connector = await self._connector_provider.get_connector_by_name(
            workspace_id=workspace_id,
            connector_name=normalized_name,
        )
        if connector is None:
            return None
        return connector.id

    def resolve_connector_config(self, connector: Any) -> dict[str, Any]:
        resolved_payload = dict(getattr(connector, "config", None) or {})
        runtime_config = dict(resolved_payload.get("config") or {})

        connection_metadata = getattr(connector, "connection_metadata", None)
        if connection_metadata is not None:
            metadata = connection_metadata.model_dump(exclude_none=True, by_alias=True)
            extra = metadata.pop("extra", {})
            for key, value in metadata.items():
                runtime_config.setdefault(key, value)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    if value is not None:
                        runtime_config.setdefault(key, value)

        if self._credential_provider is not None:
            for secret_name, secret_ref in dict(getattr(connector, "secret_references", None) or {}).items():
                runtime_config[secret_name] = self._credential_provider.resolve_secret(secret_ref)

        resolved_payload["config"] = runtime_config
        return resolved_payload

    def apply_index_namespace(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        runtime_config: dict[str, Any],
        index_name: str,
    ) -> dict[str, Any]:
        updated = dict(runtime_config)
        if connector_type == ConnectorRuntimeType.QDRANT:
            updated["collection"] = index_name
            return updated
        if connector_type == ConnectorRuntimeType.FAISS:
            configured_location = str(updated.get("location") or "~/langbridge/faiss_data").strip()
            location_path = Path(configured_location).expanduser()
            base_directory = location_path.parent if location_path.suffix else location_path
            updated["location"] = str(base_directory / index_name)
            return updated
        return updated
