import logging
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from langbridge.connectors.base import (
    SqlConnectorFactory,
    get_connector_config_factory,
)
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import SqlConnector
from langbridge.federation.connectors import DuckDbFileRemoteSource, RemoteSource, SqlConnectorRemoteSource
from langbridge.federation.executor import ArtifactStore
from langbridge.federation.models import FederationWorkflow, SMQQuery
from langbridge.federation.service import FederatedQueryService
from langbridge.runtime.providers import (
    ConnectorMetadataProvider,
    CredentialProvider,
    SecretRegistryCredentialProvider,
)
from langbridge.runtime.models import ConnectorMetadata
from langbridge.runtime.security.secrets import SecretProviderRegistry
from langbridge.runtime.settings import runtime_settings as settings
from langbridge.semantic.loader import load_semantic_model

class FederatedQueryToolRequest(BaseModel):
    workspace_id: str
    query: dict[str, Any] | str
    dialect: str = "tsql"
    workflow: FederationWorkflow
    semantic_model: dict[str, Any] | str | None = None


class FederatedQueryTool:
    def __init__(
        self,
        connector_provider: ConnectorMetadataProvider,
        secret_provider_registry: SecretProviderRegistry | None = None,
        credential_provider: CredentialProvider | None = None,
    ) -> None:
        self._connector_provider = connector_provider
        self._credential_provider = credential_provider or SecretRegistryCredentialProvider(
            registry=secret_provider_registry or SecretProviderRegistry()
        )
        self._logger = logging.getLogger(__name__)
        self._sql_connector_factory = SqlConnectorFactory()
        self._service = FederatedQueryService(
            artifact_store=ArtifactStore(base_dir=settings.FEDERATION_ARTIFACT_DIR),
        )

    async def execute_federated_query(self, query_payload: dict[str, Any]) -> dict[str, Any]:
        request = FederatedQueryToolRequest.model_validate(query_payload)
        sources = await self._build_sources(request.workflow)
        semantic_model = (
            load_semantic_model(request.semantic_model)
            if request.semantic_model is not None
            else None
        )
        self._service.register_workspace(
            workspace_id=request.workspace_id,
            workflow=request.workflow,
            sources=sources,
            semantic_model=semantic_model,
        )

        query_value: str | SMQQuery
        if isinstance(request.query, str):
            query_value = request.query
        else:
            query_value = SMQQuery.model_validate(request.query)

        result_handle = await self._service.execute(
            query=query_value,
            dialect=request.dialect,
            workspace_id=request.workspace_id,
        )
        table = await self._service.fetch_arrow(result_handle)
        rows = table.to_pylist()
        return {
            "result_handle": result_handle.model_dump(mode="json"),
            "columns": table.column_names,
            "rows": rows,
            "row_count": len(rows),
            "execution": result_handle.execution.model_dump(mode="json"),
        }

    async def explain_federated_query(self, query_payload: dict[str, Any]) -> dict[str, Any]:
        request = FederatedQueryToolRequest.model_validate(query_payload)
        sources = await self._build_sources(request.workflow)
        semantic_model = (
            load_semantic_model(request.semantic_model)
            if request.semantic_model is not None
            else None
        )
        self._service.register_workspace(
            workspace_id=request.workspace_id,
            workflow=request.workflow,
            sources=sources,
            semantic_model=semantic_model,
        )

        explain = await self._service.explain(
            query=request.query,
            dialect=request.dialect,
            workspace_id=request.workspace_id,
        )
        return explain.model_dump(mode="json")

    async def _build_sources(self, workflow: FederationWorkflow):
        sources: dict[str, RemoteSource] = {}
        source_bindings: dict[str, list[Any]] = {}
        for binding in workflow.dataset.tables.values():
            source_bindings.setdefault(binding.source_id, []).append(binding)

        for source_id, bindings in source_bindings.items():
            binding = bindings[0]
            descriptor = getattr(binding, "dataset_descriptor", None)
            descriptor_payload = descriptor.model_dump(mode="json") if descriptor is not None else {}
            metadata = binding.metadata if isinstance(binding.metadata, dict) else {}
            descriptor_source_kind = str(descriptor_payload.get("source_kind") or "").strip().lower()
            descriptor_storage_kind = str(descriptor_payload.get("storage_kind") or "").strip().lower()
            descriptor_materialization_mode = str(
                descriptor_payload.get("materialization_mode") or ""
            ).strip().lower()
            source_kind = str(metadata.get("source_kind") or descriptor_source_kind or "connector").strip().lower()
            is_file_like_source = (
                source_kind == "file"
                or descriptor_storage_kind in {"csv", "parquet", "json"}
                or (
                    descriptor_materialization_mode == "synced"
                    and descriptor_storage_kind in {"csv", "parquet", "json"}
                )
            )
            if is_file_like_source:
                sources[source_id] = DuckDbFileRemoteSource(
                    source_id=source_id,
                    bindings=bindings,
                    logger=self._logger,
                )
                continue

            connector_id = binding.connector_id
            if connector_id is None and descriptor_payload.get("connector_id"):
                connector_id = UUID(str(descriptor_payload["connector_id"]))
            if connector_id is None:
                raise ValueError(f"Connector source '{source_id}' is missing connector_id.")
            for extra_binding in bindings[1:]:
                if extra_binding.connector_id != connector_id:
                    raise ValueError(
                        f"Source id '{binding.source_id}' maps to multiple connector ids in workflow '{workflow.id}'."
                    )
            connector = await self._connector_provider.get_connector(
                workspace_id=UUID(str(workflow.workspace_id)),
                connector_id=connector_id,
            )
            if connector is None:
                raise ValueError(f"Connector '{connector_id}' not found for source '{source_id}'.")
            resolved_config = self._resolve_connector_config(connector)
            runtime_type = self._resolve_sql_connector_type(
                connector,
                source_id=source_id,
            )
            sql_connector = await self._create_sql_connector(
                connector_type=runtime_type,
                connector_config=resolved_config,
            )
            source_dialect = sql_connector.SQLGLOT_DIALECT
            sources[source_id] = SqlConnectorRemoteSource(
                source_id=source_id,
                connector=sql_connector,
                dialect=source_dialect,
                logger=self._logger,
            )

        return sources

    def _resolve_sql_connector_type(
        self,
        connector: ConnectorMetadata,
        *,
        source_id: str,
    ) -> ConnectorRuntimeType:
        connector_type = str(connector.connector_type or "").strip().upper()
        if not connector_type:
            raise ValueError(
                f"Connector '{connector.id}' for source '{source_id}' is missing connector_type."
            )
        try:
            runtime_type = ConnectorRuntimeType(connector_type)
        except ValueError as exc:
            raise ValueError(
                f"Connector '{connector.id}' for source '{source_id}' has unsupported connector type "
                f"'{connector.connector_type}'."
            ) from exc
        try:
            self._sql_connector_factory.get_sql_connector_class_reference(runtime_type)
        except ValueError as exc:
            raise ValueError(
                f"Connector '{connector.id}' for source '{source_id}' does not support SQL federation."
            ) from exc
        return runtime_type

    async def _create_sql_connector(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        connector_config: dict[str, Any],
    ) -> SqlConnector:
        try:
            self._sql_connector_factory.get_sql_connector_class_reference(connector_type)
        except ValueError as exc:
            raise ValueError(
                f"Connector type {connector_type.value} does not support SQL operations for federation."
            ) from exc
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_config.get("config", {}))
        sql_connector = self._sql_connector_factory.create_sql_connector(
            connector_type,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    def _resolve_connector_config(self, connector: ConnectorMetadata) -> dict[str, Any]:
        resolved_payload = dict(connector.config or {})
        runtime_config = dict(resolved_payload.get("config") or {})

        if connector.connection_metadata is not None:
            metadata = connector.connection_metadata.model_dump(exclude_none=True, by_alias=True)
            extra = metadata.pop("extra", {})
            for key, value in metadata.items():
                runtime_config.setdefault(key, value)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    if value is not None:
                        runtime_config.setdefault(key, value)

        for secret_name, secret_ref in connector.secret_references.items():
            runtime_config[secret_name] = self._credential_provider.resolve_secret(secret_ref)

        resolved_payload["config"] = runtime_config
        return resolved_payload


FederatedQueryExecutor = FederatedQueryTool
