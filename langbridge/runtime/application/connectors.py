
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Mapping

from langbridge.connectors.base import ConnectorRuntimeType, get_connector_config_factory
from langbridge.runtime.config.models import LocalRuntimeConnectorConfig
from langbridge.runtime.models import ConnectorSyncState
from langbridge.runtime.models.metadata import (
    ConnectionMetadata,
    ConnectionPolicy,
    ConnectorMetadata,
    LifecycleState,
    ManagementMode,
    SecretReference,
)
from langbridge.runtime.persistence.mappers.connectors import to_connector_record
from langbridge.runtime.utils.connector_runtime import (
    build_connector_runtime_payload,
    resolve_connector_capabilities,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


def _normalize_connector_type(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_connection_payload(
    *,
    connector_type: str,
    connection_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(connection_payload)
    if "path" in normalized:
        resolved_path = str(normalized.get("path") or "").strip() or None
        if resolved_path:
            if connector_type == "SQLITE":
                normalized["location"] = resolved_path
                normalized.pop("path", None)
            else:
                normalized["path"] = resolved_path
    if "location" in normalized and connector_type == "SQLITE":
        normalized_location = str(normalized.get("location") or "").strip()
        if normalized_location:
            normalized["location"] = normalized_location
    return normalized


def _extract_connection_metadata(payload: Mapping[str, Any]) -> ConnectionMetadata | None:
    known_keys = {"host", "port", "database", "schema", "warehouse", "role", "account", "user"}
    metadata_payload: dict[str, Any] = {}
    extra_payload: dict[str, Any] = {}
    for key, value in payload.items():
        if key in known_keys:
            metadata_payload[key] = value
        else:
            extra_payload[key] = value
    if not metadata_payload and not extra_payload:
        return None
    metadata_payload["extra"] = extra_payload
    return ConnectionMetadata.model_validate(metadata_payload)


class ConnectorApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    @staticmethod
    def _management_mode_value(value: ManagementMode | str) -> str:
        return str(getattr(value, "value", value))

    def _serialize_connector(self, connector: ConnectorMetadata) -> dict[str, Any]:
        capabilities = self._host._connector_capabilities(connector)
        management_mode = self._management_mode_value(connector.management_mode)
        return {
            "id": connector.id,
            "name": connector.name,
            "description": connector.description,
            "connector_type": connector.connector_type,
            "connector_family": connector.connector_family,
            "supports_sync": self._host._connector_supports_sync(connector),
            "supported_resources": list(connector.supported_resources or []),
            "sync_strategy": connector.sync_strategy,
            "capabilities": capabilities.model_dump(mode="json"),
            "management_mode": management_mode,
            "managed": management_mode == ManagementMode.CONFIG_MANAGED.value,
        }

    async def list_connectors(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for connector in self._host._connectors.values():
            items.append(self._serialize_connector(connector))
        return items

    async def create_connector(self, *, request) -> dict[str, Any]:
        normalized_request = LocalRuntimeConnectorConfig.model_validate(
            request.model_dump(mode="json")
        )
        connector_name = str(normalized_request.name or "").strip()
        if not connector_name:
            raise ValueError("Connector name is required.")
        if connector_name in self._host._connectors:
            raise ValueError(f"Connector '{connector_name}' already exists.")

        connector_type = _normalize_connector_type(normalized_request.type)
        plugin = self._host._resolve_connector_plugin_for_type(connector_type)
        connection_payload = _normalize_connection_payload(
            connector_type=connector_type,
            connection_payload=dict(normalized_request.connection or {}),
        )
        metadata_payload = dict(normalized_request.metadata or {})
        merged_connection = {**connection_payload, **metadata_payload}
        connection_metadata = _extract_connection_metadata(merged_connection)

        try:
            secret_references = {
                str(key): SecretReference.model_validate(value)
                for key, value in dict(normalized_request.secrets or {}).items()
            }
        except Exception as exc:
            raise ValueError(f"Connector '{connector_name}' defines invalid secret references.") from exc

        try:
            connection_policy = (
                ConnectionPolicy.model_validate(normalized_request.policy)
                if isinstance(normalized_request.policy, Mapping)
                else None
            )
        except Exception as exc:
            raise ValueError(f"Connector '{connector_name}' defines an invalid connection policy.") from exc

        capabilities = resolve_connector_capabilities(
            configured_capabilities=normalized_request.capabilities,
            connector_type=connector_type,
            plugin=plugin,
        )

        try:
            runtime_payload = build_connector_runtime_payload(
                config_json={"config": connection_payload},
                connection_metadata=(
                    connection_metadata.model_dump(mode="json", by_alias=True)
                    if connection_metadata is not None
                    else None
                ),
                secret_references={
                    key: value.model_dump(mode="json")
                    for key, value in secret_references.items()
                },
                secret_resolver=self._host._secret_provider_registry.resolve,
            )
            connector_runtime_type = ConnectorRuntimeType(connector_type)
            config_factory = get_connector_config_factory(connector_runtime_type)
            config_factory.create(runtime_payload.get("config") or {})
        except Exception as exc:
            raise ValueError(
                f"Connector '{connector_name}' failed validation for connector type '{connector_type}'."
            ) from exc

        connector = ConnectorMetadata(
            id=uuid.uuid4(),
            name=connector_name,
            description=normalized_request.description,
            connector_type=connector_type,
            connector_family=(
                plugin.connector_family.value.lower()
                if plugin is not None
                else ("file" if connector_type == "FILE" else None)
            ),
            workspace_id=self._host.context.workspace_id,
            config={"config": connection_payload},
            connection_metadata=connection_metadata,
            secret_references=secret_references,
            connection_policy=connection_policy,
            supported_resources=list(plugin.supported_resources) if plugin is not None else [],
            sync_strategy=(
                plugin.sync_strategy.value
                if plugin is not None and plugin.sync_strategy is not None
                else None
            ),
            capabilities=capabilities,
            is_managed=False,
            management_mode=ManagementMode.RUNTIME_MANAGED,
            lifecycle_state=LifecycleState.ACTIVE,
        )

        async with self._host._runtime_operation_scope() as uow:
            if uow is not None:
                uow.repository("connector_repository").add(to_connector_record(connector))
                await uow.commit()

        self._host._upsert_runtime_connector(connector)
        return self._serialize_connector(connector)

    async def list_sync_resources(
        self,
        *,
        connector_name: str,
    ) -> list[dict[str, Any]]:
        connector = self._host._resolve_connector(connector_name)
        api_connector = self._host._build_api_connector(connector)
        await api_connector.test_connection()
        async with self._host._runtime_operation_scope():
            states = await self._host._connector_sync_state_repository.list_for_connection(
                workspace_id=self._host.context.workspace_id,
                connection_id=connector.id,
            )
            states_by_resource = {
                str(state.resource_name or "").strip(): state
                for state in states
            }
            datasets = await self._host._dataset_repository.list_for_connection(
                workspace_id=self._host.context.workspace_id,
                connection_id=connector.id,
                limit=1000,
            )
            dataset_bindings = self._host._datasets_for_resources(datasets)

        items: list[dict[str, Any]] = []
        for resource in await api_connector.discover_resources():
            state = states_by_resource.get(resource.name)
            bound_datasets = dataset_bindings.get(resource.name, [])
            items.append(
                {
                    "name": resource.name,
                    "label": resource.label,
                    "primary_key": resource.primary_key,
                    "parent_resource": resource.parent_resource,
                    "cursor_field": resource.cursor_field,
                    "incremental_cursor_field": resource.incremental_cursor_field,
                    "supports_incremental": bool(resource.supports_incremental),
                    "default_sync_mode": str(resource.default_sync_mode or "FULL_REFRESH"),
                    "status": str(state.status) if state is not None else "never_synced",
                    "last_cursor": state.last_cursor if state is not None else None,
                    "last_sync_at": state.last_sync_at if state is not None else None,
                    "dataset_ids": [dataset.id for dataset in bound_datasets],
                    "dataset_names": [dataset.name for dataset in bound_datasets],
                    "records_synced": int(state.records_synced or 0) if state is not None else 0,
                    "bytes_synced": state.bytes_synced if state is not None else None,
                }
            )
        return items

    async def list_sync_states(
        self,
        *,
        connector_name: str,
    ) -> list[dict[str, Any]]:
        connector = self._host._resolve_connector(connector_name)
        async with self._host._runtime_operation_scope():
            states = await self._host._connector_sync_state_repository.list_for_connection(
                workspace_id=self._host.context.workspace_id,
                connection_id=connector.id,
            )
            datasets = await self._host._dataset_repository.list_for_connection(
                workspace_id=self._host.context.workspace_id,
                connection_id=connector.id,
                limit=1000,
            )
            dataset_bindings = self._host._datasets_for_resources(datasets)
            return [
                {
                    "id": state.id,
                    "workspace_id": state.workspace_id,
                    "connection_id": state.connection_id,
                    "connector_name": connector.name,
                    "connector_type": state.connector_type,
                    "resource_name": state.resource_name,
                    "sync_mode": state.sync_mode,
                    "last_cursor": state.last_cursor,
                    "last_sync_at": state.last_sync_at,
                    "state": dict(state.state_json or {}),
                    "status": state.status,
                    "error_message": state.error_message,
                    "records_synced": int(state.records_synced or 0),
                    "bytes_synced": state.bytes_synced,
                    "dataset_ids": [dataset.id for dataset in dataset_bindings.get(state.resource_name, [])],
                    "dataset_names": [dataset.name for dataset in dataset_bindings.get(state.resource_name, [])],
                    "created_at": state.created_at,
                    "updated_at": state.updated_at,
                }
                for state in states
            ]

    async def sync_connector_resources(
        self,
        *,
        connector_name: str,
        resources: list[str],
        sync_mode: str = "INCREMENTAL",
        force_full_refresh: bool = False,
    ) -> dict[str, Any]:
        if self._host.services.dataset_sync is None:
            raise RuntimeError("Dataset sync is not configured for this runtime host.")

        connector = self._host._resolve_connector(connector_name)
        connector_type = self._host._resolve_connector_runtime_type(connector)
        api_connector = self._host._build_api_connector(connector)
        await api_connector.test_connection()

        discovered_resources = {resource.name: resource for resource in await api_connector.discover_resources()}
        normalized_resources = [
            str(resource or "").strip()
            for resource in (resources or [])
            if str(resource or "").strip()
        ]
        if not normalized_resources:
            raise ValueError("At least one resource must be supplied for connector sync.")
        unknown_resources = [
            resource_name
            for resource_name in normalized_resources
            if resource_name not in discovered_resources
        ]
        if unknown_resources:
            raise ValueError(
                f"Unsupported resource(s) requested for sync: {', '.join(sorted(unknown_resources))}."
            )

        normalized_sync_mode = self._host._normalize_sync_mode(sync_mode)
        summaries: list[dict[str, Any]] = []
        active_state: ConnectorSyncState | None = None
        try:
            async with self._host._runtime_operation_scope() as uow:
                for resource_name in normalized_resources:
                    active_state = await self._host.services.dataset_sync.get_or_create_state(
                        workspace_id=self._host.context.workspace_id,
                        connection_id=connector.id,
                        connector_type=connector_type,
                        resource_name=resource_name,
                        sync_mode=normalized_sync_mode,
                    )
                    active_state.status = "running"
                    active_state.sync_mode = normalized_sync_mode
                    active_state.error_message = None
                    active_state.updated_at = datetime.now(timezone.utc)
                    summary = await self._host._runtime_host.sync_dataset(
                        workspace_id=self._host.context.workspace_id,
                        actor_id=self._host.context.actor_id,
                        connection_id=connector.id,
                        connector_record=connector,
                        connector_type=connector_type,
                        resource=discovered_resources[resource_name],
                        api_connector=api_connector,
                        state=active_state,
                        sync_mode=("FULL_REFRESH" if force_full_refresh else normalized_sync_mode),
                    )
                    summaries.append(summary)
                if uow is not None:
                    await uow.commit()
        except Exception as exc:
            if active_state is not None:
                async with self._host._runtime_operation_scope() as failure_uow:
                    await self._host.services.dataset_sync.mark_failed(
                        state=active_state,
                        error_message=str(exc),
                    )
                    if failure_uow is not None:
                        await failure_uow.commit()
            raise

        return {
            "status": "succeeded",
            "connector_id": connector.id,
            "connector_name": connector.name,
            "sync_mode": "FULL_REFRESH" if force_full_refresh else normalized_sync_mode,
            "resources": summaries,
            "summary": f"Connector sync completed for {len(summaries)} resource(s).",
        }
