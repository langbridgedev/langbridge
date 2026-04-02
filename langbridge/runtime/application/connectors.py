import inspect
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Mapping, Set

from langbridge.connectors.base import (
    ApiResource,
    ConnectorPluginMetadata,
    ConnectorRuntimeType,
    get_connector_config_factory,
    get_connector_config_schema_factory,
)
from langbridge.runtime.application.errors import ApplicationError, BusinessValidationError
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
from langbridge.runtime.models.state import ConnectorSyncMode, ConnectorSyncStatus
from langbridge.runtime.persistence.mappers.connectors import to_connector_record
from langbridge.runtime.utils.connector_runtime import (
    build_connector_runtime_payload,
    resolve_connector_capabilities,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost

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


def _extract_connection_metadata(payload: Mapping[str, Any], known_keys: Set[str] | None = None) -> ConnectionMetadata | None:
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
            "connector_type": connector.connector_type_value,
            "connector_family": connector.connector_family_value,
            "supports_sync": self._host._connector_supports_sync(connector),
            "supported_resources": list(connector.supported_resources or []),
            "default_sync_strategy": connector.default_sync_strategy_value,
            "capabilities": capabilities.model_dump(mode="json"),
            "management_mode": management_mode,
            "managed": management_mode == ManagementMode.CONFIG_MANAGED.value,
        }

    def _serialize_connector_detail(self, connector: ConnectorMetadata) -> dict[str, Any]:
        return {
            **self._serialize_connector(connector),
            "connection": dict(((connector.config or {}).get("config")) or {}),
            "metadata": (
                None
                if connector.connection_metadata is None
                else connector.connection_metadata.model_dump(mode="json", by_alias=True)
            ),
            "secrets": {
                str(key): value.model_dump(mode="json")
                for key, value in dict(connector.secret_references or {}).items()
            },
            "policy": (
                None
                if connector.connection_policy is None
                else connector.connection_policy.model_dump(mode="json")
            ),
        }

    @staticmethod
    def _require_runtime_managed_connector(connector: ConnectorMetadata) -> None:
        management_mode = str(getattr(connector.management_mode, "value", connector.management_mode)).lower()
        if management_mode != ManagementMode.RUNTIME_MANAGED.value:
            raise BusinessValidationError(
                f"Connector '{connector.name}' is config_managed and read-only in the runtime UI."
            )

    async def _resolve_api_resource(
        self,
        *,
        connector: ConnectorMetadata,
        api_connector: Any,
        resource_name: str,
        discovered_resources: dict[str, ApiResource],
        allow_placeholder: bool = False,
    ) -> ApiResource:
        normalized_name = str(resource_name or "").strip()
        if not normalized_name:
            raise BusinessValidationError("Connector resource name is required.")

        resource = discovered_resources.get(normalized_name)
        if resource is not None:
            return resource

        resolver = getattr(api_connector, "resolve_resource", None)
        if callable(resolver):
            try:
                resolved_resource = resolver(normalized_name)
                if inspect.isawaitable(resolved_resource):
                    resolved_resource = await resolved_resource
                if isinstance(resolved_resource, ApiResource):
                    discovered_resources[resolved_resource.name] = resolved_resource
                    return resolved_resource
            except Exception as exc:
                if not allow_placeholder:
                    raise BusinessValidationError(
                        f"Connector '{connector.name}' could not resolve resource '{normalized_name}'."
                    ) from exc
        elif not allow_placeholder:
            raise BusinessValidationError(
                f"Connector '{connector.name}' does not expose resource '{normalized_name}'."
            )

        placeholder_resource = ApiResource(
            name=normalized_name,
            label=normalized_name,
        )
        discovered_resources[placeholder_resource.name] = placeholder_resource
        return placeholder_resource

    async def list_connectors(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for connector in self._host._connectors.values():
            items.append(self._serialize_connector(connector))
        return items
    
    async def list_connector_types(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for plugin in self._host._get_connector_plugins():
            capabilities_schema = plugin.capabilities if plugin is not None else {}
            items.append(
                {
                    "name": plugin.connector_type,
                    "label": plugin.connector_type,
                    "description": plugin.connector_type,
                    "family": plugin.connector_family,
                    "supports_sync": plugin.api_connector_class is not None,
                    "supported_resources": list(plugin.supported_resources),
                    "default_sync_strategy": plugin.default_sync_strategy,
                    "capabilities_schema": capabilities_schema.model_dump(mode="json"),
                }
            )
        return items

    async def get_connector_type_config(self, *, connector_type: str) -> dict[str, Any]:
        normalized_type = str(connector_type or "").strip().upper()
        if not normalized_type:
            raise BusinessValidationError("Connector type is required.")

        try:
            runtime_type = ConnectorRuntimeType(normalized_type)
            schema_factory = get_connector_config_schema_factory(runtime_type)
            schema = schema_factory.create({})
        except ValueError as exc:
            raise BusinessValidationError(str(exc)) from exc

        plugin = self._host._resolve_connector_plugin_for_type(runtime_type.value)
        plugin_metadata = schema.plugin_metadata
        if plugin_metadata is None and plugin is not None:
            plugin_metadata = ConnectorPluginMetadata(
                connector_type=runtime_type,
                connector_family=plugin.connector_family,
                supported_resources=list(plugin.supported_resources),
                auth_schema=list(plugin.auth_schema),
                default_sync_strategy=plugin.default_sync_strategy,
                capabilities=plugin.capabilities,
            )

        return {
            "connector_type": runtime_type,
            "name": schema.name,
            "description": schema.description,
            "version": schema.version,
            "config": [entry.model_dump(mode="json") for entry in schema.config],
            "plugin_metadata": (
                plugin_metadata.model_dump(mode="json") if plugin_metadata is not None else None
            ),
        }


    async def get_connector(self, *, connector_name: str) -> dict[str, Any]:
        connector = self._host._resolve_connector(connector_name)
        return self._serialize_connector_detail(connector)

    async def create_connector(self, *, request) -> dict[str, Any]:
        normalized_request = LocalRuntimeConnectorConfig.model_validate(
            request.model_dump(mode="json")
        )
        connector_name = str(normalized_request.name or "").strip()
        if not connector_name:
            raise BusinessValidationError("Connector name is required.")
        if connector_name in self._host._connectors:
            raise BusinessValidationError(f"Connector '{connector_name}' already exists.")

        connector_type = ConnectorRuntimeType(
            str(getattr(normalized_request.type, "value", normalized_request.type)).strip().upper()
        )
        plugin = self._host._resolve_connector_plugin_for_type(connector_type.value)
        connection_payload = _normalize_connection_payload(
            connector_type=connector_type.value,
            connection_payload=dict(normalized_request.connection or {}),
        )
        metadata_payload = dict(normalized_request.metadata or {})
        merged_connection = {**connection_payload, **metadata_payload}

        try:
            secret_references = {
                str(key): SecretReference.model_validate(value)
                for key, value in dict(normalized_request.secrets or {}).items()
            }
        except Exception as exc:
            raise ApplicationError(f"Connector '{connector_name}' defines invalid secret references.") from exc

        try:
            connection_policy = (
                ConnectionPolicy.model_validate(normalized_request.policy)
                if isinstance(normalized_request.policy, Mapping)
                else None
            )
        except Exception as exc:
            raise ApplicationError(f"Connector '{connector_name}' defines an invalid connection policy.") from exc

        capabilities = resolve_connector_capabilities(
            configured_capabilities=normalized_request.capabilities,
            connector_type=connector_type.value,
            plugin=plugin,
        )

        try:
            config_factory = get_connector_config_factory(connector_type)
            metadata_keys = config_factory.get_metadata_keys()
            connection_metadata = _extract_connection_metadata(merged_connection, known_keys=metadata_keys)
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
            config_factory.create(runtime_payload.get("config") or {})
        except Exception as exc:
            raise ApplicationError(
                f"Connector '{connector_name}' failed validation for connector type '{connector_type}'."
            ) from exc

        connector = ConnectorMetadata(
            id=uuid.uuid4(),
            name=connector_name,
            description=normalized_request.description,
            connector_type=connector_type,
            connector_family=(
                plugin.connector_family
                if plugin is not None
                else None
            ),
            workspace_id=self._host.context.workspace_id,
            config={"config": connection_payload},
            connection_metadata=connection_metadata,
            secret_references=secret_references,
            connection_policy=connection_policy,
            supported_resources=list(plugin.supported_resources) if plugin is not None else [],
            default_sync_strategy=(
                plugin.default_sync_strategy
                if plugin is not None and plugin.default_sync_strategy is not None
                else None
            ),
            capabilities=capabilities,
            is_managed=False,
            created_by=self._host.context.actor_id,
            updated_by=self._host.context.actor_id,
            management_mode=ManagementMode.RUNTIME_MANAGED,
            lifecycle_state=LifecycleState.ACTIVE,
        )

        async with self._host._runtime_operation_scope() as uow:
            if uow is not None:
                uow.repository("connector_repository").add(to_connector_record(connector))
                await uow.commit()

        self._host._upsert_runtime_connector(connector)
        return self._serialize_connector(connector)

    async def update_connector(self, *, connector_name: str, request) -> dict[str, Any]:
        connector = self._host._resolve_connector(connector_name)
        self._require_runtime_managed_connector(connector)

        fields_set = set(getattr(request, "model_fields_set", set()))
        connector_type = ConnectorRuntimeType(str(connector.connector_type_value or "").strip().upper())
        plugin = self._host._resolve_connector_plugin_for_type(connector_type.value)

        current_connection = dict(((connector.config or {}).get("config")) or {})
        current_metadata = (
            {}
            if connector.connection_metadata is None
            else connector.connection_metadata.model_dump(mode="json", by_alias=True)
        )
        current_secrets = {
            str(key): value.model_dump(mode="json")
            for key, value in dict(connector.secret_references or {}).items()
        }
        current_policy = (
            None
            if connector.connection_policy is None
            else connector.connection_policy.model_dump(mode="json")
        )

        connection_payload = (
            _normalize_connection_payload(
                connector_type=connector_type.value,
                connection_payload=dict(request.connection or {}),
            )
            if "connection" in fields_set
            else current_connection
        )
        metadata_payload = dict(request.metadata or {}) if "metadata" in fields_set else current_metadata
        secrets_payload = dict(request.secrets or {}) if "secrets" in fields_set else current_secrets
        policy_payload = request.policy if "policy" in fields_set else current_policy
        description = request.description if "description" in fields_set else connector.description
        capabilities_input = request.capabilities if "capabilities" in fields_set else connector.capabilities
        merged_connection = {**connection_payload, **metadata_payload}

        try:
            secret_references = {
                str(key): SecretReference.model_validate(value)
                for key, value in dict(secrets_payload or {}).items()
            }
        except Exception as exc:
            raise ApplicationError(f"Connector '{connector.name}' defines invalid secret references.") from exc

        try:
            connection_policy = (
                ConnectionPolicy.model_validate(policy_payload)
                if isinstance(policy_payload, Mapping)
                else None
            )
        except Exception as exc:
            raise ApplicationError(f"Connector '{connector.name}' defines an invalid connection policy.") from exc

        capabilities = resolve_connector_capabilities(
            configured_capabilities=capabilities_input,
            connector_type=connector_type.value,
            plugin=plugin,
        )

        try:
            config_factory = get_connector_config_factory(connector_type)
            metadata_keys = config_factory.get_metadata_keys()
            connection_metadata = _extract_connection_metadata(
                merged_connection,
                known_keys=metadata_keys,
            )
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
            config_factory.create(runtime_payload.get("config") or {})
        except Exception as exc:
            raise ApplicationError(
                f"Connector '{connector.name}' failed validation for connector type '{connector_type}'."
            ) from exc

        updated_connector = connector.model_copy(
            update={
                "description": (str(description).strip() or None) if description is not None else None,
                "config": {"config": connection_payload},
                "connection_metadata": connection_metadata,
                "secret_references": secret_references,
                "connection_policy": connection_policy,
                "capabilities": capabilities,
                "updated_by": self._host.context.actor_id,
            }
        )

        async with self._host._runtime_operation_scope() as uow:
            if uow is None:
                raise ApplicationError("Runtime connector updates require persistence support.")
            repository = uow.repository("connector_repository")
            record = await repository.get_by_id_for_workspace(
                connector_id=connector.id,
                workspace_id=self._host.context.workspace_id,
            )
            if record is None:
                raise ValueError(f"Connector '{connector.name}' was not found.")
            await repository.save(to_connector_record(updated_connector))
            await uow.commit()

        self._host._upsert_runtime_connector(updated_connector)
        return self._serialize_connector_detail(updated_connector)

    async def delete_connector(self, *, connector_name: str) -> dict[str, Any]:
        connector = self._host._resolve_connector(connector_name)
        self._require_runtime_managed_connector(connector)

        async with self._host._runtime_operation_scope() as uow:
            if uow is None:
                raise ApplicationError("Runtime connector deletes require persistence support.")
            bound_datasets = await self._host._dataset_repository.list_for_connection(
                workspace_id=self._host.context.workspace_id,
                connection_id=connector.id,
                limit=1,
            )
            if bound_datasets:
                raise BusinessValidationError(
                    f"Connector '{connector.name}' cannot be deleted while datasets still reference it."
                )
            repository = uow.repository("connector_repository")
            record = await repository.get_by_id_for_workspace(
                connector_id=connector.id,
                workspace_id=self._host.context.workspace_id,
            )
            if record is None:
                raise ValueError(f"Connector '{connector.name}' was not found.")
            await repository.delete(record)
            await uow.commit()

        self._host._remove_runtime_connector(
            connector_name=connector.name,
            connector_id=connector.id,
        )
        return {"ok": True, "deleted": True, "id": connector.id, "name": connector.name}

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

        discovered_resources = {
            resource.name: resource
            for resource in await api_connector.discover_resources()
        }
        ordered_resource_names = list(discovered_resources)
        for resource_name in sorted(
            {
                *states_by_resource.keys(),
                *dataset_bindings.keys(),
            }
            - set(discovered_resources)
        ):
            ordered_resource_names.append(resource_name)

        items: list[dict[str, Any]] = []
        for resource_name in ordered_resource_names:
            resource = await self._resolve_api_resource(
                connector=connector,
                api_connector=api_connector,
                resource_name=resource_name,
                discovered_resources=discovered_resources,
                allow_placeholder=True,
            )
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
                    "status": (
                        state.status_value
                        if state is not None
                        else ConnectorSyncStatus.NEVER_SYNCED.value
                    ),
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
                    "connector_type": state.connector_type_value,
                    "resource_name": state.resource_name,
                    "sync_mode": state.sync_mode_value,
                    "last_cursor": state.last_cursor,
                    "last_sync_at": state.last_sync_at,
                    "state": dict(state.state_json or {}),
                    "status": state.status_value,
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
        connector = self._host._resolve_connector(connector_name)
        connector_type = self._host._resolve_connector_runtime_type(connector)
        api_connector = self._host._build_api_connector(connector)
        await api_connector.test_connection()

        discovered_resources = {
            resource.name: resource
            for resource in await api_connector.discover_resources()
        }
        normalized_resources = [
            str(resource or "").strip()
            for resource in (resources or [])
            if str(resource or "").strip()
        ]
        if not normalized_resources:
            raise BusinessValidationError("At least one resource must be supplied for connector sync.")

        resolved_resources: dict[str, ApiResource] = {}
        for resource_name in normalized_resources:
            resolved_resources[resource_name] = await self._resolve_api_resource(
                connector=connector,
                api_connector=api_connector,
                resource_name=resource_name,
                discovered_resources=discovered_resources,
            )

        normalized_sync_mode: ConnectorSyncMode = sync_mode
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
                    active_state.status = ConnectorSyncStatus.RUNNING
                    active_state.sync_mode = normalized_sync_mode
                    active_state.error_message = None
                    active_state.updated_at = datetime.now(timezone.utc)
                    summary = await self._host._runtime_host.sync_dataset(
                        workspace_id=self._host.context.workspace_id,
                        actor_id=self._host.context.actor_id,
                        connection_id=connector.id,
                        connector_record=connector,
                        connector_type=connector_type,
                        resource=resolved_resources[resource_name],
                        api_connector=api_connector,
                        state=active_state,
                        sync_mode=(ConnectorSyncMode.FULL_REFRESH if force_full_refresh else normalized_sync_mode),
                        flattern_into_datasets=False # TODO: add flattern_into_datasets as a parameter to this method and pass it down to the sync_dataset call
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
            "sync_mode": (
                ConnectorSyncMode.FULL_REFRESH.value
                if force_full_refresh
                else normalized_sync_mode.value
            ),
            "resources": summaries,
            "summary": f"Connector sync completed for {len(summaries)} resource(s).",
        }
