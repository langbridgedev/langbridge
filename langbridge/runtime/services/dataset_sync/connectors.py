import logging
from typing import Any

from langbridge.connectors.base import (
    ApiConnectorFactory,
    SqlConnectorFactory,
    get_connector_config_factory,
)
from langbridge.connectors.base.connector import ApiConnector, ApiResource
from langbridge.runtime.models import DatasetMetadata
from langbridge.runtime.models.metadata import ConnectorMetadata
from langbridge.runtime.security import SecretProviderRegistry
from langbridge.runtime.utils.connector_runtime import build_connector_runtime_payload


class DatasetSyncConnectorFactory:
    """Builds configured connector instances for dataset sync execution."""

    def __init__(
        self,
        *,
        secret_provider_registry: SecretProviderRegistry,
        api_connector_factory: ApiConnectorFactory | None = None,
        sql_connector_factory: SqlConnectorFactory | None = None,
        logger_name: str = "langbridge.runtime.sync.dataset",
    ) -> None:
        self._secret_provider_registry = secret_provider_registry
        self._api_connector_factory = api_connector_factory or ApiConnectorFactory()
        self._sql_connector_factory = sql_connector_factory or SqlConnectorFactory()
        self._logger = logging.getLogger(logger_name)

    def build_api_connector(self, connector_record: ConnectorMetadata) -> ApiConnector:
        if connector_record.connector_type is None:
            raise ValueError(f"Connector '{connector_record.name}' is missing connector_type.")
        runtime_payload = self._runtime_payload(connector_record)
        config_factory = get_connector_config_factory(connector_record.connector_type)
        return self._api_connector_factory.create_api_connector(
            connector_record.connector_type,
            config_factory.create(runtime_payload.get("config") or {}),
            logger=self._logger,
        )

    def build_sql_connector(self, connector_record: ConnectorMetadata) -> Any:
        if connector_record.connector_type is None:
            raise ValueError(f"Connector '{connector_record.name}' is missing connector_type.")
        runtime_payload = self._runtime_payload(connector_record)
        config_factory = get_connector_config_factory(connector_record.connector_type)
        return self._sql_connector_factory.create_sql_connector(
            connector_record.connector_type,
            config_factory.create(runtime_payload.get("config") or {}),
            logger=self._logger,
        )

    async def resolve_api_root_resource(
        self,
        *,
        dataset: DatasetMetadata,
        connector: ConnectorMetadata,
        api_connector: ApiConnector,
        resource_name: str,
    ) -> ApiResource:
        discovered_resources = {
            resource.name: resource for resource in await api_connector.discover_resources()
        }
        resource = discovered_resources.get(resource_name)
        if resource is not None:
            return resource

        resolver = getattr(api_connector, "resolve_resource", None)
        if callable(resolver):
            resolved_resource = resolver(resource_name)
            if hasattr(resolved_resource, "__await__"):
                resolved_resource = await resolved_resource
            if isinstance(resolved_resource, ApiResource):
                return resolved_resource

        raise ValueError(
            f"Dataset '{dataset.name}' is bound to connector '{connector.name}', "
            f"but connector resource '{resource_name}' was not found."
        )

    def _runtime_payload(self, connector_record: ConnectorMetadata) -> dict[str, Any]:
        return build_connector_runtime_payload(
            config_json=connector_record.config,
            connection_metadata=(
                connector_record.connection_metadata.model_dump(mode="json", by_alias=True)
                if connector_record.connection_metadata is not None
                else None
            ),
            secret_references={
                key: value.model_dump(mode="json")
                for key, value in (connector_record.secret_references or {}).items()
            },
            secret_resolver=self._secret_provider_registry.resolve,
        )
