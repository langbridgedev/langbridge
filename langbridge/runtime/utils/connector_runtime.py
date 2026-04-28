
import json
from typing import Any, Callable

from langbridge.connectors.base.config import ConnectorCapabilities as PluginConnectorCapabilities
from langbridge.plugins.connectors import ConnectorPlugin
from langbridge.runtime.models import ConnectorCapabilities
from langbridge.runtime.models import SecretReference

SecretResolver = Callable[[SecretReference], str]


def parse_connector_payload(raw_config: Any) -> dict[str, Any]:
    if isinstance(raw_config, dict):
        return dict(raw_config)
    if isinstance(raw_config, (str, bytes)):
        try:
            parsed = json.loads(raw_config)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def build_connector_runtime_payload(
    *,
    config_json: Any,
    connection_metadata: Any | None = None,
    secret_references: Any | None = None,
    secret_resolver: SecretResolver | None = None,
) -> dict[str, Any]:
    resolved_payload = parse_connector_payload(config_json)
    runtime_config = dict(resolved_payload.get("config") or {})

    if isinstance(connection_metadata, dict):
        metadata = dict(connection_metadata)
        extra = metadata.pop("extra", {})
        for key, value in metadata.items():
            if value is not None:
                runtime_config.setdefault(key, value)
        if isinstance(extra, dict):
            for key, value in extra.items():
                if value is not None:
                    runtime_config.setdefault(key, value)

    if isinstance(secret_references, dict):
        if secret_resolver is None and secret_references:
            raise ValueError("A secret resolver is required when secret references are configured.")
        for secret_name, raw_reference in secret_references.items():
            if secret_resolver is None:
                continue
            reference = (
                raw_reference
                if isinstance(raw_reference, SecretReference)
                else SecretReference.model_validate(raw_reference)
            )
            runtime_config[secret_name] = secret_resolver(reference)

    resolved_payload["config"] = runtime_config
    return resolved_payload


def resolve_connector_capabilities(
    *,
    configured_capabilities: Any | None,
    connector_type: str | None,
    plugin: ConnectorPlugin | None,
) -> ConnectorCapabilities:
    base = _default_connector_capabilities(
        connector_type=connector_type,
        plugin=plugin,
    )
    if configured_capabilities is None:
        return base

    configured = _coerce_connector_capabilities(configured_capabilities)
    return base.model_copy(
        update=configured.model_dump(mode="json", exclude_unset=True)
    )


def _default_connector_capabilities(
    *,
    connector_type: str | None,
    plugin: ConnectorPlugin | None,
) -> ConnectorCapabilities:
    if plugin is not None:
        return _coerce_connector_capabilities(plugin.capabilities)

    normalized_type = str(connector_type or "").strip().upper()
    if normalized_type == "LOCAL_FILESYSTEM":
        return ConnectorCapabilities(
            supports_live_datasets=True,
            supports_synced_datasets=False,
            supports_incremental_sync=False,
            supports_query_pushdown=False,
            supports_preview=True,
            supports_federated_execution=True,
        )
    return ConnectorCapabilities()


def _coerce_connector_capabilities(value: Any) -> ConnectorCapabilities:
    if isinstance(value, ConnectorCapabilities):
        return value
    if isinstance(value, PluginConnectorCapabilities):
        return ConnectorCapabilities.model_validate(value.model_dump(mode="json"))
    return ConnectorCapabilities.model_validate(value or {})


def resolve_supported_resources(
    *,
    plugin: ConnectorPlugin | None,
    connector_config: Any | None,
) -> list[str]:
    if plugin is not None and plugin.supported_resources:
        return list(plugin.supported_resources)

    raw_resources = getattr(connector_config, "resources", None)
    if raw_resources is None and isinstance(connector_config, dict):
        raw_resources = connector_config.get("resources")
    if isinstance(raw_resources, str):
        try:
            raw_resources = json.loads(raw_resources)
        except Exception:
            return []
    if not isinstance(raw_resources, list):
        return []

    items: list[str] = []
    seen: set[str] = set()
    for resource in raw_resources:
        key = ""
        if isinstance(resource, dict):
            key = str(resource.get("key", "") or "").strip()
        else:
            key = str(getattr(resource, "key", "") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        items.append(key)
    return items
