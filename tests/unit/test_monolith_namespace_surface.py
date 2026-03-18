from __future__ import annotations

from importlib import import_module

from langbridge.plugins import (
    ConnectorRuntimeType,
    get_connector_plugin,
    register_connector_plugin,
)
from langbridge.packages.connectors.langbridge_connectors.api.registry import (
    register_connector_plugin as legacy_register_connector_plugin,
)


def test_root_monolith_namespaces_are_importable() -> None:
    assert hasattr(import_module("langbridge.contracts"), "__getattr__")
    assert hasattr(import_module("langbridge.runtime"), "build_configured_local_runtime")
    assert hasattr(import_module("langbridge.hosting"), "create_runtime_api_app")
    assert hasattr(import_module("langbridge.federation"), "FederatedQueryService")
    assert hasattr(import_module("langbridge.semantic"), "SemanticModel")
    assert hasattr(import_module("langbridge.orchestrator"), "AgentOrchestratorFactory")
    assert hasattr(import_module("langbridge.plugins"), "register_connector_plugin")


def test_plugins_namespace_shares_the_existing_connector_registry() -> None:
    assert register_connector_plugin is legacy_register_connector_plugin

    plugin = get_connector_plugin(ConnectorRuntimeType.SHOPIFY)

    assert plugin is not None
    assert plugin.connector_type == ConnectorRuntimeType.SHOPIFY
