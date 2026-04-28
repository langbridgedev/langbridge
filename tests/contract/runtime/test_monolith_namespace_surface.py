
from importlib import import_module

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "langbridge",
        "langbridge.client",
        "langbridge.runtime",
        "langbridge.federation",
        "langbridge.semantic",
        "langbridge.plugins",
        "langbridge.hosting",
    ],
)
def test_monolith_public_namespaces_are_importable(module_name: str) -> None:
    import_module(module_name)


def test_root_namespace_exposes_sdk_entrypoint() -> None:
    root = import_module("langbridge")

    assert root.LangbridgeClient.__name__ == "LangbridgeClient"


def test_plugin_namespace_exposes_lightweight_registry_symbols() -> None:
    plugins = import_module("langbridge.plugins")

    assert callable(plugins.register_connector_plugin)
    assert plugins.ConnectorRuntimeType.SHOPIFY.value == "SHOPIFY"
