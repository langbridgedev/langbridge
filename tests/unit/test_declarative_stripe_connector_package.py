from __future__ import annotations

import importlib
import sys
from pathlib import Path

PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "langbridge-connectors"
    / "langbridge-connector-stripe"
)
PACKAGE_SRC = PACKAGE_ROOT / "src"


def _import_package_module(module_name: str):
    sys.path.insert(0, str(PACKAGE_SRC))
    try:
        return importlib.import_module(module_name)
    finally:
        sys.path.pop(0)


def test_stripe_manifest_loads_expected_shape():
    _import_package_module("langbridge_connector_stripe.manifests")
    core_module = importlib.import_module("langbridge.connectors.saas.declarative")

    manifest = core_module.load_declarative_connector_manifest(
        "langbridge_connector_stripe.manifests",
        "stripe.yaml",
    )

    assert manifest.id == "stripe"
    assert manifest.connector_type == "STRIPE"
    assert manifest.base_url == "https://api.stripe.com"
    assert manifest.test_connection_path == "/v1/account"
    assert manifest.auth.strategy == "bearer_token"
    assert manifest.pagination.cursor_param == "starting_after"
    assert manifest.incremental.request_param == "created[gte]"
    assert manifest.resource_keys == ("customers", "charges", "invoices")


def test_package_defers_manifest_contract_to_core_namespace():
    package_manifest_module = PACKAGE_SRC / "langbridge_connector_stripe" / "manifest.py"

    assert not package_manifest_module.exists()


def test_plugin_maps_manifest_to_existing_langbridge_plugin_surface():
    plugin_module = _import_package_module("langbridge_connector_stripe.plugin")
    config_module = _import_package_module("langbridge_connector_stripe.config")
    connector_module = _import_package_module("langbridge_connector_stripe.connector")

    plugin = plugin_module.get_connector_plugin()
    schema = config_module.StripeDeclarativeConnectorConfigSchemaFactory.create({})

    assert plugin.connector_type.value == "STRIPE"
    assert plugin.connector_family.value == "API"
    assert plugin.supported_resources == ("customers", "charges", "invoices")
    assert plugin.sync_strategy.value == "INCREMENTAL"
    assert plugin.api_connector_class is connector_module.StripeDeclarativeApiConnector
    assert [field.field for field in plugin.auth_schema] == ["api_key", "account_id"]
    assert [entry.field for entry in schema.config] == ["api_key", "account_id", "api_base_url"]
    assert schema.plugin_metadata is not None
    assert schema.plugin_metadata.supported_resources == ["customers", "charges", "invoices"]


def test_register_plugin_returns_registered_plugin():
    plugin_module = _import_package_module("langbridge_connector_stripe.plugin")

    plugin = plugin_module.register_plugin()

    assert plugin is plugin_module.PLUGIN


def test_dataset_examples_validate_manifest_resource_shapes():
    examples_module = _import_package_module("langbridge_connector_stripe.examples")

    payload = examples_module.load_dataset_examples()

    assert payload.connector.package == "langbridge-connector-stripe"
    assert payload.examples[0].connector_sync.resource_key == "customers"
    assert payload.examples[0].connector_sync.resource is None
    assert payload.examples[1].connector_sync.resource_key == "invoices"
    assert payload.examples[1].connector_sync.resource is None


def test_package_pyproject_declares_langbridge_connector_entrypoint():
    pyproject = (PACKAGE_ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert '[project.entry-points."langbridge.connectors"]' in pyproject
    assert 'stripe = "langbridge_connector_stripe:get_connector_plugin"' in pyproject
