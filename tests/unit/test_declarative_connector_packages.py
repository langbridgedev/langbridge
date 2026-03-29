
import importlib
import sys
from pathlib import Path

import pytest

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector
from langbridge.plugins import get_connector_plugin

CONNECTOR_CASES = (
    {
        "package_dir": "langbridge-connector-stripe",
        "module": "langbridge_connector_stripe",
        "manifest": "stripe.yaml",
        "connector_type": "STRIPE",
        "base_url": "https://api.stripe.com",
        "resources": ("customers", "charges", "invoices"),
        "auth_fields": ["api_key", "account_id"],
        "config_fields": ["api_key", "account_id", "api_base_url"],
        "entrypoint": 'stripe = "langbridge_connector_stripe:get_connector_plugin"',
        "example_resource_keys": ["customers", "invoices"],
    },
    {
        "package_dir": "langbridge-connector-shopify",
        "module": "langbridge_connector_shopify",
        "manifest": "shopify.yaml",
        "connector_type": "SHOPIFY",
        "base_url": "https://example.myshopify.com/admin/api/2026-01",
        "resources": ("customers", "draft_orders", "locations"),
        "auth_fields": ["shop_domain", "access_token"],
        "config_fields": ["shop_domain", "access_token", "api_base_url"],
        "entrypoint": 'shopify = "langbridge_connector_shopify:get_connector_plugin"',
        "example_resource_keys": ["customers", "locations"],
    },
    {
        "package_dir": "langbridge-connector-hubspot",
        "module": "langbridge_connector_hubspot",
        "manifest": "hubspot.yaml",
        "connector_type": "HUBSPOT",
        "base_url": "https://api.hubapi.com",
        "resources": ("contacts", "companies", "deals"),
        "auth_fields": ["access_token"],
        "config_fields": ["access_token", "api_base_url"],
        "entrypoint": 'hubspot = "langbridge_connector_hubspot:get_connector_plugin"',
        "example_resource_keys": ["contacts", "deals"],
    },
    {
        "package_dir": "langbridge-connector-github",
        "module": "langbridge_connector_github",
        "manifest": "github.yaml",
        "connector_type": "GITHUB",
        "base_url": "https://api.github.com",
        "resources": ("repositories", "issues", "notifications"),
        "auth_fields": ["access_token"],
        "config_fields": ["access_token", "api_base_url"],
        "entrypoint": 'github = "langbridge_connector_github:get_connector_plugin"',
        "example_resource_keys": ["issues", "repositories"],
    },
    {
        "package_dir": "langbridge-connector-jira",
        "module": "langbridge_connector_jira",
        "manifest": "jira.yaml",
        "connector_type": "JIRA",
        "base_url": "https://api.atlassian.com/ex/jira/example/rest/api/3",
        "resources": ("projects", "fields", "statuses"),
        "auth_fields": ["cloud_id", "access_token"],
        "config_fields": ["cloud_id", "access_token", "api_base_url"],
        "entrypoint": 'jira = "langbridge_connector_jira:get_connector_plugin"',
        "example_resource_keys": ["projects", "fields"],
    },
    {
        "package_dir": "langbridge-connector-asana",
        "module": "langbridge_connector_asana",
        "manifest": "asana.yaml",
        "connector_type": "ASANA",
        "base_url": "https://app.asana.com/api/1.0/workspaces/example",
        "resources": ("teams", "projects", "users"),
        "auth_fields": ["workspace_gid", "access_token"],
        "config_fields": ["workspace_gid", "access_token", "api_base_url"],
        "entrypoint": 'asana = "langbridge_connector_asana:get_connector_plugin"',
        "example_resource_keys": ["projects", "users"],
    },
)


def _package_paths(package_dir: str) -> tuple[Path, Path]:
    package_root = Path(__file__).resolve().parents[2] / "langbridge-connectors" / package_dir
    return package_root, package_root / "src"


def _import_package_module(package_src: Path, module_name: str):
    sys.path.insert(0, str(package_src))
    try:
        return importlib.import_module(module_name)
    finally:
        sys.path.pop(0)


@pytest.mark.parametrize("case", CONNECTOR_CASES, ids=[case["connector_type"] for case in CONNECTOR_CASES])
def test_manifest_plugin_examples_and_entrypoint_align(case: dict[str, object]) -> None:
    package_root, package_src = _package_paths(str(case["package_dir"]))
    module_name = str(case["module"])

    manifests_module = _import_package_module(package_src, f"{module_name}.manifests")
    assert manifests_module is not None

    manifest = importlib.import_module("langbridge.connectors.saas.declarative").load_declarative_connector_manifest(
        f"{module_name}.manifests",
        str(case["manifest"]),
    )
    plugin_module = _import_package_module(package_src, f"{module_name}.plugin")
    config_module = _import_package_module(package_src, f"{module_name}.config")
    examples_module = _import_package_module(package_src, f"{module_name}.examples")

    plugin = plugin_module.get_connector_plugin()
    assert plugin.config_schema_factory is not None
    schema = plugin.config_schema_factory.create({})
    payload = examples_module.load_dataset_examples()

    assert manifest.id == module_name.replace("langbridge_connector_", "")
    assert manifest.connector_type == case["connector_type"]
    assert manifest.base_url == case["base_url"]
    assert manifest.resource_keys == case["resources"]
    assert not (package_src / module_name / "manifest.py").exists()
    assert plugin.connector_type.value == case["connector_type"]
    assert plugin.supported_resources == case["resources"]
    assert issubclass(plugin.api_connector_class, DeclarativeHttpApiConnector)
    assert [field.field for field in plugin.auth_schema] == case["auth_fields"]
    assert [entry.field for entry in schema.config] == case["config_fields"]
    assert schema.plugin_metadata is not None
    assert schema.plugin_metadata.supported_resources == list(case["resources"])
    assert payload.connector.package == case["package_dir"]
    assert [example.connector_sync.resource_key for example in payload.examples] == case["example_resource_keys"]
    assert all(example.connector_sync.resource is None for example in payload.examples)
    pyproject = (package_root / "pyproject.toml").read_text(encoding="utf-8")
    assert '[project.entry-points."langbridge.connectors"]' in pyproject
    assert str(case["entrypoint"]) in pyproject


@pytest.mark.parametrize(
    ("runtime_type", "expected_module_prefix"),
    (
        (ConnectorRuntimeType.SHOPIFY, "langbridge_connector_shopify"),
        (ConnectorRuntimeType.HUBSPOT, "langbridge_connector_hubspot"),
        (ConnectorRuntimeType.GITHUB, "langbridge_connector_github"),
        (ConnectorRuntimeType.JIRA, "langbridge_connector_jira"),
        (ConnectorRuntimeType.ASANA, "langbridge_connector_asana"),
        (ConnectorRuntimeType.STRIPE, "langbridge_connector_stripe"),
    ),
)
def test_runtime_plugin_registry_resolves_repo_connector_packages(
    runtime_type: ConnectorRuntimeType,
    expected_module_prefix: str,
) -> None:
    plugin = get_connector_plugin(runtime_type)

    assert plugin is not None
    assert plugin.api_connector_class is not None
    assert plugin.api_connector_class.__module__.startswith(expected_module_prefix)
