from langbridge.connectors.base.config import ConnectorFamily, ConnectorRuntimeType
from langbridge.plugins.connectors import ConnectorPlugin, ConnectorPluginRegistry


def test_connector_plugin_registry_loads_callable_entrypoint(monkeypatch):
    expected_plugin = ConnectorPlugin(
        connector_type=ConnectorRuntimeType.SNOWFLAKE,
        connector_family=ConnectorFamily.DATABASE,
    )

    class FakeEntryPoint:
        name = "snowflake"

        @staticmethod
        def load():
            return lambda: expected_plugin

    registry = ConnectorPluginRegistry()
    registered = []

    monkeypatch.setattr(
        "langbridge.plugins.connectors.entry_points",
        lambda group: [FakeEntryPoint()] if group == "langbridge.connectors" else [],
    )
    monkeypatch.setattr(
        registry,
        "register",
        lambda plugin: registered.append(plugin) or plugin,
    )

    registry.load_entrypoints()

    assert registered == [expected_plugin]


def test_connector_plugin_registry_loads_plugin_instance(monkeypatch):
    plugin = ConnectorPlugin(
        connector_type=ConnectorRuntimeType.SNOWFLAKE,
        connector_family=ConnectorFamily.DATABASE,
    )

    class FakeEntryPoint:
        name = "snowflake"

        @staticmethod
        def load():
            return plugin

    registry = ConnectorPluginRegistry()

    monkeypatch.setattr(
        "langbridge.plugins.connectors.entry_points",
        lambda group: [FakeEntryPoint()] if group == "langbridge.connectors" else [],
    )

    registry.load_entrypoints()

    assert registry.get(ConnectorRuntimeType.SNOWFLAKE) is plugin
