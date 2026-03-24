"""Shared declarative SaaS connector contracts and helpers."""

from .config import (
    build_declarative_auth_schema,
    build_declarative_config_entries,
    build_declarative_connector_config_schema,
    build_declarative_plugin_metadata,
)
from .manifest import (
    DeclarativeAuthConfig,
    DeclarativeAuthHeader,
    DeclarativeConnectorManifest,
    DeclarativeConnectorResource,
    DeclarativeIncrementalConfig,
    DeclarativePaginationConfig,
    load_declarative_connector_manifest,
)
from .runtime import DeclarativeHttpApiConnector

__all__ = [
    "DeclarativeAuthConfig",
    "DeclarativeAuthHeader",
    "DeclarativeConnectorManifest",
    "DeclarativeConnectorResource",
    "DeclarativeIncrementalConfig",
    "DeclarativePaginationConfig",
    "build_declarative_auth_schema",
    "build_declarative_config_entries",
    "build_declarative_connector_config_schema",
    "build_declarative_plugin_metadata",
    "DeclarativeHttpApiConnector",
    "load_declarative_connector_manifest",
]
