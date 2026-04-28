import json
from typing import Any

from pydantic import Field, field_validator, model_validator

from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorAuthFieldSchema,
    ConnectorCapabilities,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorFamily,
    ConnectorPluginMetadata,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)

from .models import BasicHttpAuthType, BasicHttpResourceConfig


def _parse_json_mapping(value: Any, *, field_name: str) -> dict[str, str]:
    if value in (None, "", {}):
        return {}
    if isinstance(value, dict):
        return {
            str(key): str(item)
            for key, item in value.items()
            if str(key).strip() and item is not None
        }
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError(f"{field_name} must decode to a JSON object.")
        return {
            str(key): str(item)
            for key, item in parsed.items()
            if str(key).strip() and item is not None
        }
    raise ValueError(f"{field_name} must be a mapping or JSON object string.")


def _parse_resource_list(value: Any) -> list[BasicHttpResourceConfig]:
    if value in (None, "", []):
        return []
    raw_items = value
    if isinstance(value, str):
        raw_items = json.loads(value)
    if not isinstance(raw_items, list):
        raise ValueError("resources must decode to a JSON array.")
    return [BasicHttpResourceConfig.model_validate(item) for item in raw_items]


class BasicHttpConnectorConfig(BaseConnectorConfig):
    api_base_url: str
    auth_type: BasicHttpAuthType = BasicHttpAuthType.NONE
    auth_token: str | None = None
    auth_header_name: str | None = None
    auth_header_value_prefix: str | None = None
    username: str | None = None
    password: str | None = None
    test_connection_path: str | None = None
    static_headers: dict[str, str] = Field(default_factory=dict)
    resources: list[BasicHttpResourceConfig] = Field(default_factory=list)
    timeout_s: float = 30.0

    @field_validator("static_headers", mode="before")
    @classmethod
    def _coerce_static_headers(cls, value: Any) -> dict[str, str]:
        return _parse_json_mapping(value, field_name="static_headers")

    @field_validator("resources", mode="before")
    @classmethod
    def _coerce_resources(cls, value: Any) -> list[BasicHttpResourceConfig]:
        return _parse_resource_list(value)

    @model_validator(mode="after")
    def _validate_config(self) -> "BasicHttpConnectorConfig":
        if not str(self.api_base_url or "").strip():
            raise ValueError("api_base_url is required.")
        if self.auth_type in {BasicHttpAuthType.BEARER, BasicHttpAuthType.API_KEY_HEADER}:
            if not str(self.auth_token or "").strip():
                raise ValueError("auth_token is required for the selected auth_type.")
        if self.auth_type == BasicHttpAuthType.BASIC:
            if not str(self.username or "").strip():
                raise ValueError("username is required for basic auth.")
            if not str(self.password or "").strip():
                raise ValueError("password is required for basic auth.")
        return self


class BasicHttpConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.BASIC_HTTP

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return BasicHttpConnectorConfig(**config)


class BasicHttpConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.BASIC_HTTP

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        auth_schema = [
            ConnectorAuthFieldSchema(
                field="auth_token",
                label="Auth token",
                required=False,
                description="Bearer token or API key header value when auth_type requires token auth.",
                type="password",
                secret=True,
            ),
            ConnectorAuthFieldSchema(
                field="username",
                label="Username",
                required=False,
                description="Username for basic HTTP authentication.",
                type="string",
                secret=False,
            ),
            ConnectorAuthFieldSchema(
                field="password",
                label="Password",
                required=False,
                description="Password for basic HTTP authentication.",
                type="password",
                secret=True,
            ),
        ]
        return ConnectorConfigSchema(
            name="Basic HTTP",
            description=(
                "Generic HTTP JSON connector for runtime-managed APIs. "
                "Define resources as JSON and let the runtime fetch them through GET requests."
            ),
            version="1.0.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="api_base_url",
                    label="API base URL",
                    description="Base URL used to resolve relative resource paths.",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="auth_type",
                    label="Auth type",
                    description="Authentication mode for outgoing HTTP requests.",
                    type="string",
                    required=True,
                    default=BasicHttpAuthType.NONE.value,
                    value_list=[item.value for item in BasicHttpAuthType],
                ),
                ConnectorConfigEntrySchema(
                    field="auth_token",
                    label="Auth token",
                    description="Bearer token or API key header value when the selected auth type needs one.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="auth_header_name",
                    label="Auth header name",
                    description="Optional header override. Defaults to Authorization for bearer and X-API-Key for api_key_header.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="auth_header_value_prefix",
                    label="Auth header prefix",
                    description="Optional value prefix. Example: Bearer . Leave blank for raw header values.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="username",
                    label="Username",
                    description="Username for basic auth.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="password",
                    label="Password",
                    description="Password for basic auth.",
                    type="password",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="test_connection_path",
                    label="Test connection path",
                    description="Optional path used for connection testing. Defaults to the first resource path.",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="timeout_s",
                    label="Timeout seconds",
                    description="HTTP request timeout in seconds.",
                    type="number",
                    required=False,
                    default="30",
                ),
                ConnectorConfigEntrySchema(
                    field="static_headers",
                    label="Static headers JSON",
                    description=(
                        "Optional JSON object of headers added to every request, for example "
                        '{"Accept-Language": "en-GB"}.'
                    ),
                    type="textarea",
                    required=False,
                    default="{}",
                ),
                ConnectorConfigEntrySchema(
                    field="resources",
                    label="Resources JSON",
                    description=(
                        "Optional JSON array of named resource definitions. Each item must define key and path. "
                        "Optional fields: label, primary_key, response_items_field, request_params, "
                        "supports_incremental, default_sync_mode, incremental_request_param, "
                        "incremental_cursor_field, incremental_cursor_type, pagination_strategy, "
                        "limit_param, default_page_size, max_page_size, cursor_param, next_cursor_field, "
                        "response_has_more_field, response_is_last_field, response_total_field, link_header_param."
                    ),
                    type="textarea",
                    required=False,
                    default="[]",
                ),
            ],
            plugin_metadata=ConnectorPluginMetadata(
                connector_type=ConnectorRuntimeType.BASIC_HTTP,
                connector_family=ConnectorFamily.API,
                supported_resources=[],
                auth_schema=auth_schema,
                default_sync_strategy=ConnectorSyncStrategy.FULL_REFRESH,
                capabilities=ConnectorCapabilities(
                    supports_live_datasets=True,
                    supports_synced_datasets=True,
                    supports_incremental_sync=True,
                    supports_federated_execution=True,
                ),
            ),
        )
