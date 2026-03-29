
from functools import lru_cache
from importlib.resources import files

import yaml
from pydantic import BaseModel, ConfigDict, Field


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DeclarativeAuthHeader(_Base):
    header_name: str
    field: str
    required: bool = False
    description: str | None = None


class DeclarativeStaticHeader(_Base):
    header_name: str
    value: str


class DeclarativeAuthConfig(_Base):
    strategy: str
    token_field: str
    token_label: str
    header_name: str
    header_template: str
    optional_headers: list[DeclarativeAuthHeader] = Field(default_factory=list)
    static_headers: list[DeclarativeStaticHeader] = Field(default_factory=list)


class DeclarativePaginationConfig(_Base):
    strategy: str
    response_items_field: str
    limit_param: str
    cursor_param: str
    response_has_more_field: str | None = None
    next_cursor_field: str | None = None
    next_cursor_source: str = "record"
    response_is_last_field: str | None = None
    response_total_field: str | None = None
    link_header_param: str | None = None
    default_page_size: int
    max_page_size: int


class DeclarativeIncrementalConfig(_Base):
    strategy: str
    request_param: str
    cursor_field: str
    cursor_type: str


class DeclarativeConnectorResource(_Base):
    key: str
    label: str
    path: str
    primary_key: str
    supports_incremental: bool = False
    default_sync_mode: str
    description: str | None = None
    response_items_field: str | None = None
    request_params: dict[str, object] = Field(default_factory=dict)


class DeclarativeConnectorManifest(_Base):
    schema_version: str
    kind: str
    id: str
    display_name: str
    connector_type: str
    connector_family: str
    description: str
    base_url: str
    test_connection_path: str | None = None
    auth: DeclarativeAuthConfig
    pagination: DeclarativePaginationConfig
    incremental: DeclarativeIncrementalConfig
    resources: list[DeclarativeConnectorResource]

    @property
    def resource_keys(self) -> tuple[str, ...]:
        return tuple(resource.key for resource in self.resources)


@lru_cache(maxsize=None)
def load_declarative_connector_manifest(
    manifest_package: str,
    manifest_name: str,
) -> DeclarativeConnectorManifest:
    manifest_path = files(manifest_package).joinpath(manifest_name)
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(
            f"Declarative connector manifest '{manifest_package}:{manifest_name}' "
            "must load as a mapping."
        )
    return DeclarativeConnectorManifest.model_validate(payload)
