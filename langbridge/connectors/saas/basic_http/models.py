import json
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BasicHttpAuthType(str, Enum):
    NONE = "none"
    BEARER = "bearer"
    API_KEY_HEADER = "api_key_header"
    BASIC = "basic"


class BasicHttpPaginationStrategy(str, Enum):
    NONE = "none"
    CURSOR = "cursor"
    OFFSET = "offset"
    LINK_HEADER = "link_header"


class BasicHttpCursorType(str, Enum):
    STRING = "string"
    UNIX_TIMESTAMP = "unix_timestamp"
    ISO_DATETIME = "iso_datetime"


class BasicHttpResourceConfig(_BaseModel):
    key: str
    label: str | None = None
    path: str
    primary_key: str | None = "id"
    response_items_field: str | None = None
    request_params: dict[str, Any] = Field(default_factory=dict)
    supports_incremental: bool = False
    default_sync_mode: str = "FULL_REFRESH"
    incremental_request_param: str | None = None
    incremental_cursor_field: str | None = None
    incremental_cursor_type: BasicHttpCursorType = BasicHttpCursorType.STRING
    pagination_strategy: BasicHttpPaginationStrategy = BasicHttpPaginationStrategy.NONE
    limit_param: str | None = None
    default_page_size: int | None = None
    max_page_size: int | None = None
    cursor_param: str | None = None
    next_cursor_field: str | None = None
    response_has_more_field: str | None = None
    response_is_last_field: str | None = None
    response_total_field: str | None = None
    link_header_param: str | None = None

    @field_validator("request_params", mode="before")
    @classmethod
    def _parse_request_params(cls, value: Any) -> dict[str, Any]:
        if value in (None, "", {}):
            return {}
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                raise ValueError("request_params must decode to a JSON object.")
            return dict(parsed)
        raise ValueError("request_params must be a mapping or JSON object string.")

    @model_validator(mode="after")
    def _validate_resource(self) -> "BasicHttpResourceConfig":
        if not str(self.key or "").strip():
            raise ValueError("Resource key is required.")
        if not str(self.path or "").strip():
            raise ValueError("Resource path is required.")
        if self.supports_incremental and not str(self.incremental_cursor_field or "").strip():
            raise ValueError(
                f"Resource '{self.key}' enables incremental sync but does not define incremental_cursor_field."
            )
        if (
            self.pagination_strategy in {
                BasicHttpPaginationStrategy.CURSOR,
                BasicHttpPaginationStrategy.OFFSET,
            }
            and not str(self.cursor_param or "").strip()
        ):
            raise ValueError(
                f"Resource '{self.key}' uses pagination but does not define cursor_param."
            )
        if self.default_page_size is not None and self.default_page_size <= 0:
            raise ValueError(f"Resource '{self.key}' default_page_size must be greater than zero.")
        if self.max_page_size is not None and self.max_page_size <= 0:
            raise ValueError(f"Resource '{self.key}' max_page_size must be greater than zero.")
        if (
            self.default_page_size is not None
            and self.max_page_size is not None
            and self.default_page_size > self.max_page_size
        ):
            raise ValueError(
                f"Resource '{self.key}' default_page_size cannot exceed max_page_size."
            )
        return self
