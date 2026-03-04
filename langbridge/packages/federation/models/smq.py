from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


DateRange = str | list[str]


class SMQTimeDimension(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    dimension: str
    granularity: str | None = None
    date_range: DateRange | None = Field(default=None, alias="dateRange")


class SMQFilter(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    member: str | None = None
    dimension: str | None = None
    measure: str | None = None
    time_dimension: str | None = Field(default=None, alias="timeDimension")
    operator: str
    values: list[Any] = Field(default_factory=list)

    @model_validator(mode="after")
    def _ensure_member(self) -> "SMQFilter":
        if not any((self.member, self.dimension, self.measure, self.time_dimension)):
            raise ValueError("SMQ filter must include member, dimension, measure, or timeDimension.")
        return self


class SMQOrderItem(BaseModel):
    member: str
    direction: Literal["asc", "desc"] = "asc"


class SMQQuery(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    measures: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    time_dimensions: list[SMQTimeDimension] = Field(default_factory=list, alias="timeDimensions")
    filters: list[SMQFilter] = Field(default_factory=list)
    order: list[SMQOrderItem] | dict[str, str] | list[dict[str, str]] | None = None
    limit: int | None = None
    offset: int | None = None
    timezone: str | None = None
