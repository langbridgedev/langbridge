from __future__ import annotations

from typing import Any, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


DateRange = Union[str, List[str]]

class TimeDimension(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    dimension: str
    granularity: Optional[str] = None
    date_range: Optional[DateRange] = Field(default=None, alias="dateRange")
    compare_date_range: Optional[DateRange] = Field(default=None, alias="compareDateRange")

    @field_validator("granularity")
    @classmethod
    def _normalize_granularity(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return value.strip().lower()


class FilterItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    member: Optional[str] = None
    dimension: Optional[str] = None
    measure: Optional[str] = None
    time_dimension: Optional[str] = Field(default=None, alias="timeDimension")
    operator: str = "equals"
    values: Optional[List[str]] = None
    and_: Optional[List["FilterItem"]] = Field(default=None, alias="and")
    or_: Optional[List["FilterItem"]] = Field(default=None, alias="or")

    @model_validator(mode="after")
    def _ensure_member(self) -> "FilterItem":
        has_group = bool(self.and_ or self.or_)
        has_member = any((self.member, self.dimension, self.measure, self.time_dimension))
        if not has_group and not has_member:
            raise ValueError("Filter must include member, dimension, measure, timeDimension, or an and/or group.")
        if self.and_ is not None and self.or_ is not None:
            raise ValueError("Filter groups must use either and or or, not both.")
        if has_group and has_member:
            raise ValueError("Filter groups cannot also define member, dimension, measure, or timeDimension.")
        return self


FilterItem.model_rebuild()


class SemanticQuery(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    measures: List[str] = Field(default_factory=list)
    dimensions: List[str] = Field(default_factory=list)
    time_dimensions: List[TimeDimension] = Field(default_factory=list, alias="timeDimensions")
    filters: List[FilterItem] = Field(default_factory=list)
    segments: List[str] = Field(default_factory=list)
    order: Any = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    timezone: Optional[str] = None
