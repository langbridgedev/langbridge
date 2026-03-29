
from pydantic import BaseModel, ConfigDict


class RuntimeModel(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        from_attributes=True,
        populate_by_name=True,
    )


def _to_camel(value: str) -> str:
    parts = value.split("_")
    if not parts:
        return value
    head, *tail = parts
    return head + "".join(part.capitalize() for part in tail)


class RuntimeRequestModel(RuntimeModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="allow",
        from_attributes=True,
        populate_by_name=True,
    )
