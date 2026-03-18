from __future__ import annotations

from pydantic import BaseModel, ConfigDict


def _to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


class _Base(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        from_attributes=True,
    )

    def dict_json(self) -> str:
        return self.model_dump_json()


__all__ = ["_Base"]
