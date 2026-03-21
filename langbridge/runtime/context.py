import uuid
from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class RuntimeContext:
    workspace_id: uuid.UUID
    actor_id: uuid.UUID | None = None
    roles: tuple[str, ...] = field(default_factory=tuple)
    request_id: str | None = None

    @classmethod
    def build(
        cls,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None = None,
        roles: list[str] | tuple[str, ...] | None = None,
        request_id: str | None = None,
    ) -> "RuntimeContext":
        return cls(
            workspace_id=workspace_id,
            actor_id=actor_id,
            roles=tuple(roles or ()),
            request_id=request_id,
        )
