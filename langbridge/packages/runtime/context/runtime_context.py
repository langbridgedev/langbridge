import uuid
from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class RuntimeContext:
    tenant_id: uuid.UUID
    workspace_id: uuid.UUID
    user_id: uuid.UUID | None = None
    roles: tuple[str, ...] = field(default_factory=tuple)
    request_id: str | None = None

    @classmethod
    def build(
        cls,
        *,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID | None = None,
        user_id: uuid.UUID | None = None,
        roles: list[str] | tuple[str, ...] | None = None,
        request_id: str | None = None,
    ) -> "RuntimeContext":
        return cls(
            tenant_id=tenant_id,
            workspace_id=workspace_id or tenant_id,
            user_id=user_id,
            roles=tuple(roles or ()),
            request_id=request_id,
        )