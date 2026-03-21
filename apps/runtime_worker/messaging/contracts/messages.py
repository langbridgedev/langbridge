from __future__ import annotations

import uuid

from pydantic import Field, model_validator

from langbridge.contracts.base import _Base

from .base import BaseMessagePayload, MessageType, resolve_payload_model


class MessageHeaders(_Base):
    workspace_id: str | None = None
    actor_id: str | None = None
    correlation_id: str | None = None
    request_id: str | None = None

    @classmethod
    def default(cls) -> "MessageHeaders":
        return cls(correlation_id=str(uuid.uuid4()))


class MessageEnvelope(_Base):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    message_type: MessageType | str
    payload: BaseMessagePayload | dict
    headers: MessageHeaders = Field(default_factory=MessageHeaders.default)

    @model_validator(mode="after")
    def _coerce_payload(self) -> "MessageEnvelope":
        try:
            self.message_type = MessageType(self.message_type)
        except ValueError:
            pass

        payload_model = resolve_payload_model(self.message_type)
        if payload_model is not None and not isinstance(self.payload, payload_model):
            self.payload = payload_model.model_validate(self.payload)
        elif not isinstance(self.payload, BaseMessagePayload):
            self.payload = BaseMessagePayload.model_validate(self.payload)
        return self


__all__ = ["MessageEnvelope", "MessageHeaders"]
