"""Messaging contracts and broker adapters."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .broker import MessageReceipt, ReceivedMessage, RedisBroker
    from .contracts import MessageEnvelope, MessageHeaders
    from .handler import BaseMessageHandler

__all__ = [
    "MessageEnvelope",
    "MessageHeaders",
    "MessageReceipt",
    "ReceivedMessage",
    "RedisBroker",
    "BaseMessageHandler",
]


def __getattr__(name: str) -> Any:
    if name in {"MessageEnvelope", "MessageHeaders"}:
        from .contracts import MessageEnvelope, MessageHeaders

        return {
            "MessageEnvelope": MessageEnvelope,
            "MessageHeaders": MessageHeaders,
        }[name]
    if name in {"MessageReceipt", "ReceivedMessage", "RedisBroker"}:
        from .broker import MessageReceipt, ReceivedMessage, RedisBroker

        return {
            "MessageReceipt": MessageReceipt,
            "ReceivedMessage": ReceivedMessage,
            "RedisBroker": RedisBroker,
        }[name]
    if name == "BaseMessageHandler":
        from .handler import BaseMessageHandler

        return BaseMessageHandler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
