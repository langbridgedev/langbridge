"""
Factory and registry helpers for Langbridge AI LLM providers.
"""
from typing import Any, Dict, Iterable, Type, TypeVar

from .base import (
    LLMConnectionConfig,
    LLMProvider,
    LLMProviderName,
    ProviderNotRegisteredError,
)

ProviderType = TypeVar("ProviderType", bound=LLMProvider)

_REGISTRY: Dict[LLMProviderName, Type[LLMProvider]] = {}


def register_provider(cls: Type[ProviderType]) -> Type[ProviderType]:
    if not issubclass(cls, LLMProvider):
        raise TypeError("Only subclasses of LLMProvider can be registered.")
    _REGISTRY[cls.name] = cls
    return cls


def get_provider_class(name: LLMProviderName) -> Type[LLMProvider]:
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        raise ProviderNotRegisteredError(f"No provider registered for '{name.value}'.") from exc


def registered_providers() -> Iterable[LLMProviderName]:
    return tuple(_REGISTRY.keys())


def create_provider(connection: Any) -> LLMProvider:
    config = LLMConnectionConfig.from_connection(connection)
    provider_cls = get_provider_class(config.provider)
    return provider_cls(config)


def create_client_from_connection(connection: Any, **overrides: Any) -> Any:
    provider = create_provider(connection)
    return provider.create_client(**overrides)


def create_chat_model_from_connection(connection: Any, **overrides: Any) -> Any:
    """Compatibility alias. Returns provider SDK client, not external framework object."""
    return create_client_from_connection(connection, **overrides)


__all__ = [
    "register_provider",
    "get_provider_class",
    "registered_providers",
    "create_provider",
    "create_client_from_connection",
    "create_chat_model_from_connection",
]
