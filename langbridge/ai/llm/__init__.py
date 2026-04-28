from .base import (
    LLMProvider,
    LLMProviderName,
    LLMConnectionConfig,
    ProviderConfigurationError,
    ProviderNotRegisteredError,
    coerce_provider_name,
)
from .factory import (
    register_provider,
    get_provider_class,
    registered_providers,
    create_provider,
    create_client_from_connection,
    create_chat_model_from_connection,
)

# Import concrete providers to register them with the factory.
try:  # pragma: no cover - optional provider deps may be unavailable in some test environments
    from .openai import OpenAIProvider  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover
    OpenAIProvider = None  # type: ignore

try:  # pragma: no cover
    from .anthropic import AnthropicProvider  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover
    AnthropicProvider = None  # type: ignore

try:  # pragma: no cover
    from .azure import AzureOpenAIProvider  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover
    AzureOpenAIProvider = None  # type: ignore

try:  # pragma: no cover
    from .ollama import OllamaProvider  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover
    OllamaProvider = None  # type: ignore

__all__ = [
    'LLMProvider',
    'LLMProviderName',
    'LLMConnectionConfig',
    'ProviderConfigurationError',
    'ProviderNotRegisteredError',
    'coerce_provider_name',
    'register_provider',
    'get_provider_class',
    'registered_providers',
    'create_provider',
    'create_client_from_connection',
    'create_chat_model_from_connection',
    'OpenAIProvider',
    'AnthropicProvider',
    'AzureOpenAIProvider',
    'OllamaProvider',
]
