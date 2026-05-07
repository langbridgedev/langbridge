from .base import (
    LLMEmbeddingsInvocation,
    LLMEmbeddingsRequest,
    LLMProvider,
    LLMProviderName,
    LLMConnectionConfig,
    LLMInvocation,
    LLMMessage,
    LLMRequest,
    LLMResponse,
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
from .contracts import (
    StructuredOutputConfig,
    StructuredOutputContract,
    StructuredOutputIncompleteError,
    StructuredOutputMode,
    StructuredOutputRefusalError,
    StructuredOutputSchema,
    StructuredOutputUnsupportedError,
)
from .structured import (
    JsonPayloadExtractor,
    StructuredOutputError,
    StructuredOutputParser,
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
    'LLMEmbeddingsInvocation',
    'LLMEmbeddingsRequest',
    'LLMInvocation',
    'LLMMessage',
    'LLMRequest',
    'LLMResponse',
    'ProviderConfigurationError',
    'ProviderNotRegisteredError',
    'coerce_provider_name',
    'register_provider',
    'get_provider_class',
    'registered_providers',
    'create_provider',
    'create_client_from_connection',
    'create_chat_model_from_connection',
    'StructuredOutputError',
    'StructuredOutputConfig',
    'StructuredOutputContract',
    'StructuredOutputIncompleteError',
    'StructuredOutputMode',
    'StructuredOutputParser',
    'StructuredOutputRefusalError',
    'StructuredOutputSchema',
    'StructuredOutputUnsupportedError',
    'JsonPayloadExtractor',
    'OpenAIProvider',
    'AnthropicProvider',
    'AzureOpenAIProvider',
    'OllamaProvider',
]
