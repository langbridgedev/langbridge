from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, Literal, Mapping, MutableMapping, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T", bound=BaseModel)

LLMRole = Literal["system", "developer", "user", "assistant"]
LLMMessageKind = Literal[
    "instruction",
    "user_input",
    "conversation_context",
    "evidence",
    "tool_context",
    "output_contract",
]


class LLMMessage(BaseModel):
    role: LLMRole
    content: str
    kind: LLMMessageKind = "user_input"
    name: str | None = None
    trusted: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class LLMRequest(BaseModel, Generic[T]):
    """Provider-neutral async LLM request.

    `response_model` makes structured output explicit at the request boundary.
    Provider implementations should try their native structured-output path first
    and then use `LLMProvider._extract_response` as the common JSON extraction
    fallback when native enforcement is unavailable.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    purpose: str
    messages: list[LLMMessage]
    response_model: type[T] | None = Field(default=None, exclude=True)
    temperature: float = 0.0
    max_tokens: int | None = None
    provider_options: dict[str, Any] = Field(default_factory=dict)
    trace_metadata: dict[str, Any] = Field(default_factory=dict)


class LLMResponse(BaseModel, Generic[T]):
    """Normalized provider response plus optional structured output."""

    raw_response: dict[str, Any]
    text: str | None = None
    parsed: T | None = None
    response_model_name: str | None = None
    extract_mode: Literal["native_structured", "json_extractor", "text", "none"] = "none"


class LLMInvocation(BaseModel, Generic[T]):
    request: LLMRequest[T]
    response: LLMResponse[T]


class LLMEmbeddingsRequest(BaseModel):
    texts: list[str]
    embedding_model: str | None = None
    provider_options: dict[str, Any] = Field(default_factory=dict)
    trace_metadata: dict[str, Any] = Field(default_factory=dict)


class LLMEmbeddingsInvocation(BaseModel):
    request: LLMEmbeddingsRequest
    embeddings: list[list[float]]
    raw_response: dict[str, Any] | None = None


class LLMProviderName(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"
    OLLAMA = "ollama"


class ProviderConfigurationError(ValueError):
    """Raised when an LLM provider receives invalid configuration."""


class ProviderNotRegisteredError(LookupError):
    """Raised when no provider implementation is registered for a name."""


def coerce_provider_name(value: Any) -> LLMProviderName:
    if isinstance(value, LLMProviderName):
        return value
    if hasattr(value, "value"):
        return LLMProviderName(str(value.value).lower())
    if isinstance(value, str):
        return LLMProviderName(value.lower())
    raise ProviderConfigurationError("Unsupported provider value supplied.")


@dataclass(frozen=True)
class LLMConnectionConfig:
    provider: LLMProviderName
    api_key: str
    model: str
    configuration: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_connection(cls, connection: Any) -> "LLMConnectionConfig":
        provider = coerce_provider_name(cls._extract_value(connection, "provider"))
        api_key = cls._extract_value(connection, "api_key")
        model = cls._extract_value(connection, "model")
        configuration = cls._extract_value(connection, "configuration", {}) or {}

        if api_key is None:
            raise ProviderConfigurationError("LLM connection is missing an API key.")
        if model is None:
            raise ProviderConfigurationError("LLM connection is missing a model identifier.")
        if not isinstance(configuration, MutableMapping):
            configuration = dict(configuration)
        else:
            configuration = dict(configuration.items())

        return cls(
            provider=provider,
            api_key=str(api_key),
            model=str(model),
            configuration=configuration,
        )

    @classmethod
    def _extract_value(cls, obj: Any, key: str, default: Any = None) -> Any:
        if isinstance(obj, Mapping):
            return obj.get(key, default)
        return getattr(obj, key, default)


class LLMProvider(ABC):
    """Base class for SDK-backed LLM providers used by Langbridge AI."""

    name: LLMProviderName

    def __init__(self, config: LLMConnectionConfig):
        if not isinstance(config, LLMConnectionConfig):
            raise ProviderConfigurationError("LLMProvider requires an LLMConnectionConfig instance.")
        if hasattr(self, "name") and config.provider != self.name:
            raise ProviderConfigurationError(
                f"Configuration mis-match: expected provider '{self.name.value}', "
                f"got '{config.provider.value}'."
            )
        self._config = config

    @property
    def config(self) -> LLMConnectionConfig:
        return self._config

    @property
    def api_key(self) -> str:
        return self._config.api_key

    @property
    def model_name(self) -> str:
        return self._config.model

    @property
    def configuration(self) -> dict[str, Any]:
        return dict(self._config.configuration)

    def _clean_kwargs(self, params: Mapping[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in params.items() if v is not None}

    @abstractmethod
    def create_client(self, **overrides: Any) -> Any:
        """Instantiate a provider SDK client."""

    @abstractmethod
    async def ainvoke(
        self,
        request: LLMRequest[T],
    ) -> LLMInvocation[T]:
        """Asynchronously invoke the model with a structured-first request.

        Implementations should:
        1. send `request.messages` using provider-native roles where possible;
        2. if `request.response_model` is present, prefer native structured output;
        3. if native structured output is unavailable, call `_extract_response`
           on the text response to validate the parsed model consistently.
        """

    @abstractmethod
    async def acreate_embeddings(
        self,
        request: LLMEmbeddingsRequest,
    ) -> LLMEmbeddingsInvocation:
        """Asynchronously create embeddings with a structured request."""
        raise NotImplementedError(f"Provider '{self.name.value}' does not support embeddings.")

    def _extract_response(
        self,
        response: LLMResponse[Any],
        response_model: type[T] | None = None,
    ) -> LLMResponse[T]:
        """Parse text JSON into the requested Pydantic model.

        This is the common fallback providers should call after a non-native
        structured response. It intentionally imports the JSON parser lazily to
        keep `base.py` independent from `structured.py` at module import time.
        """

        if response_model is None:
            raise ProviderConfigurationError("Structured response extraction requires a response_model.")

        from .structured import StructuredOutputParser

        text = response.text if isinstance(response.text, str) else response_text(response)
        parsed = StructuredOutputParser(response_model).parse_text(text)
        return LLMResponse[T](
            raw_response=response.raw_response,
            text=text,
            parsed=parsed,
            response_model_name=response_model.__name__,
            extract_mode="json_extractor",
        )


def response_text(response: LLMResponse[Any] | Mapping[str, Any]) -> str:
    """Extract assistant text from normalized or provider-native responses."""

    raw_response: Mapping[str, Any]
    if isinstance(response, LLMResponse):
        if isinstance(response.text, str):
            return response.text
        raw_response = response.raw_response
    else:
        raw_response = response

    text = raw_response.get("text")
    if isinstance(text, str):
        return text
    choices = raw_response.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, Mapping):
            message = first.get("message")
            if isinstance(message, Mapping) and isinstance(message.get("content"), str):
                return str(message["content"])
            if isinstance(first.get("text"), str):
                return str(first["text"])
    content = raw_response.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts = [
            str(item.get("text"))
            for item in content
            if isinstance(item, Mapping) and item.get("type") == "text" and item.get("text") is not None
        ]
        if text_parts:
            return "".join(text_parts)
    return str(raw_response)
