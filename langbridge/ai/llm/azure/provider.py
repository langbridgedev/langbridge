from typing import Any

from pydantic import BaseModel

from ..base import (
    LLMEmbeddingsInvocation,
    LLMEmbeddingsRequest,
    LLMInvocation,
    LLMMessage,
    LLMProvider,
    LLMProviderName,
    LLMRequest,
    LLMResponse,
    ProviderConfigurationError,
    response_text,
)
from ..factory import register_provider
from ..contracts import (
    StructuredOutputConfig,
    StructuredOutputContract,
    StructuredOutputMode,
    StructuredOutputUnsupportedError,
)
from ..structured import StructuredOutputParser

try:  # pragma: no cover - optional dependency
    from openai import AsyncAzureOpenAI, AzureOpenAI
except ImportError as exc:  # pragma: no cover - optional dependency
    AsyncAzureOpenAI = None  # type: ignore[assignment]
    AzureOpenAI = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover - optional dependency
    _IMPORT_ERROR = None

_OPTIONAL_CONFIG_KEYS = {
    "timeout",
    "max_retries",
    "default_headers",
    "http_client",
}


def _to_dict(response: Any) -> dict[str, Any]:
    if hasattr(response, "model_dump"):
        return response.model_dump(mode="json")
    if isinstance(response, dict):
        return dict(response)
    return {"raw": response}


@register_provider
class AzureOpenAIProvider(LLMProvider):
    name = LLMProviderName.AZURE

    def _client_params(self, overrides: dict[str, Any]) -> dict[str, Any]:
        deployment = (
            self.configuration.get("deployment_name")
            or self.configuration.get("deployment")
            or self.configuration.get("azure_deployment")
        )
        if not deployment:
            raise ProviderConfigurationError(
                "Azure OpenAI configuration requires 'deployment_name' "
                "(or 'deployment'/'azure_deployment')."
            )

        endpoint = (
            self.configuration.get("azure_endpoint")
            or self.configuration.get("api_base")
            or self.configuration.get("endpoint")
        )
        if not endpoint:
            raise ProviderConfigurationError(
                "Azure OpenAI configuration requires 'azure_endpoint' "
                "(or 'api_base'/'endpoint')."
            )

        api_version = overrides.pop("api_version", None) or self.configuration.get("api_version") or "2024-05-01-preview"
        params = {
            "api_key": self.api_key,
            "azure_endpoint": endpoint,
            "api_version": api_version,
        }
        for key in _OPTIONAL_CONFIG_KEYS:
            if key in self.configuration:
                params[key] = self.configuration[key]
        params.update(overrides)
        params = self._clean_kwargs(params)
        params["_deployment"] = str(deployment)
        return params

    def create_client(self, **overrides: Any) -> Any:
        if AsyncAzureOpenAI is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = self._client_params(dict(overrides))
        params.pop("_deployment", None)
        return AsyncAzureOpenAI(**params)

    def _deployment(self) -> str:
        return str(self._client_params({})["_deployment"])

    async def ainvoke(
        self,
        request: LLMRequest[BaseModel],
    ) -> LLMInvocation[BaseModel]:
        if request.response_model is not None:
            return await self._invoke_structured(request)
        response = await self._create_text_response(request)
        return LLMInvocation(request=request, response=response)

    async def _invoke_structured(
        self,
        request: LLMRequest[BaseModel],
    ) -> LLMInvocation[BaseModel]:
        response_model = request.response_model
        if response_model is None:
            raise StructuredOutputUnsupportedError("Azure OpenAI structured invocation requires response_model.")

        mode = StructuredOutputConfig.from_mapping(self.configuration).mode
        if mode in {StructuredOutputMode.auto, StructuredOutputMode.native}:
            try:
                response = await self._create_native_structured_response(request, response_model=response_model)
                return LLMInvocation(request=request, response=response)
            except StructuredOutputUnsupportedError:
                if mode == StructuredOutputMode.native:
                    raise

        text_request = request.model_copy(
            update={
                "messages": self._messages_with_output_contract(request.messages, response_model=response_model),
                "response_model": None,
            }
        )
        text_response = await self._create_text_response(text_request)
        return LLMInvocation(
            request=request,
            response=self._extract_response(text_response, response_model=response_model),
        )

    async def _create_text_response(
        self,
        request: LLMRequest[Any],
    ) -> LLMResponse[Any]:
        client = self.create_client()
        response = await client.chat.completions.create(
            **self._clean_kwargs(
                {
                    "model": self._deployment(),
                    "messages": [self._message_payload(message) for message in request.messages],
                    "temperature": request.temperature,
                    "max_tokens": request.max_tokens,
                    **request.provider_options,
                }
            )
        )
        payload = _to_dict(response)
        return LLMResponse(raw_response=payload, text=response_text(payload), extract_mode="text")

    async def _create_native_structured_response(
        self,
        request: LLMRequest[BaseModel],
        *,
        response_model: type[BaseModel],
    ) -> LLMResponse[BaseModel]:
        client = self.create_client()
        completions = getattr(getattr(client, "chat", None), "completions", None)
        parse = getattr(completions, "parse", None)
        if not callable(parse):
            raise StructuredOutputUnsupportedError("Azure OpenAI client does not expose chat.completions.parse.")
        response = await parse(
            **self._clean_kwargs(
                {
                    "model": self._deployment(),
                    "messages": [self._message_payload(message) for message in request.messages],
                    "response_format": response_model,
                    "temperature": request.temperature,
                    "max_tokens": request.max_tokens,
                    **request.provider_options,
                }
            )
        )
        choices = getattr(response, "choices", None) or []
        if not choices:
            raise StructuredOutputUnsupportedError("Azure OpenAI structured response did not include choices.")
        parsed = getattr(getattr(choices[0], "message", None), "parsed", None)
        if parsed is None:
            raise StructuredOutputUnsupportedError("Azure OpenAI structured response did not include message.parsed.")
        payload = _to_dict(response)
        return LLMResponse(
            raw_response=payload,
            text=response_text(payload),
            parsed=StructuredOutputParser(response_model).validate_payload(parsed),
            response_model_name=response_model.__name__,
            extract_mode="native_structured",
        )

    async def acreate_embeddings(
        self,
        request: LLMEmbeddingsRequest,
    ) -> LLMEmbeddingsInvocation:
        cleaned = [text for text in request.texts if isinstance(text, str)]
        if not cleaned:
            return LLMEmbeddingsInvocation(request=request, embeddings=[], raw_response=None)
        model = request.embedding_model or self.configuration.get("embedding_model")
        if not model:
            raise ProviderConfigurationError("Azure embeddings require embedding_model.")
        client = self.create_client()
        response = await client.embeddings.create(model=model, input=cleaned, **request.provider_options)
        return LLMEmbeddingsInvocation(
            request=request,
            embeddings=[list(item.embedding) for item in response.data],
            raw_response=_to_dict(response),
        )

    @staticmethod
    def _message_payload(message: LLMMessage) -> dict[str, str]:
        role = "system" if message.role == "developer" else message.role
        return {"role": role, "content": message.content}

    @staticmethod
    def _messages_with_output_contract(
        messages: list[LLMMessage],
        *,
        response_model: type[BaseModel],
    ) -> list[LLMMessage]:
        output_contract = LLMMessage(
            role="system",
            kind="output_contract",
            trusted=True,
            content=StructuredOutputContract(response_model).system_instruction(),
        )
        return [output_contract, *messages]


__all__ = ["AzureOpenAIProvider"]
