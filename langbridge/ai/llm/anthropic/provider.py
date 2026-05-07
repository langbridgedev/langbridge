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
    response_text,
)
from ..factory import register_provider
from ..contracts import (
    StructuredOutputConfig,
    StructuredOutputContract,
    StructuredOutputMode,
    StructuredOutputSchema,
    StructuredOutputUnsupportedError,
)
from ..structured import StructuredOutputParser

try:  # pragma: no cover - optional dependency
    from anthropic import Anthropic, AsyncAnthropic
except ImportError as exc:  # pragma: no cover - optional dependency
    Anthropic = None  # type: ignore[assignment]
    AsyncAnthropic = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover - optional dependency
    _IMPORT_ERROR = None

_ALLOWED_CONFIG_KEYS = {
    "timeout",
    "max_retries",
    "default_headers",
}


def _to_dict(response: Any) -> dict[str, Any]:
    if hasattr(response, "model_dump"):
        payload = response.model_dump(mode="json")
    elif isinstance(response, dict):
        payload = dict(response)
    else:
        payload = {"raw": response}
    content = payload.get("content")
    if isinstance(content, list):
        text_parts = [
            str(item.get("text"))
            for item in content
            if isinstance(item, dict) and item.get("type") == "text" and item.get("text") is not None
        ]
        if text_parts:
            payload["text"] = "".join(text_parts)
    return payload


def _split_system(messages: list[LLMMessage]) -> tuple[str | None, list[LLMMessage]]:
    system_parts: list[str] = []
    user_messages: list[LLMMessage] = []
    for message in messages:
        if message.role == "system":
            content = message.content
            if content is not None:
                system_parts.append(str(content))
            continue
        user_messages.append(message)
    system = "\n\n".join(system_parts) if system_parts else None
    return system, user_messages


@register_provider
class AnthropicProvider(LLMProvider):
    name = LLMProviderName.ANTHROPIC

    def create_client(self, **overrides: Any) -> Any:
        if AsyncAnthropic is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = {key: self.configuration.get(key) for key in _ALLOWED_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("api_key", self.api_key)
        return AsyncAnthropic(**params)

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
            raise StructuredOutputUnsupportedError("Anthropic structured invocation requires response_model.")

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
        system, user_messages = _split_system(request.messages)
        response = await client.messages.create(
            **self._clean_kwargs(
                {
                    "model": self.model_name,
                    "messages": [self._message_payload(message) for message in user_messages],
                    "system": system,
                    "temperature": request.temperature,
                    "max_tokens": request.max_tokens or int(self.configuration.get("max_tokens") or 1024),
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
        system, user_messages = _split_system(request.messages)
        tool_name = "langbridge_structured_response"
        response = await client.messages.create(
            **self._clean_kwargs(
                {
                    "model": self.model_name,
                    "messages": [self._message_payload(message) for message in user_messages],
                    "system": system,
                    "temperature": request.temperature,
                    "max_tokens": request.max_tokens or int(self.configuration.get("max_tokens") or 1024),
                    "tools": [
                        {
                            "name": tool_name,
                            "description": f"Return a {response_model.__name__} JSON object.",
                            "input_schema": StructuredOutputSchema(response_model).as_dict(),
                        }
                    ],
                    "tool_choice": {"type": "tool", "name": tool_name},
                    **request.provider_options,
                }
            )
        )
        payload = _to_dict(response)
        for item in payload.get("content") or []:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "tool_use" and item.get("name") == tool_name:
                return LLMResponse(
                    raw_response=payload,
                    text=response_text(payload),
                    parsed=StructuredOutputParser(response_model).validate_payload(item.get("input")),
                    response_model_name=response_model.__name__,
                    extract_mode="native_structured",
                )
        raise StructuredOutputUnsupportedError("Anthropic structured response did not include the forced tool call.")

    async def acreate_embeddings(
        self,
        request: LLMEmbeddingsRequest,
    ) -> LLMEmbeddingsInvocation:
        _ = request
        raise NotImplementedError("Anthropic provider does not support embeddings.")

    @staticmethod
    def _message_payload(message: LLMMessage) -> dict[str, str]:
        role = "assistant" if message.role == "assistant" else "user"
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


__all__ = ["AnthropicProvider"]
