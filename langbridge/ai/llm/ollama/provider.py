from typing import Any, Mapping

import httpx
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

DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"
DEFAULT_OLLAMA_TIMEOUT_SECONDS = 300.0

_CLIENT_CONFIG_KEYS = {
    "timeout",
    "headers",
}


def _base_url(value: Any) -> str:
    base_url = str(value or DEFAULT_OLLAMA_BASE_URL).strip() or DEFAULT_OLLAMA_BASE_URL
    return base_url.rstrip("/")


def _configured_base_url(configuration: Mapping[str, Any]) -> str:
    return _base_url(
        configuration.get("base_url")
        or configuration.get("api_url")
        or configuration.get("api_base_url")
    )


def _response_payload(response: httpx.Response) -> dict[str, Any]:
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Ollama response must be a JSON object.")
    return payload


@register_provider
class OllamaProvider(LLMProvider):
    name = LLMProviderName.OLLAMA

    def create_client(self, **overrides: Any) -> httpx.AsyncClient:
        params = {key: self.configuration.get(key) for key in _CLIENT_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("base_url", _configured_base_url(self.configuration))
        params.setdefault("timeout", DEFAULT_OLLAMA_TIMEOUT_SECONDS)
        return httpx.AsyncClient(**params)

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
            raise StructuredOutputUnsupportedError("Ollama structured invocation requires response_model.")

        mode = StructuredOutputConfig.from_mapping(self.configuration).mode
        if mode in {StructuredOutputMode.auto, StructuredOutputMode.native}:
            response = await self._create_text_response(
                request,
                response_format=StructuredOutputSchema(response_model).as_dict(),
            )
            parsed_response = self._extract_response(response, response_model=response_model)
            return LLMInvocation(
                request=request,
                response=parsed_response.model_copy(update={"extract_mode": "native_structured"}),
            )

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
        *,
        response_format: Any = None,
    ) -> LLMResponse[Any]:
        async with self.create_client() as client:
            response = await client.post(
                "/api/chat",
                json=self._chat_payload(
                    request.messages,
                    temperature=request.temperature,
                    max_tokens=request.max_tokens,
                    response_format=response_format,
                    provider_options=request.provider_options,
                ),
            )
        payload = self._chat_response(_response_payload(response))
        return LLMResponse(raw_response=payload, text=response_text(payload), extract_mode="text")

    async def acreate_embeddings(
        self,
        request: LLMEmbeddingsRequest,
    ) -> LLMEmbeddingsInvocation:
        cleaned = [text for text in request.texts if isinstance(text, str)]
        if not cleaned:
            return LLMEmbeddingsInvocation(request=request, embeddings=[], raw_response=None)
        model = self._embedding_model(request.embedding_model)
        async with self.create_client() as client:
            response = await client.post(
                "/api/embed",
                json=self._embedding_payload(
                    model=model,
                    texts=cleaned,
                    provider_options=request.provider_options,
                ),
            )
        payload = _response_payload(response)
        return LLMEmbeddingsInvocation(
            request=request,
            embeddings=self._embedding_response(payload),
            raw_response=payload,
        )

    def _chat_payload(
        self,
        messages: list[LLMMessage],
        *,
        temperature: float,
        max_tokens: int | None,
        response_format: Any = None,
        provider_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self.model_name,
            "messages": [self._message_payload(message) for message in messages],
            "stream": False,
        }
        if provider_options:
            payload.update(dict(provider_options))
        if response_format is not None:
            payload["format"] = response_format
        elif self.configuration.get("format") is not None:
            payload["format"] = self.configuration["format"]
        elif self._json_mode_when_requested(messages):
            payload["format"] = "json"
        if self.configuration.get("keep_alive") is not None:
            payload["keep_alive"] = self.configuration["keep_alive"]
        options = self._options(temperature=temperature, max_tokens=max_tokens)
        if options:
            payload["options"] = options
        return payload

    def _embedding_payload(
        self,
        *,
        model: str,
        texts: list[str],
        provider_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model,
            "input": texts,
        }
        if provider_options:
            payload.update(dict(provider_options))
        if self.configuration.get("keep_alive") is not None:
            payload["keep_alive"] = self.configuration["keep_alive"]
        if self.configuration.get("truncate") is not None:
            payload["truncate"] = bool(self.configuration["truncate"])
        options = self.configuration.get("embedding_options") or self.configuration.get("options")
        if isinstance(options, Mapping):
            payload["options"] = dict(options)
        return payload

    def _embedding_model(self, override: str | None) -> str:
        return str(
            override
            or self.configuration.get("embedding_model")
            or self.configuration.get("embedding")
            or self.model_name
        )

    def _options(self, *, temperature: float, max_tokens: int | None) -> dict[str, Any]:
        configured = self.configuration.get("options")
        options = dict(configured) if isinstance(configured, Mapping) else {}
        options.setdefault("temperature", temperature)
        if max_tokens is not None:
            options.setdefault("num_predict", max_tokens)
        return self._clean_kwargs(options)

    @staticmethod
    def _message_payload(message: LLMMessage) -> dict[str, Any]:
        return {
            "role": str(message.role or "user"),
            "content": str(message.content or ""),
        }

    def _json_mode_when_requested(self, messages: list[LLMMessage]) -> bool:
        enabled = self.configuration.get("json_mode_when_requested", True)
        if enabled is False:
            return False
        text = "\n".join(str(message.content or "") for message in messages).casefold()
        return any(
            cue in text
            for cue in (
                "return strict json",
                "return valid json",
                "return json only",
                "json object",
            )
        )

    @staticmethod
    def _chat_response(payload: dict[str, Any]) -> LLMResponse:
        message = payload.get("message")
        if isinstance(message, Mapping):
            content = message.get("content")
            if isinstance(content, str):
                payload["text"] = content
        return payload

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

    @staticmethod
    def _embedding_response(payload: dict[str, Any]) -> list[list[float]]:
        embeddings = payload.get("embeddings")
        if not isinstance(embeddings, list):
            raise RuntimeError("Ollama embedding response missing embeddings.")
        return [list(embedding) for embedding in embeddings]


__all__ = ["OllamaProvider"]
