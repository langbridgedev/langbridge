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
    StrictJsonSchemaCompatibility,
    StructuredOutputConfig,
    StructuredOutputContract,
    StructuredOutputIncompleteError,
    StructuredOutputMode,
    StructuredOutputRefusalError,
    StructuredOutputUnsupportedError,
)
from ..structured import StructuredOutputParser

try:  # pragma: no cover - optional dependency
    from openai import AsyncOpenAI
except ImportError as exc:  # pragma: no cover - optional dependency
    AsyncOpenAI = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover - optional dependency
    _IMPORT_ERROR = None

_ALLOWED_CONFIG_KEYS = {
    "timeout",
    "max_retries",
    "base_url",
    "organization",
    "default_headers",
    "http_client",
}

_RETRYABLE_UNSUPPORTED_PARAMS = {
    "max_output_tokens",
    "max_tokens",
    "temperature",
}


def _to_dict(response: Any) -> dict[str, Any]:
    if hasattr(response, "model_dump"):
        payload = response.model_dump(mode="json")
    elif isinstance(response, dict):
        payload = dict(response)
    else:
        payload = {"raw": response}

    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str):
        payload["text"] = output_text
    return payload


@register_provider
class OpenAIProvider(LLMProvider):
    name = LLMProviderName.OPENAI

    def create_client(self, **overrides: Any) -> "AsyncOpenAI":  # type: ignore
        if AsyncOpenAI is None:  # pragma: no cover - optional dependency
            raise RuntimeError(str(_IMPORT_ERROR))
        params = {key: self.configuration.get(key) for key in _ALLOWED_CONFIG_KEYS if key in self.configuration}
        params.update(overrides)
        params = self._clean_kwargs(params)
        params.setdefault("api_key", self.api_key)
        return AsyncOpenAI(**params)

    async def ainvoke(
        self,
        request: LLMRequest[BaseModel],
    ) -> LLMInvocation[BaseModel]:
        if request.response_model is not None:
            return await self._invoke_structured(request)
        response = await self._create_text_response(request)
        return LLMInvocation(request=request, response=response)

    async def acreate_embeddings(
        self,
        request: LLMEmbeddingsRequest,
    ) -> LLMEmbeddingsInvocation:
        cleaned = [text for text in request.texts if isinstance(text, str)]
        if not cleaned:
            return LLMEmbeddingsInvocation(request=request, embeddings=[], raw_response=None)

        model = request.embedding_model or self.configuration.get("embedding_model") or "text-embedding-3-small"
        client = self.create_client()
        response = await client.embeddings.create(model=model, input=cleaned)
        raw_response = _to_dict(response)
        return LLMEmbeddingsInvocation(
            request=request,
            embeddings=[list(item.embedding) for item in response.data],
            raw_response=raw_response,
        )

    async def _invoke_structured(
        self,
        request: LLMRequest[BaseModel],
    ) -> LLMInvocation[BaseModel]:
        response_model = request.response_model
        if response_model is None:
            raise StructuredOutputUnsupportedError("OpenAI structured invocation requires response_model.")

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

    async def _create_native_structured_response(
        self,
        request: LLMRequest[BaseModel],
        *,
        response_model: type[BaseModel],
    ) -> LLMResponse[BaseModel]:
        self._ensure_native_schema_supported(response_model)
        client = self.create_client()
        parse = getattr(getattr(client, "responses", None), "parse", None)
        if not callable(parse):
            raise StructuredOutputUnsupportedError("OpenAI client does not expose responses.parse.")

        try:
            response = await self._call_with_unsupported_param_retry(
                parse,
                self._clean_kwargs(
                    {
                        "model": self.model_name,
                        "input": [self._responses_message_payload(message) for message in request.messages],
                        "text_format": response_model,
                        "temperature": request.temperature,
                        "max_output_tokens": request.max_tokens,
                        **request.provider_options,
                    }
                ),
            )
        except Exception as exc:
            if self._is_invalid_native_schema_error(exc):
                raise StructuredOutputUnsupportedError(
                    f"OpenAI native structured output rejected schema for {response_model.__name__}: {exc}"
                ) from exc
            raise
        parsed = getattr(response, "output_parsed", None)
        payload = _to_dict(response)
        self._raise_for_structured_edge_case(payload)
        if parsed is None:
            raise StructuredOutputUnsupportedError("OpenAI structured response did not include output_parsed.")

        parser = StructuredOutputParser(response_model)
        return LLMResponse(
            raw_response=payload,
            text=response_text(payload),
            parsed=parser.validate_payload(parsed),
            response_model_name=response_model.__name__,
            extract_mode="native_structured",
        )

    async def _create_text_response(
        self,
        request: LLMRequest[Any],
    ) -> LLMResponse[Any]:
        client = self.create_client()
        create = getattr(getattr(client, "responses", None), "create", None)
        if callable(create):
            response = await self._call_with_unsupported_param_retry(
                create,
                self._clean_kwargs(
                    {
                        "model": self.model_name,
                        "input": [self._responses_message_payload(message) for message in request.messages],
                        "temperature": request.temperature,
                        "max_output_tokens": request.max_tokens,
                        **request.provider_options,
                    }
                ),
            )
            payload = _to_dict(response)
            return LLMResponse(raw_response=payload, text=response_text(payload), extract_mode="text")

        completions = getattr(getattr(client, "chat", None), "completions", None)
        chat_create = getattr(completions, "create", None)
        if callable(chat_create):
            response = await self._call_with_unsupported_param_retry(
                chat_create,
                self._clean_kwargs(
                    {
                        "model": self.model_name,
                        "messages": [self._chat_message_payload(message) for message in request.messages],
                        "temperature": request.temperature,
                        "max_tokens": request.max_tokens,
                        **request.provider_options,
                    }
                ),
            )
            payload = _to_dict(response)
            return LLMResponse(raw_response=payload, text=response_text(payload), extract_mode="text")

        raise RuntimeError("OpenAI client does not expose responses.create or chat.completions.create.")

    @staticmethod
    def _responses_message_payload(message: LLMMessage) -> dict[str, str]:
        return {"role": message.role, "content": message.content}

    @staticmethod
    def _chat_message_payload(message: LLMMessage) -> dict[str, str]:
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

    @staticmethod
    def _ensure_native_schema_supported(response_model: type[BaseModel]) -> None:
        issues = StrictJsonSchemaCompatibility.for_model(response_model).issues()
        if issues:
            issue = issues[0]
            raise StructuredOutputUnsupportedError(
                f"OpenAI native structured output does not support schema for "
                f"{response_model.__name__} at {issue.path}: {issue.message}"
            )

    @staticmethod
    def _is_invalid_native_schema_error(exc: Exception) -> bool:
        body = getattr(exc, "body", None)
        if isinstance(body, dict):
            error = body.get("error")
            if isinstance(error, dict) and error.get("code") == "invalid_json_schema":
                return True
        return "invalid_json_schema" in str(exc) or "Invalid schema for response_format" in str(exc)

    @classmethod
    async def _call_with_unsupported_param_retry(
        cls,
        call: Any,
        params: dict[str, Any],
    ) -> Any:
        current_params = dict(params)
        removed_params: set[str] = set()

        while True:
            try:
                return await call(**current_params)
            except Exception as exc:
                param = cls._unsupported_request_param(exc)
                if (
                    param is None
                    or param not in current_params
                    or param not in _RETRYABLE_UNSUPPORTED_PARAMS
                    or param in removed_params
                ):
                    raise
                removed_params.add(param)
                current_params = dict(current_params)
                current_params.pop(param, None)

    @staticmethod
    def _unsupported_request_param(exc: Exception) -> str | None:
        body = getattr(exc, "body", None)
        if isinstance(body, dict):
            error = body.get("error")
            if isinstance(error, dict):
                param = error.get("param")
                message = str(error.get("message") or "")
                if isinstance(param, str) and "unsupported parameter" in message.casefold():
                    return param

        text = str(exc)
        marker = "Unsupported parameter:"
        if marker not in text:
            return None
        _, _, tail = text.partition(marker)
        quote = "'" if "'" in tail else '"'
        parts = tail.split(quote)
        if len(parts) >= 3:
            return parts[1]
        return None

    @classmethod
    def _raise_for_structured_edge_case(cls, payload: dict[str, Any]) -> None:
        status = payload.get("status")
        if status == "incomplete":
            details = payload.get("incomplete_details") or {}
            reason = details.get("reason") if isinstance(details, dict) else None
            message = "OpenAI structured response was incomplete."
            if reason:
                message = f"{message} Reason: {reason}."
            raise StructuredOutputIncompleteError(message)

        refusal = cls._find_refusal(payload)
        if refusal:
            raise StructuredOutputRefusalError(f"OpenAI structured response was refused: {refusal}")

    @classmethod
    def _find_refusal(cls, value: Any) -> str | None:
        if isinstance(value, dict):
            refusal = value.get("refusal")
            if isinstance(refusal, str) and refusal.strip():
                return refusal.strip()
            if value.get("type") == "refusal":
                text = value.get("text") or value.get("content")
                return str(text or "refusal").strip()
            for child in value.values():
                found = cls._find_refusal(child)
                if found:
                    return found
        if isinstance(value, list):
            for item in value:
                found = cls._find_refusal(item)
                if found:
                    return found
        return None


__all__ = ["OpenAIProvider"]
