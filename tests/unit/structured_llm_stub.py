from typing import Any

from langbridge.ai.llm import LLMInvocation, LLMResponse


class StructuredTextLLMStub:
    """Adapts prompt-returning test doubles to the request-based LLMProvider contract."""

    async def ainvoke(self, request: Any) -> LLMInvocation:
        prompt = "\n\n".join(str(message.content) for message in request.messages)
        text = await self.acomplete(
            prompt,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
        )
        response_model = request.response_model
        parsed = response_model.model_validate_json(text) if response_model is not None else None
        return LLMInvocation(
            request=request,
            response=LLMResponse(
                raw_response={"text": text},
                text=text,
                parsed=parsed,
                response_model_name=response_model.__name__ if response_model is not None else None,
                extract_mode="json_extractor" if response_model is not None else "text",
            ),
        )
