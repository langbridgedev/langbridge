"""Agent wrapper that exposes the semantic query builder copilot tool."""


import logging
from typing import Optional

from langbridge.orchestrator.tools.semantic_query_builder import (
    QueryBuilderCopilotRequest,
    QueryBuilderCopilotResponse,
    SemanticQueryBuilderCopilotTool,
)


class BICopilotAgent:
    """Business intelligence copilot that mediates between the UI and the copilot tool."""

    def __init__(
        self,
        tool: SemanticQueryBuilderCopilotTool,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._tool = tool
        self._logger = logger or logging.getLogger(__name__)

    def assist(self, request: QueryBuilderCopilotRequest) -> QueryBuilderCopilotResponse:
        """Synchronously invoke the copilot."""

        self._logger.info(
            "BI Copilot handling request for semantic model %s", request.semantic_model_id
        )
        return self._tool.run(request)

    async def assist_async(
        self, request: QueryBuilderCopilotRequest
    ) -> QueryBuilderCopilotResponse:
        """Asynchronously invoke the copilot."""

        self._logger.info(
            "BI Copilot (async) handling request for semantic model %s",
            request.semantic_model_id,
        )
        return await self._tool.arun(request)


__all__ = ["BICopilotAgent"]
