"""
Analyst agent that selects an analytical context and executes it through federation.
"""


import asyncio
import logging
from typing import Any, Optional, Sequence

from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.tools.sql_analyst.interfaces import (
    AnalystQueryRequest,
    AnalystQueryResponse,
)
from langbridge.orchestrator.tools.sql_analyst.tool import SqlAnalystTool
from .selector import AnalyticalContextSelector


class AnalystAgent:
    """
    Route an analytical question to the most relevant dataset-backed or
    semantic-model-backed federated analytical context.
    """

    def __init__(
        self,
        llm: LLMProvider,
        tools: Sequence[SqlAnalystTool],
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.llm = llm
        self._tools = list(tools)
        if not self._tools:
            raise ValueError("At least one analytical tool must be provided to AnalystAgent.")
        self.selector = AnalyticalContextSelector(self.llm, self._tools)
        self.logger = logger or logging.getLogger(__name__)

    def answer(
        self,
        question: str,
        *,
        conversation_context: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> AnalystQueryResponse:
        request = AnalystQueryRequest(
            question=question,
            conversation_context=conversation_context,
            filters=filters,
            limit=limit if limit is not None else 1000,
        )
        return self.answer_with_request(request)

    def answer_with_request(self, request: AnalystQueryRequest) -> AnalystQueryResponse:
        tool = self.selector.select(request)
        self.logger.info(
            "AnalystAgent selected %s analytical asset '%s'",
            tool.asset_type,
            tool.name,
        )
        return tool.run(request)

    async def answer_async(
        self,
        question: str,
        *,
        conversation_context: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> AnalystQueryResponse:
        request = AnalystQueryRequest(
            question=question,
            conversation_context=conversation_context,
            filters=filters,
            limit=limit if limit is not None else 1000,
        )
        tool = await asyncio.to_thread(self.selector.select, request)
        self.logger.info(
            "AnalystAgent selected %s analytical asset '%s'",
            tool.asset_type,
            tool.name,
        )
        return await tool.arun(request)


__all__ = ["AnalystAgent"]
