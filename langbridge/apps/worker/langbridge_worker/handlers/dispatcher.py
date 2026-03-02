from typing import List, Sequence, Type

from ..ioc import DependencyResolver
from langbridge.packages.messaging.langbridge_messaging.contracts import MessageEnvelope
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler


class WorkerMessageDispatcher:

    def __init__(
            self,
            dependency_resolver: DependencyResolver,
    ):
        self.handlers = self.__resolve_handlers()
        self._handler_map: dict[str, Type[BaseMessageHandler]] = {
            h.message_type: h for h in self.handlers
        }
        self.dependency_resolver = dependency_resolver

    async def handle_message(
            self,
            message: MessageEnvelope
    ) -> Sequence[MessageEnvelope] | None:
        handler_type: Type[BaseMessageHandler] = self._get_handler(message)
        handler = self._initalize_handler(handler_type)
        return await handler.handle(message.payload)

    def _get_handler(
        self,
        message: MessageEnvelope
    ) -> Type[BaseMessageHandler]:
        try:
            return self._handler_map[message.message_type]
        except KeyError:
            raise ValueError(
                f"No handler registered for message type '{message.message_type}'"
            )

    def _initalize_handler(
         self,
         handler: Type[BaseMessageHandler]   
    ) -> BaseMessageHandler:
        return self.dependency_resolver.resolve(handler)

    def __resolve_handlers(
            self
    ):
        # Import handler modules so BaseMessageHandler subclasses are registered.
        #TODO: create a more robust plugin system for handlers to avoid hardcoding imports here.
        from .jobs.agent_job_request_handler import AgentJobRequestHandler  # noqa: F401
        from .copilot.copilot_dashboard_request_handler import CopilotDashboardRequestHandler  # noqa: F401
        from .query.semantic_query_request_handler import SemanticQueryRequestHandler  # noqa: F401
        from .query.sql_job_request_handler import SqlJobRequestHandler  # noqa: F401
        from .test_message_handler import TestMessageHandler  # noqa: F401
        handlers: List[BaseMessageHandler] = BaseMessageHandler.__subclasses__()
        return handlers
