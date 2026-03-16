from __future__ import annotations

import inspect
from typing import Type

from dependency_injector import providers
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler

from .container import WorkerContainer


class DependencyResolver:
    def __init__(self, container: WorkerContainer) -> None:
        self._container = container

    def resolve(self, handler: Type[BaseMessageHandler]) -> BaseMessageHandler:
        kwargs: dict[str, object] = {}
        signature = inspect.signature(handler.__init__)
        for name, param in signature.parameters.items():
            if name == "self":
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            provider = getattr(self._container, name, None)
            if provider is None or not isinstance(provider, providers.Provider):
                continue
            try:
                kwargs[name] = provider()
            except Exception:
                continue
        return handler(**kwargs)
