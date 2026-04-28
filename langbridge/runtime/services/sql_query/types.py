from typing import Any, Awaitable, Callable

import sqlglot

from langbridge.runtime.models import ConnectorMetadata

RewriteExpression = Callable[[sqlglot.Expression], sqlglot.Expression]
CreateSqlConnector = Callable[..., Awaitable[Any]]
ResolveConnectorConfig = Callable[[ConnectorMetadata], dict[str, Any]]
