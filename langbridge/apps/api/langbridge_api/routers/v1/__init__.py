from typing import List

from fastapi import APIRouter

from .agents import router as agents_router
from .auth import router as auth_router
from .organizations import router as organizations_router
from .connectors import router as connectors_router
from .semantic_models import router as semantic_model_router
from .threads import router as threads_router
from .semantic_query import router as semantic_query_router
from .bi_dashboards import router as bi_dashboards_router
from .copilot import router as copilot_router
from .messages import router as messages_router
from .jobs import router as jobs_router
from .runtimes import router as runtimes_router
from .edge_tasks import router as edge_tasks_router
from .sql import router as sql_router

v1_routes: List[APIRouter] = [
    auth_router,
    agents_router,
    organizations_router,
    connectors_router,
    semantic_model_router,
    threads_router,
    semantic_query_router,
    bi_dashboards_router,
    copilot_router,
    messages_router,
    jobs_router,
    runtimes_router,
    edge_tasks_router,
    sql_router,
]

__all__ = [
    "auth_router",
    "agents_router",
    "organizations_router",
    "connectors_router",
    "semantic_model_router",
    "threads_router",
    "semantic_query_router",
    "bi_dashboards_router",
    "copilot_router",
    "messages_router",
    "jobs_router",
    "runtimes_router",
    "edge_tasks_router",
    "sql_router",
    "v1_routes",
]
