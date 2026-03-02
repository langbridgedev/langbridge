from enum import Enum


class JobType(str, Enum):
    AGENT = "agent"
    SEMANTIC_QUERY = "semantic_query"
    COPILOT_DASHBOARD = "copilot_dashboard"
    SQL = "sql"
