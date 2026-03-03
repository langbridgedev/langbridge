from enum import Enum


class JobType(str, Enum):
    AGENT = "agent"
    SEMANTIC_QUERY = "semantic_query"
    AGENTIC_SEMANTIC_MODEL = "agentic_semantic_model"
    COPILOT_DASHBOARD = "copilot_dashboard"
    SQL = "sql"
