
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .agents import AgentApplication
from .connectors import ConnectorApplication
from .datasets import DatasetApplication
from .jobs import JobApplication
from .llm_connections import LLMConnectionApplication
from .semantic import SemanticApplication
from .sql import SqlApplication
from .threads import ThreadApplication

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


@dataclass(slots=True)
class ConfiguredRuntimeApplications:
    datasets: DatasetApplication
    semantic: SemanticApplication
    sql: SqlApplication
    jobs: JobApplication
    agents: AgentApplication
    llm_connections: LLMConnectionApplication
    threads: ThreadApplication
    connectors: ConnectorApplication


def build_runtime_applications(host: "ConfiguredLocalRuntimeHost") -> ConfiguredRuntimeApplications:
    return ConfiguredRuntimeApplications(
        datasets=DatasetApplication(host),
        semantic=SemanticApplication(host),
        sql=SqlApplication(host),
        jobs=JobApplication(host),
        agents=AgentApplication(host),
        llm_connections=LLMConnectionApplication(host),
        threads=ThreadApplication(host),
        connectors=ConnectorApplication(host),
    )
