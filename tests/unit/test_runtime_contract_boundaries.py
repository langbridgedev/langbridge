from __future__ import annotations

from pathlib import Path
import tokenize

from langbridge.contracts.base import _Base
from langbridge.contracts.datasets import DatasetListResponse
from langbridge.contracts.jobs.agent_job import AgentJobStateResponse
from langbridge.contracts.jobs.dataset_job import CreateDatasetPreviewJobRequest
from langbridge.contracts.jobs.semantic_query_job import CreateSemanticQueryJobRequest
from langbridge.contracts.jobs.sql_job import CreateSqlJobRequest
from langbridge.contracts.llm_connections import LLMProvider
from langbridge.contracts.semantic.semantic_query import (
    SemanticQueryMetaResponse,
    SemanticQueryRequest,
)
from langbridge.contracts.sql import SqlExecuteRequest
from langbridge.contracts.threads import ThreadResponse
from langbridge.packages.contracts.datasets import (
    DatasetListResponse as LegacyPackageDatasetListResponse,
)
from langbridge.packages.contracts.jobs.dataset_job import (
    CreateDatasetPreviewJobRequest as LegacyPackageCreateDatasetPreviewJobRequest,
)
from langbridge.packages.contracts.jobs.semantic_query_job import (
    CreateSemanticQueryJobRequest as LegacyPackageCreateSemanticQueryJobRequest,
)
from langbridge.packages.contracts.jobs.sql_job import (
    CreateSqlJobRequest as LegacyPackageCreateSqlJobRequest,
)
from langbridge.packages.contracts.semantic.semantic_query import (
    SemanticQueryMetaResponse as LegacyPackageSemanticQueryMetaResponse,
    SemanticQueryRequest as LegacyPackageSemanticQueryRequest,
)
from langbridge.packages.contracts.sql import SqlExecuteRequest as LegacyPackageSqlExecuteRequest
from langbridge.packages.contracts.threads import ThreadResponse as LegacyPackageThreadResponse
from langbridge.packages.common.langbridge_common.contracts.datasets import (
    DatasetListResponse as LegacyCommonDatasetListResponse,
)
from langbridge.packages.common.langbridge_common.contracts.jobs import (
    CreateDatasetPreviewJobRequest as LegacyCommonCreateDatasetPreviewJobRequestFromPackage,
    CreateSemanticQueryJobRequest as LegacyCommonCreateSemanticQueryJobRequestFromPackage,
    CreateSqlJobRequest as LegacyCommonCreateSqlJobRequestFromPackage,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.semantic_query_job import (
    CreateSemanticQueryJobRequest as LegacyCommonCreateSemanticQueryJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticQueryMetaResponse as LegacyCommonSemanticQueryMetaResponseFromPackage,
    SemanticQueryRequest as LegacyCommonSemanticQueryRequestFromPackage,
)
from langbridge.packages.common.langbridge_common.contracts.sql import (
    SqlExecuteRequest as LegacyCommonSqlExecuteRequest,
)
from langbridge.packages.common.langbridge_common.contracts.threads import (
    ThreadResponse as LegacyCommonThreadResponse,
)


def test_core_runtime_services_do_not_import_common_utils_or_errors() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    runtime_files = [
        repo_root / "langbridge/packages/runtime/services/sql_query_service.py",
        repo_root / "langbridge/packages/runtime/services/semantic_query_execution_service.py",
        repo_root / "langbridge/packages/runtime/services/dataset_query_service.py",
        repo_root / "langbridge/packages/runtime/services/dataset_sync_service.py",
        repo_root / "langbridge/packages/runtime/services/agent_execution_service.py",
        repo_root / "langbridge/packages/runtime/execution/federated_query_tool.py",
    ]
    forbidden_imports = (
        "langbridge.packages.common.langbridge_common.utils",
        "langbridge.packages.common.langbridge_common.errors",
    )

    for path in runtime_files:
        source = path.read_text(encoding="utf-8")
        for forbidden in forbidden_imports:
            assert forbidden not in source, f"{path} still imports {forbidden}"


def test_selected_contract_modules_are_owned_by_root_langbridge_contracts_namespace() -> None:
    assert _Base.__module__ == "langbridge.contracts.base"
    assert DatasetListResponse.__module__ == "langbridge.contracts.datasets"
    assert SqlExecuteRequest.__module__ == "langbridge.contracts.sql"
    assert ThreadResponse.__module__ == "langbridge.contracts.threads"
    assert LLMProvider.__module__ == "langbridge.contracts.llm_connections"
    assert AgentJobStateResponse.__module__ == "langbridge.contracts.jobs.agent_job"
    assert CreateSqlJobRequest.__module__ == "langbridge.contracts.jobs.sql_job"
    assert (
        CreateDatasetPreviewJobRequest.__module__
        == "langbridge.contracts.jobs.dataset_job"
    )
    assert SemanticQueryRequest.__module__ == "langbridge.contracts.semantic.semantic_query"
    assert SemanticQueryMetaResponse.__module__ == "langbridge.contracts.semantic.semantic_query"
    assert (
        CreateSemanticQueryJobRequest.__module__
        == "langbridge.contracts.jobs.semantic_query_job"
    )


def test_package_contract_compatibility_imports_resolve_to_root_langbridge_contracts_modules() -> None:
    assert LegacyPackageDatasetListResponse is DatasetListResponse
    assert LegacyPackageSqlExecuteRequest is SqlExecuteRequest
    assert LegacyPackageThreadResponse is ThreadResponse
    assert (
        LegacyPackageCreateSqlJobRequest is CreateSqlJobRequest
    )
    assert (
        LegacyPackageCreateDatasetPreviewJobRequest is CreateDatasetPreviewJobRequest
    )
    assert (
        LegacyPackageCreateSemanticQueryJobRequest is CreateSemanticQueryJobRequest
    )
    assert (
        LegacyPackageSemanticQueryRequest is SemanticQueryRequest
    )
    assert (
        LegacyPackageSemanticQueryMetaResponse is SemanticQueryMetaResponse
    )


def test_common_contract_compatibility_imports_resolve_to_root_langbridge_contracts_modules() -> None:
    assert LegacyCommonDatasetListResponse.__module__ == "langbridge.contracts.datasets"
    assert LegacyCommonSqlExecuteRequest.__module__ == "langbridge.contracts.sql"
    assert LegacyCommonThreadResponse.__module__ == "langbridge.contracts.threads"
    assert (
        LegacyCommonCreateSemanticQueryJobRequest.__module__
        == "langbridge.contracts.jobs.semantic_query_job"
    )
    assert (
        LegacyCommonCreateSqlJobRequestFromPackage.__module__
        == "langbridge.contracts.jobs.sql_job"
    )
    assert (
        LegacyCommonCreateDatasetPreviewJobRequestFromPackage.__module__
        == "langbridge.contracts.jobs.dataset_job"
    )
    assert (
        LegacyCommonCreateSemanticQueryJobRequestFromPackage.__module__
        == "langbridge.contracts.jobs.semantic_query_job"
    )
    assert (
        LegacyCommonSemanticQueryRequestFromPackage.__module__
        == "langbridge.contracts.semantic.semantic_query"
    )
    assert (
        LegacyCommonSemanticQueryMetaResponseFromPackage.__module__
        == "langbridge.contracts.semantic.semantic_query"
    )


def test_owned_contract_areas_prefer_root_langbridge_contracts_imports() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    forbidden_imports = (
        "langbridge.packages.contracts.base",
        "langbridge.packages.contracts.llm_connections",
        "langbridge.packages.contracts.datasets",
        "langbridge.packages.contracts.sql",
        "langbridge.packages.contracts.threads",
        "langbridge.packages.contracts.semantic.semantic_query",
        "langbridge.packages.contracts.jobs.type",
        "langbridge.packages.contracts.jobs.agent_job",
        "langbridge.packages.contracts.jobs.sql_job",
        "langbridge.packages.contracts.jobs.dataset_job",
        "langbridge.packages.contracts.jobs.semantic_query_job",
        "langbridge.packages.common.langbridge_common.contracts.base",
        "langbridge.packages.common.langbridge_common.contracts.llm_connections",
        "langbridge.packages.common.langbridge_common.contracts.datasets",
        "langbridge.packages.common.langbridge_common.contracts.sql",
        "langbridge.packages.common.langbridge_common.contracts.threads",
        "langbridge.packages.common.langbridge_common.contracts.semantic.semantic_query",
        "langbridge.packages.common.langbridge_common.contracts.jobs.type",
        "langbridge.packages.common.langbridge_common.contracts.jobs.agent_job",
        "langbridge.packages.common.langbridge_common.contracts.jobs.sql_job",
        "langbridge.packages.common.langbridge_common.contracts.jobs.dataset_job",
        "langbridge.packages.common.langbridge_common.contracts.jobs.semantic_query_job",
    )
    allowed_paths = {
        repo_root / "langbridge/packages/contracts",
        repo_root / "langbridge/packages/common/langbridge_common/contracts",
        repo_root / "tests/unit/test_runtime_contract_boundaries.py",
    }

    for path in repo_root.rglob("*.py"):
        if any(str(path).startswith(str(allowed)) for allowed in allowed_paths):
            continue
        with tokenize.open(path) as handle:
            source = handle.read()
        for forbidden in forbidden_imports:
            assert forbidden not in source, f"{path} still imports {forbidden}"


def test_contract_base_keeps_alias_and_json_behavior() -> None:
    class ExamplePayload(_Base):
        example_value: int

    payload = ExamplePayload.model_validate({"exampleValue": 7})

    assert payload.example_value == 7
    assert payload.model_dump(by_alias=True) == {"exampleValue": 7}
    assert '"example_value":7' in payload.dict_json()
