from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
import json
import logging
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import re

from .errors import AuthError, ConnectorError, QueryValidationError
from langbridge.connectors.base.config import BaseConnectorConfig, ConnectorRuntimeType
from langbridge.connectors.base.metadata import TableMetadata, ColumnMetadata, ForeignKeyMetadata

@dataclass(slots=True)
class QueryResult:
    """
    Normalised SQL execution result.
    """

    columns: List[str]
    rows: List[List[Any]]
    rowcount: int
    elapsed_ms: int
    sql: str

    def json_safe(self) -> Dict[str, Any]:
        """Return structure suitable for JSON serialization."""
        return {
            "columns": self.columns,
            "rows": [
                [
                    _json_safe(cell)
                    for cell in row
                ]
                for row in self.rows
            ],
            "rowcount": self.rowcount,
            "elapsed_ms": self.elapsed_ms,
            "sql": self.sql,
        }


@dataclass(slots=True)
class NoSqlQueryResult:
    """
    Normalised document query result.
    """

    collection: str
    documents: List[Dict[str, Any]]
    rowcount: int
    elapsed_ms: int
    query: Dict[str, Any]

    def json_safe(self) -> Dict[str, Any]:
        return {
            "collection": self.collection,
            "documents": [_json_safe_nested(document) for document in self.documents],
            "rowcount": self.rowcount,
            "elapsed_ms": self.elapsed_ms,
            "query": _json_safe_nested(self.query),
        }


@dataclass(slots=True)
class ApiResource:
    name: str
    label: str | None = None
    primary_key: str | None = None
    parent_resource: str | None = None
    cursor_field: str | None = None
    incremental_cursor_field: str | None = None
    supports_incremental: bool = False
    default_sync_mode: str = "FULL_REFRESH"


@dataclass(slots=True)
class ApiExtractResult:
    resource: str
    records: List[Dict[str, Any]]
    status: str = "success"
    next_cursor: str | None = None
    checkpoint_cursor: str | None = None
    child_records: Dict[str, List[Dict[str, Any]]] | None = None


@dataclass(slots=True)
class ApiSyncResult:
    resource: str
    status: str
    records_synced: int = 0
    datasets_created: List[str] | None = None


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()  # datetime/date/time
        except Exception:
            pass
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def _json_safe_nested(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe_nested(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_nested(item) for item in value]
    return _json_safe(value)


SQL_COMMENT_RE = re.compile(r"--.*?$|/\*.*?\*/", re.MULTILINE | re.DOTALL)
SQL_LIMIT_RE = re.compile(r"\blimit\s+\d+", re.IGNORECASE)
SQL_COMMAND_RE = re.compile(r"^\s*(\w+)", re.IGNORECASE)

FORBIDDEN_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "merge",
    "create",
    "replace",
)

from langbridge.runtime.logger import get_root_logger
root_logger = get_root_logger()

def ensure_select_statement(sql: str) -> None:
    """Raise QueryValidationError if the SQL statement is not a SELECT."""
    stripped = SQL_COMMENT_RE.sub("", sql).strip()
    if not stripped:
        raise QueryValidationError("Empty SQL statement.")
    match = SQL_COMMAND_RE.match(stripped)
    if not match:
        raise QueryValidationError("Unable to determine SQL command.")
    command = match.group(1).lower()
    root_logger.debug("SQL command detected: %s", command)
    if command != "select" and not stripped.lower().startswith("with "):
        raise QueryValidationError(f"Only SELECT queries are permitted {sql}.")
    lowered = stripped.lower()
    # if any(keyword in lowered for keyword in FORBIDDEN_KEYWORDS):
    #     raise QueryValidationError("Query contains prohibited keywords for read-only access.")


# def apply_limit(sql: str, max_rows: Optional[int]) -> str:
#     if not max_rows or max_rows <= 0:
#         return sql
#     if SQL_LIMIT_RE.search(sql):
#         return sql
#     terminating_semicolon = ";" if sql.strip().endswith(";") else ""
#     base = sql.strip().rstrip(";")
#     return f"{base}\nLIMIT {max_rows}{terminating_semicolon}"


class Connector(ABC):
    """
    Base class for all connectors.
    """

    pass

class ManagedConnector(Connector):
    """
    Base class for managed connectors.
    """
    
    pass

class ApiConnector(Connector):
    """
    Base class for API connectors.
    """
    
    RUNTIME_TYPE: ConnectorRuntimeType | None = None
    
    def __init__(
        self,
        config: BaseConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    async def test_connection(self) -> None:
        """
        Test the API connection.
        Raises ConnectorError if the connection fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def discover_resources(self) -> List[ApiResource]:
        """
        Return the top-level resources available from the API source.
        """
        raise NotImplementedError

    @abstractmethod
    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        """
        Extract one logical resource payload from the API source.
        """
        raise NotImplementedError

    @abstractmethod
    async def sync_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiSyncResult:
        """
        Sync one logical resource into the dataset layer.
        """
        raise NotImplementedError


class NoSqlConnector(Connector):
    """
    Base class for document-oriented connectors.
    """

    RUNTIME_TYPE: ConnectorRuntimeType | None = None

    def __init__(
        self,
        config: BaseConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    async def test_connection(self) -> None:
        """
        Test the database connection.
        Raises ConnectorError if the connection fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def list_collections(self) -> List[str]:
        raise NotImplementedError

    async def query_documents(
        self,
        *,
        collection: str,
        query: Mapping[str, Any] | None = None,
        projection: Sequence[str] | Mapping[str, int | bool] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
        limit: int | None = 100,
    ) -> NoSqlQueryResult:
        self.logger.debug(
            "Executing document query (collection=%s limit=%s query=%s)",
            collection,
            limit,
            query,
        )
        start = time.perf_counter()
        try:
            documents = await self._query_documents(
                collection=collection,
                query=query,
                projection=projection,
                sort=sort,
                limit=limit,
            )
        except AuthError:
            raise
        except PermissionError:
            raise
        except TimeoutError:
            raise
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"Document query failed: {exc}") from exc

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        normalized_documents = [
            _json_safe_nested(document) for document in documents
        ]
        rowcount = len(normalized_documents)
        self.logger.debug(
            "Document query completed (collection=%s rows=%s elapsed_ms=%s)",
            collection,
            rowcount,
            elapsed_ms,
        )
        return NoSqlQueryResult(
            collection=collection,
            documents=normalized_documents,
            rowcount=rowcount,
            elapsed_ms=elapsed_ms,
            query=dict(query or {}),
        )

    @abstractmethod
    async def _query_documents(
        self,
        *,
        collection: str,
        query: Mapping[str, Any] | None = None,
        projection: Sequence[str] | Mapping[str, int | bool] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
        limit: int | None = 100,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError


class VecotorDBConnector(Connector):
    """
    Base class for Vector DB connectors.
    """

    RUNTIME_TYPE: ConnectorRuntimeType | None = None
    
    def __init__(
        self,
        config: BaseConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    async def test_connection(self) -> None:
        """
        Test the Vector DB connection.
        Raises ConnectorError if the connection fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_vectors(
        self,
        vectors: Sequence[Sequence[float]],
        *,
        metadata: Optional[Sequence[Any]] = None,
    ) -> List[int]:
        """Add or replace vector entries in the index and return their ids."""
        raise NotImplementedError

    @abstractmethod
    async def search(
        self,
        vector: Sequence[float],
        *,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Search similar vectors and return match metadata payloads."""
        raise NotImplementedError

class SqlConnector(Connector):
    """
    Base class for SQL connectors.
    """

    RUNTIME_TYPE: ConnectorRuntimeType | None = None
    SQLGLOT_DIALECT: str = "tsql"
    EXPRESSION_REWRITE: bool = False
    
    def __init__(
        self,
        config: BaseConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    async def test_connection(self) -> None:
        """
        Test the database connection.
        Raises ConnectorError if the connection fails.
        """
        raise NotImplementedError
    
    def test_connection_sync(self) -> None:
        """
        Test the database connection.
        Raises ConnectorError if the connection fails.
        """
        try:
            # Run the async test_connection in a fresh event loop
            asyncio.run(self.test_connection())
        except ConnectorError as e:
            raise e
        except Exception as e:
            raise ConnectorError(f"Connection test failed: {e}") from e

    @abstractmethod
    async def fetch_schemas(self) -> List[str]:
        raise NotImplementedError
    
    def fetch_schemas_sync(self) -> List[str]:
        try:
            return asyncio.run(self.fetch_schemas())
        except Exception as e:
            raise ConnectorError(f"Fetch schemas failed: {e}") from e

    @abstractmethod
    async def fetch_tables(self, schema:str) -> List[str]:
        raise NotImplementedError
    
    def fetch_tables_sync(self, schema:str) -> List[str]:
        try:
            return asyncio.run(self.fetch_tables(schema))
        except Exception as e:
            raise ConnectorError(f"Fetch tables failed: {e}") from e
        
    @abstractmethod
    async def fetch_table_metadata(self, schema:str, table:str) -> TableMetadata:
        raise NotImplementedError
    
    def fetch_table_metadata_sync(self, schema:str, table:str) -> TableMetadata:
        try:
            return asyncio.run(self.fetch_table_metadata(schema, table))
        except Exception as e:
            raise ConnectorError(f"Fetch table metadata failed: {e}") from e
    
    @abstractmethod
    async def fetch_columns(self, schema:str, table:str) -> List[ColumnMetadata]:
        raise NotImplementedError
    
    def fetch_columns_sync(self, schema:str, table:str) -> List[ColumnMetadata]:
        try:
            return asyncio.run(self.fetch_columns(schema, table))
        except Exception as e:
            raise ConnectorError(f"Fetch columns failed: {e}") from e
        
    @abstractmethod
    async def fetch_foreign_keys(self, schema:str, table:str) -> List[ForeignKeyMetadata]:
        raise NotImplementedError
    
    # @abstractmethod
    # def rewrite_expression(self, expression: "sqlglot.Expression") -> "sqlglot.Expression":
    #     """
    #     Rewrite a sqlglot Expression to be compatible with the target SQL dialect.
    #     Must be implemented by subclasses that support expression rewriting.
    #     """
    #     raise NotImplementedError
    
    def fetch_foreign_keys_sync(self, schema:str, table:str) -> List[ForeignKeyMetadata]:
        try:
            return asyncio.run(self.fetch_foreign_keys(schema, table))
        except Exception as e:
            raise ConnectorError(f"Fetch foreign keys failed: {e}") from e

    
    async def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        max_rows: Optional[int] = 5000,
        timeout_s: Optional[int] = 30,
    ) -> QueryResult:
        ensure_select_statement(sql)
        # prepared_sql = apply_limit(sql, max_rows)
        
        self.logger.debug(
            "Executing SQL (max_rows=%s timeout_s=%s): %s",
            max_rows,
            timeout_s,
            sql,
        )
        
        start = time.perf_counter()
        try:
            columns, rows = await self._execute_select(
                sql,
                params or {},
                timeout_s=timeout_s,
            )
        except QueryValidationError:
            raise
        except AuthError:
            raise
        except PermissionError:
            raise
        except TimeoutError:
            raise
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"Execution failed: {exc}") from exc

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        rowcount = len(rows)
        self.logger.debug(
            "Execution completed (rows=%s elapsed_ms=%s)", rowcount, elapsed_ms
        )
        return QueryResult(columns=columns, rows=rows, rowcount=rowcount, elapsed_ms=elapsed_ms, sql=sql)

    @abstractmethod
    async def _execute_select(
        self,
        sql: str,
        params: Dict[str, Any],
        *,
        timeout_s: Optional[int] = 30,
    ) -> Tuple[List[str], List[List[Any]]]:
        """
        Execute a SELECT query and return the results.
        Must be implemented by subclasses.
        """
        raise NotImplementedError

class ManagedVectorDB(VecotorDBConnector):
    """
    Base class for managed Vector DB connectors.
    """
    
    @staticmethod
    @abstractmethod
    async def create_managed_instance(
        kwargs: Any,
        logger: Optional[logging.Logger] = None,
    ) -> "ManagedVectorDB":
        """Create and return a new managed vector DB instance."""
        raise NotImplementedError
    
    @abstractmethod
    async def create_index(
        self,
        dimension: int,
        *,
        metric: str = "cosine",
    ) -> None:
        """Create a new vector index."""
        raise NotImplementedError
    
    @abstractmethod
    async def delete_index(self) -> None:
        """Delete the vector index."""
        raise NotImplementedError
    

class StorageConnector(Connector):
    """
    Base class for storage connectors.
    """
    RUNTIME_TYPE: ConnectorRuntimeType | None = None

    def __init__(
        self,
        config: BaseConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    async def list_buckets(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    async def list_objects(self, bucket: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    async def get_object(self, bucket: str, key: str) -> bytes:
        raise NotImplementedError

    async def configure_duckdb_connection(
        self,
        connection: Any,
        *,
        storage_uris: Sequence[str],
        options: Mapping[str, Any] | None = None,
    ) -> None:
        """Allow storage backends to prepare a DuckDB connection for remote scans."""
        return None

    async def resolve_duckdb_scan_uris(
        self,
        storage_uris: Sequence[str],
        *,
        options: Mapping[str, Any] | None = None,
    ) -> List[str]:
        """Allow storage backends to translate logical object URIs into DuckDB-readable paths."""
        return [
            str(storage_uri or "").strip()
            for storage_uri in storage_uris
            if str(storage_uri or "").strip()
        ]

class ManagedStorageConnector(StorageConnector):
    """
    Base class for managed storage connectors.
    """
    
    @staticmethod
    @abstractmethod
    async def create_managed_instance(
        kwargs: Any,
        logger: Optional[logging.Logger] = None,
    ) -> "ManagedStorageConnector":
        """Create and return a new managed storage connector instance."""
        raise NotImplementedError

    @abstractmethod
    async def create_bucket(self, bucket_name: str) -> None:
        """Create a new storage bucket."""
        raise NotImplementedError

    @abstractmethod
    async def delete_bucket(self, bucket_name: str) -> None:
        """Delete a storage bucket."""
        raise NotImplementedError

    @abstractmethod
    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from a storage bucket."""
        raise NotImplementedError

    @abstractmethod
    async def upload_object(self, bucket: str, key: str, data: bytes) -> None:
        """Upload an object to a storage bucket."""
        raise NotImplementedError    

    @abstractmethod
    async def update_object(self, bucket: str, key: str, data: bytes) -> None:
        """Update an existing object in a storage bucket."""
        raise NotImplementedError

async def run_sync(fn, *args, **kwargs):
    """
    Run blocking call in default thread pool.
    """

    return await asyncio.to_thread(fn, *args, **kwargs)
