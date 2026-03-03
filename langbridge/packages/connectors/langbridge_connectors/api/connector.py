from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
from enum import Enum
import json
import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import re

import sqlglot

from langbridge.packages.common.langbridge_common.errors.connector_errors import AuthError, ConnectorError, QueryValidationError
from langbridge.packages.connectors.langbridge_connectors.api.config import BaseConnectorConfig, ConnectorRuntimeType
from langbridge.packages.connectors.langbridge_connectors.api.metadata import TableMetadata, ColumnMetadata, ForeignKeyMetadata


class ConnectorType(Enum):
    SQL = "SQL"
    NO_SQL = "NO_SQL"
    VECTOR_DB = "VECTOR_DB"

class SqlDialetcs(Enum):
    POSTGRES = "POSTGRES"
    MYSQL = "MYSQL"
    MARIADB = "MARIADB"
    MONGODB = "MONGODB"
    SNOWFLAKE = "SNOWFLAKE"
    REDSHIFT = "REDSHIFT"
    BIGQUERY = "BIGQUERY"
    SQLSERVER = "SQLSERVER"
    ORACLE = "ORACLE"
    SQLITE = "SQLITE"
    TRINO = "TRINO"

class VectorDBType(Enum):
    FAISS = "FAISS"
    PINECONE = "PINECONE"
    WEAVIATE = "WEAVIATE"
    MILVUS = "MILVUS"
    QDRANT = "QDRANT"

class VectorType(Enum):
    EMBEDDING = "EMBEDDING"
    IMAGE = "IMAGE"
    AUDIO = "AUDIO"
    VIDEO = "VIDEO"
    
    
# TODO: remove this mapping after refactoring ConnectorType to ConnectorRuntimeType
ConnectorRuntimeTypeSqlDialectMap: Dict[ConnectorRuntimeType, SqlDialetcs] = {
    ConnectorRuntimeType.POSTGRES: SqlDialetcs.POSTGRES,
    ConnectorRuntimeType.MYSQL: SqlDialetcs.MYSQL,
    ConnectorRuntimeType.MARIADB: SqlDialetcs.MARIADB,
    ConnectorRuntimeType.MONGODB: SqlDialetcs.MONGODB,
    ConnectorRuntimeType.SNOWFLAKE: SqlDialetcs.SNOWFLAKE,
    ConnectorRuntimeType.REDSHIFT: SqlDialetcs.REDSHIFT,
    ConnectorRuntimeType.BIGQUERY: SqlDialetcs.BIGQUERY,
    ConnectorRuntimeType.SQLSERVER: SqlDialetcs.SQLSERVER,
    ConnectorRuntimeType.ORACLE: SqlDialetcs.ORACLE,
    ConnectorRuntimeType.SQLITE: SqlDialetcs.SQLITE,
    ConnectorRuntimeType.TRINO: SqlDialetcs.TRINO,
}

ConnectorRuntimeTypeVectorDBMap: Dict[ConnectorRuntimeType, VectorDBType] = {
    ConnectorRuntimeType.FAISS: VectorDBType.FAISS,
    ConnectorRuntimeType.QDRANT: VectorDBType.QDRANT,
}

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

from langbridge.packages.common.langbridge_common.logging.logger import get_root_logger
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
    if any(keyword in lowered for keyword in FORBIDDEN_KEYWORDS):
        raise QueryValidationError("Query contains prohibited keywords for read-only access.")


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
    
    CONNECTOR_TYPE: ConnectorType = ConnectorType.NO_SQL
    
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
    
class VecotorDBConnector(Connector):
    """
    Base class for Vector DB connectors.
    """
        
    CONNECTOR_TYPE: ConnectorType = ConnectorType.VECTOR_DB
    VECTOR_DB_TYPE: VectorDBType
    
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
    
    CONNECTOR_TYPE: ConnectorType = ConnectorType.SQL
    DIALECT: SqlDialetcs
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
    
    

async def run_sync(fn, *args, **kwargs):
    """
    Run blocking call in default thread pool.
    """

    return await asyncio.to_thread(fn, *args, **kwargs)
