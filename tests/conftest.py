
import importlib.util
import sys
import types


def _find_optional_spec(module_name: str):
    try:
        return importlib.util.find_spec(module_name)
    except ModuleNotFoundError:
        return None


if _find_optional_spec("redis.asyncio") is None:
    redis_module = types.ModuleType("redis")
    redis_asyncio_module = types.ModuleType("redis.asyncio")
    redis_exceptions_module = types.ModuleType("redis.exceptions")

    class _FakeRedis:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return cls()

    class _RedisError(Exception):
        pass

    class _ResponseError(_RedisError):
        pass

    redis_asyncio_module.Redis = _FakeRedis
    redis_module.asyncio = redis_asyncio_module
    redis_exceptions_module.RedisError = _RedisError
    redis_exceptions_module.ResponseError = _ResponseError
    sys.modules["redis"] = redis_module
    sys.modules["redis.asyncio"] = redis_asyncio_module
    sys.modules["redis.exceptions"] = redis_exceptions_module


if _find_optional_spec("pyarrow") is None:
    pyarrow_module = types.ModuleType("pyarrow")
    pyarrow_ipc_module = types.ModuleType("pyarrow.ipc")
    pyarrow_parquet_module = types.ModuleType("pyarrow.parquet")

    class _FakeArrowBuffer:
        def to_pybytes(self) -> bytes:
            return b""

    class _FakeBufferOutputStream:
        def getvalue(self) -> _FakeArrowBuffer:
            return _FakeArrowBuffer()

    class _FakeArrowTable:
        num_rows = 0
        schema = None

    class _FakeStreamWriter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def write_table(self, table) -> None:
            _ = table

    def _table(*args, **kwargs):
        return _FakeArrowTable()

    def _new_stream(*args, **kwargs):
        return _FakeStreamWriter()

    def _read_table(*args, **kwargs):
        return _FakeArrowTable()

    def _write_table(*args, **kwargs) -> None:
        return None

    pyarrow_module.Table = _FakeArrowTable
    pyarrow_module.BufferOutputStream = _FakeBufferOutputStream
    pyarrow_module.table = _table
    pyarrow_module.__version__ = "0.0.0"
    pyarrow_ipc_module.new_stream = _new_stream
    pyarrow_parquet_module.read_table = _read_table
    pyarrow_parquet_module.write_table = _write_table

    sys.modules["pyarrow"] = pyarrow_module
    sys.modules["pyarrow.ipc"] = pyarrow_ipc_module
    sys.modules["pyarrow.parquet"] = pyarrow_parquet_module


if _find_optional_spec("duckdb") is None:
    duckdb_module = types.ModuleType("duckdb")

    class _FakeDuckDbConnection:
        def execute(self, *args, **kwargs):
            return self

        def fetch_arrow_table(self):
            return sys.modules["pyarrow"].table({})

        def fetchall(self):
            return []

        def close(self) -> None:
            return None

    def _connect(*args, **kwargs):
        return _FakeDuckDbConnection()

    duckdb_module.connect = _connect
    duckdb_module.DuckDBPyConnection = _FakeDuckDbConnection
    sys.modules["duckdb"] = duckdb_module


if _find_optional_spec("jose") is None:
    jose_module = types.ModuleType("jose")

    class _JWTError(Exception):
        pass

    class _JwtFacade:
        @staticmethod
        def encode(*args, **kwargs):
            return "token"

        @staticmethod
        def decode(*args, **kwargs):
            return {}

    jose_module.JWTError = _JWTError
    jose_module.jwt = _JwtFacade()
    sys.modules["jose"] = jose_module
