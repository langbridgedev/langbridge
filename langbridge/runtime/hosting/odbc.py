from __future__ import annotations

import asyncio
import logging
import os
import re
import struct
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import duckdb
import sqlglot
from sqlglot import exp

from langbridge.runtime.bootstrap import ConfiguredLocalRuntimeHost
from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.hosting.auth import RuntimeAuthConfig, RuntimeAuthMode
from langbridge.runtime.models.jobs import CreateSqlJobRequest, SqlWorkbenchMode

_LOGGER = logging.getLogger(__name__)
_PROTO_VERSION = 196608
_SSL_REQUEST_CODE = 80877103
_CANCEL_REQUEST_CODE = 80877102
_PUBLIC_SCHEMA = "public"
_DATABASE_NAME = "langbridge"
_SERVER_VERSION = "16.0-langbridge"
_DEFAULT_MAX_ROWS = 10000
_DEFAULT_PORT = 15432
_PUBLIC_SCHEMA_RE = re.compile(r'(?i)(?<![A-Za-z0-9_"])public\s*\.')
_POSITIONAL_PARAM_RE = re.compile(r"\$([1-9][0-9]*)\b")
_SYSTEM_CATALOG_TABLES = {
    "pg_am",
    "pg_attribute",
    "pg_attrdef",
    "pg_class",
    "pg_constraint",
    "pg_database",
    "pg_description",
    "pg_enum",
    "pg_extension",
    "pg_index",
    "pg_namespace",
    "pg_operator",
    "pg_proc",
    "pg_range",
    "pg_roles",
    "pg_sequence",
    "pg_tables",
    "pg_type",
    "pg_views",
}

_PG_TYPE_OIDS = {
    "bool": 16,
    "bytea": 17,
    "int8": 20,
    "int2": 21,
    "int4": 23,
    "text": 25,
    "oid": 26,
    "float4": 700,
    "float8": 701,
    "varchar": 1043,
    "date": 1082,
    "time": 1083,
    "timestamp": 1114,
    "timestamptz": 1184,
    "numeric": 1700,
}

_DUCKDB_TYPE_MAP = {
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "int": "INTEGER",
    "int2": "SMALLINT",
    "int4": "INTEGER",
    "int8": "BIGINT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "number": "DOUBLE",
    "float": "DOUBLE",
    "float4": "FLOAT",
    "float8": "DOUBLE",
    "double": "DOUBLE",
    "double precision": "DOUBLE",
    "real": "REAL",
    "numeric": "DECIMAL(38, 10)",
    "decimal": "DECIMAL(38, 10)",
    "date": "DATE",
    "time": "TIME",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    "datetime": "TIMESTAMP",
    "string": "VARCHAR",
    "text": "VARCHAR",
    "varchar": "VARCHAR",
    "char": "VARCHAR",
    "uuid": "VARCHAR",
    "json": "VARCHAR",
}


@dataclass(slots=True, frozen=True)
class RuntimeOdbcEndpointConfig:
    host: str = "127.0.0.1"
    port: int = _DEFAULT_PORT
    database_name: str = _DATABASE_NAME
    schema_name: str = _PUBLIC_SCHEMA
    max_rows: int = _DEFAULT_MAX_ROWS

    @classmethod
    def from_env(
        cls,
        *,
        host: str | None = None,
        port: int | None = None,
        max_rows: int | None = None,
    ) -> "RuntimeOdbcEndpointConfig":
        resolved_host = (
            str(host or os.getenv("LANGBRIDGE_RUNTIME_ODBC_HOST") or "127.0.0.1").strip()
            or "127.0.0.1"
        )
        resolved_port = int(port or int(os.getenv("LANGBRIDGE_RUNTIME_ODBC_PORT", str(_DEFAULT_PORT))))
        resolved_max_rows = int(
            max_rows or int(os.getenv("LANGBRIDGE_RUNTIME_ODBC_MAX_ROWS", str(_DEFAULT_MAX_ROWS)))
        )
        return cls(
            host=resolved_host,
            port=resolved_port,
            max_rows=max(1, resolved_max_rows),
        )


@dataclass(slots=True)
class RuntimeOdbcQueryResult:
    columns: list[dict[str, Any]]
    rows: list[tuple[Any, ...]]
    command_tag: str = "SELECT 0"


@dataclass(slots=True)
class _PreparedStatement:
    name: str
    query: str
    parameter_count: int


@dataclass(slots=True)
class _BoundPortal:
    name: str
    statement_name: str
    parameters: list[Any]
    result_formats: list[int]


class RuntimeOdbcQueryGateway:
    """Route BI-facing SQL to the federated runtime path."""

    def __init__(
        self,
        *,
        runtime_host: ConfiguredLocalRuntimeHost,
        context: RuntimeContext,
        config: RuntimeOdbcEndpointConfig | None = None,
    ) -> None:
        self._runtime_host = runtime_host
        self._base_context = context
        self._config = config or RuntimeOdbcEndpointConfig()

    async def execute(
        self,
        query: str,
        *,
        parameters: list[Any] | None = None,
        max_rows: int | None = None,
    ) -> RuntimeOdbcQueryResult:
        normalized_query = str(query or "").strip().rstrip(";")
        if not normalized_query:
            return RuntimeOdbcQueryResult(columns=[], rows=[], command_tag="EMPTY")

        rendered_query = self._render_query_with_parameters(
            query=normalized_query,
            parameters=parameters or [],
        )
        if self._is_session_command(rendered_query):
            return self._execute_session_command(rendered_query)
        if self._is_metadata_query(rendered_query):
            return await self._execute_metadata_query(rendered_query)

        runtime_query = self._normalize_runtime_query(rendered_query)
        effective_max_rows = self._effective_limit(max_rows=max_rows)
        scoped_host = self._runtime_host.with_context(
            RuntimeContext.build(
                workspace_id=self._base_context.workspace_id,
                actor_id=self._base_context.actor_id,
                roles=self._base_context.roles,
                request_id=str(uuid.uuid4()),
            )
        )
        payload = await scoped_host.execute_sql(
            request=CreateSqlJobRequest(
                sql_job_id=uuid.uuid4(),
                workspace_id=scoped_host.context.workspace_id,
                actor_id=scoped_host.context.actor_id,
                workbench_mode=SqlWorkbenchMode.dataset,
                execution_mode="federated",
                query=runtime_query,
                query_dialect="postgres",
                params={},
                requested_limit=effective_max_rows,
                requested_timeout_seconds=30,
                enforced_limit=effective_max_rows,
                enforced_timeout_seconds=30,
                allow_dml=False,
                allow_federation=True,
                selected_datasets=[],
                explain=False,
                correlation_id=scoped_host.context.request_id,
            )
        )
        columns = self._normalize_columns(payload.get("columns"), payload.get("rows"))
        rows = self._normalize_rows(columns=columns, rows=payload.get("rows"))
        return RuntimeOdbcQueryResult(
            columns=columns,
            rows=rows,
            command_tag=f"SELECT {len(rows)}",
        )

    async def describe(
        self,
        query: str,
        *,
        parameters: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        result = await self.execute(
            query,
            parameters=parameters,
            max_rows=1,
        )
        return list(result.columns)

    def _effective_limit(self, *, max_rows: int | None) -> int:
        if max_rows is None or max_rows <= 0:
            return self._config.max_rows
        return max(1, min(int(max_rows), self._config.max_rows))

    @staticmethod
    def _render_query_with_parameters(*, query: str, parameters: list[Any]) -> str:
        if not parameters:
            return query

        def _replace(match: re.Match[str]) -> str:
            index = int(match.group(1)) - 1
            if index < 0 or index >= len(parameters):
                raise ValueError(f"Missing SQL parameter ${index + 1}.")
            return _sql_literal(parameters[index])

        return _POSITIONAL_PARAM_RE.sub(_replace, query)

    @staticmethod
    def _is_session_command(query: str) -> bool:
        lowered = query.strip().lower()
        return lowered.startswith(
            (
                "begin",
                "start transaction",
                "commit",
                "rollback",
                "set ",
                "reset ",
                "discard ",
                "deallocate ",
            )
        )

    def _execute_session_command(self, query: str) -> RuntimeOdbcQueryResult:
        lowered = query.strip().lower()
        if lowered.startswith("show "):
            return self._show_setting(query)
        if lowered.startswith("begin") or lowered.startswith("start transaction"):
            return RuntimeOdbcQueryResult(columns=[], rows=[], command_tag="BEGIN")
        if lowered.startswith("commit"):
            return RuntimeOdbcQueryResult(columns=[], rows=[], command_tag="COMMIT")
        if lowered.startswith("rollback"):
            return RuntimeOdbcQueryResult(columns=[], rows=[], command_tag="ROLLBACK")
        return RuntimeOdbcQueryResult(columns=[], rows=[], command_tag="SET")

    @staticmethod
    def _is_metadata_query(query: str) -> bool:
        lowered = query.strip().lower()
        if any(
            token in lowered
            for token in (
                "information_schema.",
                "pg_catalog.",
                "show ",
                "version()",
                "current_database()",
                "current_catalog",
                "current_schema()",
                "current_schemas(",
            )
        ):
            return True
        try:
            expression = sqlglot.parse_one(query, read="postgres")
        except sqlglot.ParseError:
            return False
        for table in expression.find_all(exp.Table):
            schema_name = str(table.db or "").strip().lower()
            table_name = str(table.name or "").strip().lower()
            if schema_name in {"information_schema", "pg_catalog"}:
                return True
            if table_name in _SYSTEM_CATALOG_TABLES:
                return True
        return False

    async def _execute_metadata_query(self, query: str) -> RuntimeOdbcQueryResult:
        lowered = query.strip().lower()
        if lowered.startswith("show "):
            return self._show_setting(query)
        if "version()" in lowered:
            return RuntimeOdbcQueryResult(
                columns=[{"name": "version", "type_oid": _PG_TYPE_OIDS["text"]}],
                rows=[(f"Langbridge runtime federated endpoint ({_SERVER_VERSION})",)],
                command_tag="SELECT 1",
            )
        if "current_database()" in lowered or "current_catalog" in lowered:
            return RuntimeOdbcQueryResult(
                columns=[{"name": "current_database", "type_oid": _PG_TYPE_OIDS["text"]}],
                rows=[(self._config.database_name,)],
                command_tag="SELECT 1",
            )
        if "current_schema()" in lowered:
            return RuntimeOdbcQueryResult(
                columns=[{"name": "current_schema", "type_oid": _PG_TYPE_OIDS["text"]}],
                rows=[(self._config.schema_name,)],
                command_tag="SELECT 1",
            )
        if "current_schemas(" in lowered:
            return RuntimeOdbcQueryResult(
                columns=[{"name": "current_schemas", "type_oid": _PG_TYPE_OIDS["text"]}],
                rows=[("{public}",)],
                command_tag="SELECT 1",
            )

        connection = duckdb.connect(database=":memory:")
        try:
            await self._materialize_metadata_catalog(connection)
            result = connection.execute(self._rewrite_metadata_query_for_duckdb(query))
            rows = [tuple(row) for row in result.fetchall()]
            description = result.description or []
            columns = [
                {
                    "name": str(item[0]),
                    "type_oid": _infer_type_oid_from_duckdb(item[1] if len(item) > 1 else None),
                }
                for item in description
            ]
            return RuntimeOdbcQueryResult(
                columns=columns,
                rows=rows,
                command_tag=f"SELECT {len(rows)}",
            )
        finally:
            connection.close()

    @staticmethod
    def _rewrite_metadata_query_for_duckdb(query: str) -> str:
        try:
            expression = sqlglot.parse_one(query, read="postgres")
        except sqlglot.ParseError:
            return query
        if not isinstance(expression, exp.Select):
            return query

        order = expression.args.get("order")
        if not isinstance(order, exp.Order):
            return query

        required_aliases = {
            item.this.name
            for item in order.expressions
            if isinstance(item, exp.Ordered)
            and isinstance(item.this, exp.Column)
            and not str(item.this.table or "").strip()
            and str(item.this.name or "").strip()
        }
        if not required_aliases:
            return query

        updated = expression.copy()
        projections = list(updated.expressions or [])
        rewritten = False
        for alias_name in required_aliases:
            candidates = [
                index
                for index, projection in enumerate(projections)
                if isinstance(projection, exp.Column)
                and str(projection.name or "").strip().lower() == alias_name.lower()
                and str(projection.table or "").strip()
            ]
            if len(candidates) != 1:
                continue
            projection = projections[candidates[0]]
            projections[candidates[0]] = exp.alias_(projection.copy(), alias_name, quoted=False)
            rewritten = True

        if not rewritten:
            return query
        updated.set("expressions", projections)
        return updated.sql(dialect="postgres")

    def _show_setting(self, query: str) -> RuntimeOdbcQueryResult:
        setting_name = query.strip()[5:].strip().strip(";").strip().lower()
        values = {
            "search_path": self._config.schema_name,
            "transaction_read_only": "on",
            "server_version": _SERVER_VERSION,
            "standard_conforming_strings": "on",
            "integer_datetimes": "on",
            "client_encoding": "UTF8",
            "server_encoding": "UTF8",
            "application_name": "langbridge",
            "timezone": "UTC",
            "datestyle": "ISO, MDY",
        }
        return RuntimeOdbcQueryResult(
            columns=[{"name": setting_name, "type_oid": _PG_TYPE_OIDS["text"]}],
            rows=[(values.get(setting_name, ""),)],
            command_tag="SHOW",
        )

    async def _materialize_metadata_catalog(self, connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute("CREATE TABLE IF NOT EXISTS pg_range (rngtypid BIGINT, rngsubtype BIGINT)")
        datasets = await self._runtime_host.list_datasets()
        connection.execute('CREATE SCHEMA IF NOT EXISTS "public"')
        for item in datasets:
            dataset_ref = str(item.get("id") or item.get("name") or "").strip()
            dataset_name = str(item.get("name") or "").strip()
            if not dataset_ref or not dataset_name:
                continue
            details = await self._runtime_host.get_dataset(dataset_ref=dataset_ref)
            sql_alias = self._normalize_identifier(
                details.get("sql_alias") or dataset_name,
                default=dataset_name,
            )
            columns = details.get("columns") if isinstance(details, dict) else []
            column_sql = ", ".join(
                f'{_quote_identifier(self._normalize_identifier(column.get("name"), default=f"column_{index + 1}"))} '
                f'{self._duckdb_type_for_column(column.get("data_type"))}'
                for index, column in enumerate(columns or [])
                if isinstance(column, dict)
            )
            if not column_sql:
                column_sql = '"value" VARCHAR'
            connection.execute(
                f'CREATE TABLE "public".{_quote_identifier(sql_alias)} ({column_sql})'
            )

    def _normalize_runtime_query(self, query: str) -> str:
        try:
            expression = sqlglot.parse_one(query, read="postgres")
        except sqlglot.ParseError:
            return _PUBLIC_SCHEMA_RE.sub("", query)

        def _strip_public_schema(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Table) and str(node.db or "").strip().lower() == self._config.schema_name:
                next_node = node.copy()
                next_node.set("db", None)
                return next_node
            return node

        return expression.transform(_strip_public_schema).sql(dialect="postgres")

    @staticmethod
    def _normalize_columns(raw_columns: Any, raw_rows: Any) -> list[dict[str, Any]]:
        columns_payload = raw_columns if isinstance(raw_columns, list) else []
        rows = raw_rows if isinstance(raw_rows, list) else []
        normalized = []
        for index, item in enumerate(columns_payload):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            inferred_value = None
            for row in rows:
                if isinstance(row, dict) and row.get(name) is not None:
                    inferred_value = row.get(name)
                    break
                if isinstance(row, (list, tuple)) and index < len(row) and row[index] is not None:
                    inferred_value = row[index]
                    break
            normalized.append(
                {
                    "name": name,
                    "type_oid": (
                        _infer_type_oid(item.get("type"))
                        if item.get("type") is not None
                        else _infer_type_oid_from_value(inferred_value)
                    ),
                }
            )
        if normalized:
            return normalized
        if rows and isinstance(rows[0], dict):
            return [
                {"name": str(name), "type_oid": _infer_type_oid_from_value(value)}
                for name, value in rows[0].items()
            ]
        return []

    @staticmethod
    def _normalize_rows(*, columns: list[dict[str, Any]], rows: Any) -> list[tuple[Any, ...]]:
        if not isinstance(rows, list):
            return []
        if not rows:
            return []
        column_names = [str(column["name"]) for column in columns]
        normalized: list[tuple[Any, ...]] = []
        for row in rows:
            if isinstance(row, dict):
                if not column_names:
                    column_names = [str(key) for key in row.keys()]
                normalized.append(tuple(row.get(name) for name in column_names))
                continue
            if isinstance(row, (list, tuple)):
                normalized.append(tuple(row))
                continue
            normalized.append((row,))
        return normalized

    @staticmethod
    def _normalize_identifier(value: Any, *, default: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
        return normalized or re.sub(r"[^A-Za-z0-9_]+", "_", default.strip().lower()).strip("_") or "dataset"

    @staticmethod
    def _duckdb_type_for_column(data_type: Any) -> str:
        normalized = str(data_type or "").strip().lower()
        return _DUCKDB_TYPE_MAP.get(normalized, "VARCHAR")


class RuntimeOdbcEndpoint:
    """Minimal PostgreSQL-compatible SQL endpoint for BI drivers."""

    def __init__(
        self,
        *,
        runtime_host: ConfiguredLocalRuntimeHost,
        auth_config: RuntimeAuthConfig,
        config: RuntimeOdbcEndpointConfig | None = None,
    ) -> None:
        self._runtime_host = runtime_host
        self._auth_config = auth_config
        self._config = config or RuntimeOdbcEndpointConfig()
        self._server: asyncio.AbstractServer | None = None

    @property
    def config(self) -> RuntimeOdbcEndpointConfig:
        return self._config

    @property
    def bound_port(self) -> int | None:
        if self._server is None or not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        if self._auth_config.mode not in {RuntimeAuthMode.none, RuntimeAuthMode.static_token}:
            raise ValueError(
                "The runtime ODBC endpoint currently supports auth modes none and static_token only."
            )
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self._config.host,
            port=self._config.port,
        )
        _LOGGER.info(
            "Runtime ODBC endpoint listening on %s:%s",
            self._config.host,
            self._config.port,
        )

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        statements: dict[str, _PreparedStatement] = {}
        portals: dict[str, _BoundPortal] = {}
        try:
            base_context = await self._startup(reader=reader, writer=writer)
            gateway = RuntimeOdbcQueryGateway(
                runtime_host=self._runtime_host,
                context=base_context,
                config=self._config,
            )
            while True:
                message_type = await reader.readexactly(1)
                length = _unpack_int32(await reader.readexactly(4))
                payload = await reader.readexactly(length - 4)
                code = message_type.decode("ascii")
                if code == "X":
                    break
                if code == "Q":
                    query = _read_cstring(payload, 0)[0]
                    for statement_sql in _split_simple_query_statements(query):
                        await self._execute_query(
                            writer=writer,
                            gateway=gateway,
                            query=statement_sql,
                            max_rows=None,
                            include_row_description=True,
                        )
                    await self._write_ready(writer)
                    continue
                if code == "P":
                    statement, offset = _read_cstring(payload, 0)
                    query, offset = _read_cstring(payload, offset)
                    declared_parameter_count = _unpack_int16(payload[offset : offset + 2])
                    parameter_count = max(
                        declared_parameter_count,
                        _count_query_parameters(query),
                    )
                    statements[statement] = _PreparedStatement(
                        name=statement,
                        query=query,
                        parameter_count=parameter_count,
                    )
                    await _write_message(writer, "1", b"")
                    continue
                if code == "B":
                    portal, offset = _read_cstring(payload, 0)
                    statement_name, offset = _read_cstring(payload, offset)
                    format_count = _unpack_int16(payload[offset : offset + 2])
                    offset += 2
                    parameter_formats = []
                    for _ in range(format_count):
                        parameter_formats.append(_unpack_int16(payload[offset : offset + 2]))
                        offset += 2
                    parameter_count = _unpack_int16(payload[offset : offset + 2])
                    offset += 2
                    parameters: list[Any] = []
                    for index in range(parameter_count):
                        length_value = _unpack_signed_int32(payload[offset : offset + 4])
                        offset += 4
                        if length_value == -1:
                            parameters.append(None)
                            continue
                        raw_value = payload[offset : offset + length_value]
                        offset += length_value
                        if not parameter_formats:
                            fmt = 0
                        elif len(parameter_formats) == 1:
                            fmt = parameter_formats[0]
                        else:
                            fmt = parameter_formats[index]
                        if fmt != 0:
                            raise ValueError("Binary bind parameters are not supported by the runtime ODBC endpoint.")
                        parameters.append(raw_value.decode("utf-8"))
                    result_format_count = _unpack_int16(payload[offset : offset + 2])
                    offset += 2
                    result_formats = []
                    for _ in range(result_format_count):
                        result_formats.append(_unpack_int16(payload[offset : offset + 2]))
                        offset += 2
                    if any(fmt != 0 for fmt in result_formats):
                        raise ValueError("Binary result formats are not supported by the runtime ODBC endpoint.")
                    portals[portal] = _BoundPortal(
                        name=portal,
                        statement_name=statement_name,
                        parameters=parameters,
                        result_formats=result_formats or [0],
                    )
                    await _write_message(writer, "2", b"")
                    continue
                if code == "D":
                    describe_type = chr(payload[0])
                    name, _ = _read_cstring(payload, 1)
                    if describe_type == "S":
                        statement = statements.get(name)
                        count = 0 if statement is None else statement.parameter_count
                        body = struct.pack("!H", count) + b"".join(struct.pack("!I", 0) for _ in range(count))
                        await _write_message(writer, "t", body)
                        if statement is None:
                            await _write_message(writer, "n", b"")
                            continue
                        columns = await gateway.describe(
                            statement.query,
                            parameters=[None] * count,
                        )
                        if columns:
                            await _write_message(writer, "T", _encode_row_description(columns))
                        else:
                            await _write_message(writer, "n", b"")
                        continue
                    if describe_type == "P":
                        portal = portals.get(name)
                        if portal is None:
                            raise ValueError(f"Unknown portal '{name}'.")
                        statement = statements.get(portal.statement_name)
                        if statement is None:
                            raise ValueError(f"Unknown statement '{portal.statement_name}'.")
                        columns = await gateway.describe(
                            statement.query,
                            parameters=portal.parameters,
                        )
                        if columns:
                            await _write_message(writer, "T", _encode_row_description(columns))
                        else:
                            await _write_message(writer, "n", b"")
                        continue
                    raise ValueError(f"Unsupported describe target '{describe_type}'.")
                if code == "E":
                    portal_name, offset = _read_cstring(payload, 0)
                    max_rows = _unpack_int32(payload[offset : offset + 4])
                    portal = portals.get(portal_name)
                    if portal is None:
                        raise ValueError(f"Unknown portal '{portal_name}'.")
                    statement = statements.get(portal.statement_name)
                    if statement is None:
                        raise ValueError(f"Unknown statement '{portal.statement_name}'.")
                    await self._execute_query(
                        writer=writer,
                        gateway=gateway,
                        query=statement.query,
                        parameters=portal.parameters,
                        max_rows=max_rows,
                        include_row_description=False,
                    )
                    continue
                if code == "S":
                    await self._write_ready(writer)
                    continue
                if code == "H":
                    await writer.drain()
                    continue
                if code == "C":
                    close_type = chr(payload[0])
                    name, _ = _read_cstring(payload, 1)
                    if close_type == "S":
                        statements.pop(name, None)
                    elif close_type == "P":
                        portals.pop(name, None)
                    else:
                        raise ValueError(f"Unsupported close target '{close_type}'.")
                    await _write_message(writer, "3", b"")
                    continue
                raise ValueError(f"Unsupported frontend message '{code}'.")
        except asyncio.IncompleteReadError:
            return
        except Exception as exc:
            await _write_error(writer, message=str(exc))
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # pragma: no cover - defensive cleanup
                return

    async def _startup(
        self,
        *,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> RuntimeContext:
        startup_params: dict[str, str] = {}
        while True:
            length = _unpack_int32(await reader.readexactly(4))
            payload = await reader.readexactly(length - 4)
            code = _unpack_int32(payload[:4])
            if code == _SSL_REQUEST_CODE:
                writer.write(b"N")
                await writer.drain()
                continue
            if code == _CANCEL_REQUEST_CODE:
                raise ValueError("Cancellation requests are not supported.")
            if code != _PROTO_VERSION:
                raise ValueError("Unsupported PostgreSQL protocol version.")
            startup_params = _parse_startup_parameters(payload[4:])
            break

        if self._auth_config.mode == RuntimeAuthMode.static_token:
            await _write_message(writer, "R", struct.pack("!I", 3))
            message_type = (await reader.readexactly(1)).decode("ascii")
            length = _unpack_int32(await reader.readexactly(4))
            payload = await reader.readexactly(length - 4)
            if message_type != "p":
                raise ValueError("Password authentication is required.")
            password, _ = _read_cstring(payload, 0)
            if password != self._auth_config.static_token:
                raise ValueError("Invalid runtime token.")

        await _write_message(writer, "R", struct.pack("!I", 0))
        await _write_message(writer, "S", _cstring("client_encoding") + _cstring("UTF8"))
        await _write_message(writer, "S", _cstring("server_encoding") + _cstring("UTF8"))
        await _write_message(writer, "S", _cstring("server_version") + _cstring(_SERVER_VERSION))
        await _write_message(writer, "S", _cstring("DateStyle") + _cstring("ISO, MDY"))
        await _write_message(writer, "S", _cstring("TimeZone") + _cstring("UTC"))
        await _write_message(writer, "S", _cstring("standard_conforming_strings") + _cstring("on"))
        await _write_message(writer, "S", _cstring("integer_datetimes") + _cstring("on"))
        await _write_message(writer, "K", struct.pack("!II", os.getpid() & 0x7FFFFFFF, uuid.uuid4().int & 0x7FFFFFFF))
        await self._write_ready(writer)

        username = str(startup_params.get("user") or "runtime").strip() or "runtime"
        _LOGGER.debug("Accepted runtime ODBC connection for user=%s", username)
        return RuntimeContext.build(
            workspace_id=self._auth_config.static_workspace_id or self._runtime_host.context.workspace_id,
            actor_id=self._auth_config.static_actor_id or self._runtime_host.context.actor_id,
            roles=self._auth_config.static_roles or self._runtime_host.context.roles,
            request_id=str(uuid.uuid4()),
        )

    async def _execute_query(
        self,
        *,
        writer: asyncio.StreamWriter,
        gateway: RuntimeOdbcQueryGateway,
        query: str,
        parameters: list[Any] | None = None,
        max_rows: int | None = None,
        include_row_description: bool = True,
    ) -> None:
        result = await gateway.execute(
            query,
            parameters=parameters,
            max_rows=max_rows,
        )
        if result.columns and include_row_description:
            await _write_message(writer, "T", _encode_row_description(result.columns))
        if result.rows:
            for row in result.rows:
                await _write_message(writer, "D", _encode_data_row(row))
        elif not result.rows and result.command_tag == "EMPTY":
            await _write_message(writer, "I", b"")
            return
        await _write_message(writer, "C", _cstring(result.command_tag))

    @staticmethod
    async def _write_ready(writer: asyncio.StreamWriter) -> None:
        await _write_message(writer, "Z", b"I")


def _encode_row_description(columns: list[dict[str, Any]]) -> bytes:
    body = struct.pack("!H", len(columns))
    for column in columns:
        body += _cstring(str(column.get("name") or "column"))
        body += struct.pack("!I", 0)
        body += struct.pack("!H", 0)
        body += struct.pack("!I", int(column.get("type_oid") or _PG_TYPE_OIDS["text"]))
        body += struct.pack("!H", -1 & 0xFFFF)
        body += struct.pack("!I", -1 & 0xFFFFFFFF)
        body += struct.pack("!H", 0)
    return body


def _encode_data_row(values: tuple[Any, ...]) -> bytes:
    body = struct.pack("!H", len(values))
    for value in values:
        if value is None:
            body += struct.pack("!I", 0xFFFFFFFF)
            continue
        encoded = _text_value(value).encode("utf-8")
        body += struct.pack("!I", len(encoded))
        body += encoded
    return body


def _parse_startup_parameters(payload: bytes) -> dict[str, str]:
    parts = payload.split(b"\x00")
    params: dict[str, str] = {}
    items = [part.decode("utf-8") for part in parts if part]
    for index in range(0, len(items) - 1, 2):
        params[items[index]] = items[index + 1]
    return params


async def _write_message(
    writer: asyncio.StreamWriter,
    message_type: str,
    payload: bytes,
) -> None:
    writer.write(message_type.encode("ascii"))
    writer.write(struct.pack("!I", len(payload) + 4))
    writer.write(payload)
    await writer.drain()


async def _write_error(
    writer: asyncio.StreamWriter,
    *,
    message: str,
    code: str = "XX000",
    severity: str = "ERROR",
) -> None:
    payload = (
        b"S" + _cstring(severity)
        + b"V" + _cstring(severity)
        + b"C" + _cstring(code)
        + b"M" + _cstring(message)
        + b"\x00"
    )
    try:
        await _write_message(writer, "E", payload)
        await _write_message(writer, "Z", b"I")
    except Exception:  # pragma: no cover - best effort on broken sockets
        return


def _read_cstring(payload: bytes, offset: int) -> tuple[str, int]:
    terminator = payload.index(b"\x00", offset)
    return payload[offset:terminator].decode("utf-8"), terminator + 1


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _unpack_int16(payload: bytes) -> int:
    return struct.unpack("!H", payload)[0]


def _unpack_int32(payload: bytes) -> int:
    return struct.unpack("!I", payload)[0]


def _unpack_signed_int32(payload: bytes) -> int:
    return struct.unpack("!i", payload)[0]


def _infer_type_oid(raw_type: Any) -> int:
    normalized = str(raw_type or "").strip().lower()
    return _PG_TYPE_OIDS.get(normalized, _PG_TYPE_OIDS["text"])


def _count_query_parameters(query: str) -> int:
    matches = [int(match.group(1)) for match in _POSITIONAL_PARAM_RE.finditer(query)]
    return max(matches, default=0)


def _infer_type_oid_from_duckdb(raw_type: Any) -> int:
    normalized = str(raw_type or "").strip().lower()
    for key, oid in (
        ("bool", _PG_TYPE_OIDS["bool"]),
        ("int2", _PG_TYPE_OIDS["int2"]),
        ("int4", _PG_TYPE_OIDS["int4"]),
        ("int8", _PG_TYPE_OIDS["int8"]),
        ("double", _PG_TYPE_OIDS["float8"]),
        ("float", _PG_TYPE_OIDS["float8"]),
        ("decimal", _PG_TYPE_OIDS["numeric"]),
        ("date", _PG_TYPE_OIDS["date"]),
        ("time", _PG_TYPE_OIDS["time"]),
        ("timestamp with time zone", _PG_TYPE_OIDS["timestamptz"]),
        ("timestamp", _PG_TYPE_OIDS["timestamp"]),
        ("varchar", _PG_TYPE_OIDS["varchar"]),
    ):
        if key in normalized:
            return oid
    return _PG_TYPE_OIDS["text"]


def _infer_type_oid_from_value(value: Any) -> int:
    if value is None:
        return _PG_TYPE_OIDS["text"]
    if isinstance(value, bool):
        return _PG_TYPE_OIDS["bool"]
    if isinstance(value, int):
        return _PG_TYPE_OIDS["int8"]
    if isinstance(value, float):
        return _PG_TYPE_OIDS["float8"]
    if isinstance(value, Decimal):
        return _PG_TYPE_OIDS["numeric"]
    if isinstance(value, date) and not isinstance(value, datetime):
        return _PG_TYPE_OIDS["date"]
    if isinstance(value, time):
        return _PG_TYPE_OIDS["time"]
    if isinstance(value, datetime):
        return _PG_TYPE_OIDS["timestamptz"] if value.tzinfo is not None else _PG_TYPE_OIDS["timestamp"]
    return _PG_TYPE_OIDS["text"]


def _text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, time):
        return f"TIME '{value.isoformat()}'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _quote_identifier(value: str) -> str:
    escaped = str(value).replace('"', '""')
    return f'"{escaped}"'


def _split_simple_query_statements(query: str) -> list[str]:
    try:
        expressions = sqlglot.parse(query, read="postgres")
    except sqlglot.ParseError:
        normalized = str(query or "").strip()
        return [normalized] if normalized else []

    statements = [
        expression.sql(dialect="postgres")
        for expression in expressions
        if expression is not None and str(expression.sql(dialect="postgres")).strip()
    ]
    return statements or ([str(query or "").strip()] if str(query or "").strip() else [])
