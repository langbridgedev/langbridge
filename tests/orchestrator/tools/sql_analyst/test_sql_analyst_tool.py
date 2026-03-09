import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[5] / "langbridge" / "langbridge"))

import sqlite3
from typing import Any, Sequence


from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.tool import SqlAnalystTool
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.interfaces import AnalystQueryRequest, QueryResult, SemanticModel


class DummyLLM:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        return self._sql


class DummyConnector:
    DIALECT = type("D", (), {"name": "DUMMY"})

    def __init__(self) -> None:
        self.executed_sql: str | None = None

    async def execute(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_s: int | None = None,
    ) -> Any:
        self.executed_sql = sql

        class Result:
            def __init__(self, sql_text: str) -> None:
                self.columns = ["value"]
                self.rows = [(42,)]
                self.rowcount = len(self.rows)
                self.elapsed_ms = 5
                self.sql = sql_text

        return Result(sql)


class SQLiteConnector:
    DIALECT = type("D", (), {"name": "SQLITE"})

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def execute(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_s: int | None = None,
    ) -> Any:
        cursor = self._conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        class Result:
            def __init__(self, columns: Sequence[str], rows: Sequence[Sequence[Any]], sql_text: str) -> None:
                self.columns = list(columns)
                self.rows = [tuple(row) for row in rows]
                self.rowcount = len(self.rows)
                self.elapsed_ms = 0
                self.sql = sql_text

        return Result(columns, rows, sql)


def _semantic_model(name: str, entity: str) -> SemanticModel:
    return SemanticModel(
        version="1.0",
        name=name,
        tables={
            entity: {
                "name": entity,
                "dimensions": [
                    {"name": "order_id", "type": "integer"},
                ],
            }
        },
        relationships=[],
        metrics={"total_orders": {"expression": "COUNT(*)"}},
    )


def test_sql_analyst_tool_transpiles_to_target_dialect() -> None:
    llm = DummyLLM("SELECT order_id::text FROM orders")
    connector = DummyConnector()
    tool = SqlAnalystTool(
        llm=llm,
        semantic_model=_semantic_model("sales", "orders"),
        connector=connector,
        dialect="bigquery",
    )

    response = tool.run(AnalystQueryRequest(question="How many orders?", limit=10))

    assert response.error is None
    assert response.sql_canonical == "SELECT order_id::text FROM orders"
    assert response.sql_executable == "SELECT CAST(order_id AS STRING) FROM orders"
    assert connector.executed_sql == response.sql_executable
    assert response.result == QueryResult(
        columns=["value"],
        rows=[(42,)],
        rowcount=1,
        elapsed_ms=5,
        source_sql=response.sql_executable,
    )


def test_sql_analyst_tool_runs_against_sqlite() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, amount REAL)")
    conn.execute("INSERT INTO orders (amount) VALUES (100.0), (50.0)")
    conn.commit()

    llm = DummyLLM("SELECT COUNT(*) AS order_count FROM orders")
    connector = SQLiteConnector(conn)
    tool = SqlAnalystTool(
        llm=llm,
        semantic_model=_semantic_model("sales_sqlite", "orders"),
        connector=connector,
        dialect="sqlite",
    )

    response = tool.run(AnalystQueryRequest(question="How many orders?"))

    assert response.error is None
    assert response.result is not None
    assert response.result.rows[0][0] == 2

    conn.close()
