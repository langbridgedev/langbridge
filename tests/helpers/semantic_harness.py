
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import duckdb
import yaml

from langbridge.semantic.graph_compiler import SemanticGraphSource, compile_semantic_graph
from langbridge.semantic.loader import load_semantic_graph, load_semantic_model
from langbridge.semantic.model import SemanticModel
from langbridge.semantic.query import SemanticQuery, SemanticQueryEngine

from tests.helpers.sql_normalize import normalize_rows, normalize_sql


class SemanticHarness:
    def __init__(self, fixture_root: Path | None = None) -> None:
        self.fixture_root = fixture_root or Path(__file__).resolve().parents[1] / "fixtures"
        self._engine = SemanticQueryEngine()

    def read_text(self, *parts: str) -> str:
        return self.fixture_root.joinpath(*parts).read_text(encoding="utf-8")

    def read_yaml(self, *parts: str) -> dict[str, Any]:
        payload = yaml.safe_load(self.read_text(*parts))
        if not isinstance(payload, dict):
            raise ValueError(f"Fixture {'/'.join(parts)} must contain a mapping payload.")
        return payload

    def read_json(self, *parts: str) -> Any:
        return json.loads(self.read_text(*parts))

    def load_semantic_model_fixture(self, name: str) -> SemanticModel:
        return load_semantic_model(self.read_text("semantic_models", f"{name}.yml"))

    def load_semantic_graph_fixture(self, name: str) -> SemanticModel:
        semantic_graph = load_semantic_graph(self.read_text("semantic_models", f"{name}.yml"))
        source_models: list[SemanticGraphSource] = []
        for source in semantic_graph.source_models:
            fixture_name = self._fixture_name_for_alias(source.alias)
            source_models.append(
                SemanticGraphSource(
                    model_id=source.id,
                    key=source.alias,
                    alias=source.alias,
                    name=source.name,
                    description=source.description,
                    connector_id=None,
                    model=self.load_semantic_model_fixture(fixture_name),
                )
            )
        relationships = [
            relationship.model_dump(mode="json", exclude_none=True)
            for relationship in (semantic_graph.relationships or [])
        ]
        metrics = {
            key: metric.model_dump(mode="json", exclude_none=True)
            for key, metric in (semantic_graph.metrics or {}).items()
        }
        compiled_model, _ = compile_semantic_graph(
            source_models=source_models,
            relationships=relationships or None,
            metrics=metrics or None,
            name=semantic_graph.name,
            description=semantic_graph.description,
            version=semantic_graph.version,
        )
        return compiled_model

    def load_unified_model_fixture(self, name: str) -> SemanticModel:
        return self.load_semantic_graph_fixture(name)

    def load_query_fixture(self, name: str, *, kind: str = "semantic") -> SemanticQuery:
        return SemanticQuery.model_validate(self.read_yaml("queries", kind, f"{name}.yml"))

    def compile_query(
        self,
        *,
        model: SemanticModel,
        query_name: str,
        dialect: str,
        kind: str = "semantic",
    ) -> str:
        query = self.load_query_fixture(query_name, kind=kind)
        plan = self._engine.compile(query, model, dialect=dialect)
        return normalize_sql(plan.sql, read_dialect=dialect, write_dialect=dialect)

    def compile_model_fixture(
        self,
        *,
        model_name: str,
        query_name: str,
        dialect: str,
        semantic_graph: bool = False,
        unified: bool = False,
        kind: str = "semantic",
    ) -> str:
        use_semantic_graph = semantic_graph or unified
        model = (
            self.load_semantic_graph_fixture(model_name)
            if use_semantic_graph
            else self.load_semantic_model_fixture(model_name)
        )
        return self.compile_query(model=model, query_name=query_name, dialect=dialect, kind=kind)

    def expected_sql(self, *, dialect: str, query_name: str) -> str:
        path = self.fixture_root / "expected" / "semantic_sql" / dialect / f"{query_name}.sql"
        if path.exists():
            return path.read_text(encoding="utf-8").strip()

        manifest = self.read_yaml("expected", "semantic_sql", "goldens.yml")
        return str(manifest[dialect][query_name]).strip()

    def assert_sql_fixture(
        self,
        *,
        model_name: str,
        query_name: str,
        dialect: str,
        semantic_graph: bool = False,
        unified: bool = False,
        kind: str = "semantic",
    ) -> None:
        actual = self.compile_model_fixture(
            model_name=model_name,
            query_name=query_name,
            dialect=dialect,
            semantic_graph=semantic_graph,
            unified=unified,
            kind=kind,
        )
        assert actual == self.expected_sql(dialect=dialect, query_name=query_name)

    def execute_model_fixture(
        self,
        *,
        model_name: str,
        query_name: str,
        dialect: str = "duckdb",
    ) -> list[dict[str, Any]]:
        model = self.load_semantic_model_fixture(model_name)
        sql = self.compile_query(model=model, query_name=query_name, dialect=dialect)
        return self.execute_sql(sql=sql, model=model)

    def execute_sql(self, *, sql: str, model: SemanticModel) -> list[dict[str, Any]]:
        connection = duckdb.connect(database=":memory:")
        try:
            for dataset_key, dataset in model.datasets.items():
                csv_path = self._dataset_path(dataset_key=dataset_key, relation_name=dataset.get_relation_name(dataset_key))
                if dataset.schema_name:
                    connection.execute(
                        f'CREATE SCHEMA IF NOT EXISTS "{dataset.schema_name}"'
                    )
                    relation_name = f'"{dataset.schema_name}"."{dataset.get_relation_name(dataset_key)}"'
                else:
                    relation_name = f'"{dataset.get_relation_name(dataset_key)}"'
                normalized_path = csv_path.as_posix().replace("'", "''")
                connection.execute(
                    f"CREATE OR REPLACE VIEW {relation_name} AS "
                    f"SELECT * FROM read_csv_auto('{normalized_path}', header=true)"
                )

            cursor = connection.execute(sql)
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return normalize_rows(rows)
        finally:
            connection.close()

    def expected_rows(self, name: str) -> list[dict[str, Any]]:
        path = self.fixture_root / "expected" / "result_sets" / f"{name}.json"
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
        else:
            payload = self.read_json("expected", "result_sets", "goldens.json")[name]
        if not isinstance(payload, list):
            raise ValueError(f"Expected rows fixture {name} must contain a list.")
        return normalize_rows(payload)

    def _dataset_path(self, *, dataset_key: str, relation_name: str) -> Path:
        relation_candidate = self.fixture_root / "datasets" / f"{relation_name}.csv"
        if relation_candidate.exists():
            return relation_candidate
        dataset_candidate = self.fixture_root / "datasets" / f"{dataset_key}.csv"
        if dataset_candidate.exists():
            return dataset_candidate
        raise FileNotFoundError(
            f"No dataset fixture found for dataset_key={dataset_key} relation_name={relation_name}."
        )

    @staticmethod
    def _fixture_name_for_alias(alias: str) -> str:
        return str(alias or "").strip().lower().replace(" ", "_")
