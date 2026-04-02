from typing import Any, Mapping


def load_extension(connection: Any, extension_name: str) -> None:
    normalized_name = str(extension_name or "").strip().lower()
    if not normalized_name:
        raise ValueError("DuckDB extension name is required.")
    try:
        connection.execute(f"LOAD {normalized_name}")
    except Exception:
        connection.execute(f"INSTALL {normalized_name}")
        connection.execute(f"LOAD {normalized_name}")


def create_secret(
    connection: Any,
    *,
    secret_name: str,
    clauses: Mapping[str, Any],
) -> None:
    rendered_clauses: list[str] = []
    for clause_name, clause_value in clauses.items():
        if clause_value in {None, ""}:
            continue
        rendered_clauses.append(
            f"{clause_name} {_render_sql_value(clause_value, keyword=clause_name)}"
        )
    if not rendered_clauses:
        raise ValueError("At least one DuckDB secret clause is required.")
    connection.execute(
        "CREATE OR REPLACE SECRET "
        f"{secret_name} (\n    "
        + ",\n    ".join(rendered_clauses)
        + "\n)"
    )


def _render_sql_value(value: Any, *, keyword: str | None = None) -> str:
    if keyword in {"TYPE", "PROVIDER"}:
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"
