
import pytest

from langbridge.runtime.utils.sql import (
    apply_result_redaction,
    enforce_preview_limit,
    enforce_read_only_sql,
    normalize_sql_dialect,
    render_sql_with_params,
    transpile_sql,
)


def test_enforce_preview_limit_adds_top_for_tsql_select() -> None:
    sql, limit = enforce_preview_limit("SELECT id, name FROM dbo.users", max_rows=25)
    assert "TOP 25" in sql.upper()
    assert "FROM dbo.users" in sql
    assert limit == 25


def test_enforce_preview_limit_keeps_smaller_existing_top() -> None:
    sql, limit = enforce_preview_limit("SELECT TOP 5 id FROM dbo.users", max_rows=100)
    assert "TOP 5" in sql.upper()
    assert "TOP 100" not in sql.upper()
    assert limit == 5


def test_enforce_read_only_sql_rejects_dml_when_disallowed() -> None:
    with pytest.raises(ValueError):
        enforce_read_only_sql("DELETE FROM dbo.users WHERE id = 1", allow_dml=False)


def test_render_sql_with_params_supports_template_and_colon_patterns() -> None:
    rendered = render_sql_with_params(
        "SELECT * FROM dbo.orders WHERE created_at >= :start_date AND region = {{region}}",
        {"start_date": "2026-01-01", "region": "EMEA"},
    )
    assert ":start_date" not in rendered
    assert "{{region}}" not in rendered
    assert "'2026-01-01'" in rendered
    assert "'EMEA'" in rendered


def test_apply_result_redaction_hashes_configured_columns() -> None:
    rows, applied = apply_result_redaction(
        rows=[{"id": 1, "email": "someone@example.com"}],
        redaction_rules={"email": "hash"},
    )
    assert applied is True
    assert rows[0]["id"] == 1
    assert rows[0]["email"] != "someone@example.com"
    assert len(rows[0]["email"]) == 16


def test_apply_result_redaction_can_omit_columns() -> None:
    rows, applied = apply_result_redaction(
        rows=[{"id": 1, "ssn": "123-45-6789"}],
        redaction_rules={"ssn": "omit"},
    )
    assert applied is True
    assert rows[0]["id"] == 1
    assert "ssn" not in rows[0]


def test_transpile_sql_converts_tsql_top_to_postgres_limit() -> None:
    output = transpile_sql(
        "SELECT TOP 5 id FROM dbo.users ORDER BY id DESC",
        source_dialect="tsql",
        target_dialect="postgres",
    )
    assert "LIMIT 5" in output.upper()


def test_normalize_sql_dialect_maps_sqlserver_aliases() -> None:
    assert normalize_sql_dialect("sqlserver") == "tsql"
    assert normalize_sql_dialect("mssql") == "tsql"
