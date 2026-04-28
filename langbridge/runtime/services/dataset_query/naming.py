import hashlib
import re
import uuid


class DatasetSelectionNamer:
    """Builds stable dataset names, aliases, tags, and selection signatures."""

    def selection_signature(
        self,
        schema_name: str,
        table_name: str,
        selected_columns: list[str],
    ) -> str:
        normalized_columns = ",".join(
            sorted({column.strip().lower() for column in selected_columns if column and column.strip()})
        ) or "*"
        payload = f"{schema_name.strip().lower()}|{table_name.strip().lower()}|{normalized_columns}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def render_name_template(
        self,
        naming_template: str,
        *,
        connection_id: uuid.UUID,
        schema_name: str,
        table_name: str,
    ) -> str:
        template = (naming_template or "{schema}.{table}").strip() or "{schema}.{table}"
        return (
            template.replace("{connection}", str(connection_id).replace("-", "_"))
            .replace("{schema}", schema_name.strip() or "schema")
            .replace("{table}", table_name.strip() or "table")
        )

    def normalize_tags(self, tags: list[str]) -> list[str]:
        normalized = [tag.strip() for tag in tags if tag and tag.strip()]
        lowered = {tag.lower() for tag in normalized}
        if "auto-generated" not in lowered:
            normalized.append("auto-generated")
        return normalized

    def dataset_sql_alias(self, name: str) -> str:
        alias = re.sub(r"[^a-z0-9_]+", "_", str(name or "").strip().lower())
        alias = re.sub(r"_+", "_", alias).strip("_")
        if not alias:
            return "dataset"
        if alias[0].isdigit():
            return f"dataset_{alias}"
        return alias
