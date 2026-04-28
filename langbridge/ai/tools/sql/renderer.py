from typing import Optional

from .interfaces import (
    AnalyticalContext,
    AnalyticalField,
    SemanticModelLike,
)


def render_analysis_context(context: AnalyticalContext, semantic_model: Optional[SemanticModelLike] = None) -> str:
    if context.asset_type == "semantic_model":
        return _render_semantic_model_context(context, semantic_model)
    return _render_dataset_context(context)

def _render_dataset_context(context: AnalyticalContext) -> str:
    parts: list[str] = [f"Dataset asset: {context.asset_name}"]
    if context.description:
        parts.append(f"Description: {context.description}")
    if context.tags:
        parts.append(f"Tags: {', '.join(context.tags)}")
    if context.datasets:
        parts.append("Datasets:")
        for dataset in context.datasets:
            line = f"  - {dataset.sql_alias} ({dataset.dataset_name})"
            descriptor = ", ".join(
                value
                for value in (dataset.source_kind, dataset.storage_kind)
                if value
            )
            if descriptor:
                line = f"{line} [{descriptor}]"
            parts.append(line)
            if dataset.description:
                parts.append(f"      description: {dataset.description}")
            if dataset.columns:
                parts.append("      columns:")
                for column in dataset.columns:
                    column_line = f"        * {dataset.sql_alias}.{column.name}"
                    if column.data_type:
                        column_line = f"{column_line} ({column.data_type})"
                    parts.append(column_line)
    if context.relationships:
        parts.append("Relationships:")
        for relationship in context.relationships:
            parts.append(f"  - {relationship}")
    return "\n".join(parts)

def _render_semantic_model_context(context: AnalyticalContext, semantic_model: Optional[SemanticModelLike] = None) -> str:
    parts: list[str] = [f"Semantic model asset: {context.asset_name}"]
    if context.description:
        parts.append(f"Description: {context.description}")
    if context.tags:
        parts.append(f"Tags: {', '.join(context.tags)}")
    if context.datasets:
        parts.append("Backed by datasets:")
        for dataset in context.datasets:
            parts.append(f"  - {dataset.sql_alias} ({dataset.dataset_name})")
    if context.tables:
        parts.append("Tables:")
        for table in context.tables:
            parts.append(f"  - {table}")
    if semantic_model is not None:
        parts.extend(_render_semantic_model_definitions(semantic_model))
    _append_field_block(parts, "Dimensions", context.dimensions)
    _append_field_block(parts, "Measures", context.measures)
    if context.metrics:
        parts.append("Metrics:")
        for metric in context.metrics:
            line = f"  - {metric.name}"
            if metric.expression:
                line = f"{line}: {metric.expression}"
            if metric.description:
                line = f"{line} ({metric.description})"
            parts.append(line)
    if context.relationships:
        parts.append("Relationships:")
        for relationship in context.relationships:
            parts.append(f"  - {relationship}")
    return "\n".join(parts)


def _render_semantic_model_definitions(semantic_model) -> list[str]:
    parts: list[str] = ["Semantic definitions:"]
    for table_key, table in (getattr(semantic_model, "tables", {}) or {}).items():
        parts.append(f"  - table {table_key}")
        for dimension in table.dimensions or []:
            expression_sql = str(dimension.expression or dimension.name).strip()
            line = f"      dimension {table_key}.{dimension.name}"
            if expression_sql:
                line = f"{line} => {expression_sql}"
            if dimension.type:
                line = f"{line} [{dimension.type}]"
            parts.append(line)
        for measure in table.measures or []:
            expression_sql = str(measure.expression or measure.name).strip()
            line = f"      measure {table_key}.{measure.name}"
            aggregation = str(measure.aggregation or "").strip().lower()
            if aggregation:
                line = f"{line} ({aggregation})"
            if expression_sql:
                line = f"{line} => {expression_sql}"
            parts.append(line)
    return parts

def _append_field_block(parts: list[str], title: str, fields: list[AnalyticalField]) -> None:
    if not fields:
        return
    parts.append(f"{title}:")
    for field in fields:
        line = f"  - {field.name}"
        if field.synonyms:
            line = f"{line} (synonyms: {', '.join(field.synonyms)})"
        parts.append(line)
        
__all__ = ["render_analysis_context"]
