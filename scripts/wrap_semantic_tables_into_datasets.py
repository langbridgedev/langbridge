import argparse
import json
from pathlib import Path
from typing import Any

import yaml


def build_dataset_payloads(
    *,
    semantic_payload: dict[str, Any],
    workspace_id: str,
    connection_id: str,
    default_tag: str,
) -> list[dict[str, Any]]:
    tables = semantic_payload.get("tables")
    if not isinstance(tables, dict):
        return []

    model_name = str(semantic_payload.get("name") or "semantic-model").strip() or "semantic-model"
    payloads: list[dict[str, Any]] = []
    for table_key, raw_table in tables.items():
        if not isinstance(raw_table, dict):
            continue
        if raw_table.get("dataset_id") or raw_table.get("datasetId"):
            continue

        schema_name = str(raw_table.get("schema") or "").strip()
        table_name = str(raw_table.get("name") or "").strip()
        if not table_name:
            continue

        dataset_name = f"{table_key}_{table_name}".replace(".", "_").replace("-", "_")
        payloads.append(
            {
                "workspaceId": workspace_id,
                "name": dataset_name.lower(),
                "description": f"Migrated from semantic model '{model_name}' table '{table_key}'.",
                "tags": [default_tag, f"semantic:{model_name}", f"table:{table_key}"],
                "datasetType": "TABLE",
                "connectionId": connection_id,
                "schemaName": schema_name or None,
                "tableName": table_name,
                "status": "published",
            }
        )

    return payloads


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate /v1/datasets create payloads for semantic tables that still "
            "bind directly to physical tables."
        )
    )
    parser.add_argument("--input", required=True, help="Path to semantic model YAML file.")
    parser.add_argument("--workspace-id", required=True, help="Workspace id.")
    parser.add_argument("--connection-id", required=True, help="Legacy connector id for table bindings.")
    parser.add_argument(
        "--tag",
        default="semantic-migration",
        help="Tag applied to generated dataset payloads.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output JSON file path. If omitted, prints to stdout.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    parsed = yaml.safe_load(input_path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise SystemExit("Semantic model payload must be a YAML mapping.")

    payloads = build_dataset_payloads(
        semantic_payload=parsed,
        workspace_id=args.workspace_id,
        connection_id=args.connection_id,
        default_tag=args.tag,
    )

    output = json.dumps(payloads, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(output + "\n", encoding="utf-8")
        print(f"Wrote {len(payloads)} dataset payload(s) to {output_path}.")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
