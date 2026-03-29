
import argparse
import json
import os
from pathlib import Path

import yaml

from langbridge.client import LangbridgeClient
from langbridge.runtime import build_configured_local_runtime


BASE_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the complex Langbridge runtime example.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=BASE_DIR,
        help="Example directory that contains langbridge_config.yml.",
    )
    parser.add_argument(
        "--skip-agent-check",
        action="store_true",
        help="Skip optional agent invocation even if OPENAI_API_KEY is set.",
    )
    return parser.parse_args()


def _print_section(title: str, payload: object) -> None:
    print(f"\n## {title}")
    print(json.dumps(payload, indent=2, default=str))


def _write_validation_config(config_path: Path) -> Path:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Runtime config must be a mapping.")
    runtime_payload = payload.setdefault("runtime", {})
    if not isinstance(runtime_payload, dict):
        raise ValueError("runtime config must be a mapping.")
    runtime_payload["metadata_store"] = {"type": "in_memory"}
    runtime_payload["migrations"] = {"auto_apply": False}
    validation_config_path = config_path.with_name("langbridge_config.validation.yml")
    validation_config_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return validation_config_path


def main() -> None:
    args = parse_args()
    base_dir = args.base_dir.resolve()
    config_path = base_dir / "langbridge_config.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config: {config_path}")

    validation_config_path = _write_validation_config(config_path)
    runtime_host = build_configured_local_runtime(
        config_path=str(validation_config_path),
        apply_migrations=False,
    )
    try:
        with LangbridgeClient.for_local_runtime(
            runtime_host=runtime_host,
            default_workspace_id=runtime_host.context.workspace_id,
            default_actor_id=runtime_host.context.actor_id,
        ) as client:
            connectors = client.connectors.list()
            datasets = client.datasets.list()
            _print_section(
                "Connectors",
                [{"name": item.name, "type": item.connector_type} for item in connectors.items],
            )
            _print_section(
                "Datasets",
                [{"name": item.name, "connector": item.connector} for item in datasets.items],
            )

            sales_preview = client.datasets.query("sales_orders", limit=3)
            _print_section("Sales Orders Preview", sales_preview.rows)

            commerce_result = client.semantic.query(
                "commerce_performance",
                measures=[
                    "sales_orders.net_revenue",
                    "sales_orders.gross_margin",
                    "order_line_items.units_sold",
                ],
                dimensions=[
                    "sales_orders.order_channel",
                    "customer_profiles.segment",
                ],
                filters=[
                    {
                        "member": "sales_orders.order_status",
                        "operator": "equals",
                        "values": ["fulfilled"],
                    }
                ],
                order={"sales_orders.net_revenue": "desc"},
                limit=8,
            )
            _print_section("Commerce Semantic Query", commerce_result.rows)

            growth_result = client.semantic.query(
                "growth_performance",
                measures=[
                    "customer_month_revenue.monthly_net_revenue",
                    "campaign_attribution.influenced_pipeline",
                    "campaign_attribution.assisted_signups",
                ],
                dimensions=[
                    "campaign_attribution.acquisition_channel",
                    "customer_profiles.segment",
                ],
                order={"customer_month_revenue.monthly_net_revenue": "desc"},
                limit=8,
            )
            _print_section("Growth Semantic Query", growth_result.rows)

            support_preview = client.datasets.query("customer_month_support", limit=5)
            _print_section("Support Dataset Preview", support_preview.rows)

            if args.skip_agent_check:
                print("\n## Agents\nSkipped by flag.")
                return
            if not os.getenv("OPENAI_API_KEY"):
                print("\n## Agents\nSkipped because OPENAI_API_KEY is not set.")
                return

            commerce_agent = client.agents.ask(
                "Summarize the top order channels by net revenue and gross margin.",
                agent_name="commerce_analyst",
                timeout_s=90.0,
            )
            growth_agent = client.agents.ask(
                "Compare marketing spend and influenced pipeline by acquisition channel.",
                agent_name="growth_analyst",
                timeout_s=90.0,
            )
            _print_section("Commerce Agent", commerce_agent.text or commerce_agent.summary)
            _print_section("Growth Agent", growth_agent.text or growth_agent.summary)
    finally:
        validation_config_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
