
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import yaml

from langbridge.client import LangbridgeClient
from langbridge.runtime import build_configured_local_runtime


def test_complex_runtime_example_boots_and_executes_queries(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    source_example_dir = repo_root / "examples" / "legacy" / "complex_runtime"
    example_dir = tmp_path / "complex_runtime"
    shutil.copytree(source_example_dir, example_dir)

    subprocess.run(
        [sys.executable, str(example_dir / "setup.py"), "--base-dir", str(example_dir)],
        check=True,
    )

    source_config_path = example_dir / "langbridge_config.yml"
    config_payload = yaml.safe_load(source_config_path.read_text(encoding="utf-8"))
    growth_profile = next(profile for profile in config_payload["ai"]["profiles"] if profile["name"] == "growth_analyst")
    growth_scope = growth_profile["scope"]
    assert growth_scope["semantic_models"] == ["growth_performance"]
    assert growth_scope["datasets"] == [
        "customer_month_revenue",
        "campaign_attribution",
        "channel_spend_targets",
        "customer_month_support",
        "customer_profiles",
    ]
    assert growth_scope["query_policy"] == "semantic_preferred"
    assert growth_profile["research"]["enabled"] is True
    assert growth_profile["web_search"]["enabled"] is True

    growth_db_path = example_dir / "data" / "growth_ops.db"
    duckdb_conn = duckdb.connect()
    try:
        column_type = duckdb_conn.execute(
            "SELECT typeof(attribution_month) FROM sqlite_scan(?, 'campaign_attribution_monthly') LIMIT 1",
            [str(growth_db_path)],
        ).fetchone()
        assert column_type == ("DATE",)
    finally:
        duckdb_conn.close()

    runtime_payload = config_payload.setdefault("runtime", {})
    runtime_payload["metadata_store"] = {"type": "in_memory"}
    runtime_payload["migrations"] = {"auto_apply": False}
    validation_config_path = example_dir / "langbridge_config.validation.yml"
    validation_config_path.write_text(
        yaml.safe_dump(config_payload, sort_keys=False),
        encoding="utf-8",
    )

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
            connector_names = sorted(item.name for item in connectors.items)
            assert connector_names == [
                "commerce_warehouse",
                "growth_warehouse",
                "planning_files",
            ]

            datasets = client.datasets.list()
            dataset_names = sorted(item.name for item in datasets.items)
            assert dataset_names == [
                "campaign_attribution",
                "channel_spend_targets",
                "customer_month_revenue",
                "customer_month_support",
                "customer_profiles",
                "order_line_items",
                "sales_orders",
            ]

            sales_preview = client.datasets.query("sales_orders", limit=3)
            assert sales_preview.status == "succeeded"
            assert len(sales_preview.rows) == 3

            line_item_sync = client.sync.run(dataset="order_line_items")
            assert line_item_sync.status == "succeeded"

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
                limit=6,
            )
            assert commerce_result.status == "succeeded"
            assert commerce_result.rows
            first_commerce_row = commerce_result.rows[0]
            assert "sales_orders.net_revenue" in first_commerce_row
            assert "order_line_items.units_sold" in first_commerce_row

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
                limit=6,
            )
            assert growth_result.status == "succeeded"
            assert growth_result.rows
            first_growth_row = growth_result.rows[0]
            assert "customer_month_revenue.monthly_net_revenue" in first_growth_row
            assert "campaign_attribution.assisted_signups" in first_growth_row

            growth_by_year = client.semantic.query(
                "growth_performance",
                measures=["customer_month_revenue.monthly_net_revenue"],
                dimensions=["customer_month_revenue.order_channel"],
                time_dimensions=[
                    {
                        "dimension": "customer_month_revenue.revenue_month",
                        "granularity": "year",
                    }
                ],
                order={"customer_month_revenue.revenue_month": "asc"},
                limit=20,
            )
            assert growth_by_year.status == "succeeded"
            assert growth_by_year.rows
            first_growth_year_row = growth_by_year.rows[0]
            assert "customer_month_revenue.monthly_net_revenue" in first_growth_year_row
            assert "customer_month_revenue.revenue_month_year" in first_growth_year_row

            support_preview = client.datasets.query("customer_month_support", limit=5)
            assert support_preview.status == "succeeded"
            assert support_preview.rows
    finally:
        validation_config_path.unlink(missing_ok=True)
