# Complex Self-Hosted Runtime Example

This example is a realistic local Langbridge deployment for demoing the runtime
product in self-hosted mode. It intentionally goes beyond a single-dataset
commerce walkthrough and shows a governed runtime with multiple connectors,
multiple datasets, multiple semantic models, and two scoped agents.

Everything in this folder is runtime-local. It does not depend on
`langbridge-cloud`.

## What The Setup Contains

- `langbridge_config.yml`
  A full local runtime config with explicit metadata persistence, DuckDB
  execution, three connectors, seven datasets, two semantic models, one LLM
  connection, and two agents.
- `setup.py`
  Seeds local fixture data under `./data/`.
- `validate.py`
  Boots the runtime locally through the SDK and exercises the example.

## Connectors

- `commerce_warehouse`
  SQLite-backed operational commerce warehouse.
- `growth_warehouse`
  SQLite-backed growth and customer operations mart.
- `planning_files`
  File-backed connector serving a monthly channel spend CSV.

## Datasets

- `sales_orders`
  Order-level revenue and margin facts from `commerce_warehouse`.
- `order_line_items`
  Product-level sales detail from `commerce_warehouse`.
- `customer_month_revenue`
  Customer-month-channel revenue rollup from `commerce_warehouse`.
- `customer_profiles`
  Shared customer dimension from `growth_warehouse`.
- `campaign_attribution`
  Customer-month-channel attribution facts from `growth_warehouse`.
- `customer_month_support`
  Customer-month support workload rollup from `growth_warehouse`.
- `channel_spend_targets`
  File-backed monthly spend and demand plan from `planning_files`.

All datasets in this example use the current runtime `materialization_mode:
live` path. A synced dataset is intentionally not included here because the
clean synced examples in this repo depend on sync-capable SaaS connectors and
their own mock or service setup. This example stays focused on one coherent
self-hosted deployment that runs locally with no extra services.

## Semantic Models

- `commerce_performance`
  Uses `sales_orders`, `order_line_items`, and `customer_profiles` to answer:
  revenue, margin, refunds, product mix, order channels, and segment
  performance.
- `growth_performance`
  Uses `customer_month_revenue`, `customer_profiles`, and
  `campaign_attribution` to answer:
  acquisition efficiency, influenced pipeline, and cohort conversion across
  connectors.

`channel_spend_targets` and `customer_month_support` stay available as governed
runtime datasets and are intentionally left outside the growth semantic model so
their different grains do not create misleading fanout in semantic answers.

The two models deliberately overlap on `customer_profiles` so the runtime can
show how a shared customer dimension anchors multiple governed analytical
surfaces.

## Agents

- `commerce_analyst`
  Focused on orders, revenue, gross margin, refunds, fulfillment regions, and
  product mix. It can use the `commerce_performance` semantic model plus scoped
  dataset SQL on the commerce datasets.
- `growth_analyst`
  Focused on channel efficiency, influenced pipeline, conversion, segment
  performance, and support pressure. It can use the `growth_performance`
  semantic model plus scoped dataset SQL on growth, spend, and support datasets.

The agents have different prompts, different tool bindings, and different
connector scope. `commerce_analyst` is intentionally blocked from the
`planning_files` connector.

## Initialize The Example

From the repository root:

```bash
python examples/complex_runtime/setup.py
```

That creates:

- `examples/complex_runtime/data/commerce.db`
- `examples/complex_runtime/data/growth_ops.db`
- `examples/complex_runtime/data/channel_spend_targets.csv`

If you want to use the agents, export an API key first:

```bash
export OPENAI_API_KEY=...
```

## Run The Runtime Host

Apply runtime metadata migrations:

```bash
langbridge migrate --config examples/complex_runtime/langbridge_config.yml
```

Start the runtime host:

```bash
langbridge serve --config examples/complex_runtime/langbridge_config.yml --host 127.0.0.1 --port 8000
```

Start the host with the bundled UI:

```bash
langbridge serve --config examples/complex_runtime/langbridge_config.yml --host 127.0.0.1 --port 8000 --features ui
```

## Validate Locally Through The SDK

Run the example validator:

```bash
python examples/complex_runtime/validate.py
```

That script will:

- boot the configured runtime in-process
- list connectors and datasets
- preview sample dataset rows
- run one semantic query against each semantic model
- optionally call both configured agents when `OPENAI_API_KEY` is set

## Useful API Calls

List connectors:

```bash
curl http://127.0.0.1:8000/api/runtime/v1/connectors
```

List datasets:

```bash
curl http://127.0.0.1:8000/api/runtime/v1/datasets
```

Run a commerce semantic query:

```bash
curl -X POST http://127.0.0.1:8000/api/runtime/v1/semantic/query \
  -H "Content-Type: application/json" \
  -d '{
    "semantic_models": ["commerce_performance"],
    "measures": [
      "sales_orders.net_revenue",
      "sales_orders.gross_margin",
      "order_line_items.units_sold"
    ],
    "dimensions": [
      "sales_orders.order_channel",
      "customer_profiles.segment"
    ],
    "filters": [
      {
        "member": "sales_orders.order_status",
        "operator": "equals",
        "values": ["fulfilled"]
      }
    ],
    "order": {"sales_orders.net_revenue": "desc"},
    "limit": 10
  }'
```

Run a growth semantic query:

```bash
curl -X POST http://127.0.0.1:8000/api/runtime/v1/semantic/query \
  -H "Content-Type: application/json" \
  -d '{
    "semantic_models": ["growth_performance"],
    "measures": [
      "customer_month_revenue.monthly_net_revenue",
      "campaign_attribution.influenced_pipeline",
      "campaign_attribution.assisted_signups"
    ],
    "dimensions": [
      "campaign_attribution.acquisition_channel",
      "customer_profiles.segment"
    ],
    "order": {"customer_month_revenue.monthly_net_revenue": "desc"},
    "limit": 10
  }'
```

## Example Prompts

For `commerce_analyst`:

- `Which order channels drove the highest net revenue and gross margin in Q3 2025?`
- `Show refund exposure by segment and fulfillment region.`
- `Which product categories have the strongest gross margin contribution?`

For `growth_analyst`:

- `Compare influenced pipeline, net revenue, and marketing spend by acquisition channel.`
- `Which customer segments generate the most revenue per unit of marketing spend?`
- `Do regions with higher support load also underperform on marketing efficiency?`

## Notes

- the runtime metadata store is explicit and local to this example
- connector and dataset boundaries stay runtime-scoped
- the example is designed for manual demos, local validation, and self-hosted
  runtime testing
- remove generated state by deleting `examples/complex_runtime/.langbridge/` and
  `examples/complex_runtime/data/`
