# Local SDK Federated Query Notebook

This example shows Langbridge federating live sources at query time from a local
configured runtime.

Sources:

- a Postgres sales database
- a Postgres CRM database
- a local CSV with campaign attribution tags

## What This Example Covers

- `LangbridgeClient.local(...)`
- dataset preview across multiple source types
- semantic query across federated sources
- direct federated SQL across multiple datasets
- canonical local agent authoring through `agents[].definition.tools`

## Files

- `docker-compose.yml`
- `seeds/sales/`
- `seeds/crm/`
- `marketing_campaign.csv`
- `langbridge_config.yml`
- `example.ipynb`

## Install

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install notebook ipykernel pandas
```

## Start The Source Systems

From this example folder:

```bash
docker compose up -d --wait
```

To tear the demo down later:

```bash
docker compose down -v
```

## Run The Notebook

From the repository root:

```bash
jupyter notebook examples/sdk/federated_query/example.ipynb
```

## What The Notebook Demonstrates

1. Building a configured local runtime with `LangbridgeClient.local(config_path="langbridge_config.yml")`
2. Previewing each runtime dataset
3. Running a semantic query across sales, CRM, and CSV-backed data
4. Running federated SQL joins at runtime without passing manual alias mappings

## Federated SQL Ergonomics

The notebook now uses `client.sql.query(...)` without `selected_datasets` for the standard
cross-dataset example. The runtime federates across all eligible workspace datasets and derives
canonical SQL aliases from dataset metadata such as `sales_orders`, `crm_contacts`, and
`marketing_campaigns`.

When you want to narrow planner scope, you can still pass dataset ids:

```python
sql_result = client.sql.query(
    query="SELECT ...",
    selected_datasets=[
        dataset_ids["sales_orders"],
        dataset_ids["crm_contacts"],
        dataset_ids["marketing_campaigns"],
    ],
)
```

## Join Path

The example uses the shared CRM contact identifier:

- `sales.customers.crm_contact_external_id`
- `crm.contacts.contact_external_id`
- `marketing_campaign.csv.contact_external_id`

That keeps the federation story explicit: revenue, lifecycle data, and campaign
tags remain in their original systems while the runtime joins them at execution
time.

## Agent Definition Shape

The example config in `langbridge_config.yml` now defines its local analytics
agent through `agents[].definition.tools`. It includes:

- a governed SQL tool bound to the federated semantic model
- a dataset-backed SQL tool bound to multiple federated datasets

That matches the orchestrator definition model instead of relying on the older
single `semantic_model` / single `dataset` shortcut.
