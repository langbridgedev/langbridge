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
4. Running federated SQL joins at runtime

## Join Path

The example uses the shared CRM contact identifier:

- `sales.customers.crm_contact_external_id`
- `crm.contacts.contact_external_id`
- `marketing_campaign.csv.contact_external_id`

That keeps the federation story explicit: revenue, lifecycle data, and campaign
tags remain in their original systems while the runtime joins them at execution
time.
