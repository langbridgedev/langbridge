# Local SDK Semantic Query Notebook

This example is a notebook-first walkthrough of the Langbridge runtime SDK.

It runs entirely against a configured local runtime built from the monolith
`langbridge.*` package surface.

## What This Example Covers

- `LangbridgeClient.local(...)`
- dataset listing and preview
- semantic query
- direct SQL query
- agent ask against a configured local agent

## Files

- `example.ipynb`
- `setup.py`
- `langbridge_config.yml`
- `example_sdk_usage.py`

## Install

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install notebook ipykernel pandas matplotlib
```

`LangbridgeClient.local(...)` boots the configured runtime in-process, so
installing the repo root is the simplest way to ensure runtime dependencies such
as DuckDB, PyArrow, and connector implementations are available.

To enable the local analytics agent, export an LLM key:

```bash
export OPENAI_API_KEY=...
```

## Run The Example

Seed the local warehouse:

```bash
python examples/sdk/semantic_query/setup.py
```

Start Jupyter:

```bash
jupyter notebook examples/sdk/semantic_query/example.ipynb
```

## What The Notebook Demonstrates

1. Building a configured local runtime with `LangbridgeClient.local(config_path="langbridge_config.yml")`
2. Listing runtime datasets
3. Previewing dataset rows through the runtime dataset service
4. Running semantic query against `commerce_performance`
5. Running direct SQL against the configured connector
6. Asking a configured local analytics agent

## Notes

- this example is runtime-local, not cloud-hosted
- the runtime context is workspace-scoped even in local mode
- the config file in this folder is aligned with the seeded warehouse structure
