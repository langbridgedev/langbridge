# Local Development

## Python Environment

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\Scripts\activate  # Windows PowerShell
pip install -r requirements.txt
pip install -e .
```

## Main Local Runtime Loop

Seed the demo database:

```bash
python examples/sdk/semantic_query/setup.py
```

Run the self-hosted runtime host:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --host 127.0.0.1 --port 8000
```

The runtime docs will be available at `http://127.0.0.1:8000/api/runtime/docs`.

## Containerized Local Runtime

Self-hosted runtime host:

```bash
docker compose --profile host up --build runtime-host
```

Queued worker stack:

```bash
docker compose up --build db redis worker
```

## Examples

- `examples/runtime_host/`
- `examples/runtime_host_sync/`
- `examples/sdk/semantic_query/`
- `examples/sdk/federated_query/`

## Tests

Run the full test suite:

```bash
pytest -q tests
```

Or keep iteration tight with:

```bash
pytest -q tests/unit
```

## Cloud Split

If you need hosted API, hosted worker, or web changes, switch to:

- `../langbridge-cloud/apps/api`
- `../langbridge-cloud/apps/worker`
- `../langbridge-cloud/apps/web`
