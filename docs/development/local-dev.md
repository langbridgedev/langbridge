# Local Development

## Python Environment

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\Scripts\activate  # Windows PowerShell
pip install -r requirements/dev.txt
pip install -e .
```

## Main Runtime Loop

Seed the demo database:

```bash
python examples/sdk/semantic_query/setup.py
```

Run the self-hosted runtime host:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --host 127.0.0.1 --port 8000
```

The runtime docs will be available at `http://127.0.0.1:8000/api/runtime/docs`.

## Runtime UI Development

Run the runtime host with the UI feature enabled:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui
```

Then start the React app from `apps/runtime_ui`:

```bash
cd apps/runtime_ui
npm install
npm run dev
```

Build the production assets back into `langbridge/ui/static` with:

```bash
npm run build
```

## MCP Development

Run the runtime host with the MCP endpoint enabled:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features mcp
```

The MCP server will be mounted at `http://127.0.0.1:8000/mcp`.

## Containerized Local Runtime

Self-hosted runtime host:

```bash
docker compose --profile host up --build runtime-host
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
