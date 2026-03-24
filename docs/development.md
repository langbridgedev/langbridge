# Development

This section documents development of the Langbridge runtime repository.

## Development Docs

- `docs/development/local-dev.md`
- `docs/development/worker-dev.md` for preview distributed execution notes
- `docs/deployment/self-hosted.md`
- `docs/deployment/hybrid.md`

## Common Commands

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt
pip install -e .
```

Run the runtime host:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --host 127.0.0.1 --port 8000
```

Run the runtime host with the UI and MCP surfaces:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui,mcp
```

Build the runtime-owned UI bundle:

```bash
cd apps/runtime_ui
npm install
npm run build
```

Run tests:

```bash
pytest -q tests
```

Bring up the local runtime host container:

```bash
docker compose --profile host up --build runtime-host
```

The main development path in this repo is the single-node runtime host. Coordinator/worker scale-out remains a preview direction rather than the default local workflow.
