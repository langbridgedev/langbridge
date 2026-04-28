# Repository Guidelines

## Project Structure & Module Organization
Langbridge is a runtime-first repository. Runtime-owned Python code lives under `langbridge/`, UI source lives under `apps/runtime_ui`, examples live under `examples/`, and tests live under `tests/`. Documentation belongs in `docs/`. Treat the runtime host, SDK, connectors, semantic layer, federation engine, MCP surface, and packaged UI as part of one coherent runtime product.

## Build, Test, and Development Commands
- `python -m venv .venv && ./.venv/Scripts/activate && pip install -r requirements/dev.txt && pip install -e .` installs backend dependencies on Windows PowerShell.
- `source .venv/bin/activate && pip install -r requirements/dev.txt && pip install -e .` is the equivalent on macOS/Linux.
- `langbridge serve --config examples/runtime_host/langbridge_config.yml` starts the runtime host.
- `langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui,mcp` starts the host with the runtime UI and MCP endpoint enabled.
- `cd apps/runtime_ui && npm install && npm run dev` starts the React UI dev server.
- `cd apps/runtime_ui && npm run build` builds the UI bundle into `langbridge/ui/static`.
- `docker compose --profile host up --build runtime-host` starts the containerized runtime host.
- `pytest -q tests` runs the test suite; narrow scope with paths such as `pytest -q tests/unit`.

## Coding Style & Naming Conventions
Use 4-space indentation, type hints, and cohesive domain-oriented modules. Python functions use snake_case. Keep runtime modules grouped by responsibility under `langbridge.runtime`, `langbridge.semantic`, `langbridge.federation`, `langbridge.connectors`, and `langbridge.ai`. In the React app, components use PascalCase and utilities/hooks use camelCase.

## Testing Guidelines
Create pytest modules as `test_<feature>.py`, reuse fixtures from `tests/conftest.py`, and cover both success and failure paths. When changing runtime host behavior, update API and CLI tests together. When changing the UI, rebuild `langbridge/ui/static` from `apps/runtime_ui` and keep the host-facing smoke tests passing.

## Commit & Pull Request Guidelines
Use short present-tense commit subjects such as `add runtime ui feature flag`. Pull requests should explain the runtime behavior change, call out any config or API impacts, and include screenshots for visible UI changes.

## Security & Configuration Tips
Never commit secrets, `.env*`, `.db`, or logs. Document new runtime configuration in `docs/development.md` or `docs/deployment/self-hosted.md`. Keep auth defaults conservative and keep connector credentials on the runtime side.
