# Repository Guidelines

## Project Structure & Module Organization
Langbridge is the runtime-focused repository in a two-repo split. Hosted cloud surfaces now live in `../langbridge-cloud/apps` (`api`, `worker`, `web`). Runtime-owned code lives in `langbridge/packages` plus thin runtime assembly apps under `langbridge/apps` such as `runtime_worker`. Core tests are grouped in `tests/` (connectors, orchestrator, unit), while module-specific fixtures may live in `langbridge/tests`. Docs and semantic/runtime references stay in `docs/`.

## Build, Test, and Development Commands
- `python -m venv .venv && ./.venv/Scripts/activate && pip install -r langbridge/requirements.txt` installs backend deps (use `source .venv/bin/activate` on macOS/Linux).
- Control-plane API development now lives in `../langbridge-cloud/apps/api`; start it from that repo for API work.
- `python -m langbridge.apps.runtime_worker.main` runs the portable runtime worker assembly; use `../langbridge-cloud/apps/worker` for hosted orchestration worker work.
- `cd ../langbridge-cloud/apps/web && npm install && npm run dev` runs the Next.js dev server; `npm run build && npm run start` serves the production bundle.
- `docker compose up --build` launches the runtime worker plus supporting local infrastructure, and can pull the cloud API/migration images for integrated verification when needed.
- `pytest -q tests` executes backend suites; limit scope with subpaths such as `pytest tests/orchestrator`.
- Schema migrations now live in `../langbridge-cloud/apps/api/alembic`; run them from the cloud repo.
- `cd ../langbridge-cloud/apps/web && npm run lint` must be clean before opening a PR when the moved UI is part of the change.

## Coding Style & Naming Conventions
Use 4-space indentation, type hints, and FastAPI dependency-injection patterns. Keep backend modules cohesive by domain (`semantic/translators/*.py`, `connectors/shopify/*.py`). Functions are snake_case, generated route IDs remain kebab-case via `custom_generate_unique_id`. On the frontend, components are PascalCase, hooks/utilities are camelCase, and Tailwind/PostCSS formatting is enforced by the shared ESLint config.

## Testing Guidelines
Create pytest modules as `test_<feature>.py`, reuse fixtures from `tests/conftest.py`, and cover positive plus failure flows for orchestrators/connectors. Pair any API contract change with expectation tests and regenerate client types. UI changes should keep `npm run lint` clean and add Playwright or React Testing Library coverage when stateful behavior is introduced.

## Commit & Pull Request Guidelines
Write present-tense commit subjects similar to `change semantic translator to support dialect target`. Pull requests should explain motivation, link the tracking issue, attach UI screenshots or GIFs for visible changes, and flag migrations, semantic-model edits, or config impacts so reviewers can verify `docs/semantic-model.md` and `docs/api.md`.

## Security & Configuration Tips
Settings flow from `.env` via `langbridge/config.py`; never commit secrets, `.env*`, `.db`, or log files (already Git-ignored). Document new configuration knobs in `docs/development.md`, choose safe defaults, and coordinate with ops when changing agent hosts or OAuth credentials. Enable Shopify/GitHub/Google integrations only after syncing backend env vars with `../langbridge-cloud/apps/web/.env.local` so redirect URIs stay aligned.
