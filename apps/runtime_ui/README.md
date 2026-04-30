# Runtime UI

This app is the source for the runtime-owned UI shell.

## Model

- Source code lives in `apps/runtime_ui`
- Production assets build into `langbridge/ui/static`
- The Python runtime host serves the built output from `langbridge.ui`
- Routes are runtime-first and single-workspace scoped:
  - `/`
  - `/connectors`
  - `/datasets`
  - `/semantic-models`
  - `/sql`
  - `/agents`
  - `/chat`
  - `/dashboards`
  - `/settings`

## Auth flow

- If runtime auth is disabled, the UI opens directly.
- If runtime auth is enabled and local operator sessions are enabled with no admin yet, the UI shows a bootstrap form for the first admin account.
- Local operator auth follows the runtime metadata store: `in_memory` stays ephemeral, while `sqlite` and `postgres` persist auth/session state in the runtime metadata DB.
- If runtime auth is enabled and local operator sessions are enabled with an existing admin, the UI shows a login form.
- If runtime auth is enabled but local operator sessions are disabled, the UI reports bearer-only auth instead of showing signup or cloud auth flows.
- Cloud signup, SSO landing flows, and org/project selectors are intentionally not part of this app.

## Commands

Install dependencies:

```bash
npm install
```

Run the Vite dev server against a local runtime host:

```bash
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --features ui
cd apps/runtime_ui
npm run dev
```

By default, Vite proxies `/api/*` to `http://127.0.0.1:8000`.

Build the production assets into the Python package:

```bash
cd apps/runtime_ui
npm run build
```
