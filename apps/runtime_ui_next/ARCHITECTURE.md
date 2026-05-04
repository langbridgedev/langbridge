# Langbridge Runtime UI Next Architecture

`runtime_ui_next` is the side-by-side candidate for the next Langbridge runtime UI. It lives in
`langbridge/apps` because it is a runtime-local product surface for operating a portable Langbridge
runtime. It does not replace `langbridge/apps/runtime_ui` yet.

## Phase 1 Scope

Phase 1 turns the moved concept app into a production-oriented scaffold:

- Keep the current `runtime_ui` packaged app untouched.
- Use `react-router-dom` instead of the concept app's custom history listener.
- Use the same runtime auth/API foundation as the current production UI.
- Keep the new chat-first information architecture and shell.
- Build locally to `dist` rather than `langbridge/ui/static`.

## Phase 2 Scope

Phase 2 makes chat runtime-backed and markdown-driven:

- `/chat` creates real runtime threads and loads live agents/recent threads.
- `/chat/:threadId` loads runtime messages, submits agent runs, streams job progress, resumes active jobs, and supports thread rename.
- Assistant answers render `content.answer_markdown` first.
- `{{artifact:id}}` placeholders render inline table, chart, SQL, or diagnostics artifacts.
- `summary`, `result`, `visualization`, and `diagnostics` remain supporting metadata and fallback inputs, not the primary answer layout.
- Diagnostics are shown as compact execution metadata, runtime thinking, checks, and SQL cards with raw JSON behind an explicit action.

## Structure

- `src/app`: app shell, React Router route composition, active-route metadata.
- `src/components`: shared UI primitives, navigation, artifacts, resource details.
- `src/features`: product areas: chat, query workspace, dashboards, configuration.
- `src/hooks`: runtime auth and shared async/local-storage hooks.
- `src/lib`: low-level runtime API and formatting utilities shared with `runtime_ui`.
- `src/services`: feature service adapters that normalize API data and provide mock fallback where the feature is not fully ported.
- `src/mocks`: fixture data used only as fallback or visual scaffolding.
- `src/styles`: style entrypoint and layered CSS.

## Routes

Primary routes:

- `/chat`
- `/chat/:threadId`
- `/query-workspace`
- `/dashboards`
- `/dashboards/:dashboardId`
- `/configure/:section`

Compatibility redirects:

- `/` -> `/chat`
- `/c` -> `/chat`
- `/query` -> `/query-workspace`
- `/sql` -> `/query-workspace`
- `/configure` -> `/configure/connectors`

## Runtime API Boundary

The canonical API foundation is `src/lib/runtimeApi.js`, ported from the existing runtime UI. Feature
services should call this layer directly or through a thin feature adapter. New feature code should not
create a second fetch/error/auth implementation.

Current feature adapters:

- `chatService`: lists threads and agents, with fallback examples for unfinished chat UI states.
- `configurationService`: lists/details/actions for connectors, datasets, semantic models, and agents.
- `queryService`: still mostly scaffolded for visual iteration.
- `dashboardService`: still mostly scaffolded for visual iteration.
- `navigationService`: adapts recent/project lists to the active workspace.

Chat-specific rendering should continue to prefer markdown plus artifacts. Do not add a second non-markdown answer layout unless the backend contract changes.

## Auth Flow

`useRuntimeAuth` mirrors the current production runtime UI:

- `GET /api/runtime/v1/auth/bootstrap`
- `POST /api/runtime/v1/auth/bootstrap`
- `POST /api/runtime/v1/auth/login`
- `POST /api/runtime/v1/auth/logout`
- `GET /api/runtime/v1/auth/me`

When `auth_enabled` is false, the UI treats the runtime as a single-user local runtime and avoids sign-out
affordances.

## Development

The dev server runs on port `5177` and proxies relative `/api/...` calls to the runtime:

```bash
npm install
npm run dev
```

Default proxy target:

```text
http://127.0.0.1:8000
```

Override with:

```bash
LANGBRIDGE_RUNTIME_URL=http://127.0.0.1:8000 npm run dev
```

## Build Isolation

`runtime_ui_next` currently builds to its own `dist` directory. Do not point it at
`langbridge/ui/static` until the replacement cutover phase.

## Migration Rule

For feature parity work, preserve proven behavior from `runtime_ui` and wrap it in the new shell. Avoid
rewriting mature runtime workflows unless the existing behavior is intentionally being changed.
