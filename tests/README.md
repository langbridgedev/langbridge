# Test Suite Layout

Langbridge tests are organized by both scope and runtime domain.

## Scopes

- `tests/unit/`: isolated tests for a single module or small set of collaborators. These should avoid booting the runtime host, HTTP app, configured local runtime, or persistent metadata stores unless the adapter itself is the unit under test.
- `tests/integration/`: tests that exercise multiple runtime components together, including `TestClient`, `build_configured_local_runtime`, SDK-through-runtime flows, filesystem-backed configs, SQLite metadata stores, and example projects.
- `tests/contract/`: compatibility tests for public schemas, request/response shapes, job contracts, and stable runtime API behavior.

## Unit Domains

- `tests/unit/ai/`: AI orchestration, analyst, presentation, LLM providers, and agent execution service tests.
- `tests/unit/runtime/`: runtime host facade, bootstrap-adjacent services, persistence adapters, auth, background tasks, migrations, ODBC, jobs, and runtime utilities.
- `tests/unit/semantic/`: semantic query, semantic SQL, translator, and vector semantic tests.
- `tests/unit/federation/`: federation planner, executor, diagnostics, artifact cache, and stage DAG tests.
- `tests/unit/connectors/`: connector packages, connector factories, remote sources, and connector-specific behavior.
- `tests/unit/client/`: SDK/client behavior that does not require a real runtime host.

## Markers

Markers are registered in `pyproject.toml` and strict marker validation is enabled.
Use markers when a path alone is not enough to describe the test, for example:

```python
pytestmark = [pytest.mark.integration, pytest.mark.runtime]
```

## Guidelines

- Prefer shared helpers in `tests/helpers/` over duplicating large fake providers or runtime builders.
- Keep test files focused. A file over roughly 500 lines should usually be split by behavior or API surface.
- Put reusable fixture data under `tests/fixtures/`.
- Do not add tests for deprecated modules or deleted compatibility surfaces.
