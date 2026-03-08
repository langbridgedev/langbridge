# Development

This page now points to the split development docs.

## Development Docs

- Local development: `docs/development/local-dev.md`
- Worker development: `docs/development/worker-dev.md`
- Hosted deployment: `docs/deployment/hosted.md`
- Hybrid deployment: `docs/deployment/hybrid.md`
- Self-hosted deployment: `docs/deployment/self-hosted.md`

## Quick Commands

- API: `uvicorn langbridge.apps.api.langbridge_api.main:app --reload`
- Worker: `python -m langbridge.apps.worker.langbridge_worker.main`
- UI: `cd client && npm install && npm run dev`
- Unit tests: `pytest -q tests/unit`
- Frontend lint: `cd client && npm run lint`

## Notes

- SQL and semantic execution run through Worker + Federated Query Engine.
- Federated SQL authoring is dataset-first and runs through worker-mediated federation.
