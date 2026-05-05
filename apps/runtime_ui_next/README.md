# Langbridge Runtime UI

Chat-first Langbridge runtime UI.

This app is the packaged production runtime UI source. The legacy UI source remains in
`langbridge/apps/runtime_ui` for rollback/reference during migration.

## Run Locally

```bash
npm install
npm run dev
```

The dev server runs on port `5177` and proxies `/api` to `http://127.0.0.1:8000` by default.

## Build

```bash
npm run build
```

The build output is written to `../../langbridge/ui/static`, which the Python runtime host serves
under `/ui` when the `ui` feature is enabled.
