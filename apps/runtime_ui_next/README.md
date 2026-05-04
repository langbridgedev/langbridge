# Langbridge Runtime UI Next

Side-by-side candidate for the next chat-first Langbridge runtime UI.

This app is not the packaged production UI yet. The current production UI remains
`langbridge/apps/runtime_ui`.

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

The build output stays in `dist` during the side-by-side migration phase.
