# Local Development

## Backend

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\Scripts\activate  # Windows PowerShell
pip install -r langbridge/requirements.txt
```

Run API:

```bash
cd ../langbridge-cloud
python scripts/export_control_plane_openapi.py
PYTHONPATH=apps/api uvicorn langbridge_cloud_api.main:app --reload
```

Run Worker:

```bash
python -m langbridge.apps.runtime_worker.main
```

## Web App

```bash
cd ../langbridge-cloud/apps/web
npm install
npm run dev
```

## Core Local Endpoints

- UI: `http://localhost:3000`
- API docs: `http://localhost:8000/docs`

## SQL Workbench Quick Check

1. Open `/sql/{organizationId}` in UI.
2. Select a connector.
3. Run a query with `TOP` or `LIMIT`.
4. Confirm job lifecycle, results, and history.

## Testing

```bash
pytest -q tests/unit
cd ../langbridge-cloud/apps/web && npm run lint
```
