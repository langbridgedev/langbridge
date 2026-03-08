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
uvicorn langbridge.apps.api.langbridge_api.main:app --reload
```

Run Worker:

```bash
python -m langbridge.apps.worker.langbridge_worker.main
```

## Frontend

```bash
cd client
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
cd client && npm run lint
```
