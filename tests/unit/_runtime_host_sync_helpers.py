
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.parse import parse_qs, urlparse

from langbridge.runtime.settings import runtime_settings


_CUSTOMERS = [
    {
        "id": "cus_001",
        "created": 1710000000,
        "email": "ada@example.com",
        "name": "Ada Lovelace",
        "metadata": {"segment": "enterprise"},
    },
    {
        "id": "cus_002",
        "created": 1710003600,
        "email": "grace@example.com",
        "name": "Grace Hopper",
        "metadata": {"segment": "mid_market"},
    },
]


class _MockStripeHandler(BaseHTTPRequestHandler):
    server_version = "LangbridgeMockStripe/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/v1/account":
            self._send_json(200, {"id": "acct_demo", "object": "account"})
            return
        if parsed.path == "/v1/customers":
            params = parse_qs(parsed.query)
            created_gte = params.get("created[gte]", [None])[0]
            records = list(_CUSTOMERS)
            if created_gte and str(created_gte).isdigit():
                records = [
                    record
                    for record in records
                    if int(record.get("created") or 0) >= int(created_gte)
                ]
            self._send_json(
                200,
                {
                    "object": "list",
                    "data": records,
                    "has_more": False,
                },
            )
            return
        self._send_json(404, {"error": "not found"})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return None

    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@contextmanager
def mock_stripe_api() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MockStripeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@contextmanager
def runtime_storage_dirs(base_dir: Path) -> Iterator[None]:
    dataset_dir = str((base_dir / ".cache" / "datasets").resolve())
    federation_dir = str((base_dir / ".cache" / "federation").resolve())
    original_dataset_dir = runtime_settings.DATASET_FILE_LOCAL_DIR
    original_federation_dir = runtime_settings.FEDERATION_ARTIFACT_DIR
    object.__setattr__(runtime_settings, "DATASET_FILE_LOCAL_DIR", dataset_dir)
    object.__setattr__(runtime_settings, "FEDERATION_ARTIFACT_DIR", federation_dir)
    try:
        yield
    finally:
        object.__setattr__(runtime_settings, "DATASET_FILE_LOCAL_DIR", original_dataset_dir)
        object.__setattr__(runtime_settings, "FEDERATION_ARTIFACT_DIR", original_federation_dir)


def write_sync_runtime_config(
    directory: Path,
    *,
    api_base_url: str,
    declared_synced_datasets: list[dict[str, object]] | None = None,
) -> Path:
    datasets_block = ""
    if declared_synced_datasets:
        dataset_lines = ["", "datasets:"]
        for item in declared_synced_datasets:
            sync_lines: list[str] = []
            dataset_lines.extend(
                [
                    f"  - name: {item['name']}",
                    "    connector: billing_demo",
                    "    source:",
                    "      kind: resource",
                    f"      resource: {item['resource']}",
                    "    materialization:",
                    "      mode: synced",
                ]
            )
            cadence = str(item.get("cadence") or "").strip()
            if cadence:
                sync_lines.append(f"        cadence: {cadence}")
            if "sync_on_start" in item:
                sync_on_start = bool(item.get("sync_on_start"))
                sync_lines.append(
                    f"        sync_on_start: {'true' if sync_on_start else 'false'}"
                )
            if sync_lines:
                dataset_lines.append("      sync:")
                dataset_lines.extend(sync_lines)
            else:
                dataset_lines.append("      sync: {}")
        datasets_block = "\n".join(dataset_lines)
    config_path = directory / "langbridge_sync.yml"
    config_path.write_text(
        f"""
version: 1

runtime:
  mode: local
  metadata_store:
    type: sqlite
    path: .langbridge/metadata.db
  execution:
    engine: duckdb
    duckdb:
      path: .langbridge/duckdb/runtime.duckdb
      temp_directory: .langbridge/tmp

connectors:
  - name: billing_demo
    type: stripe
    description: Demo billing connector for hosted sync tests.
    connection:
      api_key: sk_test_demo
      api_base_url: {api_base_url}
{datasets_block}
""".strip(),
        encoding="utf-8",
    )
    return config_path
