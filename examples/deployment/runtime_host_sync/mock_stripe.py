
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

_CUSTOMERS = [
    {
        "id": "cus_001",
        "created": 1710000000,
        "email": "ada@example.com",
        "name": "Ada Lovelace",
        "currency": "usd",
        "metadata": {"segment": "enterprise", "region": "uk"},
    },
    {
        "id": "cus_002",
        "created": 1710003600,
        "email": "grace@example.com",
        "name": "Grace Hopper",
        "currency": "usd",
        "metadata": {"segment": "mid_market", "region": "us"},
    },
    {
        "id": "cus_003",
        "created": 1710007200,
        "email": "margaret@example.com",
        "name": "Margaret Hamilton",
        "currency": "gbp",
        "metadata": {"segment": "enterprise", "region": "uk"},
    },
]


class MockStripeHandler(BaseHTTPRequestHandler):
    server_version = "LangbridgeMockStripe/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        if parsed.path == "/v1/account":
            self._send_json(
                200,
                {
                    "id": "acct_self_hosted_demo",
                    "object": "account",
                    "country": "GB",
                    "email": "owner@example.com",
                },
            )
            return
        if parsed.path == "/v1/customers":
            params = parse_qs(parsed.query, keep_blank_values=True)
            records, has_more = _list_customers(params)
            self._send_json(
                200,
                {
                    "object": "list",
                    "url": "/v1/customers",
                    "has_more": has_more,
                    "data": records,
                },
            )
            return
        self._send_json(404, {"error": {"message": "not found"}})

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return None

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _list_customers(params: dict[str, list[str]]) -> tuple[list[dict[str, Any]], bool]:
    records = list(_CUSTOMERS)
    created_gte = (params.get("created[gte]") or [None])[0]
    if created_gte and str(created_gte).isdigit():
        min_created = int(created_gte)
        records = [record for record in records if int(record.get("created") or 0) >= min_created]

    starting_after = (params.get("starting_after") or [None])[0]
    if starting_after:
        for index, record in enumerate(records):
            if str(record.get("id")) == str(starting_after):
                records = records[index + 1 :]
                break

    limit = _parse_limit((params.get("limit") or [None])[0], default=100)
    has_more = len(records) > limit
    return records[:limit], has_more


def _parse_limit(value: str | None, *, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return max(1, parsed)


def main() -> None:
    host = os.getenv("MOCK_STRIPE_HOST", "127.0.0.1")
    port = _parse_limit(os.getenv("MOCK_STRIPE_PORT", "12111"), default=12111)
    server = ThreadingHTTPServer((host, port), MockStripeHandler)
    print(f"Mock Stripe API listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
