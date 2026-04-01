"""Fetch a Shopify Admin API access token for the shopify_sync example.

This script targets the current Dev Dashboard client-credentials flow:

  POST https://{shop}.myshopify.com/admin/oauth/access_token

Admin-created custom apps use a different manual flow in Shopify admin.
"""
import argparse
import json
import os
import sys
from pathlib import Path
from urllib import error, parse, request
import dotenv

DEFAULT_ENV_PATH = Path(__file__).with_name(".env")
dotenv.load_dotenv(DEFAULT_ENV_PATH)

def normalize_shop_domain(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        raise ValueError("Shopify shop domain is required.")
    if normalized.startswith("https://"):
        normalized = normalized[len("https://") :]
    elif normalized.startswith("http://"):
        normalized = normalized[len("http://") :]
    normalized = normalized.strip("/")
    normalized = normalized.split("/", 1)[0]
    if "." not in normalized:
        normalized = f"{normalized}.myshopify.com"
    if not normalized.endswith(".myshopify.com"):
        raise ValueError(
            "Shopify shop domain must be a valid *.myshopify.com domain or bare shop name."
        )
    return normalized


def fetch_access_token(
    *,
    shop_domain: str,
    client_id: str,
    client_secret: str,
    timeout_seconds: float,
) -> dict[str, object]:
    payload = parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    url = f"https://{shop_domain}/admin/oauth/access_token"
    req = request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(raw_body)
        except json.JSONDecodeError:
            detail = raw_body
        raise RuntimeError(
            f"Shopify token request failed with HTTP {exc.code}: {detail}"
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Shopify token request failed: {exc.reason}") from exc


def update_env_file(*, path: Path, shop_domain: str, access_token: str) -> None:
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    replacements = {
        "SHOPIFY_SHOP_DOMAIN": shop_domain,
        "SHOPIFY_ACCESS_TOKEN": access_token,
    }
    updated_lines: list[str] = []
    seen_keys: set[str] = set()

    for line in existing_lines:
        key, separator, _value = line.partition("=")
        if separator and key in replacements:
            updated_lines.append(f"{key}={replacements[key]}")
            seen_keys.add(key)
        else:
            updated_lines.append(line)

    for key, value in replacements.items():
        if key not in seen_keys:
            updated_lines.append(f"{key}={value}")

    path.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch a Shopify Admin API access token for a Dev Dashboard app installed on a shop."
        )
    )
    parser.add_argument(
        "--shop-domain",
        default=os.getenv("SHOPIFY_SHOP_DOMAIN", ""),
        help="Shopify shop domain or bare shop name. Defaults to SHOPIFY_SHOP_DOMAIN.",
    )
    parser.add_argument(
        "--client-id",
        default=os.getenv("SHOPIFY_CLIENT_ID", ""),
        help="Shopify app client ID. Defaults to SHOPIFY_CLIENT_ID.",
    )
    parser.add_argument(
        "--client-secret",
        default=os.getenv("SHOPIFY_CLIENT_SECRET", ""),
        help="Shopify app client secret. Defaults to SHOPIFY_CLIENT_SECRET.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds. Defaults to 30.",
    )
    parser.add_argument(
        "--write-env",
        nargs="?",
        const=str(DEFAULT_ENV_PATH),
        default=None,
        help=(
            "Optionally write SHOPIFY_SHOP_DOMAIN and SHOPIFY_ACCESS_TOKEN to an env file. "
            "Defaults to examples/shopify_sync/.env when provided without a path."
        ),
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print only the access token.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        shop_domain = normalize_shop_domain(args.shop_domain)
    except ValueError as exc:
        parser.error(str(exc))

    client_id = str(args.client_id or "").strip()
    client_secret = str(args.client_secret or "").strip()
    if not client_id:
        parser.error("Shopify client ID is required via --client-id or SHOPIFY_CLIENT_ID.")
    if not client_secret:
        parser.error(
            "Shopify client secret is required via --client-secret or SHOPIFY_CLIENT_SECRET."
        )

    try:
        payload = fetch_access_token(
            shop_domain=shop_domain,
            client_id=client_id,
            client_secret=client_secret,
            timeout_seconds=args.timeout,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        print(
            f"Shopify response did not include an access_token: {json.dumps(payload, indent=2)}",
            file=sys.stderr,
        )
        return 1

    if args.write_env:
        update_env_file(
            path=Path(args.write_env),
            shop_domain=shop_domain,
            access_token=access_token,
        )

    if args.raw:
        print(access_token)
        return 0

    print(f"Fetched Shopify Admin API access token for {shop_domain}.")
    print()
    print("Export for the current shell:")
    print(f"export SHOPIFY_SHOP_DOMAIN={shop_domain}")
    print(f"export SHOPIFY_ACCESS_TOKEN={access_token}")
    if args.write_env:
        print()
        print(f"Updated env file: {Path(args.write_env)}")
    if payload.get("scope"):
        print()
        print(f"Granted scopes: {payload['scope']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
