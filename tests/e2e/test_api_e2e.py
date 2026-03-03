import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import httpx
import pytest


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_api(process: subprocess.Popen[str], base_url: str, timeout_seconds: float = 90.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("API process exited before becoming healthy.")
        try:
            response = httpx.get(f"{base_url}/api/v1/auth/health", timeout=2.0)
            if response.status_code == 200:
                return
        except Exception as exc:  # pragma: no cover - startup timing dependent
            last_error = exc
        time.sleep(0.5)
    raise TimeoutError(f"API failed to start before timeout. Last error: {last_error}")


@pytest.fixture(scope="module")
def running_api() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    e2e_cache = repo_root / ".cache" / "e2e"
    e2e_cache.mkdir(parents=True, exist_ok=True)
    db_name = f"e2e_{uuid.uuid4().hex}.db"

    env = os.environ.copy()
    env.update(
        {
            "ENVIRONMENT": "local",
            "LOCAL_DB": f".cache/e2e/{db_name}",
            "JWT_SECRET": "e2e-jwt-secret",
            "GITHUB_CLIENT_ID": "e2e-client-id",
            "GITHUB_CLIENT_SECRET": "e2e-client-secret",
            "GOOGLE_CLIENT_ID": "e2e-google-client-id",
            "GOOGLE_CLIENT_SECRET": "e2e-google-client-secret",
            "SERVICE_USER_SECRET": "e2e-service-secret",
            "BACKEND_URL": base_url,
            "FRONTEND_URL": "http://localhost:3000",
            "UVICORN_RELOAD": "false",
        }
    )

    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "langbridge.apps.api.langbridge_api.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]
    log_file = tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False)
    process = subprocess.Popen(
        command,
        cwd=str(repo_root),
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_for_api(process, base_url)
        yield base_url
    except Exception:
        log_file.flush()
        log_path = Path(log_file.name)
        try:
            startup_output = log_path.read_text(encoding="utf-8")
        except OSError:
            startup_output = ""
        if "ModuleNotFoundError" in startup_output:
            pytest.skip(
                "E2E API dependencies are missing in this environment. "
                "Install backend requirements before running tests/e2e."
            )
        raise AssertionError(
            "API failed to start for E2E tests. "
            f"Startup output:\n{startup_output[-4000:]}"
        )
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        log_file.close()
        try:
            Path(log_file.name).unlink(missing_ok=True)
        except OSError:
            pass


def test_protected_routes_require_auth(running_api: str) -> None:
    with httpx.Client(base_url=running_api, timeout=20.0) as client:
        response = client.get("/api/v1/organizations")
        assert response.status_code == 401

        org_id = uuid.uuid4()
        model_id = uuid.uuid4()
        single_query_response = client.post(
            f"/api/v1/semantic-query/{org_id}/{model_id}/q",
            json={
                "organizationId": str(org_id),
                "semanticModelId": str(model_id),
                "query": {"dimensions": ["orders.id"], "limit": 1},
            },
        )
        assert single_query_response.status_code == 401

        unified_query_response = client.post(
            f"/api/v1/semantic-query/{org_id}/unified/q",
            json={
                "organizationId": str(org_id),
                "connectorId": str(uuid.uuid4()),
                "semanticModelIds": [str(uuid.uuid4())],
                "query": {"dimensions": ["orders.id"], "limit": 1},
            },
        )
        assert unified_query_response.status_code == 401


def test_native_auth_org_project_thread_e2e(running_api: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    email = f"e2e_{suffix}@example.com"
    username = f"e2e_user_{suffix}"
    password = "Passw0rd!"

    with httpx.Client(base_url=running_api, timeout=20.0) as client:
        register_response = client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": password, "username": username},
        )
        assert register_response.status_code == 200, register_response.text
        assert register_response.json().get("ok") is True
        assert "langbridge_token" in client.cookies

        me_response = client.get("/api/v1/auth/me")
        assert me_response.status_code == 200, me_response.text
        assert me_response.json().get("user", {}).get("username") == username

        list_orgs_response = client.get("/api/v1/organizations")
        assert list_orgs_response.status_code == 200, list_orgs_response.text
        initial_orgs = list_orgs_response.json()
        assert len(initial_orgs) >= 1

        create_org_response = client.post(
            "/api/v1/organizations",
            json={"name": f"E2E Org {suffix}"},
        )
        assert create_org_response.status_code == 201, create_org_response.text
        organization = create_org_response.json()
        organization_id = organization["id"]

        create_project_response = client.post(
            f"/api/v1/organizations/{organization_id}/projects",
            json={"name": f"E2E Project {suffix}"},
        )
        assert create_project_response.status_code == 201, create_project_response.text
        project = create_project_response.json()
        project_id = project["id"]

        create_thread_response = client.post(
            f"/api/v1/thread/{organization_id}/",
            json={"project_id": project_id, "title": f"E2E Thread {suffix}"},
        )
        assert create_thread_response.status_code == 201, create_thread_response.text
        thread = create_thread_response.json()
        thread_id = thread["id"]

        list_threads_response = client.get(f"/api/v1/thread/{organization_id}/")
        assert list_threads_response.status_code == 200, list_threads_response.text
        thread_ids = {item["id"] for item in list_threads_response.json().get("threads", [])}
        assert thread_id in thread_ids

        get_thread_response = client.get(f"/api/v1/thread/{organization_id}/{thread_id}")
        assert get_thread_response.status_code == 200, get_thread_response.text

        update_thread_response = client.put(
            f"/api/v1/thread/{organization_id}/{thread_id}",
            json={"title": "Updated E2E Thread"},
        )
        assert update_thread_response.status_code == 200, update_thread_response.text
        assert update_thread_response.json()["title"] == "Updated E2E Thread"

        list_messages_response = client.get(
            f"/api/v1/thread/{organization_id}/{thread_id}/messages"
        )
        assert list_messages_response.status_code == 200, list_messages_response.text
        assert isinstance(list_messages_response.json().get("messages"), list)

        delete_thread_response = client.delete(f"/api/v1/thread/{organization_id}/{thread_id}")
        assert delete_thread_response.status_code == 204, delete_thread_response.text

        get_deleted_response = client.get(f"/api/v1/thread/{organization_id}/{thread_id}")
        assert get_deleted_response.status_code == 404, get_deleted_response.text


def test_org_settings_catalog_and_crud_e2e(running_api: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    email = f"e2e_settings_{suffix}@example.com"
    username = f"e2e_settings_user_{suffix}"
    password = "Passw0rd!"

    with httpx.Client(base_url=running_api, timeout=20.0) as client:
        register_response = client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": password, "username": username},
        )
        assert register_response.status_code == 200, register_response.text

        create_org_response = client.post(
            "/api/v1/organizations",
            json={"name": f"E2E Settings Org {suffix}"},
        )
        assert create_org_response.status_code == 201, create_org_response.text
        organization_id = create_org_response.json()["id"]

        catalog_response = client.get("/api/v1/organizations/environment/catalog")
        assert catalog_response.status_code == 200, catalog_response.text
        catalog = catalog_response.json()
        assert isinstance(catalog, list)
        assert any(item.get("settingKey") == "support_email" for item in catalog)

        set_response = client.post(
            f"/api/v1/organizations/{organization_id}/environment/support_email",
            json={
                "settingKey": "support_email",
                "settingValue": f"settings-{suffix}@example.com",
            },
        )
        assert set_response.status_code == 201, set_response.text
        payload = set_response.json()
        assert payload["settingKey"] == "support_email"
        assert payload["settingValue"] == f"settings-{suffix}@example.com"
        assert payload.get("category") == "General"

        list_response = client.get(f"/api/v1/organizations/{organization_id}/environment")
        assert list_response.status_code == 200, list_response.text
        settings = list_response.json()
        support_email_setting = next(
            (setting for setting in settings if setting["settingKey"] == "support_email"),
            None,
        )
        assert support_email_setting is not None
        assert support_email_setting["settingValue"] == f"settings-{suffix}@example.com"

        delete_response = client.delete(
            f"/api/v1/organizations/{organization_id}/environment/support_email"
        )
        assert delete_response.status_code == 204, delete_response.text

        list_after_delete = client.get(f"/api/v1/organizations/{organization_id}/environment")
        assert list_after_delete.status_code == 200, list_after_delete.text
        assert all(
            setting["settingKey"] != "support_email"
            for setting in list_after_delete.json()
        )
