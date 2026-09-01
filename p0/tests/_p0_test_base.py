"""
p0-local test base — shared client, auth, envelope validator, and setup.

Per the team TEST CASES GUIDE we avoid a local conftest.py; instead p0 test
files import what they need from this module explicitly:

    from p0.tests._p0_test_base import client, get_auth_headers, assert_standard_response

It is p0-specific on purpose (kept out of the shared utility/test_config.py so it
never affects p1/p2 test runs). Importing it decrypts secrets once (needs
MASTER_KEY in the environment) and wraps the CDM router in a FastAPI app whose
admin gate is overridden to an admin identity, so admin-gated endpoints pass.
"""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from utility.secret_manager import load_secrets

load_secrets()

from p0.api.main import router as cdm_router
from p0.api.config import USER_CONFIG_FILE
from p0.Login.auth.auth_deps import get_current_user, TokenData
import p0.api.routers.context as ctx

BACKEND_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND_DIR))

CDM_MOUNT = getattr(cdm_router, "prefix", "") or ""

cdm_app = FastAPI()
cdm_app.include_router(cdm_router)


from typing import Optional

from fastapi import Cookie, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from http import HTTPStatus


@cdm_app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    """Pass a dict detail through as the flat response body (matching the standard
    envelope), rather than FastAPI's default wrapping of {\"detail\": ...}."""
    detail = exc.detail
    if isinstance(detail, dict):
        return JSONResponse(status_code=exc.status_code, content=detail)
    return JSONResponse(status_code=exc.status_code, content={"detail": detail})


@cdm_app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError):
    """Mirror the gateway's validation handler (app.py) so test responses match prod."""
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "message": "Validation failed",
            "errors": [
                {
                    "field": e["loc"][-1],
                    "message": e["msg"].replace("Value error, ", ""),
                }
                for e in exc.errors()
            ],
        },
    )


async def _admin_stub() -> TokenData:
    """Force an admin identity so admin-gated endpoints pass in tests.

    Overriding get_current_user is more robust than the AUTH_ENABLED env switch:
    auth_deps captures AUTH_ENABLED at import time, and other test modules may
    import it before this module sets the env var. The override always wins."""
    return TokenData(
        sub="test-admin", email="test@example.com", username="tester", roles=["admin"]
    )


async def _cookie_auth_stub(access_token: Optional[str] = Cookie(None)) -> TokenData:
    """Module-level default override: behaves like the real get_current_user for
    cookie presence — raises 401 when the cookie is absent, returns admin when any
    cookie value is present.  Tests that need full admin access use the ``client``
    fixture, which replaces this with _admin_stub for their duration."""
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "success": False,
                "message": "Could not validate credentials",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )
    return TokenData(
        sub="test-admin", email="test@example.com", username="tester", roles=["admin"]
    )


cdm_app.dependency_overrides[get_current_user] = _cookie_auth_stub

_seg = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip("=")
_DEFAULT_COOKIE_TOKEN = (
    f"{_seg({'alg': 'HS256', 'typ': 'JWT'})}."
    f"{_seg({'sub': 'test-admin', 'email': 'test@example.com', 'preferred_username': 'tester', 'roles': ['admin']})}.sig"
)

DEV_TOKEN = _DEFAULT_COOKIE_TOKEN

TEST_PLANT_MARKER = "_testcase"
PLANT_CODE = f"plant_1{TEST_PLANT_MARKER}"


def assert_test_plant(plant_code_id: str) -> str:
    """Refuse to let a test create or drop a plant that isn't clearly a test plant.

    Guards the destructive helpers in the real-provisioning suites, which issue
    CREATE/DROP DATABASE against the configured Postgres.
    """
    if not plant_code_id.endswith(TEST_PLANT_MARKER):
        raise AssertionError(
            f"refusing to touch plant '{plant_code_id}': test plants must end with "
            f"'{TEST_PLANT_MARKER}'"
        )
    return plant_code_id


import atexit as _atexit
import logging as _logging
import p0.api.plants as _plants_boot

assert_test_plant(PLANT_CODE)
_plants_boot._drop_plant_databases(PLANT_CODE)
_plants_boot._unregister_plant(PLANT_CODE)
_plants_boot._provision_plant_databases(PLANT_CODE)


def _drop_test_plant_db():
    _logging.disable(_logging.CRITICAL)
    try:
        _plants_boot._drop_plant_databases(PLANT_CODE)
        _plants_boot._unregister_plant(PLANT_CODE)  # sync may adopt it into the registry
    except Exception:
        pass


_atexit.register(_drop_test_plant_db)


def get_auth_headers():
    """Standard mock authorization headers/cookies for a p0 request."""
    return {
        "Cookie": f"access_token={DEV_TOKEN}",
        "Content-Type": "application/json",
        "accept": "application/json",
        "X-PlantCodeId": PLANT_CODE,
    }


def assert_standard_response(response, status_code=200):
    """Validate the standard {success, message, data} response envelope."""
    assert response.status_code == status_code, response.text
    data = response.json()
    assert (
        data.get("success") is True
        or data.get("Success") is True
        or data.get("statusCode") == 200
    )
    assert any(k in data for k in ["message", "Message"])
    assert any(k in data for k in ["data", "Data"])
    return data


class MountedTestClient(TestClient):
    """TestClient that prepends the CDM mount prefix to absolute request paths
    and defaults the plant scope, so tests can address endpoints as
    ``/configRouter/plants`` without repeating the mount or plant_code_id."""

    def request(self, method, url, *args, **kwargs):
        if isinstance(url, str) and url.startswith("/"):
            if CDM_MOUNT and not url.startswith(CDM_MOUNT):
                url = CDM_MOUNT + url
            if "plant_code_id=" not in url and "plant_code_id" not in str(
                kwargs.get("params") or {}
            ):
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}plant_code_id={PLANT_CODE}"
        return super().request(method, url, *args, **kwargs)


def _apply_plant_env(request, monkeypatch):
    """Make the default test plant valid and route plant-scoped work to the real
    per-plant test database (see _session_test_plant_db).

    validate_plant_code(PLANT_CODE) passes and connection routing resolves to the
    test plant's own database, so reads/writes land where the tests can inspect
    them. Provisioning/dropping are stubbed to protect that session database: a
    suite not marked @pytest.mark.real_provisioning cannot CREATE/DROP a database,
    whatever it asks the API for. The real-provisioning suites opt out of all of
    this (see test_plant_provisioning.py)."""
    if request.node.get_closest_marker("real_provisioning"):
        return
    import p0.api.plants as _plants

    test_db = _plants.plant_db_name(PLANT_CODE)

    def _is_test_plant(plant_code_id: str) -> bool:
        return (plant_code_id or "").strip() == PLANT_CODE

    def _no_provisioning(plant_code_id, *_args, **_kwargs):
        raise AssertionError(
            f"test tried to provision a database for '{plant_code_id}' without "
            f"@pytest.mark.real_provisioning"
        )

    def _no_drop(plant_code_id, *_args, **_kwargs):
        raise AssertionError(
            f"test tried to drop the database for '{plant_code_id}' without "
            f"@pytest.mark.real_provisioning"
        )

    monkeypatch.setattr(_plants, "plant_db_exists", _is_test_plant)
    monkeypatch.setattr(_plants, "plant_is_registered", _is_test_plant)
    monkeypatch.setattr(
        _plants,
        "registered_plant_db_name",
        lambda pc: test_db if _is_test_plant(pc) else None,
    )
    monkeypatch.setattr(_plants, "_provision_plant_databases", _no_provisioning)
    monkeypatch.setattr(_plants, "_drop_plant_databases", _no_drop)


@pytest.fixture(scope="session")
def app():
    """The wrapped CDM FastAPI app — for tests that need dependency_overrides or
    to drive it with their own TestClient."""
    return cdm_app


@pytest.fixture()
def plant_env(request, monkeypatch):
    """Explicit form of the plant-scope setup for tests that hit the data layer
    directly (no HTTP client). Client-based tests get it automatically."""
    _apply_plant_env(request, monkeypatch)
    yield


@pytest.fixture()
def client(request, monkeypatch):
    """Per-test mounted client with the default admin cookie + plant scope, plus
    the plant-scope setup applied (validate passes, routing → shared test DB)."""
    _apply_plant_env(request, monkeypatch)
    cdm_app.dependency_overrides[get_current_user] = _admin_stub
    try:
        with MountedTestClient(
            cdm_app,
            cookies={"access_token": _DEFAULT_COOKIE_TOKEN},
            headers={"X-PlantCodeId": PLANT_CODE},
        ) as c:
            yield c
    finally:
        cdm_app.dependency_overrides[get_current_user] = _cookie_auth_stub


@pytest.fixture()
def sandbox_user_config():
    """Snapshot user_config.yaml, restore after the test, so config-writing
    endpoints don't pollute the dev tree. Returns the path to read/poke."""
    path = Path(USER_CONFIG_FILE)
    backup_bytes = path.read_bytes() if path.exists() else None
    try:
        yield path
    finally:
        if backup_bytes is None:
            path.unlink(missing_ok=True)
        else:
            path.write_bytes(backup_bytes)


@pytest.fixture()
def sandbox_sap_stage(tmp_path, monkeypatch):
    """Redirect the SAP stage dir to a temp location so commit-flow tests can
    populate it and assert cleanup — without touching the real RustFS/local
    stage dir."""
    fake_root = tmp_path / "stage"
    sap_dir = fake_root / "sap"
    sap_dir.mkdir(parents=True)
    (sap_dir / "entities").mkdir()
    (sap_dir / "source").mkdir()

    real_resolver = ctx._resolve_stage_dir

    def _fake_resolve(stage: str, plant_code_id: str | None = None) -> str:
        if stage == "sap":
            return str(sap_dir)
        return real_resolver(stage, plant_code_id)

    monkeypatch.setattr(ctx, "_resolve_stage_dir", _fake_resolve)

    yield sap_dir
