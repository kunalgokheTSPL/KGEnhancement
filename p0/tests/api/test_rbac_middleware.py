"""RBAC tests for all 7 p0 routers using FastAPI dependency_overrides.

The router-level ``_admin_only = [Depends(require_admin)]`` gate (main.py) is
tested by temporarily swapping ``cdm_app.dependency_overrides[get_current_user]``:

  - No auth  (401): override raises HTTPException(401)
  - Non-admin (403): override returns viewer TokenData; require_admin fires 403
  - Admin     (200): ``client`` fixture — admin override already active
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.skip(reason="auth moved to unified middleware in app.py")

from contextlib import contextmanager

from fastapi import HTTPException
from fastapi.testclient import TestClient

from p0.tests._p0_test_base import app, client, cdm_app, PLANT_CODE, CDM_MOUNT
from p0.Login.auth.auth_deps import get_current_user, TokenData


_viewer_token_data = TokenData(
    sub="viewer", email="viewer@test.com", username="viewer", roles=["user"]
)
_P = {"plant_code_id": PLANT_CODE}


async def _no_auth_stub():
    raise HTTPException(status_code=401, detail="Not authenticated")


async def _viewer_stub():
    return _viewer_token_data


@contextmanager
def _as_auth(stub):
    """Temporarily swap get_current_user override and yield a plain TestClient."""
    prev = cdm_app.dependency_overrides.get(get_current_user)
    cdm_app.dependency_overrides[get_current_user] = stub
    try:
        with TestClient(cdm_app) as c:
            yield c
    finally:
        if prev is None:
            cdm_app.dependency_overrides.pop(get_current_user, None)
        else:
            cdm_app.dependency_overrides[get_current_user] = prev


# ===================================================================== flow

def test_flow_no_token_returns_401():
    with _as_auth(_no_auth_stub) as c:
        r = c.get(f"{CDM_MOUNT}/flow/state", params=_P)
    assert r.status_code == 401


def test_flow_non_admin_returns_403():
    with _as_auth(_viewer_stub) as c:
        r = c.get(f"{CDM_MOUNT}/flow/state", params=_P)
    assert r.status_code == 403


def test_flow_admin_returns_200(client):
    r = client.get("/flow/state")
    assert r.status_code == 200


# ===================================================================== logs

def test_logs_no_token_returns_401():
    with _as_auth(_no_auth_stub) as c:
        r = c.get(f"{CDM_MOUNT}/logs", params=_P)
    assert r.status_code == 401


def test_logs_non_admin_returns_403():
    with _as_auth(_viewer_stub) as c:
        r = c.get(f"{CDM_MOUNT}/logs", params=_P)
    assert r.status_code == 403


def test_logs_admin_returns_200(client):
    r = client.get("/logs")
    assert r.status_code == 200


# ===================================================================== data

def test_data_no_token_returns_401():
    with _as_auth(_no_auth_stub) as c:
        r = c.get(f"{CDM_MOUNT}/data/tables", params=_P)
    assert r.status_code == 401


def test_data_non_admin_returns_403():
    with _as_auth(_viewer_stub) as c:
        r = c.get(f"{CDM_MOUNT}/data/tables", params=_P)
    assert r.status_code == 403


def test_data_admin_returns_200(client):
    r = client.get("/data/tables")
    assert r.status_code == 200


# ================================================================= pipeline

def test_pipeline_no_token_returns_401():
    with _as_auth(_no_auth_stub) as c:
        r = c.get(f"{CDM_MOUNT}/pipeline/jobs", params=_P)
    assert r.status_code == 401


def test_pipeline_non_admin_returns_403():
    with _as_auth(_viewer_stub) as c:
        r = c.get(f"{CDM_MOUNT}/pipeline/jobs", params=_P)
    assert r.status_code == 403


def test_pipeline_admin_returns_200(client):
    r = client.get("/pipeline/jobs")
    assert r.status_code == 200


# =============================================================== connectors

def test_connectors_no_token_returns_401():
    with _as_auth(_no_auth_stub) as c:
        r = c.get(f"{CDM_MOUNT}/connectors/jobs", params=_P)
    assert r.status_code == 401


def test_connectors_non_admin_returns_403():
    with _as_auth(_viewer_stub) as c:
        r = c.get(f"{CDM_MOUNT}/connectors/jobs", params=_P)
    assert r.status_code == 403


def test_connectors_admin_returns_200(client):
    r = client.get("/connectors/jobs")
    assert r.status_code == 200


# ================================================================= context

def test_context_no_token_returns_401():
    with _as_auth(_no_auth_stub) as c:
        r = c.get(f"{CDM_MOUNT}/context/getCommitHistory", params=_P)
    assert r.status_code == 401


def test_context_non_admin_returns_403():
    with _as_auth(_viewer_stub) as c:
        r = c.get(f"{CDM_MOUNT}/context/getCommitHistory", params=_P)
    assert r.status_code == 403


def test_context_admin_returns_200(client):
    r = client.get("/context/getCommitHistory")
    assert r.status_code == 200


# =========================================================== config_router

def test_config_no_token_returns_401():
    with _as_auth(_no_auth_stub) as c:
        r = c.get(f"{CDM_MOUNT}/configRouter/plants")
    assert r.status_code == 401


def test_config_non_admin_returns_403():
    with _as_auth(_viewer_stub) as c:
        r = c.get(f"{CDM_MOUNT}/configRouter/plants")
    assert r.status_code == 403


def test_config_admin_returns_200(client):
    r = client.get("/configRouter/plants")
    assert r.status_code == 200
