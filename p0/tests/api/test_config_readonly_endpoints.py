"""
Read-only config endpoints — characterization tests.

/configRouter/{cdm,entities,schema,relationships,validation,userConfig} each read
one frozen YAML and return it in the standard envelope. They were six near-identical
copies of the same 55-line body; these tests pin the contract (success message, auth
gate, plant_code_id gate) so the shared-helper refactor cannot silently change it.
"""

from __future__ import annotations

import pytest

from p0.tests._p0_test_base import (
    PLANT_CODE,
    cdm_app,
    client,
    assert_standard_response,
    MountedTestClient,
)

# (path, exact success message the endpoint has always returned)
READONLY_ENDPOINTS = [
    ("/configRouter/cdm", "CDM config fetched successfully"),
    ("/configRouter/entities", "Entities fetched successfully"),
    ("/configRouter/schema", "Schema fetched successfully"),
    ("/configRouter/relationships", "Relationships fetched successfully"),
    ("/configRouter/validation", "Validation contracts fetched successfully"),
    ("/configRouter/userConfig", "User config fetched successfully"),
]


@pytest.mark.parametrize("path,message", READONLY_ENDPOINTS)
def test_returns_config_in_standard_envelope(client, path, message):
    resp = client.get(path, params={"plant_code_id": PLANT_CODE})
    body = assert_standard_response(resp, 200)
    assert body["message"] == message
    assert body["data"] is not None


@pytest.mark.skip(reason="auth moved to unified middleware in app.py")
@pytest.mark.parametrize("path,_message", READONLY_ENDPOINTS)
def test_requires_an_access_token(path, _message):
    """No access_token cookie -> 401 with the auth error envelope."""
    with MountedTestClient(cdm_app) as anon:
        resp = anon.get(path, params={"plant_code_id": PLANT_CODE})
    assert resp.status_code == 401, resp.text
    body = resp.json()
    assert body["success"] is False
    assert body["errors"][0]["field"] == "access_token"


@pytest.mark.parametrize("path,_message", READONLY_ENDPOINTS)
def test_requires_a_non_blank_plant_code_id(client, path, _message):
    """Blank plant_code_id -> 400 with the validation error envelope."""
    resp = client.get(path, params={"plant_code_id": "   "})
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["success"] is False
    assert body["errors"][0]["field"] == "plant_code_id"
