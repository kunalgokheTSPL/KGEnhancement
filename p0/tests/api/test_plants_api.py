"""
Plant registry API — lifecycle test suite (aligned with the team TEST CASES GUIDE).

Sequential lifecycle: create -> read (list) -> delete, sharing the created
plant_code_id between tests via a module global. Every assertion validates the
standard {success, message, data} response envelope via assert_standard_response
rather than hardcoding DB-specific values.

Uses the centralized p0 test base (client / auth / validator) instead of a local
conftest. Creating a plant provisions a real per-plant database, so this suite
carries @pytest.mark.real_provisioning and always deletes what it creates.
"""

import uuid

import pytest

from p0.tests._p0_test_base import client, assert_standard_response, assert_test_plant

pytestmark = pytest.mark.real_provisioning

PLANT_CODE_ID = f"plants_api_{uuid.uuid4().hex[:8]}_testcase"
PLANT_CREATED = False


@pytest.fixture(scope="module", autouse=True)
def _drop_leftover_plant():
    """Safety net: remove the plant's database and registry row after the suite
    even if the delete test never ran, so real provisioning never leaves an
    orphan behind."""
    yield
    from p0.api import plants as _plants

    assert_test_plant(PLANT_CODE_ID)
    _plants._drop_plant_databases(PLANT_CODE_ID)
    _plants._unregister_plant(PLANT_CODE_ID)


def test_create_plant(client):
    global PLANT_CREATED

    response = client.post(
        "/configRouter/plants",
        json={
            "plant_code_id": PLANT_CODE_ID,
            "label": "pytest plant",
            "industry": "generic",
        },
    )
    data = assert_standard_response(response, status_code=201)
    assert data["data"]["plant_code_id"] == PLANT_CODE_ID
    PLANT_CREATED = True


def test_list_plants_contains_created(client):
    if not PLANT_CREATED:
        pytest.skip("No plant created --- skipping list check")

    response = client.get(f"/configRouter/plants?plant_code_id={PLANT_CODE_ID}")
    data = assert_standard_response(response)
    codes = [p["plant_code_id"] for p in data["data"]["plants"]]
    assert PLANT_CODE_ID in codes


def test_create_duplicate_plant_conflict(client):
    if not PLANT_CREATED:
        pytest.skip("No plant created --- skipping duplicate check")

    response = client.post(
        "/configRouter/plants",
        json={"plant_code_id": PLANT_CODE_ID},
    )
    assert response.status_code == 409


def test_delete_plant(client):
    global PLANT_CREATED
    if not PLANT_CREATED:
        pytest.skip("No plant created --- skipping delete check")

    response = client.delete(
        f"/configRouter/plants/{PLANT_CODE_ID}?plant_code_id={PLANT_CODE_ID}"
    )
    data = assert_standard_response(response)
    assert data["data"]["status"] == "deleted"
    PLANT_CREATED = False


def test_list_plants_excludes_deleted(client):
    response = client.get(f"/configRouter/plants?plant_code_id={PLANT_CODE_ID}")
    data = assert_standard_response(response)
    codes = [p["plant_code_id"] for p in data["data"]["plants"]]
    assert PLANT_CODE_ID not in codes
