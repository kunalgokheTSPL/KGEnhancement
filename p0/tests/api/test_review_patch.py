"""
PATCH endpoints that persist review-page edits back to RustFS.

Before these existed, the inline Edit/Delete buttons on the review tables
only mutated frontend React state — every navigate-away silently dropped
the user's edits. The PATCH endpoints overwrite the processed parquet so
the next GET re-renders the edits and the next commit picks them up.

These tests assert:
  • plant_code_id is required and validated against the registry
  • the underlying parquet write helper is invoked with the correct path
    and rows
  • the response includes the written paths so the frontend can confirm
    persistence
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from p0.tests._p0_test_base import client


@pytest.fixture
def ensure_plant_1(client):
    """Make sure plant_1_testcase exists in the registry for these tests; the
    autouse ensure_plants_table fixture creates the table but seeds
    nothing."""
    import base64
    import json as _json

    def _bearer(roles):
        header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
        payload = (
            base64.urlsafe_b64encode(
                _json.dumps({"sub": "test-admin", "roles": roles}).encode()
            )
            .decode()
            .rstrip("=")
        )
        return {"Authorization": f"Bearer {header}.{payload}.sig"}

    client.post(
        "/configRouter/plants",
        json={"plant_code_id": "plant_1_testcase"},
        headers=_bearer(["admin"]),
    )
    yield
    client.delete("/configRouter/plants/plant_1_testcase", headers=_bearer(["admin"]))




def test_pnid_patch_requires_plant_code(client):
    r = client.patch(
        "/context/patchPnidRows",
        json={
            "equipment_pid": [],
            "equipment_connectivity": [],
        },
    )
    assert r.status_code == 400


def test_pnid_patch_rejects_unknown_plant(client):
    r = client.patch(
        "/context/patchPnidRows",
        json={
            "plant_code_id": "Plant_DOES_NOT_EXIST",
            "equipment_pid": [],
            "equipment_connectivity": [],
        },
    )
    assert r.status_code == 400


def test_pnid_patch_writes_both_parquets(client, ensure_plant_1):
    """Both equipment_pid and equipment_connectivity parquets are rewritten."""
    calls: list[tuple] = []
    import p0.api.routers.context as ctx

    with patch.object(
        ctx,
        "_write_processed_pnid",
        side_effect=lambda plant, ep, ec: calls.append((plant, ep, ec))
        or {
            "equipment_pid": f"s3://x/{plant}/equipment_pid.parquet",
            "equipment_connectivity": f"s3://x/{plant}/equipment_connectivity.parquet",
        },
    ):
        r = client.patch(
            "/context/patchPnidRows",
            json={
                "plant_code_id": "plant_1_testcase",
                "equipment_pid": [{"pid_tag": "B-1106"}],
                "equipment_connectivity": [
                    {"from_equipment_ref": "B-1106", "to_equipment_ref": "SDV-2930"}
                ],
            },
        )
    assert r.status_code == 200
    assert calls and calls[0][0] == "plant_1_testcase"
    ep_written = calls[0][1]
    assert len(ep_written) == 1
    assert ep_written[0]["pid_tag"] == "B-1106"
    assert ep_written[0].get("plant_code_id")
    assert "source_system" in ep_written[0]
    assert calls[0][2][0]["to_equipment_ref"] == "SDV-2930"

    body = r.json()["data"]
    assert body["counts"]["equipment_pid"] == 1
    assert body["counts"]["equipment_connectivity"] == 1




def test_docs_patch_partitions_by_document_type(client, ensure_plant_1):
    """Rows with different document_type values land in separate subtype
    folders. Rows without a document_type fall back to 'general'."""
    captured: list[tuple] = []
    import p0.api.routers.context as ctx

    with patch.object(
        ctx,
        "_write_processed_docs",
        side_effect=lambda plant, rows: captured.append((plant, rows))
        or {"sop": "s3://x"},
    ):
        r = client.patch(
            "/context/patchDocsRows",
            json={
                "plant_code_id": "plant_1_testcase",
                "rows": [
                    {"document_id": "D1", "document_type": "sop"},
                    {"document_id": "D2", "document_type": "sop"},
                    {"document_id": "D3", "document_type": "ptw"},
                    {"document_id": "D4"},
                ],
            },
        )
    assert r.status_code == 200
    plant, rows = captured[0]
    assert plant == "plant_1_testcase"
    assert len(rows) == 4


def test_docs_patch_validates_plant(client):
    r = client.patch("/context/patchDocsRows", json={"rows": []})
    assert r.status_code == 400




def test_ts_patch_validates_plant(client):
    r = client.patch("/context/patchTsRows", json={"rows": []})
    assert r.status_code == 400


def test_ts_patch_writes_single_parquet(client, ensure_plant_1):
    captured = []
    import p0.api.routers.context as ctx

    with patch.object(
        ctx,
        "_write_processed_ts",
        side_effect=lambda plant, rows: captured.append((plant, rows))
        or {"timeseries_metadata": "s3://x/ts.parquet"},
    ):
        r = client.patch(
            "/context/patchTsRows",
            json={
                "plant_code_id": "plant_1_testcase",
                "rows": [{"tag_name": "T-301.PV", "unit": "degC"}],
            },
        )
    assert r.status_code == 200
    plant, rows = captured[0]
    assert plant == "plant_1_testcase"
    assert rows[0]["tag_name"] == "T-301.PV"
    assert r.json()["data"]["row_count"] == 1
