"""flow_state — durable per-file onboarding flow tracking."""

from __future__ import annotations

import os
import tempfile

import pytest

from p0.tests._p0_test_base import app, client




def _write(content: bytes) -> str:
    tmp = tempfile.NamedTemporaryFile(prefix="flow_test_", delete=False, dir="/tmp")
    tmp.write(content)
    tmp.flush()
    tmp.close()
    return tmp.name


def test_hash_full_is_stable_and_content_sensitive():
    from p0.api.services import flow

    a = _write(b"col1,col2\n1,2\n")
    b = _write(b"col1,col2\n1,2\n")
    c = _write(b"col1,col2\n9,9\n")
    try:
        ha, hb, hc = (flow.hash_file(p) for p in (a, b, c))
        assert ha == hb, "identical metadata bytes must hash equal (same file)"
        assert ha != hc, "different bytes must hash differently (new file, no lock)"
        assert len(ha) == 64
    finally:
        for p in (a, b, c):
            os.unlink(p)


def test_hash_bytes_matches_full_file_hash():
    """Docs upload holds bytes in memory and uses hash_bytes; it must agree with."""
    from p0.api.services import flow

    content = b"%PDF-1.4 fake doc bytes"
    p = _write(content)
    try:
        assert flow.hash_bytes(content) == flow.hash_file(p)
        assert flow.hash_bytes(content) != flow.hash_bytes(content + b"x")
    finally:
        os.unlink(p)


def test_hash_bytes_salt_distinguishes_same_bytes():
    """Same file under two different doc type/subtype combos must get DISTINCT."""
    from p0.api.services import flow

    content = b"%PDF-1.4 identical bytes"
    h_sop = flow.hash_bytes(content, "report.pdf", salt="sop")
    h_ds = flow.hash_bytes(content, "report.pdf", salt="datasheet")
    assert h_sop != h_ds
    assert h_sop == flow.hash_bytes(content, "report.pdf", salt="sop")
    assert flow.hash_bytes(content) == flow.hash_bytes(content, "x", salt="")


def test_hash_sampled_detects_head_tail_change():
    from p0.api.services import flow

    big = b"HEAD" + b"x" * (3 * 1024 * 1024) + b"TAIL"
    p1 = _write(big)
    p2 = _write(b"DIFF" + b"x" * (3 * 1024 * 1024) + b"TAIL")
    try:
        h1 = flow.hash_file(p1, sampled=True)
        h2 = flow.hash_file(p2, sampled=True)
        assert h1 != h2
        assert flow.hash_file(p1, sampled=True) == h1
    finally:
        os.unlink(p1)
        os.unlink(p2)




@pytest.fixture
def plant(client):
    import base64
    import json as _json

    def _bearer(roles):
        h = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
        p = (
            base64.urlsafe_b64encode(
                _json.dumps({"sub": "test-admin", "roles": roles}).encode()
            )
            .decode()
            .rstrip("=")
        )
        return {"Authorization": f"Bearer {h}.{p}.sig"}

    client.post(
        "/configRouter/plants", json={"plant_code_id": "plant_1_testcase"}, headers=_bearer(["admin"])
    )
    yield "plant_1_testcase"
    client.delete("/configRouter/plants/plant_1_testcase", headers=_bearer(["admin"]))


def _cleanup(client, plant_code_id, flow="ts"):
    r = client.get(f"/flow/state?plant_code_id={plant_code_id}&flow={flow}")
    for f in r.json()["data"].get("files") or []:
        client.delete(f"/flow/{f['flow_uid']}")


def test_flow_lifecycle_single_file(client, plant):
    h = "a" * 64
    try:
        r = client.post(
            "/flow/upsert",
            json={
                "plant_code_id": plant,
                "flow": "ts",
                "content_hash": h,
                "file_name": "meta.csv",
                "file_type": "metadata",
                "stage": "staged",
                "status": "done",
                "rustfs_path": "s3://x/meta.csv",
            },
        )
        assert r.status_code == 200, r.text
        uid = r.json()["data"]["flow_uid"]
        assert r.json()["data"]["stage"] == "staged"

        r = client.post(
            "/flow/upsert",
            json={
                "plant_code_id": plant,
                "flow": "ts",
                "content_hash": h,
                "stage": "processed",
                "row_count": 60,
            },
        )
        assert r.json()["data"]["flow_uid"] == uid, "same hash must update same row"
        assert r.json()["data"]["stage"] == "processed"
        assert r.json()["data"]["row_count"] == 60
        assert r.json()["data"]["rustfs_path"] == "s3://x/meta.csv", "COALESCE keeps breadcrumb"

        lst = client.get(f"/flow/state?plant_code_id={plant}&flow=ts").json()["data"]["files"]
        assert len([f for f in lst if f["content_hash"] == h]) == 1

        r = client.post(f"/flow/{uid}/reset", json={"plant_code_id": plant})
        assert r.json()["data"]["stage"] == "uploaded"
        assert r.json()["data"]["row_count"] is None

        assert client.delete(f"/flow/{uid}").status_code == 200
        assert client.get(f"/flow/state/{uid}").status_code == 404
    finally:
        _cleanup(client, plant)


def test_two_different_files_coexist(client, plant):
    """The core fix: two different files must NOT collide → no UI lock."""
    try:
        h1, h2 = "1" * 64, "2" * 64
        client.post(
            "/flow/upsert",
            json={
                "plant_code_id": plant,
                "flow": "ts",
                "content_hash": h1,
                "file_name": "A.csv",
                "stage": "processed",
            },
        )
        client.post(
            "/flow/upsert",
            json={
                "plant_code_id": plant,
                "flow": "ts",
                "content_hash": h2,
                "file_name": "B.csv",
                "stage": "uploaded",
            },
        )
        files = client.get(f"/flow/state?plant_code_id={plant}&flow=ts").json()["data"]["files"]
        hashes = {f["content_hash"] for f in files}
        assert {h1, h2} <= hashes, "both files must have their own row"
    finally:
        _cleanup(client, plant)


def test_upsert_requires_identity(client, plant):
    r = client.post("/flow/upsert", json={"plant_code_id": plant, "flow": "ts"})
    assert r.status_code == 400


def test_advance_flow_for_stage_accepts_stage_kwarg(client, plant):
    """Regression: the pipeline helper takes the p0 stage positionally AND a."""
    from p0.api.routers.pipeline import _advance_flow_for_stage

    h = "f" * 64
    client.post(
        "/flow/upsert",
        json={
            "plant_code_id": plant,
            "flow": "ts",
            "content_hash": h,
            "file_name": "x.csv",
            "stage": "staged",
        },
    )
    try:
        _advance_flow_for_stage(
            "ts", plant, stage="processing", status="running", process_job_id="job-xyz"
        )
        files = client.get(f"/flow/state?plant_code_id={plant}&flow=ts").json()["data"]["files"]
        row = next(f for f in files if f["content_hash"] == h)
        assert row["stage"] == "processing"
        assert row["process_job_id"] == "job-xyz"
    finally:
        _cleanup(client, plant)


def test_docs_flow_files_coexist_by_subtype(client, plant):
    """Docs track one row per FILE under the 'docs' flow, with file_type holding."""
    try:
        client.post(
            "/flow/upsert",
            json={
                "plant_code_id": plant,
                "flow": "docs",
                "content_hash": "d1" * 32,
                "file_name": "manual.pdf",
                "file_type": "sop",
                "stage": "processed",
            },
        )
        client.post(
            "/flow/upsert",
            json={
                "plant_code_id": plant,
                "flow": "docs",
                "content_hash": "d2" * 32,
                "file_name": "incident.pdf",
                "file_type": "rca",
                "stage": "staged",
            },
        )
        files = client.get(f"/flow/state?plant_code_id={plant}&flow=docs").json()["data"]["files"]
        by_type = {f["file_name"]: f["file_type"] for f in files}
        assert by_type == {"manual.pdf": "sop", "incident.pdf": "rca"}
        ts = client.get(f"/flow/state?plant_code_id={plant}&flow=ts").json()["data"]["files"]
        assert ts == []
    finally:
        _cleanup(client, plant, flow="docs")


def test_the_same_bytes_under_two_names_are_two_rows():
    """A file is identified by its content AND its name."""
    from p0.api.services import flow as flow_service

    assert "file_name" in flow_service.CONFLICT_KEY


def test_the_conflict_key_columns_are_never_overwritten():
    from p0.api.services import flow as flow_service

    for column in flow_service.CONFLICT_KEY:
        assert column in flow_service._IMMUTABLE_ON_CONFLICT


def test_a_missing_file_name_cannot_create_duplicate_rows():
    """NULLs are distinct in a Postgres unique index, so the key must never be NULL."""
    from p0.api.services import flow as flow_service

    for blank in (None, "", "   "):
        name = flow_service._canonical_file_name(blank, "abc123def456789")
        assert name
        assert name == flow_service._canonical_file_name(blank, "abc123def456789")
    assert flow_service._canonical_file_name("real.xlsx", "h") == "real.xlsx"


def test_stage_is_still_reset_so_a_committed_connector_reopens():
    """Re-opening on a new file is required; only the name is pinned."""
    from p0.api.services import flow as flow_service

    assert "stage" not in flow_service._IMMUTABLE_ON_CONFLICT
    assert "labels" not in flow_service._IMMUTABLE_ON_CONFLICT
