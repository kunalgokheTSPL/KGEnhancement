"""
Upload-authority tests.

The plant_code_id chosen on the upload page must override whatever plant_code_id
the uploaded file contains. This protects against the historical mess
where the same DB had rows tagged B010 / CP01 / CDM / CEMENT_PLANT_1 /
NULL — different sources of truth all leaking into one table.

We test the helper ``_apply_plant_code`` directly (pure function) plus
the SAP commit endpoint (the only commit with a tractable shape for
verification without disk / RustFS round-trips).
"""

from __future__ import annotations

import pytest

from p0.tests._p0_test_base import client

from p0.api.services.transforms import _apply_plant_code


@pytest.fixture(autouse=True)
def _register_plant_1(monkeypatch):
    import p0.api.plants as _plants

    test_db = _plants.plant_db_name("plant_1_testcase")

    def _is_plant_1(plant_code_id: str) -> bool:
        return (plant_code_id or "").strip() == "plant_1_testcase"

    monkeypatch.setattr(_plants, "plant_db_exists", _is_plant_1)
    monkeypatch.setattr(_plants, "plant_is_registered", _is_plant_1)
    monkeypatch.setattr(
        _plants, "registered_plant_db_name",
        lambda pc: test_db if _is_plant_1(pc) else None,
    )
    yield




def test_apply_plant_code_overrides_existing_value():
    rows = [
        {"id": 1, "plant_code_id": "B010"},
        {"id": 2, "plant_code_id": "CP01"},
    ]
    out = _apply_plant_code(rows, "plant_1_testcase")
    assert all(r["plant_code_id"] == "plant_1_testcase" for r in out)


def test_apply_plant_code_sets_when_missing():
    rows = [{"id": 1}, {"id": 2}]
    out = _apply_plant_code(rows, "plant_1_testcase")
    assert all(r["plant_code_id"] == "plant_1_testcase" for r in out)


def test_apply_plant_code_noop_when_param_none():
    rows = [{"id": 1, "plant_code_id": "B010"}, {"id": 2}]
    out = _apply_plant_code(rows, None)
    assert out[0]["plant_code_id"] == "B010"
    assert "plant_code_id" not in out[1]


def test_apply_plant_code_noop_when_param_empty_string():
    rows = [{"id": 1, "plant_code_id": "B010"}]
    out = _apply_plant_code(rows, "")
    assert out[0]["plant_code_id"] == "B010"


def test_apply_plant_code_rejects_unknown_plant():
    """Validation happens up-front so an unknown plant_code_id raises 400
    before we touch any rows."""
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        _apply_plant_code([{"id": 1}], "Plant_NEVER_REGISTERED")
    assert excinfo.value.status_code == 400




@pytest.fixture
def captured_writes(monkeypatch):
    """Stub _write_to_db so the test can inspect what would land in
    Postgres without actually writing."""
    import p0.api.routers.context as ctx

    calls: list[tuple[str, list[dict]]] = []

    def _fake(table_name, rows, drop_existing=False, plant_code_id=None):
        calls.append((table_name, [dict(r) for r in rows]))
        return len(rows)

    monkeypatch.setattr(ctx, "_write_to_db", _fake)
    return calls


@pytest.fixture(autouse=True)
def _stub_side_effects(monkeypatch):
    import p0.api.routers.context as ctx

    monkeypatch.setattr(ctx, "_cleanup_stage", lambda *a: None)
    monkeypatch.setattr(ctx, "_replace_by_natural_key", lambda *a, **k: 0)


def test_sap_commit_overrides_plant_code_in_every_row(client, captured_writes):
    """File rows say 'B010', selector says 'plant_1_testcase' → DB gets 'plant_1_testcase'."""
    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "equipment": [
                    {"equipment_id": "EQ-A", "plant_code_id": "B010"},
                    {
                        "equipment_id": "EQ-B",
                        "plant_code_id": "CP01",
                    },
                ],
            },
        },
    )
    assert r.status_code == 200, r.text

    _, rows = captured_writes[0]
    assert all(r["plant_code_id"] == "plant_1_testcase" for r in rows), rows


def test_sap_commit_requires_plant_code(client, captured_writes):
    """SAP commit now REQUIRES plant_code_id (parity with pnid/docs/ts). A body
    without it is rejected with 400 — the upload-authority guarantee can't be
    enforced without knowing the target plant. (Previously this passed file
    values through unchanged; that loophole is closed.)"""
    r = client.post(
        "/context/commitSapContext",
        json={
            "tables": {
                "equipment": [
                    {"equipment_id": "EQ-A", "plant_code_id": "B010"},
                ],
            },
        },
    )
    assert r.status_code == 400
    assert "plant_code_id" in r.text
    assert captured_writes == [], "no DB write should happen when validation fails"


def test_sap_commit_rejects_unknown_plant_code(client, captured_writes):
    """Unknown plant_code_id → 400 before any DB work runs."""
    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "Plant_NEVER_REGISTERED",
            "tables": {
                "equipment": [{"equipment_id": "EQ-A"}],
            },
        },
    )
    assert r.status_code == 400
    assert captured_writes == [], "writer must not be called when validation fails"
