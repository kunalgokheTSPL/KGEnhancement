"""
SAP commit → Postgres write tests.

What we're protecting:
  - The /context/sap/commit endpoint dispatches rows to ``_write_to_db``
    once per table in the request body.
  - User-defined column renames from ``user_config.yaml`` are applied
    *before* the row hits ``_write_to_db`` (so SAP field names like
    ``EQUNR`` become canonical names like ``equipment_id``).
  - Table-name routing: canonical-schema tables write directly, custom
    tables write directly, anything else falls back to a ``cdm_sap_``
    prefix.
  - The underlying type coercion (string into BIGSERIAL, empty into
    NUMERIC, etc.) is already covered exhaustively in
    test_pnid_commit_db_write — these tests only check the SAP-side
    dispatch is wired up so coercion gets the chance to run.
"""

from __future__ import annotations


import pytest

from p0.tests._p0_test_base import client, sandbox_user_config




@pytest.fixture
def captured_writes(monkeypatch):
    """Replace _write_to_db with a recorder. Tests inspect what would have
    been written without touching Postgres."""
    import p0.api.routers.context as ctx

    calls: list[tuple[str, list[dict]]] = []

    def _fake(table_name, rows, drop_existing=False, plant_code_id=None):
        calls.append((table_name, [dict(r) for r in rows]))
        return len(rows)

    monkeypatch.setattr(ctx, "_write_to_db", _fake)
    return calls


@pytest.fixture(autouse=True)
def _stub_kg_and_cleanup(monkeypatch):
    """SAP commit also fires KG snapshot + stage cleanup — orthogonal to
    what we're testing, and they hit disk/RustFS. Stub them all. Also stub
    the new plant-validation + dedup pre-delete (both hit external systems)."""
    import p0.api.routers.context as ctx
    import p0.api.plants as plants

    monkeypatch.setattr(ctx, "_cleanup_stage", lambda *_: None)
    monkeypatch.setattr(ctx, "_replace_by_natural_key", lambda *a, **k: 0)
    monkeypatch.setattr(plants, "validate_plant_code", lambda _p: None)




def test_commit_routes_canonical_table_directly(client, captured_writes):
    """`equipment` is in the canonical schema → write goes to "equipment"
    (no `cdm_sap_` prefix)."""
    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "equipment": [{"equipment_id": "EQ-001", "plant_code_id": "TEST"}],
            },
        },
    )
    assert r.status_code == 200, r.text

    assert len(captured_writes) == 1
    db_table, rows_written = captured_writes[0]
    assert db_table == "equipment"
    assert rows_written[0]["equipment_id"] == "EQ-001"


def test_commit_prefixes_unknown_table_with_cdm_sap(client, captured_writes):
    """An arbitrary table name not in the canonical schema gets a
    `cdm_sap_` prefix so it can't collide with curated tables."""
    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "some_random_zztable": [{"col1": "value"}],
            },
        },
    )
    assert r.status_code == 200

    assert len(captured_writes) == 1
    db_table, _ = captured_writes[0]
    assert db_table == "cdm_sap_some_random_zztable"


def test_commit_applies_user_column_renames(
    client, captured_writes, sandbox_user_config, monkeypatch
):
    """User overrides in user_config.yaml rewrite source column names
    before the row hits _write_to_db.

    The frontend may still send `EQUNR` (the SAP field name); the
    overrides map it to `equipment_id` (the canonical column). After
    commit, `equipment_id` is what _write_to_db sees, not `EQUNR`.
    """
    import yaml

    sandbox_user_config.write_text(
        yaml.safe_dump(
            {
                "sap_processing": {
                    "column_rename_overrides": {
                        "equipment": {"mappings": {"EQUNR": "equipment_id"}},
                    },
                },
            }
        )
    )

    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "equipment": [{"EQUNR": "10001452", "plant_code_id": "CDM"}],
            },
        },
    )
    assert r.status_code == 200

    _, rows = captured_writes[0]
    assert "EQUNR" not in rows[0], "user rename must remove old key"
    assert rows[0]["equipment_id"] == "10001452"


def test_commit_processes_each_table_independently(client, captured_writes):
    """Multiple tables in one request → one _write_to_db call per table,
    each with its own rows."""
    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "equipment": [{"equipment_id": "EQ-1"}, {"equipment_id": "EQ-2"}],
                "work_order": [{"work_order_id": "WO-1"}],
            },
        },
    )
    assert r.status_code == 200

    by_table = {t: rows for t, rows in captured_writes}
    assert "equipment" in by_table
    assert "work_order" in by_table
    assert len(by_table["equipment"]) == 2
    assert len(by_table["work_order"]) == 1


def test_commit_skips_empty_table(client, captured_writes):
    """A table with 0 rows doesn't trigger _write_to_db at all — keeps
    the log clean and avoids unnecessary CREATE TABLE roundtrips."""
    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "equipment": [{"equipment_id": "EQ-1"}],
                "work_order": [],
            },
        },
    )
    assert r.status_code == 200

    tables_written = [t for t, _ in captured_writes]
    assert "equipment" in tables_written
    assert "work_order" not in tables_written


def test_commit_returns_total_rows_written(client, captured_writes):
    """The response surfaces the total across all tables — frontend uses
    this in the success toast."""
    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "equipment": [{"a": 1}, {"a": 2}, {"a": 3}],
                "work_order": [{"b": 1}],
            },
        },
    )
    assert r.status_code == 200
    assert r.json()["data"]["rows_written"] == 4
    assert r.json()["data"]["status"] == "committed"
