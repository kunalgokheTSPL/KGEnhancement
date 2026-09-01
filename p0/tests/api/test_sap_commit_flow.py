"""SAP commit-flow tests."""

from __future__ import annotations

import pandas as pd
import pytest

from p0.tests._p0_test_base import client, sandbox_sap_stage




def _populate_sap_stage(sap_dir):
    """Write a couple of entity parquet files so GET /context/sap sees data."""
    eq = pd.DataFrame(
        [
            {"EQUNR": "1001", "EQKTX": "Crusher A"},
            {"EQUNR": "1002", "EQKTX": "Crusher B"},
        ]
    )
    eq.to_parquet(sap_dir / "entities" / "sap_equipment.parquet")
    wo = pd.DataFrame([{"AUFNR": "5001", "EQUNR": "1001"}])
    wo.to_parquet(sap_dir / "entities" / "sap_workorder.parquet")


@pytest.fixture()
def patched_commit_deps(monkeypatch):
    """Stub out everything in commit_sap_context that touches external systems:."""
    import p0.api.routers.context as ctx
    import p0.api.plants as plants

    monkeypatch.setattr(ctx, "_write_to_db", lambda table, rows, plant_code_id=None: len(rows))
    monkeypatch.setattr(
        ctx, "_replace_by_natural_key", lambda cur, table, rows, key, plant: 0
    )
    monkeypatch.setattr(ctx, "_get_conn", lambda *a, **k: _FakeConn())
    monkeypatch.setattr(plants, "validate_plant_code", lambda _p: None)
    yield


class _FakeConn:
    """Minimal connection stub for the dedup pre-delete path."""

    def cursor(self):
        return _FakeCur()

    def commit(self):
        pass

    def close(self):
        pass


class _FakeCur:
    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return (False,)

    rowcount = 0




def test_get_sap_lists_staged_tables_before_commit(client, sandbox_sap_stage):
    """Fresh stage → /context/sap returns the table list → button enabled."""
    _populate_sap_stage(sandbox_sap_stage)

    r = client.get("/context/getSapContext")
    assert r.status_code == 200

    body = r.json()["data"]
    assert body["source"] == "sap"
    table_names = {t["name"] for t in body["tables"]}
    assert {"sap_equipment", "sap_workorder"}.issubset(table_names), (
        f"expected both staged tables to surface, got {table_names}"
    )


def test_commit_returns_status_committed(
    client, sandbox_sap_stage, patched_commit_deps
):
    """Successful commit returns {status: committed, rows_written, tables}."""
    _populate_sap_stage(sandbox_sap_stage)

    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "sap_equipment": [{"EQUNR": "1001", "EQKTX": "Crusher A"}],
            },
        },
    )
    assert r.status_code == 200, r.text

    body = r.json()["data"]
    assert body["status"] == "committed"
    assert body["rows_written"] == 1
    assert "sap_equipment" in " ".join(body["tables"])


def test_processed_output_survives_the_commit(
    client, sandbox_sap_stage, patched_commit_deps
):
    """The processed output is kept after commit, for reference and re-commit."""
    _populate_sap_stage(sandbox_sap_stage)

    assert any((sandbox_sap_stage / "entities").iterdir())

    r = client.post(
        "/context/commitSapContext",
        json={
            "plant_code_id": "plant_1_testcase",
            "tables": {
                "sap_equipment": [{"EQUNR": "1001", "EQKTX": "Crusher A"}],
            },
        },
    )
    assert r.status_code == 200

    assert any((sandbox_sap_stage / "entities").iterdir()), (
        "the processed output must survive the commit"
    )

    r = client.get("/context/getSapContext")
    assert r.status_code == 200
    assert r.json()["data"]["tables"], "the committed tables stay readable for reference"


def test_an_empty_body_commits_the_reviewed_tables(
    client, sandbox_sap_stage, patched_commit_deps
):
    """The backend owns the reviewed state; the client need not resend it."""
    _populate_sap_stage(sandbox_sap_stage)

    r = client.post(
        "/context/commitSapContext", json={"plant_code_id": "plant_1_testcase", "tables": {}}
    )
    assert r.status_code == 200, r.text

    body = r.json()["data"]
    assert body["status"] == "committed"
    assert body["rows_written"] > 0, "the staged tables should have been committed"
    assert body["tables"], "the committed tables should be named in the response"


def test_a_commit_with_nothing_staged_is_a_loud_409(client, sandbox_sap_stage, patched_commit_deps):
    """Never processed, never reviewed — that is a skipped step, not a no-op."""
    for stale in (sandbox_sap_stage / "entities").iterdir():
        stale.unlink()

    r = client.post(
        "/context/commitSapContext", json={"plant_code_id": "plant_1_testcase", "tables": {}}
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["success"] is False
    assert "pipeline/run" in body["message"]
