"""
Timeseries commit → updated_at stamping test.

What we're protecting:
  ``timeseries_metadata.updated_at`` has no DEFAULT in schema.yaml, and the
  UPSERT binds ``row.get("updated_at")`` directly. If the commit handler didn't
  stamp it, the column committed as NULL. This regressed once and is easy to
  reintroduce, so lock it in: every row handed to the DB writer must carry a
  non-null ``updated_at``.

We mock the DB-writing function (and the KG/cleanup side-effects) so no real
Postgres is needed — we only assert what rows the writer was handed.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from p0.tests._p0_test_base import client

from p0.api.routers import context as ctx


@pytest.fixture
def _no_side_effects():
    """Stub out the non-DB side-effects of a ts commit so the test is hermetic."""
    with (
        patch.object(ctx, "_cleanup_stage", lambda *a, **k: None),
        patch.object(ctx, "_record_commit_audit", lambda *a, **k: None),
    ):
        yield


def test_ts_commit_stamps_updated_at_on_every_row(client, _no_side_effects):
    """POST /context/ts/commit must stamp a non-null updated_at on every row
    before the upsert — otherwise the column lands NULL (no DB default)."""
    captured = {}

    def _fake_upsert(rows, plant_code_id=None):
        captured["rows"] = rows
        return (len(rows), 0, 0)

    with patch.object(ctx, "_upsert_ts_metadata", _fake_upsert):
        resp = client.post(
            "/context/commitTsContext",
            json={
                "plant_code_id": "plant_1_testcase",
                "rows": [
                    {"tag_name": "TAG-1", "op_limit_h": "10"},
                    {"tag_name": "TAG-2"},
                ],
            },
        )

    assert resp.status_code == 200, resp.text
    rows = captured.get("rows")
    assert rows is not None and len(rows) == 2
    for r in rows:
        assert r.get("updated_at") is not None, f"updated_at not stamped on {r}"


def test_ts_commit_requires_plant_code(client, _no_side_effects):
    """Canonical body requires plant_code_id (400 when missing)."""
    with patch.object(ctx, "_upsert_ts_metadata", lambda rows, plant_code_id=None: (0, 0, 0)):
        resp = client.post("/context/commitTsContext", json={"rows": [{"tag_name": "T"}]})
    assert resp.status_code == 400
