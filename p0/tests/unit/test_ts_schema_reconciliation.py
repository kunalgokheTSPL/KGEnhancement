"""A lagging table must be reconciled, and a missing key column must fail loudly."""

from __future__ import annotations

import pytest

import p0.api.database.cdm_writer as cdm_writer

DECLARED = {
    "plant_code_id": "VARCHAR(64)",
    "site": "VARCHAR(120)",
    "tag_name": "VARCHAR(255)",
    "area": "VARCHAR(120)",
    "ts_uid": "BIGSERIAL",
}


class _FakeCursor:
    """A cursor whose table gains columns as ALTER statements are executed."""

    def __init__(self, present: set[str], refuse: set[str] | None = None) -> None:
        self.present = set(present)
        self.refuse = set(refuse or ())
        self.executed: list[str] = []
        self._rows: list[tuple] = []

    def execute(self, sql: str, params=None) -> None:
        self.executed.append(sql)
        if "information_schema.columns" in sql:
            self._rows = [(c, "character varying") for c in sorted(self.present)]
            return
        if sql.startswith("ALTER TABLE"):
            name = sql.split('ADD COLUMN IF NOT EXISTS "')[1].split('"')[0]
            if name in self.refuse:
                raise RuntimeError("permission denied")
            self.present.add(name)
        self._rows = []

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_a_column_the_table_lacks_is_added_with_its_declared_type():
    cur = _FakeCursor({"plant_code_id", "site", "tag_name"})
    conn = _FakeConn()
    live = cdm_writer._live_columns(cur, "timeseries_metadata")
    result = cdm_writer._reconcile_columns(
        cur, conn, "timeseries_metadata", DECLARED, live
    )
    assert "area" in result
    assert any('ADD COLUMN IF NOT EXISTS "area" VARCHAR(120)' in s for s in cur.executed)


def test_a_serial_column_is_never_added_by_reconciliation():
    cur = _FakeCursor({"plant_code_id", "site", "tag_name", "area"})
    conn = _FakeConn()
    live = cdm_writer._live_columns(cur, "timeseries_metadata")
    cdm_writer._reconcile_columns(cur, conn, "timeseries_metadata", DECLARED, live)
    assert not any("ts_uid" in s for s in cur.executed if s.startswith("ALTER"))


def test_a_column_that_cannot_be_added_is_reported_not_silently_dropped(caplog):
    cur = _FakeCursor({"plant_code_id", "site", "tag_name"}, refuse={"area"})
    conn = _FakeConn()
    live = cdm_writer._live_columns(cur, "timeseries_metadata")
    result = cdm_writer._reconcile_columns(
        cur, conn, "timeseries_metadata", DECLARED, live
    )
    assert "area" not in result
    assert conn.rollbacks == 1


def test_reconciliation_alters_nothing_when_the_table_is_already_current():
    cur = _FakeCursor({"plant_code_id", "site", "tag_name", "area"})
    conn = _FakeConn()
    live = cdm_writer._live_columns(cur, "timeseries_metadata")
    cdm_writer._reconcile_columns(cur, conn, "timeseries_metadata", DECLARED, live)
    assert [s for s in cur.executed if s.startswith("ALTER TABLE")] == []
    assert conn.commits == 0


def test_a_missing_upsert_key_column_raises_rather_than_writing(monkeypatch):
    cur = _FakeCursor({"plant_code_id", "tag_name", "area"})
    conn = _FakeConn()
    conn.cursor = lambda: cur
    conn.close = lambda: None
    monkeypatch.setattr(cdm_writer, "_get_conn", lambda plant_code_id=None: conn)
    monkeypatch.setattr(
        cdm_writer, "_ts_metadata_schema", lambda: ("ts_uid", dict(DECLARED))
    )
    with pytest.raises(RuntimeError, match="upsert key column"):
        cdm_writer._upsert_ts_metadata(
            [{"plant_code_id": "BOCPP", "tag_name": "T1"}], plant_code_id="BOCPP"
        )
    assert [s for s in cur.executed if s.startswith("INSERT")] == []
