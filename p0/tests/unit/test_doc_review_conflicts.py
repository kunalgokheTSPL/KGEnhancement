"""A reviewer editing a row someone else already moved is refused, not merged."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.database import cdm_writer

STORED = [
    ("S1", "REC-1", 3),
    ("S1", "REC-2", 1),
]


class _Cursor:
    """Answers the version probe and records every delete the writer sends."""

    def __init__(self, stored=(), present=()):
        self.stored = list(stored)
        self.present = set(present)
        self.deletes = []
        self.rowcount = 0
        self._rows = []

    def execute(self, sql, params=None):
        head = sql.strip().upper()
        if head.startswith("DELETE"):
            self.deletes.append(params)
            self.rowcount = 1 if (params[1], params[2]) in self.present else 0
        else:
            self._rows = self.stored

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        return None

    def close(self):
        return None


def _wire(monkeypatch, cursor):
    monkeypatch.setattr(cdm_writer, "_get_conn", lambda p=None: _Conn(cursor))
    return cursor


def _row(record_id, row_version=None, site="S1"):
    row = {"site": site, "record_id": record_id}
    if row_version is not None:
        row["row_version"] = row_version
    return row


def test_a_row_edited_from_the_version_it_was_read_at_is_accepted(monkeypatch):
    _wire(monkeypatch, _Cursor(stored=STORED))

    assert cdm_writer._stale_doc_rows([_row("REC-1", 3)], "P1") == []


def test_a_row_someone_else_moved_first_is_reported_as_stale(monkeypatch):
    _wire(monkeypatch, _Cursor(stored=STORED))

    stale = cdm_writer._stale_doc_rows([_row("REC-1", 2)], "P1")

    assert stale == [
        {
            "site": "S1",
            "record_id": "REC-1",
            "expected_row_version": 2,
            "current_row_version": 3,
        }
    ]


def test_only_the_stale_rows_of_a_batch_are_held_back(monkeypatch):
    _wire(monkeypatch, _Cursor(stored=STORED))

    stale = cdm_writer._stale_doc_rows([_row("REC-1", 1), _row("REC-2", 1)], "P1")

    assert [s["record_id"] for s in stale] == ["REC-1"]


def test_a_row_that_claims_no_version_is_not_conflict_checked(monkeypatch):
    cursor = _wire(monkeypatch, _Cursor(stored=STORED))

    assert cdm_writer._stale_doc_rows([_row("REC-1")], "P1") == []
    assert cursor._rows == []


def test_a_brand_new_row_cannot_conflict_with_anything(monkeypatch):
    _wire(monkeypatch, _Cursor(stored=STORED))

    assert cdm_writer._stale_doc_rows([_row("REC-9", 1)], "P1") == []


def test_a_version_that_is_not_a_number_is_not_treated_as_a_conflict(monkeypatch):
    _wire(monkeypatch, _Cursor(stored=STORED))

    assert cdm_writer._stale_doc_rows([_row("REC-1", "later")], "P1") == []


def test_the_version_probe_is_scoped_to_one_plant(monkeypatch):
    class _Probe(_Cursor):
        def __init__(self):
            super().__init__(stored=STORED)
            self.sql = None
            self.params = None

        def execute(self, sql, params=None):
            self.sql, self.params = sql, params
            super().execute(sql, params)

    cursor = _wire(monkeypatch, _Probe())
    cdm_writer._stale_doc_rows([_row("REC-1", 3)], "P1")

    assert '"plant_code_id" = %s' in cursor.sql
    assert cursor.params[0] == "P1"


def test_a_reviewer_delete_removes_exactly_the_rows_it_named(monkeypatch):
    cursor = _wire(monkeypatch, _Cursor(present={("S1", "REC-1"), ("S1", "REC-2")}))

    deleted, missing = cdm_writer._delete_doc_rows(
        [_row("REC-1"), _row("REC-2")], "P1"
    )

    assert (deleted, missing) == (2, [])
    assert [p[2] for p in cursor.deletes] == ["REC-1", "REC-2"]


def test_a_delete_is_keyed_on_plant_site_and_record(monkeypatch):
    cursor = _wire(monkeypatch, _Cursor(present={("S1", "REC-1")}))

    cdm_writer._delete_doc_rows([_row("REC-1")], "P1")

    assert cursor.deletes[0] == ("P1", "S1", "REC-1")


def test_deleting_a_row_that_is_already_gone_is_reported_not_miscounted(monkeypatch):
    _wire(monkeypatch, _Cursor(present={("S1", "REC-1")}))

    deleted, missing = cdm_writer._delete_doc_rows(
        [_row("REC-1"), _row("REC-404")], "P1"
    )

    assert deleted == 1
    assert missing == [{"site": "S1", "record_id": "REC-404"}]


def test_a_delete_without_a_record_id_touches_nothing(monkeypatch):
    cursor = _wire(monkeypatch, _Cursor(present={("S1", "REC-1")}))

    assert cdm_writer._delete_doc_rows([{"site": "S1"}], "P1") == (0, [])
    assert cursor.deletes == []


def test_an_empty_delete_list_never_opens_a_connection(monkeypatch):
    def _boom(*_a, **_k):
        raise AssertionError("connected for nothing")

    monkeypatch.setattr(cdm_writer, "_get_conn", _boom)

    assert cdm_writer._delete_doc_rows([], "P1") == (0, [])
