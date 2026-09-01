"""A re-ingest updates a document row in place and never reverts a human edit."""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.database import cdm_writer

IDENTITY = ("plant_code_id", "site", "record_id")

ROW = {
    "plant_code_id": "P1",
    "site": "S1",
    "record_id": "REC-1",
    "document_id": "DOC-1",
    "source_file": "manual.pdf",
    "document_type": "rca_reports",
}


class _Cursor:
    """Answers the schema probe and records every upsert the writer sends."""

    def __init__(self, live_cols, verdicts=None, failing=()):
        self.live_cols = live_cols
        self.upserts = []
        self.verdicts = list(verdicts or [])
        self.failing = set(failing)
        self._rows = []
        self._result = None

    def execute(self, sql, params=None):
        if "information_schema.columns" in sql:
            self._rows = list(self.live_cols.items())
        elif sql.strip().upper().startswith("INSERT"):
            if len(self.upserts) in self.failing:
                self.upserts.append((sql, params))
                raise RuntimeError("null value violates not-null constraint")
            self.upserts.append((sql, params))
            self._result = self.verdicts.pop(0) if self.verdicts else (True,)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._result


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        return None

    def close(self):
        return None


def _schema_cols():
    pk, cols = cdm_writer._doc_metadata_schema()
    return [c for c in cols if c != pk]


def _upsert(monkeypatch, rows, live_cols=None, editor=None, verdicts=None, failing=()):
    """Run the upsert writer against a table declaring exactly live_cols."""
    live = {c: "text" for c in _schema_cols()} if live_cols is None else live_cols
    cursor = _Cursor(live, verdicts=verdicts, failing=failing)
    monkeypatch.setattr(cdm_writer, "_get_conn", lambda p=None: _Conn(cursor))
    counts = cdm_writer._upsert_doc_metadata(rows, "P1", editor=editor)
    return counts, cursor


def _bound(cursor, live_cols, column, call=0):
    _, values = cursor.upserts[call]
    order = [c for c in _schema_cols() if c in live_cols]
    return values[order.index(column)]


def _sql(editor=None):
    return cdm_writer._doc_upsert_sql(_schema_cols(), editor)


def _setters(editor=None):
    """Only the DO UPDATE SET clause, with the conflict guard cut off."""
    return _sql(editor).split("DO UPDATE SET ")[1].split(" WHERE ")[0]


def test_a_document_row_is_matched_on_plant_site_and_record():
    target = _sql().split("ON CONFLICT (")[1].split(")")[0]

    assert target == '"plant_code_id", "site", "record_id"'


def test_the_pipeline_never_overwrites_a_row_a_human_edited():
    assert "document_metadata.edited_by IS NULL AND" in _sql()


def test_a_reviewer_writes_over_a_row_the_pipeline_would_be_blocked_on():
    assert "edited_by IS NULL" not in _sql(editor="ana")


def test_every_accepted_write_raises_the_row_version():
    setters = _setters()

    assert '"row_version" = document_metadata."row_version" + 1' in setters
    assert '"row_version" = EXCLUDED' not in setters


def test_a_reviewer_write_records_who_edited_the_row_and_when():
    setters = _setters(editor="ana")

    assert '"edited_by" = EXCLUDED."edited_by"' in setters
    assert '"edited_at" = EXCLUDED."edited_at"' in setters


def test_a_pipeline_write_does_not_claim_authorship_of_the_row():
    setters = _setters()

    assert "edited_by" not in setters
    assert "edited_at" not in setters


def test_the_identity_of_a_row_is_never_rewritten_by_an_update():
    setters = _setters()

    for col in IDENTITY:
        assert f'"{col}" =' not in setters


def test_a_row_keeps_the_time_it_first_arrived():
    setters = _setters()

    assert '"ingested_at"' not in setters
    assert '"created_at"' not in setters
    assert '"updated_at" = EXCLUDED."updated_at"' in setters


def test_re_ingesting_an_identical_row_is_not_counted_as_a_change():
    predicate = _sql().split(" WHERE ")[1]

    for col in ("created_at", "updated_at", "ingested_at", "row_version"):
        assert col not in predicate
    assert "source_file" in predicate


def test_a_restamped_ingest_time_alone_never_looks_like_an_edit():
    changing = {"ingested_at", "updated_at", "created_at"}

    assert not (changing & set(cdm_writer._doc_payload_columns(_schema_cols())))


def test_the_writer_reports_what_the_database_actually_did(monkeypatch):
    rows = [dict(ROW, record_id=f"REC-{n}") for n in range(3)]
    verdicts = [(True,), (False,), None]

    counts, _ = _upsert(monkeypatch, rows, verdicts=verdicts)

    assert counts == (1, 1, 1)


def test_a_rejected_row_does_not_take_the_rest_of_the_batch_with_it(monkeypatch):
    rows = [dict(ROW, record_id=f"REC-{n}") for n in range(3)]

    counts, cursor = _upsert(monkeypatch, rows, failing={1})

    assert counts == (2, 0, 0)
    assert len(cursor.upserts) == 3


def test_a_reviewer_write_stamps_the_reviewers_name_on_the_row(monkeypatch):
    live = {c: "text" for c in _schema_cols()}

    _, cursor = _upsert(monkeypatch, [dict(ROW)], live_cols=live, editor="ana")

    assert _bound(cursor, live, "edited_by") == "ana"
    assert _bound(cursor, live, "edited_at") is not None


def test_a_pipeline_write_leaves_the_row_unclaimed(monkeypatch):
    live = {c: "text" for c in _schema_cols()}

    _, cursor = _upsert(monkeypatch, [dict(ROW)], live_cols=live)

    assert _bound(cursor, live, "edited_by") is None
    assert _bound(cursor, live, "edited_at") is None
    assert _bound(cursor, live, "row_version") == 1


def test_a_value_is_bound_as_the_type_the_live_column_actually_has(monkeypatch):
    live = {c: "text" for c in _schema_cols()}
    live["chunk_index"] = "integer"

    _, cursor = _upsert(monkeypatch, [dict(ROW, chunk_index="4")], live_cols=live)

    assert _bound(cursor, live, "chunk_index") == 4


def test_a_table_behind_the_schema_drops_the_new_columns_not_every_row(monkeypatch):
    live = {c: "text" for c in _schema_cols() if c != "chunk_index"}

    counts, cursor = _upsert(monkeypatch, [dict(ROW)], live_cols=live)

    assert counts == (1, 0, 0)
    assert "chunk_index" not in cursor.upserts[0][0]


def test_a_table_without_the_identity_columns_refuses_to_upsert(monkeypatch):
    live = {c: "text" for c in _schema_cols() if c != "site"}

    with pytest.raises(RuntimeError):
        _upsert(monkeypatch, [dict(ROW)], live_cols=live)


def test_an_empty_batch_never_opens_a_connection(monkeypatch):
    def _boom(*_a, **_k):
        raise AssertionError("connected for nothing")

    monkeypatch.setattr(cdm_writer, "_get_conn", _boom)

    assert cdm_writer._upsert_doc_metadata([], "P1") == (0, 0, 0)
