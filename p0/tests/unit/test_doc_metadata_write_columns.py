"""The document_metadata insert follows the live table, not just schema.yaml."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.database import cdm_writer

PROVENANCE = ("chunk_index", "page_start", "page_end", "source_pages")

ROW = {
    "plant_code_id": "P1",
    "record_id": "REC-1",
    "document_id": "DOC-1",
    "source_file": "manual.pdf",
    "chunk_index": 3,
    "page_start": 7,
    "page_end": 9,
    "source_pages": "7-9",
}


class _Cursor:
    """Records what the writer would send, answering only the schema probe."""

    def __init__(self, live_cols):
        self.live_cols = live_cols
        self.inserts = []
        self._rows = []

    def execute(self, sql, params=None):
        if "information_schema.columns" in sql:
            self._rows = list(self.live_cols.items())
        elif sql.strip().upper().startswith("INSERT"):
            self.inserts.append((sql, params))

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


def _schema_cols():
    pk, cols = cdm_writer._doc_metadata_schema()
    return [c for c in cols if c != pk]


def _write(monkeypatch, live_cols):
    """Run the writer against a table declaring exactly live_cols."""
    cursor = _Cursor(live_cols)
    monkeypatch.setattr(cdm_writer, "_get_conn", lambda p=None: _Conn(cursor))
    monkeypatch.setattr(
        cdm_writer, "_replace_by_source_file", lambda *a, **k: (0, set(), {})
    )
    monkeypatch.setattr(cdm_writer, "_stamp_commit_timestamps", lambda *a, **k: None)
    written = cdm_writer._write_to_doc_metadata_table([dict(ROW)], "P1")
    return written, cursor


def _bound(cursor, live_cols, column):
    sql, values = cursor.inserts[0]
    order = [c for c in _schema_cols() if c in live_cols]
    return values[order.index(column)]


def test_a_migrated_table_carries_page_provenance_into_the_insert(monkeypatch):
    live = {c: "integer" if c in PROVENANCE[:3] else "text" for c in _schema_cols()}
    written, cursor = _write(monkeypatch, live)

    assert written == 1
    sql, _ = cursor.inserts[0]
    for col in PROVENANCE:
        assert f'"{col}"' in sql
    assert _bound(cursor, live, "chunk_index") == 3
    assert _bound(cursor, live, "source_pages") == "7-9"


def test_a_table_behind_the_schema_drops_the_new_columns_not_every_row(monkeypatch):
    live = {c: "text" for c in _schema_cols() if c not in PROVENANCE}
    written, cursor = _write(monkeypatch, live)

    assert written == 1
    sql, _ = cursor.inserts[0]
    for col in PROVENANCE:
        assert col not in sql
    assert _bound(cursor, live, "record_id") == "REC-1"


def test_an_unreadable_table_falls_back_to_the_declared_schema(monkeypatch):
    written, cursor = _write(monkeypatch, {})

    assert written == 1
    sql, values = cursor.inserts[0]
    assert len(values) == len(_schema_cols())
    for col in PROVENANCE:
        assert f'"{col}"' in sql
