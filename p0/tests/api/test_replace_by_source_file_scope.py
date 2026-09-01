"""Tests for _replace_by_source_file scope_col (the doc-type collision fix).

The docs commit replaces rows for a source_file. Without scoping, committing
'report.pdf' under one document type would DELETE the already-committed rows of
the SAME filename under a DIFFERENT type. scope_col='document_type' narrows the
replace to the (source_file, document_type) pairs in the batch. sap/pnid (no
scope_col) keep the original source_file-only delete.

Uses a mocked cursor (the suite's convention for DB-path logic) and inspects the
DELETE SQL it builds, rather than hitting Postgres.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from p0.api.database.cdm_writer import _replace_by_source_file


def _cursor(has_columns):
    """A cursor whose information_schema EXISTS checks return True for the given
    column names (and table/created_at). Captures the DELETE statement separately
    so it isn't overwritten by later EXISTS lookups."""
    cur = MagicMock()
    cur._delete_sql = None

    def _execute(sql, params=None):
        s = " ".join(str(sql).split()).lower()
        if s.startswith("delete from"):
            cur._delete_sql = s
        if "information_schema.tables" in s:
            cur._next_one = (True,)
        elif "information_schema.columns" in s:
            col = ""
            for c in has_columns | {"source_file", "created_at", "document_type"}:
                if f"column_name = '{c}'" in s:
                    col = c
                    break
            else:
                col = params[1] if params and len(params) > 1 else ""
            cur._next_one = (col in has_columns,)
        else:
            cur._next_one = (None,)

    cur.execute.side_effect = _execute
    cur.fetchone.side_effect = lambda: getattr(cur, "_next_one", (True,))
    cur.fetchall.return_value = []
    cur.rowcount = 3
    return cur


def test_with_scope_col_builds_pair_scoped_delete():
    cur = _cursor(has_columns={"source_file", "document_type", "created_at"})
    rows = [
        {"source_file": "report.pdf", "document_type": "datasheet", "plant_code_id": "P1"},
    ]
    _replace_by_source_file(
        cur, "document_metadata", rows, "P1", scope_col="document_type"
    )
    assert "delete from" in cur._delete_sql
    assert '(source_file, "document_type")' in cur._delete_sql
    assert "unnest" in cur._delete_sql


def test_without_scope_col_deletes_by_source_file_only():
    cur = _cursor(has_columns={"source_file", "created_at"})
    rows = [{"source_file": "f.pdf", "plant_code_id": "P1"}]
    _replace_by_source_file(cur, "document_metadata", rows, "P1")
    assert "where source_file = any(" in cur._delete_sql
    assert "document_type" not in cur._delete_sql


def test_scope_col_ignored_when_rows_lack_the_value():
    cur = _cursor(has_columns={"source_file", "document_type", "created_at"})
    rows = [{"source_file": "f.pdf", "plant_code_id": "P1"}]
    _replace_by_source_file(
        cur, "document_metadata", rows, "P1", scope_col="document_type"
    )
    assert "where source_file = any(" in cur._delete_sql
