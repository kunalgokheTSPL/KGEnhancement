"""
Docs commit → Postgres write tests.

These mirror test_pnid_commit_db_write.py but cover the docs path's
quirks: it has its own writer (_write_to_doc_metadata_table) and an
upstream normaliser (_normalise_doc_row inside commit_docs_context).

What we're protecting:
  - the normaliser passes values through *without* stringifying so the
    downstream coercer can see native bools / numerics / None.
  - the writer uses _coerce_for_pg per column type (no more 'or None'
    that ate legitimate False / 0 values).
  - one bad row doesn't poison the rest of the batch (SAVEPOINT isolation).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from p0.tests._p0_test_base import client

from p0.api.database.cdm_writer import _write_to_doc_metadata_table


def _mock_conn(col_types):
    """Build a mocked psycopg conn whose information_schema lookup returns
    the supplied {name: data_type} mapping for document_metadata."""
    cur = MagicMock()
    cur.fetchall.return_value = [(name, dtype) for name, dtype in col_types.items()]
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


@pytest.fixture
def patch_get_conn(monkeypatch):
    import p0.api.database.cdm_writer as ctx

    def _setter(conn):
        monkeypatch.setattr(ctx, "_get_conn", lambda *a, **k: conn)

    return _setter


@pytest.fixture(autouse=True)
def _stub_replace_by_source_file(monkeypatch):
    """Skip the replace-by-source_file delete pass in every test in this
    file — we're testing the writer, not the dedup logic. Otherwise the
    cur.fetchall mocking would conflict with the writer's
    information_schema lookup. The stub returns "nothing deleted, no
    files matched, no original created_at preserved" so the writer takes
    the cold-start path."""
    import p0.api.database.cdm_writer as ctx

    monkeypatch.setattr(
        ctx,
        "_replace_by_source_file",
        lambda *a, **k: (0, set(), {}),
    )
    monkeypatch.setattr(
        ctx,
        "_stamp_commit_timestamps",
        lambda rows, _orig: rows,
    )
    import p0.api.plants as plants

    monkeypatch.setattr(plants, "validate_plant_code", lambda _p: None)


@pytest.fixture
def patch_doc_schema(monkeypatch):
    """document_metadata schema is loaded from YAML — patch the loader so
    we don't have to keep the test in sync with whatever the canonical
    YAML currently says."""
    import p0.api.database.cdm_writer as ctx

    monkeypatch.setattr(
        ctx,
        "_doc_metadata_schema",
        lambda: (
            "document_uid",
            {
                "document_uid": "BIGSERIAL",
                "plant_code_id": "VARCHAR(50)",
                "document_id": "VARCHAR(120)",
                "document_type": "VARCHAR(80)",
                "title": "TEXT",
                "extracted_entities": "JSON",
                "extracted_relationships": "JSON",
                "asset_match_confidence": "NUMERIC(6,4)",
                "is_active": "BOOLEAN",
                "updated_at": "TIMESTAMP",
            },
        ),
    )


def test_doc_writer_passes_native_types_through(patch_get_conn, patch_doc_schema):
    """Empty numeric → NULL; "true" → True; "0.95" → 0.95."""
    conn, cur = _mock_conn(
        {
            "plant_code_id": "character varying",
            "document_id": "character varying",
            "document_type": "character varying",
            "title": "text",
            "extracted_entities": "jsonb",
            "asset_match_confidence": "numeric",
            "is_active": "boolean",
            "updated_at": "timestamp without time zone",
        }
    )
    patch_get_conn(conn)

    rows = [
        {
            "plant_code_id": "CDM",
            "document_id": "DOC-1",
            "document_type": "SOP",
            "title": "Crusher Maintenance",
            "extracted_entities": None,
            "asset_match_confidence": "",
            "is_active": "true",
            "updated_at": "2026-06-03T10:03:15",
        }
    ]
    n = _write_to_doc_metadata_table(rows)
    assert n == 1

    insert = [
        c
        for c in cur.execute.call_args_list
        if c.args and c.args[0].startswith("INSERT INTO")
    ][0]
    sql, args = insert.args
    assert None in args, "empty numeric must be SQL NULL"
    assert True in args, "boolean 'true' string must be coerced to True"
    assert "Crusher Maintenance" in args, "text must pass through unchanged"


def test_doc_writer_isolates_bad_row(patch_get_conn, patch_doc_schema):
    """One row that raises during INSERT shouldn't kill the batch."""
    conn, cur = _mock_conn(
        {
            "plant_code_id": "character varying",
            "document_id": "character varying",
        }
    )
    patch_get_conn(conn)

    call_count = {"n": 0}
    original_execute = cur.execute

    def _execute(sql, params=None):
        if sql.startswith("INSERT INTO"):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("simulated row failure on doc 2")
        return (
            original_execute(sql, params)
            if params is not None
            else original_execute(sql)
        )

    cur.execute = _execute

    rows = [
        {"plant_code_id": "CDM", "document_id": "DOC-1"},
        {"plant_code_id": "CDM", "document_id": "DOC-2"},
        {"plant_code_id": "CDM", "document_id": "DOC-3"},
    ]
    n = _write_to_doc_metadata_table(rows)
    assert n == 2, "rows DOC-1 and DOC-3 must have committed"




def test_relationships_column_in_canonical_schema():
    """The schema YAML now defines extracted_relationships → so
    _doc_metadata_schema (and therefore the writer) sees it. This catches
    YAML/DB drift if the column gets removed from schema.yaml later."""
    from p0.api.database.cdm_writer import _doc_metadata_schema

    _, cols = _doc_metadata_schema()
    assert "extracted_relationships" in cols
    assert cols["extracted_relationships"].upper() == "JSON"


def test_relationships_list_serialises_to_json_string(patch_get_conn, patch_doc_schema):
    """A list payload (the natural shape from the docs pipeline) is
    JSON-encoded by _coerce_for_pg before binding to the INSERT."""
    import json

    conn, cur = _mock_conn(
        {
            "plant_code_id": "character varying",
            "document_id": "character varying",
            "extracted_relationships": "json",
        }
    )
    patch_get_conn(conn)

    rows = [
        {
            "plant_code_id": "CDM",
            "document_id": "DOC-RELN-1",
            "extracted_relationships": [
                {"from": "EQ-A", "to": "EQ-B", "kind": "feeds"}
            ],
        }
    ]
    n = _write_to_doc_metadata_table(rows)
    assert n == 1

    insert = [
        c
        for c in cur.execute.call_args_list
        if c.args and c.args[0].startswith("INSERT INTO")
    ][0]
    sql, args = insert.args
    json_args = [a for a in args if isinstance(a, str) and a.startswith("[")]
    assert json_args, "no JSON-shaped argument bound — list wasn't serialised"
    assert json.loads(json_args[0]) == [{"from": "EQ-A", "to": "EQ-B", "kind": "feeds"}]


def test_relationships_alias_maps_to_canonical_column(client):
    """The frontend may send the column under the ``relationships`` alias.
    _DOC_COL_MAP rewrites it to extracted_relationships before
    _CANONICAL filtering. We verify by inspecting what the writer is
    called with — easier than asserting on the DB round trip."""
    import p0.api.routers.context as ctx

    captured: list[list[dict]] = []

    original_writer = ctx._write_to_doc_metadata_table

    def _capture(rows, plant_code_id=None):
        captured.append([dict(r) for r in rows])
        return len(rows)

    ctx._write_to_doc_metadata_table = _capture
    try:
        r = client.post(
            "/context/commitDocsContext",
            json=[
                {
                    "plant_code_id": "CDM",
                    "document_id": "DOC-ALIAS",
                    "relationships": [{"from": "X", "to": "Y"}],
                }
            ],
        )
    finally:
        ctx._write_to_doc_metadata_table = original_writer

    assert r.status_code == 200
    assert captured, "writer was never called"
    normalised = captured[0][0]
    assert "extracted_relationships" in normalised
    assert "relationships" not in normalised
    assert normalised["extracted_relationships"] == [{"from": "X", "to": "Y"}]
