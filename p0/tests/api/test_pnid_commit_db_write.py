"""
PNID commit → Postgres write tests.

What we're protecting:
  the typed-column INSERT path. Historically ``_write_to_db`` did
  ``str(row.get(c, ""))`` for every value, which made the bigint PK reject
  ``'pid:<hash>'`` strings and the numeric ``confidence`` column reject
  empty strings — taking down the whole batch for a single bad cell.

These tests assert:
  - ``_coerce_for_pg`` returns SQL-safe Python values for every column type
    we actually have in the canonical schema (bigint, numeric, boolean,
    timestamp, text).
  - ``_write_to_db`` excludes BIGSERIAL columns from the INSERT so the
    sequence assigns them.
  - one bad row in a batch doesn't poison the rest — SAVEPOINT isolation.

We don't need a real Postgres for the coercion tests. The integration test
mocks the DB cursor so we can assert what SQL would have been executed and
what arguments would have been bound.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from p0.api.database.cdm_writer import _write_to_db
from p0.api.services.transforms import _coerce_for_pg




class TestCoerce:
    def test_empty_string_becomes_null(self):
        assert _coerce_for_pg("", "numeric") is None
        assert _coerce_for_pg("   ", "bigint") is None
        assert _coerce_for_pg("", "text") is None

    def test_none_stays_none(self):
        assert _coerce_for_pg(None, "bigint") is None
        assert _coerce_for_pg(None, "text") is None

    def test_bigint_parses_strings_and_ints(self):
        assert _coerce_for_pg("42", "bigint") == 42
        assert _coerce_for_pg(42, "bigint") == 42
        assert _coerce_for_pg(42.0, "bigint") == 42

    def test_bigint_rejects_app_uids_as_null(self):
        assert (
            _coerce_for_pg("pid:dba578b2bb3373d73292ad2d541b13d45baddc79", "bigint")
            is None
        )
        assert _coerce_for_pg("not-a-number", "integer") is None

    def test_numeric_parses_floats(self):
        assert _coerce_for_pg("0.95", "numeric") == 0.95
        assert _coerce_for_pg(0.95, "numeric") == 0.95
        assert _coerce_for_pg("1e3", "numeric") == 1000.0

    def test_numeric_rejects_garbage_as_null(self):
        assert _coerce_for_pg("not-a-number", "numeric") is None
        assert _coerce_for_pg("", "double precision") is None

    def test_boolean_string_forms(self):
        for s in ("true", "True", "T", "1", "yes", "YES"):
            assert _coerce_for_pg(s, "boolean") is True
        for s in ("false", "F", "0", "no"):
            assert _coerce_for_pg(s, "boolean") is False

    def test_boolean_unknown_to_null(self):
        assert _coerce_for_pg("maybe", "boolean") is None

    def test_boolean_passthrough(self):
        assert _coerce_for_pg(True, "boolean") is True
        assert _coerce_for_pg(False, "boolean") is False

    def test_text_passthrough(self):
        assert _coerce_for_pg("hello", "text") == "hello"
        assert _coerce_for_pg("Crusher A", "character varying") == "Crusher A"

    def test_timestamp_passes_through(self):
        assert (
            _coerce_for_pg("2026-06-03T10:03:15", "timestamp without time zone")
            == "2026-06-03T10:03:15"
        )

    def test_json_dict_serialises(self):
        import json

        out = _coerce_for_pg({"a": 1, "b": [2, 3]}, "json")
        assert json.loads(out) == {"a": 1, "b": [2, 3]}

    def test_json_list_serialises(self):
        import json

        out = _coerce_for_pg([{"x": 1}, {"y": 2}], "jsonb")
        assert json.loads(out) == [{"x": 1}, {"y": 2}]

    def test_json_valid_string_passes_through(self):
        assert _coerce_for_pg('{"a": 1}', "json") == '{"a": 1}'

    def test_json_raw_text_gets_wrapped(self):
        """The production failure from api.log: ``extracted_entities`` got
        raw text like ``"page: AGE..."``. Wrap it as a JSON string so the
        column accepts it, instead of dying with "Token \"page\" is invalid"
        and taking the whole batch down."""
        import json

        out = _coerce_for_pg("page: AGE-001 confidence 0.92", "json")
        assert json.loads(out) == "page: AGE-001 confidence 0.92"

    def test_json_empty_string_to_null(self):
        assert _coerce_for_pg("", "json") is None
        assert _coerce_for_pg("   ", "jsonb") is None

    def test_json_none_to_null(self):
        assert _coerce_for_pg(None, "json") is None




def _mock_conn_with_columns(col_meta: list[tuple[str, str, str]]):
    """Build a mocked psycopg connection whose information_schema query
    returns the supplied (name, data_type, default) triples.

    Every other cur.execute returns nothing; conn.commit / conn.close are
    real MagicMock no-ops.
    """
    cur = MagicMock()
    cur.fetchall.return_value = col_meta
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


@pytest.fixture
def patch_get_conn(monkeypatch):
    """Replace _get_conn with a factory we control per-test."""
    import p0.api.database.cdm_writer as ctx

    holder = {"conn": None}

    def _setter(conn):
        holder["conn"] = conn
        monkeypatch.setattr(ctx, "_get_conn", lambda *a, **k: conn)

    return _setter


def test_write_skips_autogen_columns(patch_get_conn):
    """A bigint column with nextval default (BIGSERIAL) must NOT appear in
    the INSERT — the sequence assigns it."""
    conn, cur = _mock_conn_with_columns(
        [
            ("pid_uid", "bigint", "nextval('equipment_pid_pid_uid_seq'::regclass)"),
            ("plant_code_id", "character varying", ""),
            ("pid_tag", "character varying", ""),
            ("confidence", "numeric", ""),
        ]
    )
    patch_get_conn(conn)

    rows = [
        {
            "pid_uid": "pid:dba578b2",
            "plant_code_id": "CDM",
            "pid_tag": "B-1106",
            "confidence": "0.95",
        }
    ]
    n = _write_to_db("equipment_pid", rows)
    assert n == 1

    insert_calls = [
        c
        for c in cur.execute.call_args_list
        if c.args and c.args[0].startswith("INSERT INTO")
    ]
    assert len(insert_calls) == 1
    sql, args = insert_calls[0].args
    assert '"pid_uid"' not in sql, "BIGSERIAL pid_uid must not be in INSERT"
    assert '"plant_code_id"' in sql
    assert '"pid_tag"' in sql
    assert '"confidence"' in sql
    assert 0.95 in args


def test_write_maps_empty_numeric_to_null(patch_get_conn):
    """Empty string into a numeric column must become SQL NULL."""
    conn, cur = _mock_conn_with_columns(
        [
            ("connectivity_uid", "bigint", "nextval('seq'::regclass)"),
            ("plant_code_id", "character varying", ""),
            ("confidence", "numeric", ""),
        ]
    )
    patch_get_conn(conn)

    rows = [{"plant_code_id": "CDM", "confidence": ""}]
    _write_to_db("equipment_connectivity", rows)

    insert_calls = [
        c
        for c in cur.execute.call_args_list
        if c.args and c.args[0].startswith("INSERT INTO")
    ]
    sql, args = insert_calls[0].args
    assert args[sql.index('"confidence"') and -1] is None or None in args
    assert "" not in args


def test_write_isolates_bad_row_via_savepoint(patch_get_conn):
    """One row that raises during INSERT shouldn't take down the batch.

    We simulate the error by making the INSERT execute raise on the second
    row only. The first and third rows must still commit.
    """
    conn, cur = _mock_conn_with_columns(
        [
            ("conn_uid", "bigint", "nextval('seq'::regclass)"),
            ("plant_code_id", "character varying", ""),
        ]
    )
    patch_get_conn(conn)

    call_count = {"n": 0}
    original_execute = cur.execute

    def _execute(sql, params=None):
        if sql.startswith("INSERT INTO"):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("simulated row failure")
        return (
            original_execute(sql, params)
            if params is not None
            else original_execute(sql)
        )

    cur.execute = _execute

    rows = [
        {"plant_code_id": "A"},
        {"plant_code_id": "B"},
        {"plant_code_id": "C"},
    ]
    n = _write_to_db("equipment_connectivity", rows)
    assert n == 2, "rows A and C must have committed despite B failing"


def test_write_returns_zero_when_no_writable_columns(patch_get_conn):
    """If every column in the row is autogen, we have nothing to insert."""
    conn, cur = _mock_conn_with_columns(
        [
            ("conn_uid", "bigint", "nextval('seq'::regclass)"),
        ]
    )
    patch_get_conn(conn)
    n = _write_to_db("equipment_connectivity", [{"conn_uid": "pid:hash"}])
    assert n == 0


def test_write_zero_rows_short_circuits(patch_get_conn):
    """Empty input → 0, no DB call."""
    conn, cur = _mock_conn_with_columns([])
    patch_get_conn(conn)
    assert _write_to_db("anything", []) == 0
    conn.cursor.assert_not_called()
