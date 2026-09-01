"""The timeseries upsert batches its rows without changing what a commit records."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.database import cdm_writer
from p0.api.database.cdm_writer import (
    _TS_ALWAYS_OVERWRITE,
    _TS_BATCH_SIZE,
    _TS_CONFLICT_COLS,
    _ts_dedupe_values,
    _ts_merge_values,
)

COLS = ["plant_code_id", "site", "tag_name", "description", "unit", "source_file"]


def _row(tag, description=None, unit=None, source_file=None):
    return ["P1", "-", tag, description, unit, source_file]


def test_distinct_keys_are_left_alone():
    values = [_row("a"), _row("b"), _row("c")]
    assert _ts_dedupe_values(values, COLS) == values


def test_duplicate_conflict_keys_collapse_to_one_row():
    out = _ts_dedupe_values([_row("a", "first"), _row("a", "second")], COLS)
    assert len(out) == 1


def test_later_non_null_wins_over_earlier():
    out = _ts_dedupe_values([_row("a", "first"), _row("a", "second")], COLS)
    assert out[0][COLS.index("description")] == "second"


def test_earlier_value_survives_a_later_null():
    out = _ts_dedupe_values([_row("a", "first"), _row("a", None)], COLS)
    assert out[0][COLS.index("description")] == "first"


def test_columns_merge_independently_across_duplicates():
    out = _ts_dedupe_values(
        [_row("a", description="d", unit=None), _row("a", description=None, unit="degC")],
        COLS,
    )
    assert out[0][COLS.index("description")] == "d"
    assert out[0][COLS.index("unit")] == "degC"


def test_always_overwrite_column_takes_the_later_null():
    assert "source_file" in _TS_ALWAYS_OVERWRITE
    out = _ts_dedupe_values([_row("a", source_file="one.xlsx"), _row("a")], COLS)
    assert out[0][COLS.index("source_file")] is None


def test_merge_keeps_the_conflict_key_intact():
    merged = _ts_merge_values(_row("a", "first"), _row("a", "second"), COLS)
    for col in _TS_CONFLICT_COLS:
        assert merged[COLS.index(col)] == _row("a")[COLS.index(col)]


def test_rows_differing_only_by_site_are_separate_keys():
    a = ["P1", "-", "t", None, None, None]
    b = ["P1", "north", "t", None, None, None]
    assert len(_ts_dedupe_values([a, b], COLS)) == 2


def test_dedupe_preserves_first_seen_order():
    out = _ts_dedupe_values([_row("b"), _row("a"), _row("b")], COLS)
    assert [r[COLS.index("tag_name")] for r in out] == ["b", "a"]


def test_batch_size_is_set_above_one():
    assert _TS_BATCH_SIZE > 1


def test_upsert_sends_one_statement_per_page(monkeypatch):
    captured = {}

    class _Cur:
        def execute(self, sql, *a, **k):
            if "information_schema" not in sql:
                raise AssertionError("per-row execute must not be used for the upsert")
            captured["looked_up_types"] = True

        def fetchall(self):
            return [
                (c, "character varying") for c in cdm_writer._ts_metadata_schema()[1]
            ]

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            captured["committed"] = True

        def close(self):
            pass

    def _fake_execute_values(cur, sql, values, page_size=None, fetch=False):
        captured["sql"] = sql
        captured["page_size"] = page_size
        captured["fetch"] = fetch
        captured["n"] = len(values)
        return [(True,) for _ in values]

    monkeypatch.setattr(cdm_writer, "_get_conn", lambda plant_code_id=None: _Conn())
    monkeypatch.setattr(cdm_writer, "execute_values", _fake_execute_values)

    rows = [{"plant_code_id": "P1", "site": "-", "tag_name": f"t{i}"} for i in range(5)]
    inserted, updated, noop = cdm_writer._upsert_ts_metadata(rows, plant_code_id="P1")

    assert "VALUES %s" in captured["sql"]
    assert captured["fetch"] is True
    assert captured["page_size"] == _TS_BATCH_SIZE
    assert (inserted, updated, noop) == (5, 0, 0)
    assert captured["committed"] is True


def test_counts_still_add_up_to_the_rows_submitted(monkeypatch):
    class _Cur:
        def execute(self, sql, *a, **k):
            pass

        def fetchall(self):
            return []

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            pass

        def close(self):
            pass

    def _fake_execute_values(cur, sql, values, page_size=None, fetch=False):
        return [(True,), (False,)]

    monkeypatch.setattr(cdm_writer, "_get_conn", lambda plant_code_id=None: _Conn())
    monkeypatch.setattr(cdm_writer, "execute_values", _fake_execute_values)

    rows = [{"plant_code_id": "P1", "site": "-", "tag_name": f"t{i}"} for i in range(6)]
    inserted, updated, noop = cdm_writer._upsert_ts_metadata(rows, plant_code_id="P1")

    assert (inserted, updated) == (1, 1)
    assert inserted + updated + noop == len(rows)
