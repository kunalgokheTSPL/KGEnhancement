"""The generic table write batches its rows and collapses what the unique key forbids."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.database import cdm_writer
from p0.api.database.cdm_writer import (
    _WRITE_BATCH_SIZE,
    _dedupe_rows,
    _insert_page,
    _merge_rows,
)

KEY = ["plant_code_id", "site", "pid_tag"]


def _row(tag, equipment_type=None, description=None):
    return {
        "plant_code_id": "P1",
        "site": "-",
        "pid_tag": tag,
        "equipment_type": equipment_type,
        "description": description,
    }


def test_batch_size_is_set_above_one():
    assert _WRITE_BATCH_SIZE > 1


def test_distinct_keys_are_left_alone():
    rows = [_row("A"), _row("B"), _row("C")]
    assert len(_dedupe_rows(rows, KEY)) == 3


def test_duplicate_keys_collapse_to_one_row():
    rows = [_row("A"), _row("A"), _row("B")]
    assert len(_dedupe_rows(rows, KEY)) == 2


def test_later_value_wins_over_an_earlier_blank():
    out = _dedupe_rows([_row("A"), _row("A", equipment_type="PUMP")], KEY)
    assert out[0]["equipment_type"] == "PUMP"


def test_earlier_value_survives_a_later_blank():
    out = _dedupe_rows([_row("A", equipment_type="PUMP"), _row("A")], KEY)
    assert out[0]["equipment_type"] == "PUMP"


def test_columns_merge_independently_across_duplicates():
    out = _dedupe_rows(
        [_row("A", equipment_type="PUMP"), _row("A", description="feed pump")], KEY
    )
    assert out[0]["equipment_type"] == "PUMP"
    assert out[0]["description"] == "feed pump"


def test_a_whitespace_only_value_counts_as_blank():
    out = _dedupe_rows([_row("A", equipment_type="PUMP"), _row("A", equipment_type="  ")], KEY)
    assert out[0]["equipment_type"] == "PUMP"


def test_dedupe_preserves_first_seen_order():
    out = _dedupe_rows([_row("B"), _row("A"), _row("B")], KEY)
    assert [r["pid_tag"] for r in out] == ["B", "A"]


def test_without_a_unique_key_every_row_is_kept():
    rows = [_row("A"), _row("A")]
    assert len(_dedupe_rows(rows, [])) == 2


def test_merge_keeps_the_key_intact():
    merged = _merge_rows(_row("A"), _row("A", equipment_type="PUMP"))
    assert merged["pid_tag"] == "A"


class _Cur:
    def __init__(self, reject=()):
        self.reject = set(reject)
        self.statements = []

    def execute(self, sql, *a, **k):
        self.statements.append(sql)


def test_a_clean_page_is_one_round_trip(monkeypatch):
    calls = []
    monkeypatch.setattr(
        cdm_writer, "execute_values", lambda cur, sql, vals: calls.append(len(vals))
    )
    cur = _Cur()
    written, rejected = _insert_page(cur, "INSERT INTO t (a) VALUES %s", [[1], [2], [3]])
    assert calls == [3]
    assert (written, rejected) == (3, [])


def test_a_failing_page_falls_back_to_one_row_at_a_time(monkeypatch):
    def _fake(cur, sql, vals):
        if len(vals) > 1 or vals[0][0] == 2:
            raise ValueError("bad row 2")

    monkeypatch.setattr(cdm_writer, "execute_values", _fake)
    cur = _Cur()
    written, rejected = _insert_page(cur, "INSERT INTO t (a) VALUES %s", [[1], [2], [3]])
    assert written == 2
    assert len(rejected) == 1
    assert "bad row 2" in rejected[0]


def test_an_empty_page_writes_nothing(monkeypatch):
    monkeypatch.setattr(
        cdm_writer, "execute_values", lambda *a, **k: (_ for _ in ()).throw(AssertionError)
    )
    assert _insert_page(_Cur(), "INSERT INTO t (a) VALUES %s", []) == (0, [])


class _IndexCur:
    """A cursor that answers the unique-index lookup the way postgres does."""

    def __init__(self, rows):
        self.rows = rows
        self.sql = ""

    def execute(self, sql, *a, **k):
        self.sql = sql

    def fetchall(self):
        return self.rows


def test_the_key_is_read_from_unique_indexes_not_only_constraints():
    cur = _IndexCur([("uq_equip", "plant_code_id"), ("uq_equip", "site"), ("uq_equip", "normalized_asset")])
    assert cdm_writer._unique_key_columns(cur, "equipment") == [
        "plant_code_id",
        "site",
        "normalized_asset",
    ]
    assert "pg_index" in cur.sql


def test_the_primary_key_is_not_used_as_a_dedupe_key():
    cur = _IndexCur([])
    cdm_writer._unique_key_columns(cur, "equipment")
    assert "indisprimary" in cur.sql


def test_a_partial_index_is_not_used_as_a_dedupe_key():
    cur = _IndexCur([])
    cdm_writer._unique_key_columns(cur, "equipment")
    assert "indpred IS NULL" in cur.sql


def test_a_table_with_no_unique_index_dedupes_nothing():
    assert cdm_writer._unique_key_columns(_IndexCur([]), "equipment") == []


def test_the_narrowest_index_wins():
    cur = _IndexCur([
        ("wide", "a"), ("wide", "b"), ("wide", "c"),
        ("narrow", "a"), ("narrow", "b"),
    ])
    assert cdm_writer._unique_key_columns(cur, "t") == ["a", "b"]
