"""The timeseries upsert must target an index Postgres can actually infer."""

from __future__ import annotations

import os

import yaml

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.config import SCHEMA_FROZEN_FILE
from p0.api.database.cdm_writer import _TS_CONFLICT_COLS, _ts_row_values, _ts_site

TABLE = "timeseries_metadata"


def _tdef(name):
    """The declared definition of one CDM table, straight from the frozen schema."""
    with open(SCHEMA_FROZEN_FILE, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    tables = cfg.get("schema") or cfg.get("tables") or {}
    return tables.get(name) or {}


def _unique_column_sets(name):
    """Every column set the table declares as unique, as frozensets."""
    tdef = _tdef(name)
    uniques = list(tdef.get("unique") or [])
    uniques += [c for c in (tdef.get("constraints") or []) if c.get("type") == "unique"]
    return {frozenset(u["columns"]) for u in uniques}


def test_the_upsert_conflict_target_is_a_declared_unique_constraint():
    declared = _unique_column_sets(TABLE)
    assert frozenset(_TS_CONFLICT_COLS) in declared, (
        "ON CONFLICT names columns with no matching unique index, so Postgres "
        "rejects every timeseries commit. target="
        f"{sorted(_TS_CONFLICT_COLS)} declared={[sorted(s) for s in declared]}"
    )


def test_every_conflict_column_exists_in_the_table():
    assert set(_TS_CONFLICT_COLS) <= set(_tdef(TABLE).get("columns") or {})


def test_site_is_not_null_so_a_row_may_never_send_it_as_none():
    assert "NOT NULL" in (_tdef(TABLE).get("columns") or {})["site"]


def test_a_row_without_a_site_is_given_the_schema_default():
    assert _ts_site({}) == "-"
    assert _ts_site({"site": None}) == "-"
    assert _ts_site({"site": "   "}) == "-"


def test_a_row_that_names_its_site_keeps_it_trimmed():
    assert _ts_site({"site": "  PLATFORM_A "}) == "PLATFORM_A"


def test_the_site_value_sent_to_postgres_is_never_none():
    values = _ts_row_values(
        {"plant_code_id": "P1", "tag_name": "T1"}, list(_TS_CONFLICT_COLS)
    )
    assert values[_TS_CONFLICT_COLS.index("site")] == "-"


def test_a_blank_string_still_reaches_postgres_as_null():
    assert _ts_row_values({"unit": ""}, ["unit"]) == [None]


def test_a_second_source_does_not_blank_what_the_first_one_filled():
    from p0.api.database.cdm_writer import _ts_update_setter

    setter = _ts_update_setter("alerts_prime_alarm_hi_count")
    assert setter == (
        '"alerts_prime_alarm_hi_count" = COALESCE('
        'EXCLUDED."alerts_prime_alarm_hi_count", '
        'timeseries_metadata."alerts_prime_alarm_hi_count")'
    )


def test_bookkeeping_columns_still_reflect_the_latest_upload():
    from p0.api.database.cdm_writer import _ts_update_setter

    for column in ("updated_at", "source_file", "source_system"):
        assert _ts_update_setter(column) == f'"{column}" = EXCLUDED."{column}"'


def test_every_data_column_is_merged_rather_than_replaced():
    from p0.api.database.cdm_writer import (
        _TS_ALWAYS_OVERWRITE,
        _TS_CONFLICT_COLS,
        _ts_metadata_schema,
        _ts_update_setter,
    )

    pk, col_types = _ts_metadata_schema()
    data_cols = [
        c for c in col_types
        if c != pk and c not in _TS_CONFLICT_COLS and c not in _TS_ALWAYS_OVERWRITE
    ]
    assert data_cols
    for column in data_cols:
        assert "COALESCE" in _ts_update_setter(column), f"{column} would be overwritten"
