"""Counts arrive from pandas as floats, so the timeseries writer coerces by column type."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.database.cdm_writer import _ts_row_values
from p0.api.services.transforms import _coerce_for_pg

COLS = ["plant_code_id", "site", "tag_name", "pvhi_count", "pvhi_percent"]
TYPES = {
    "plant_code_id": "character varying",
    "site": "character varying",
    "tag_name": "character varying",
    "pvhi_count": "bigint",
    "pvhi_percent": "numeric",
}


def test_integral_float_string_becomes_an_int():
    assert _coerce_for_pg("1126934.0", "bigint") == 1126934


def test_integral_float_becomes_an_int():
    assert _coerce_for_pg(4487.0, "bigint") == 4487


def test_plain_integer_string_still_works():
    assert _coerce_for_pg("42", "bigint") == 42


def test_non_integral_float_is_not_silently_truncated():
    assert _coerce_for_pg("1.5", "bigint") is None


def test_unparseable_value_is_still_none():
    assert _coerce_for_pg("not a number", "bigint") is None


def test_row_values_coerce_counts_when_types_are_known():
    row = {"plant_code_id": "P1", "tag_name": "t", "pvhi_count": "1126934.0"}
    values = _ts_row_values(row, COLS, TYPES)
    assert values[COLS.index("pvhi_count")] == 1126934


def test_row_values_leave_values_alone_without_types():
    row = {"plant_code_id": "P1", "tag_name": "t", "pvhi_count": "1126934.0"}
    values = _ts_row_values(row, COLS)
    assert values[COLS.index("pvhi_count")] == "1126934.0"


def test_site_still_defaults_under_coercion():
    values = _ts_row_values({"plant_code_id": "P1", "tag_name": "t"}, COLS, TYPES)
    assert values[COLS.index("site")] == "-"


def test_blank_string_still_becomes_null():
    row = {"plant_code_id": "P1", "tag_name": "t", "pvhi_percent": ""}
    values = _ts_row_values(row, COLS, TYPES)
    assert values[COLS.index("pvhi_percent")] is None
