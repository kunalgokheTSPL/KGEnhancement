"""Regression tests for _parse_ts_series — the TS-values timestamp parser.

These are pure (no DB / IoTDB), guarding the fix for the ISO-vs-dayfirst bug:
a blanket dayfirst=True dropped ~60% of rows in ISO files (every date with
day-of-month > 12 failed to parse), while genuine DD-MM-YYYY exports still
need dayfirst. The parser sniffs the format per column.
"""

from __future__ import annotations

import pandas as pd

from p0.api.database.timeseries import _parse_ts_series


def test_iso_dates_day_gt_12_all_parse():
    s = pd.Series(
        [
            "2025-01-01 00:00:00",
            "2025-01-13 00:00:00",
            "2025-05-24 21:45:00",
            "2025-12-31 23:45:00",
        ]
    )
    out = _parse_ts_series(s)
    assert out.isna().sum() == 0
    assert out.iloc[1] == pd.Timestamp("2025-01-13 00:00:00")
    assert out.iloc[2] == pd.Timestamp("2025-05-24 21:45:00")


def test_iso_with_slashes_year_first():
    s = pd.Series(["2025/01/13 00:00:00", "2025/05/24 21:45:00"])
    out = _parse_ts_series(s)
    assert out.isna().sum() == 0
    assert out.iloc[0] == pd.Timestamp("2025-01-13 00:00:00")


def test_dayfirst_ddmmyyyy_not_swapped():
    s = pd.Series(["13/01/2025 08:00:00", "25/12/2025 23:30:00", "01/02/2025 00:00:00"])
    out = _parse_ts_series(s)
    assert out.isna().sum() == 0
    assert out.iloc[0] == pd.Timestamp("2025-01-13 08:00:00")
    assert out.iloc[1] == pd.Timestamp("2025-12-25 23:30:00")
    assert out.iloc[2] == pd.Timestamp("2025-02-01 00:00:00")


def test_garbage_coerced_to_nat_not_raised():
    s = pd.Series(["2025-01-13 00:00:00", "not a date", ""])
    out = _parse_ts_series(s)
    assert out.iloc[0] == pd.Timestamp("2025-01-13 00:00:00")
    assert pd.isna(out.iloc[1])
