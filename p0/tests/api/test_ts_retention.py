"""
Timeseries-retention helpers in p0/api/routers/connectors.py.

What we're protecting:

  _datetime_to_epoch_ms
      Pandas 2.x flipped the default datetime resolution from ns to us, which
      made the historical ``s.astype("int64") // 1_000_000`` produce SECONDS
      instead of ms — the bug that made stored IoTDB timestamps look like
      1970. The helper now coerces tz-naive / tz-aware / any dtype precision
      to int64 epoch ms cleanly. These tests lock that down so the next
      pandas upgrade doesn't silently re-break it.

  _apply_retention_window
      Q1-mode pre-ingest filter: keeps only rows where
      ``time >= t_max_file - window_ms`` so a 5-year file with 1-year
      retention isn't shipped to IoTDB whole-cloth. Tests cover:
        - 5-year file / 1-year retention drops 4 years
        - file span < window keeps everything
        - retention unconfigured is a no-op pass-through
        - empty dataframe doesn't crash
        - exact-boundary window matches the configured value
        - tz-naive and tz-aware inputs both work

  These are the helpers behind the user-visible 'retention' selector
  on the timeseries upload UI — regressions here silently miss data or
  ship too much, both bad.
"""

from __future__ import annotations

import pandas as pd
import pytest

from p0.api.database.timeseries import (
    _apply_retention_window,
    _datetime_to_epoch_ms,
)




_EXPECTED_MS = [1780315200000, 1780401600000]


def test_tz_naive_input_to_ms():
    """A tz-naive datetime column is treated as UTC and converted cleanly."""
    s = pd.to_datetime(pd.Series(["2026-06-01 12:00", "2026-06-02 12:00"]))
    assert _datetime_to_epoch_ms(s).tolist() == _EXPECTED_MS


def test_tz_aware_utc_to_ms():
    """tz-aware UTC datetimes give the same int64 ms as tz-naive."""
    s = pd.to_datetime(pd.Series(["2026-06-01 12:00", "2026-06-02 12:00"]), utc=True)
    assert _datetime_to_epoch_ms(s).tolist() == _EXPECTED_MS


def test_tz_aware_non_utc_converts_to_utc_first():
    """Asia/Kolkata 17:30 IST = 12:00 UTC. Verifies tz_convert happens
    before the int cast — otherwise the wall-clock value (17:30) would
    leak into the result."""
    s = pd.Series(["2026-06-01 17:30:00+0530", "2026-06-02 17:30:00+0530"])
    s = pd.to_datetime(s).dt.tz_convert("Asia/Kolkata")
    assert _datetime_to_epoch_ms(s).tolist() == _EXPECTED_MS


def test_us_precision_input_does_not_lose_factor_of_1000():
    """Pandas 2.x defaults to datetime64[us]. The OLD idiom
    ``s.astype("int64") // 1_000_000`` would have returned 1780315200
    (seconds, off by 1000×). The helper must always return ms."""
    s = pd.to_datetime(pd.Series(["2026-06-01 12:00"]))
    assert s.dtype == pd.api.types.pandas_dtype("datetime64[us]")
    assert _datetime_to_epoch_ms(s).iloc[0] == _EXPECTED_MS[0]




@pytest.fixture
def patch_retention_config(monkeypatch):
    """Patch the yaml loader so each test can declare the retention it wants
    without touching the on-disk user_config.yaml."""
    import p0.api.database.timeseries as conn_mod

    def _set(value, unit):
        if value is None:
            monkeypatch.setattr(conn_mod, "load_yaml", lambda _: {})
        else:
            monkeypatch.setattr(
                conn_mod,
                "load_yaml",
                lambda _: {"timeseries": {"retention": {"value": value, "unit": unit}}},
            )

    return _set


def _ts_df(
    start: str, end: str, freq: str = "D", tz: str | None = "UTC"
) -> pd.DataFrame:
    """Synth a wide-format TS dataframe with a 'timestamp' column."""
    idx = pd.date_range(start, end, freq=freq, inclusive="left", tz=tz)
    return pd.DataFrame({"timestamp": idx, "temp": range(len(idx))})


def test_5y_file_with_1y_retention_drops_4_years(patch_retention_config):
    """The exact case the user described: 5-year file uploaded, 1-year
    retention configured → only the most recent year reaches IoTDB."""
    patch_retention_config(1, "years")
    df = _ts_df("2021-01-01", "2026-01-01")
    out, stats = _apply_retention_window(df, "timestamp", "fiveyear.csv")

    assert stats["retention_applied"] is True
    assert stats["rows_kept"] in range(364, 367), (
        f"expected ~365, got {stats['rows_kept']}"
    )
    assert stats["rows_dropped"] >= 1450, "should drop ~4 years of daily rows"
    assert len(out) == stats["rows_kept"]


def test_short_file_with_long_retention_keeps_everything(patch_retention_config):
    """5-day file with 1-year retention → 0 dropped, all rows kept.
    Documents the 'no shrinkage when file < window' branch."""
    patch_retention_config(1, "years")
    df = _ts_df("2026-05-27", "2026-06-01", freq="h")
    out, stats = _apply_retention_window(df, "timestamp", "short.csv")
    assert stats["rows_dropped"] == 0
    assert len(out) == len(df)


def test_unconfigured_retention_is_passthrough(patch_retention_config):
    """When user_config has no timeseries.retention block, the helper
    leaves the df untouched and returns retention_applied=False so the
    caller can branch on it for logging / reports."""
    patch_retention_config(None, None)
    df = _ts_df("2021-01-01", "2022-01-01")
    out, stats = _apply_retention_window(df, "timestamp", "noret.csv")
    assert stats["retention_applied"] is False
    assert len(out) == len(df)


def test_empty_dataframe_does_not_crash(patch_retention_config):
    """Edge case: ingest path may parse a CSV that turns out to be empty
    after dropping non-numeric columns. Helper must return cleanly."""
    patch_retention_config(1, "years")
    df = pd.DataFrame({"timestamp": pd.to_datetime([]), "temp": []})
    out, stats = _apply_retention_window(df, "timestamp", "empty.csv")
    assert len(out) == 0
    assert stats["rows_total"] == 0
    assert stats["retention_applied"] is False


def test_10_day_window_keeps_exactly_240_hours(patch_retention_config):
    """Boundary verification: a 31-day hourly file + 10-day retention
    should keep 240 hours (10 × 24) of data inclusive of the right edge."""
    patch_retention_config(10, "days")
    df = _ts_df("2026-05-01", "2026-06-01", freq="h")
    out, stats = _apply_retention_window(df, "timestamp", "exact.csv")
    span_hours = (
        out["timestamp"].max() - out["timestamp"].min()
    ).total_seconds() / 3600
    assert 239 <= span_hours <= 241, f"expected ~240h, got {span_hours}"


def test_tz_naive_input_works(patch_retention_config):
    """Some uploaded files have no tz info — the helper must still apply
    retention against the file's own t_max."""
    patch_retention_config(1, "years")
    df = _ts_df("2021-01-01", "2026-01-01", tz=None)
    out, stats = _apply_retention_window(df, "timestamp", "tznaive.csv")
    assert stats["rows_kept"] in range(364, 367)
    assert stats["retention_applied"] is True


def test_report_carries_anchor_and_cutoff(patch_retention_config):
    """The Q3 report contract: stats must include t_max_file_ms and
    cutoff_ms so the frontend can render "kept N rows from <date> to <date>"."""
    patch_retention_config(1, "years")
    df = _ts_df("2021-01-01", "2026-01-01")
    _, stats = _apply_retention_window(df, "timestamp", "report.csv")

    assert stats["t_max_file_ms"] is not None
    assert stats["cutoff_ms"] is not None
    assert stats["t_max_file_ms"] - stats["cutoff_ms"] == stats["window_ms"]
    assert abs(stats["t_min_kept_ms"] - stats["cutoff_ms"]) < 86_400_000 + 1
