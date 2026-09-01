"""Uploading a second file must not erase the first one's reviewable rows."""

from __future__ import annotations

import pandas as pd

from p0.utils.canonical_entities import merge_entity_file_on_disk

REVIEW_KEY = ["plant_code_id", "site", "tag_name", "upload_batch_id"]


def _rows(batch: str, tags: list[str], area: str = "A1") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "plant_code_id": ["BOCPP"] * len(tags),
            "site": ["SITE-1"] * len(tags),
            "tag_name": tags,
            "upload_batch_id": [batch] * len(tags),
            "area": [area] * len(tags),
        }
    )


def _write(df: pd.DataFrame, path: str) -> pd.DataFrame:
    merged = merge_entity_file_on_disk(df, path, REVIEW_KEY)
    merged.to_parquet(path)
    return merged


def test_three_sequential_uploads_all_survive(tmp_path):
    path = str(tmp_path / "ts_timeseries_metadata.parquet")
    _write(_rows("b1", ["131PIA113A", "131PIA113B"]), path)
    _write(_rows("b2", ["404FIA101"]), path)
    final = _write(_rows("b3", ["421PDIA105A", "421PDIA105B"]), path)
    assert len(final) == 5
    assert set(final["upload_batch_id"]) == {"b1", "b2", "b3"}


def test_rerunning_the_same_batch_is_idempotent(tmp_path):
    path = str(tmp_path / "ts_timeseries_metadata.parquet")
    _write(_rows("b1", ["131PIA113A", "131PIA113B"]), path)
    again = _write(_rows("b1", ["131PIA113A", "131PIA113B"]), path)
    assert len(again) == 2


def test_the_same_tag_in_two_batches_stays_reviewable_in_both(tmp_path):
    path = str(tmp_path / "ts_timeseries_metadata.parquet")
    _write(_rows("b1", ["131PIA113A"], area="OLD"), path)
    final = _write(_rows("b2", ["131PIA113A"], area="NEW"), path)
    assert len(final) == 2
    by_batch = dict(zip(final["upload_batch_id"], final["area"]))
    assert by_batch == {"b1": "OLD", "b2": "NEW"}


def test_a_rerun_that_blanks_a_field_does_not_lose_the_earlier_value(tmp_path):
    path = str(tmp_path / "ts_timeseries_metadata.parquet")
    _write(_rows("b1", ["131PIA113A"], area="UNIT-7"), path)
    blank = _rows("b1", ["131PIA113A"])
    blank["area"] = ""
    final = _write(blank, path)
    assert list(final["area"]) == ["UNIT-7"]


def test_a_different_site_is_not_confused_with_the_same_tag(tmp_path):
    path = str(tmp_path / "ts_timeseries_metadata.parquet")
    _write(_rows("b1", ["131PIA113A"]), path)
    other = _rows("b1", ["131PIA113A"])
    other["site"] = "SITE-2"
    final = _write(other, path)
    assert len(final) == 2
