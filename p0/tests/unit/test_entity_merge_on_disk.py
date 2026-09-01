"""Incremental entity merge."""

import pandas as pd

from p0.utils.canonical_entities import merge_entity_file_on_disk


def _seed(tmp_path):
    p = str(tmp_path / "equipment.parquet")
    pd.DataFrame(
        {
            "equipment_uid": ["u1", "u2"],
            "normalized_asset": ["P-1001", "P-1002"],
            "confidence": [0.91, 0.85],
            "n_drawings": [1, 2],
        }
    ).to_parquet(p)
    return p


def test_a_second_upload_keeps_the_first_uploads_rows(tmp_path):
    p = _seed(tmp_path)
    run2 = pd.DataFrame(
        {
            "equipment_uid": ["u3"],
            "normalized_asset": ["E-3001"],
            "confidence": [0.77],
            "n_drawings": [1],
        }
    )
    merged = merge_entity_file_on_disk(run2, p, "equipment_uid")
    assert sorted(merged["normalized_asset"]) == ["E-3001", "P-1001", "P-1002"]


def test_preserved_rows_keep_their_numeric_dtype(tmp_path):
    p = _seed(tmp_path)
    run2 = pd.DataFrame(
        {
            "equipment_uid": ["u3"],
            "normalized_asset": ["E-3001"],
            "confidence": [0.77],
            "n_drawings": [1],
        }
    )
    merged = merge_entity_file_on_disk(run2, p, "equipment_uid")
    assert merged["confidence"].dtype == "float64"
    assert merged["n_drawings"].dtype == "int64"
    assert not any(isinstance(v, str) for v in merged["confidence"])
    assert round(float(merged["confidence"].sum()), 4) == 2.53


def test_a_re_upload_of_the_same_key_replaces_rather_than_duplicates(tmp_path):
    p = _seed(tmp_path)
    run2 = pd.DataFrame(
        {
            "equipment_uid": ["u1"],
            "normalized_asset": ["P-1001"],
            "confidence": [0.99],
            "n_drawings": [4],
        }
    )
    merged = merge_entity_file_on_disk(run2, p, "equipment_uid")
    assert len(merged) == 2
    row = merged[merged["equipment_uid"] == "u1"].iloc[0]
    assert float(row["confidence"]) == 0.99


def test_no_primary_key_means_no_merge(tmp_path):
    p = _seed(tmp_path)
    run2 = pd.DataFrame({"equipment_uid": ["u3"], "normalized_asset": ["E-3001"]})
    assert len(merge_entity_file_on_disk(run2, p, None)) == 1


def test_a_key_seen_in_two_uploads_keeps_both_uploads_attributes(tmp_path):
    path = str(tmp_path / "ts.parquet")
    pd.DataFrame(
        {
            "ts_uid": ["t1"],
            "tag_name": ["TAG-1"],
            "alarm_priority": ["HIGH"],
            "excursion_count": [None],
        }
    ).to_parquet(path)
    second = pd.DataFrame(
        {
            "ts_uid": ["t1"],
            "tag_name": ["TAG-1"],
            "alarm_priority": [None],
            "excursion_count": ["7"],
        }
    )
    merged = merge_entity_file_on_disk(second, path, "ts_uid")
    assert len(merged) == 1
    row = merged.iloc[0]
    assert row["alarm_priority"] == "HIGH"
    assert row["excursion_count"] == "7"


def test_the_newer_upload_wins_wherever_it_actually_has_a_value(tmp_path):
    path = str(tmp_path / "ts.parquet")
    pd.DataFrame({"ts_uid": ["t1"], "unit": ["degC"], "priority": ["LOW"]}).to_parquet(
        path
    )
    second = pd.DataFrame({"ts_uid": ["t1"], "unit": ["barg"], "priority": ["HIGH"]})
    merged = merge_entity_file_on_disk(second, path, "ts_uid")
    assert merged.iloc[0]["unit"] == "barg"
    assert merged.iloc[0]["priority"] == "HIGH"


def test_a_blank_string_counts_as_missing_not_as_a_value(tmp_path):
    path = str(tmp_path / "ts.parquet")
    pd.DataFrame({"ts_uid": ["t1"], "unit": ["degC"]}).to_parquet(path)
    merged = merge_entity_file_on_disk(
        pd.DataFrame({"ts_uid": ["t1"], "unit": ["   "]}), path, "ts_uid"
    )
    assert merged.iloc[0]["unit"] == "degC"
