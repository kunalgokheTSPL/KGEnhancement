"""Tests for shared SAP merge and fan-out aggregation helpers."""

from __future__ import annotations

import pandas as pd

from p0.source_processing.sap.sap_merging_impl import (
    _merge_safe,
    _sum_numeric_first_other,
)


def test_merge_safe_keeps_both_sides_of_an_overlapping_column_with_duplicate_keys():
    left = pd.DataFrame(
        {
            "AUFNR": ["100", "100", "200"],
            "STATUS": ["OPEN", "OPEN", "CLOSED"],
        }
    )
    right = pd.DataFrame(
        {
            "AUFNR": ["100", "200"],
            "STATUS": ["DONE", "DONE"],
        }
    )

    out = _merge_safe(left, right, on="AUFNR", how="left", suffixes=("", "_r"))

    # The overlapping non-key column must survive on both sides, suffixed —
    # not be dropped before pd.merge ever sees it.
    assert "STATUS" in out.columns
    assert "STATUS_r" in out.columns

    # The duplicate join key on the left ("100" appears twice) must not be
    # collapsed — both rows get joined against the single matching right row.
    assert len(out) == 3
    assert list(out["STATUS"]) == ["OPEN", "OPEN", "CLOSED"]
    assert list(out["STATUS_r"]) == ["DONE", "DONE", "DONE"]


def test_merge_safe_validate_m1_rejects_a_duplicated_right_key():
    left = pd.DataFrame({"EQUNR": ["E1", "E2"]})
    right = pd.DataFrame({"EQUNR": ["E1", "E1"], "DESC": ["a", "b"]})

    try:
        _merge_safe(left, right, on="EQUNR", how="left", validate="m:1")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError(
            "validate='m:1' should reject a right table with duplicate keys"
        )


def test_sum_numeric_first_other_does_not_sum_gjahr():
    df = pd.DataFrame(
        {
            "AUFNR": ["1000", "1000", "1000"],
            "DMBTR": [100, 200, 300],
            "GJAHR": [2024, 2024, 2024],
        }
    )

    out = _sum_numeric_first_other(
        df,
        ["AUFNR"],
        {"DMBTR", "GJAHR"},
    )

    assert len(out) == 1
    assert out.iloc[0]["DMBTR"] == 600
    assert out.iloc[0]["GJAHR"] == 2024


def test_sum_numeric_first_other_sums_text_costs():
    df = pd.DataFrame(
        {
            "AUFNR": ["1000", "1000", "1000"],
            "DMBTR": ["1,100", "1,200", "1,300"],
        }
    )

    out = _sum_numeric_first_other(
        df,
        ["AUFNR"],
        {"DMBTR"},
    )

    assert len(out) == 1
    assert out.iloc[0]["DMBTR"] == 3600