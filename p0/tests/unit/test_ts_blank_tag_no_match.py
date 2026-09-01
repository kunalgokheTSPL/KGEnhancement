"""A tag that carries no name must never be matched to an asset."""

from __future__ import annotations

import pandas as pd

from p0.utils.ts_asset_identity import _extract_tag_asset_key, _pick_best_reference

BLANKS = [float("nan"), None, "nan", "NaN", "NULL", "none", "NaT", "<NA>", "N/A", "  "]

REFERENCE = pd.DataFrame(
    {
        "asset_key": ["NANOFILTER-1", "K-2410A"],
        "equipment_id": ["E-NANO", "E-K"],
        "normalized_asset": ["NANOFILTER-1", "K-2410A"],
    }
)


def test_a_blank_tag_yields_no_asset_key():
    assert [_extract_tag_asset_key(v) for v in BLANKS] == [""] * len(BLANKS)


def test_a_blank_tag_never_produces_an_edge():
    for value in BLANKS:
        key = _extract_tag_asset_key(value)
        assert _pick_best_reference(key, REFERENCE)[1] == ""


def test_a_real_tag_that_starts_like_a_blank_still_matches():
    key = _extract_tag_asset_key("NANOFILTER-1")
    assert _pick_best_reference(key, REFERENCE)[1] == "E-NANO"


def test_a_real_tag_is_unaffected_by_the_blank_guard():
    key = _extract_tag_asset_key("K-2410A.PV")
    assert _pick_best_reference(key, REFERENCE)[1] == "E-K"
