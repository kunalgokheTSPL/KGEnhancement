"""FLOC-tier matching: a functional location's leaf tag, hyphen-stripped,
resolves against a P&ID equipment tag when the SAP equipment id itself
doesn't match anything ."""

import pandas as pd

from p0.utils.sap_asset_identity import (
    _extract_floc_tag,
    _pick_best_reference,
    _strip_hyphens,
)


def test_extract_floc_tag_tries_the_specific_half_before_the_parent():
    # For a hyphenated leaf, the specific tag (right of the hyphen) is the
    # more precise P&ID match candidate and should be tried first.
    assert _extract_floc_tag("BOCP.ELG.MGE.A7510-751FCV1420") == ["751FCV1420", "A7510"]


def test_extract_floc_tag_handles_a_leaf_with_no_hyphen():
    assert _extract_floc_tag("BODB.LIQ.CRN.X5420") == ["X5420"]


def test_extract_floc_tag_uppercases_the_result():
    assert _extract_floc_tag("bocp.elg.mge.a7510-751fcv1420") == ["751FCV1420", "A7510"]


def test_extract_floc_tag_returns_empty_for_blank_or_missing_input():
    assert _extract_floc_tag(None) == []
    assert _extract_floc_tag("") == []
    assert _extract_floc_tag("   ") == []


def test_extract_floc_tag_returns_only_specific_when_the_parent_has_no_digit():
    # Parent segment regex requires letters followed by a digit — a purely
    # alphabetic parent (no equipment number) has nothing to extract, but
    # the specific half can still be a candidate on its own.
    assert _extract_floc_tag("BOCP.ELG.MGE.ABCDEF-751FCV1420") == ["751FCV1420"]


def test_extract_floc_tag_falls_back_to_the_bare_leaf_when_no_digit_and_no_hyphen():
    assert _extract_floc_tag("BOCP.ELG.MGE.ABCDEF") == ["ABCDEF"]


def test_strip_hyphens_removes_hyphens_and_upcases():
    assert _strip_hyphens("A-7510") == "A7510"
    assert _strip_hyphens("a-75-10") == "A7510"


def test_strip_hyphens_handles_blank_or_missing_input():
    assert _strip_hyphens(None) == ""
    assert _strip_hyphens("") == ""


def _ref_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["asset_key", "normalized_asset", "equipment_tag", "source"])


def test_pick_best_reference_uses_floc_tier_when_sap_key_has_no_exact_match():
    # SAP equipment id doesn't match the reference directly, but its
    # functional location's leaf tag matches a hyphenated P&ID tag once
    # both sides are hyphen-stripped — 
    ref = _ref_df([
        {
            "asset_key": "A7510751FCV1420PID",
            "normalized_asset": "A-7510",
            "equipment_tag": "A-7510",
            "source": "pid_tag",
        },
    ])

    normalized_asset, equipment_tag, confidence, method = _pick_best_reference(
        "751FCV1420", ref, floc="BOCP.ELG.MGE.A7510-751FCV1420"
    )

    assert normalized_asset == "A-7510"
    assert equipment_tag == "A-7510"
    assert confidence == 0.85
    assert method == "floc"


def test_pick_best_reference_prefers_specific_floc_half_over_parent():
    # The reference only has the specific instrument tag, not the parent
    # equipment tag — the specific-half-first ordering is what finds it.
    ref = _ref_df([
        {
            "asset_key": "101LCV101APID",
            "normalized_asset": "101LCV101A",
            "equipment_tag": "101LCV101A",
            "source": "pid_tag",
        },
    ])

    normalized_asset, equipment_tag, confidence, method = _pick_best_reference(
        "SOMESAPID", ref, floc="V1010-101LCV101A"
    )

    assert normalized_asset == "101LCV101A"
    assert equipment_tag == "101LCV101A"
    assert confidence == 0.85
    assert method == "floc"


def test_pick_best_reference_prefers_exact_over_floc():
    ref = _ref_df([
        {
            "asset_key": "751FCV1420",
            "normalized_asset": "751FCV1420",
            "equipment_tag": "751FCV1420",
            "source": "pid_tag",
        },
        {
            "asset_key": "SOMETHINGELSE",
            "normalized_asset": "A-7510",
            "equipment_tag": "A-7510",
            "source": "pid_tag",
        },
    ])

    normalized_asset, equipment_tag, confidence, method = _pick_best_reference(
        "751FCV1420", ref, floc="BOCP.ELG.MGE.A7510-751FCV1420"
    )

    assert normalized_asset == "751FCV1420"
    assert equipment_tag == "751FCV1420"
    assert confidence == 1.0
    assert method == "exact"


def test_pick_best_reference_returns_none_when_floc_tag_matches_nothing():
    ref = _ref_df([
        {
            "asset_key": "UNRELATED",
            "normalized_asset": "K-9999",
            "equipment_tag": "K-9999",
            "source": "pid_tag",
        },
    ])

    normalized_asset, equipment_tag, confidence, method = _pick_best_reference(
        "751FCV1420", ref, floc="BOCP.ELG.MGE.A7510-751FCV1420"
    )

    assert normalized_asset == ""
    assert equipment_tag == ""
    assert confidence == 0.0
    assert method == "none"


def test_pick_best_reference_with_no_floc_falls_through_to_none():
    ref = _ref_df([
        {
            "asset_key": "SOMETHINGELSE",
            "normalized_asset": "A-7510",
            "equipment_tag": "A-7510",
            "source": "pid_tag",
        },
    ])

    normalized_asset, equipment_tag, confidence, method = _pick_best_reference(
        "751FCV1420", ref, floc=None
    )

    assert normalized_asset == ""
    assert equipment_tag == ""
    assert confidence == 0.0
    assert method == "none"
