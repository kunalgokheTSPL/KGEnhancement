"""Pins identity normalisation to whole-value null sentinels."""

from __future__ import annotations

from p0.utils.identity import _norm


def test_a_null_sentinel_is_dropped():
    for sentinel in ("nan", "NaN", " NONE ", "nat", "null"):
        assert _norm(sentinel) == ""


def test_a_sentinel_substring_inside_a_real_tag_survives():
    """It used to strip NAN and NONE anywhere, so NANOFILTER-7 became OFILTER-7."""
    assert _norm("NANOFILTER-7") == "NANOFILTER-7"
    assert _norm("FINANCE-PUMP") == "FINANCE-PUMP"
    assert _norm("NONESUCH-1") == "NONESUCH-1"
    assert _norm("MONITOR-NAN") == "MONITOR-NAN"


def test_normalisation_still_upper_cases_and_collapses_space():
    assert _norm("  k-2410a  ") == "K-2410A"
    assert _norm("pump   one") == "PUMP ONE"


def test_both_sides_of_the_equipment_join_agree_on_a_sentinel_substring():
    """The connectivity side never went through this, so the two sides diverged."""
    from p0.utils.asset_identity import _norm as asset_norm

    for value in ("NANOFILTER-7", "MONITOR-NAN", "K-2410A"):
        assert _norm(value) == asset_norm(value)
