"""Tag suffix conventions belong to the plant's template, not to p0's source."""

from __future__ import annotations

from p0.utils.ts_asset_identity import (
    _DEFAULT_POLICY,
    _extract_tag_asset_key,
    matching_policy,
)


def _cfg(**xref):
    return {"cross_reference": {"timeseries": xref}}


def test_the_builtin_policy_still_strips_the_known_suffixes():
    assert _extract_tag_asset_key("241FT108ENERGY") == "241FT108"


def test_a_template_can_declare_its_own_aggregate_suffixes():
    policy = matching_policy(_cfg(aggregate_suffixes=["RUNHOURS"]))
    assert policy["aggregate_suffixes"] == ["RUNHOURS"]
    assert _extract_tag_asset_key("241FT108RUNHOURS", policy) == "241FT108"


def test_a_suffix_the_template_drops_is_no_longer_stripped():
    policy = matching_policy(_cfg(aggregate_suffixes=["RUNHOURS"]))
    assert _extract_tag_asset_key("241FT108ENERGY", policy) == "241FT108ENERGY"


def test_a_template_can_declare_its_own_instrument_suffixes():
    policy = matching_policy(_cfg(instrument_suffixes=["QQ"]))
    assert _extract_tag_asset_key("241FT108QQ", policy) == "241FT108"


def test_instrument_suffixes_still_absorb_a_trailing_number():
    assert _extract_tag_asset_key("241FT108SZ2") == "241FT108"


def test_an_empty_declaration_falls_back_to_the_builtin():
    policy = matching_policy(_cfg(aggregate_suffixes=[]))
    assert policy["aggregate_suffixes"] == _DEFAULT_POLICY["aggregate_suffixes"]
