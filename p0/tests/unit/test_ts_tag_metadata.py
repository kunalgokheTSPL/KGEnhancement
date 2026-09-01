"""Golden tests for the deterministic TS tag-metadata parsers (LLM-free, in-memory).

normalize_tag / base-tag / measurement-code / uom / description / asset-linking are the
domain logic that turns raw timeseries tags into CDM timeseries_metadata. These pin the
exact parse and the link-confidence tiers, so a regression writes wrong metadata visibly.
"""

from __future__ import annotations

from p0.source_processing.ts.build_ts_tag_metadata import (
    PidComponent,
    build_description,
    extract_base_tag,
    extract_measurement_code,
    infer_uom,
    link_asset_name,
    normalize_tag,
)


def test_normalize_tag_uppercases_strips_spaces_and_leading_underscore():
    assert normalize_tag("  ti-101 ") == "TI-101"
    assert normalize_tag("a b_c") == "AB_C"
    assert normalize_tag("_lead") == "LEAD"


def test_extract_base_tag_takes_segment_before_first_underscore():
    assert extract_base_tag("101_TI_002") == "101"
    assert extract_base_tag("nounderscore") == "NOUNDERSCORE"


def test_extract_measurement_code_finds_two_letter_code():
    assert extract_measurement_code("101_TI_002") == "TI"
    assert extract_measurement_code("999") == ""  # numeric-only -> no code


def test_infer_uom_prefers_explicit_then_description_keywords():
    assert infer_uom("101_TI", "temperature", "degC") == "degC"  # explicit wins
    assert infer_uom("101_XX", "total count of trips", "") == "count"
    assert infer_uom("101_XX", "running total", "") == "total"


def test_build_description_uses_raw_when_present():
    assert (
        build_description("101_TI_002", "Reactor  Temperature", "Reactor")
        == "Reactor Temperature"  # whitespace collapsed
    )


def test_link_asset_name_confidence_tiers():
    exact = [PidComponent("101_TI_002", "instrument", "Reactor TI", "f")]
    asset, method, conf = link_asset_name("101_TI_002", "sec", exact)
    assert (asset, method, conf) == ("Reactor TI", "exact_tag", 1.0)

    base = [PidComponent("101", "equipment", "Reactor Vessel", "f")]
    asset, method, conf = link_asset_name("101_TI_002", "sec", base)
    assert (asset, method, conf) == ("Reactor Vessel", "exact_base", 0.95)

    unrelated = [PidComponent("999", "equipment", "Other", "f")]
    asset, method, conf = link_asset_name("101_TI_002", "sec", unrelated)
    assert (method, conf) == ("fallback_base_tag", 0.4)
    assert asset == "101"  # falls back to the base tag
