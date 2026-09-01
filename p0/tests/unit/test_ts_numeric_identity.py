"""A tag's numbers are its identity, so a near-neighbour is never the same asset."""

from __future__ import annotations

import pandas as pd

from p0.utils.ts_asset_identity import (
    _numeric_parts,
    _pick_best_reference,
    _same_asset_number,
    enrich_timeseries_asset_identity,
)


def _ref(keys):
    return pd.DataFrame(
        {
            "asset_key": [k for k in keys],
            "match_key": [k for k in keys],
            "normalized_asset": [k for k in keys],
            "identity_asset": [k for k in keys],
            "equipment_id": [k for k in keys],
            "source": ["normalized_asset"] * len(keys),
        }
    )


def test_the_numbers_are_read_in_order():
    assert _numeric_parts("060BDV131") == ("60", "131")


def test_a_leading_zero_does_not_make_a_different_loop():
    assert _numeric_parts("060BDV111") == _numeric_parts("60BDV111")


def test_two_tags_differing_only_in_a_number_are_not_one_asset():
    assert not _same_asset_number("060BDV131", "060BDV111")


def test_two_tags_differing_only_in_letters_can_be_one_asset():
    assert _same_asset_number("060PI104", "060PIC104")


def test_a_sibling_valve_is_not_fuzzy_matched():
    _, equipment, _, _ = _pick_best_reference("060BDV131", _ref(["060BDV111"]))
    assert equipment == ""


def test_a_sibling_transmitter_is_not_fuzzy_matched():
    _, equipment, _, _ = _pick_best_reference("241FT108", _ref(["241FT109"]))
    assert equipment == ""


def test_the_real_tag_still_matches_exactly():
    _, equipment, conf, how = _pick_best_reference("060BDV111", _ref(["060BDV111"]))
    assert (equipment, conf, how) == ("060BDV111", 1.0, "exact")


def test_a_letter_only_difference_still_fuzzy_matches():
    _, equipment, _, how = _pick_best_reference("060PIC104", _ref(["060PICA104"]))
    assert equipment == "060PICA104"
    assert how == "fuzzy"


def test_contains_requires_the_numbers_to_agree():
    _, equipment, _, _ = _pick_best_reference("060BDV1", _ref(["060BDV111"]))
    assert equipment == ""


def test_contains_prefers_the_closest_length_not_the_first_row():
    ref = _ref(["UUUU402PI101A", "U402PI101A"])
    _, equipment, _, how = _pick_best_reference("402PI101A", ref)
    assert how == "contains"
    assert equipment == "U402PI101A"


def test_the_matcher_reads_the_separator_free_key():
    ref = pd.DataFrame(
        {
            "asset_key": ["K 2410A"],
            "match_key": ["K2410A"],
            "normalized_asset": ["K-2410A"],
            "identity_asset": ["K-2410A"],
            "equipment_id": ["K-2410A"],
            "source": ["normalized_asset"],
        }
    )
    _, equipment, _, how = _pick_best_reference("K2410A", ref)
    assert (equipment, how) == ("K-2410A", "exact")


def test_an_unmatched_tag_says_so_instead_of_claiming_a_pattern_match():
    df = pd.DataFrame({"tag_name": ["999ZZZ999.PV"]})
    out = enrich_timeseries_asset_identity(
        df, reference_candidates=[], identity_cfg=None
    )
    assert out["asset_match_method"].iloc[0] == "tag_pattern"


def _reference_file(tmp_path, tags):
    path = tmp_path / "equipment_pid.csv"
    pd.DataFrame(
        {"normalized_asset": tags, "equipment_id": tags, "pid_tag": tags}
    ).to_csv(path, index=False)
    return str(path)


def test_a_tag_the_pid_does_not_have_is_reported_as_unmatched(tmp_path):
    ref = _reference_file(tmp_path, ["060BDV111"])
    df = pd.DataFrame({"tag_name": ["060BDV131.PV"]})
    out = enrich_timeseries_asset_identity(df, reference_candidates=[ref])
    assert out["asset_match_method"].iloc[0] == "tag_pattern_no_pid_match"
    assert str(out["equipment_id"].iloc[0] or "") == ""


def test_a_tag_the_pid_does_have_is_reported_as_matched(tmp_path):
    ref = _reference_file(tmp_path, ["060BDV111"])
    df = pd.DataFrame({"tag_name": ["060BDV111.PV"]})
    out = enrich_timeseries_asset_identity(df, reference_candidates=[ref])
    assert out["asset_match_method"].iloc[0] == "pid_match_key"
    assert out["equipment_id"].iloc[0] == "060BDV111"


def test_the_trailing_letter_names_a_sibling_not_a_type():
    from p0.utils.ts_asset_identity import _same_asset_identity, _sibling_suffix

    assert _sibling_suffix("402PI101I") == "I"
    assert _sibling_suffix("060PIC104") == ""
    assert not _same_asset_identity("402PI101I", "402PI101A")


def test_letters_before_the_number_may_still_differ():
    from p0.utils.ts_asset_identity import _same_asset_identity

    assert _same_asset_identity("060PI104", "060PIC104")


def test_an_alarm_variant_does_not_link_to_a_sibling_instrument():
    _, equipment, _, _ = _pick_best_reference("402PI101H", _ref(["402PI101A"]))
    assert equipment == ""


def test_the_same_sibling_still_fuzzy_matches_on_a_type_letter():
    _, equipment, _, how = _pick_best_reference("402PIA101A", _ref(["402PI101A"]))
    assert equipment == "402PI101A"
    assert how == "fuzzy"
