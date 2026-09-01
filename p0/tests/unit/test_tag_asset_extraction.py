"""Deriving an asset key from a historian tag."""

import pandas as pd

from p0.utils.ts_asset_identity import (
    _extract_tag_asset_key,
    _pick_best_reference,
    tag_asset_part,
)


def test_the_asset_part_stops_at_the_attribute_not_inside_the_asset_name():
    assert tag_asset_part("GL-HDR.FT-0505") == "GL-HDR"
    assert tag_asset_part("BO-B V-200A.Previous Day Average") == "BO-B V-200A"
    assert tag_asset_part("241ASV143.PV") == "241ASV143"


def test_a_duplicate_suffix_is_dropped():
    assert tag_asset_part("GL-HDR.FT-0505#9d13f07d") == "GL-HDR"


def test_a_controller_path_prefix_is_dropped():
    assert tag_asset_part("FCS0101!241FT108.PV") == "241FT108"


def test_a_hyphenated_asset_is_not_truncated_to_its_first_letter():
    assert _extract_tag_asset_key("E-P0210A") == "EP0210A"
    assert _extract_tag_asset_key("GL-HDR.FT-0505") == "GLHDR"


def test_a_multi_word_asset_keeps_all_its_words():
    assert _extract_tag_asset_key("LP Gas From BEP-A.Total") == "LPGASFROMBEPA"


def test_an_equipment_shaped_tag_still_resolves_to_the_equipment():
    assert _extract_tag_asset_key("K-2410A.SPEED") == "K2410A"
    assert _extract_tag_asset_key("241ASV143.PV") == "241ASV143"


def _ref():
    return pd.DataFrame(
        {
            "asset_key": ["E 2330", "K 2410A"],
            "match_key": ["E2330", "K2410A"],
            "normalized_asset": ["E-2330", "K-2410A"],
            "identity_asset": ["E-2330", "K-2410A"],
            "equipment_id": ["E-2330", "K-2410A"],
            "source": ["pid", "pid"],
        }
    )


def test_a_single_character_key_never_matches_an_equipment():
    _, equipment, conf, how = _pick_best_reference("E", _ref())
    assert equipment == ""
    assert how == "key_too_short"


def test_a_real_key_still_matches():
    _, equipment, conf, how = _pick_best_reference("K2410A", _ref())
    assert equipment == "K-2410A"
    assert how == "exact"
