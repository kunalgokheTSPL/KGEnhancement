"""Unit tests for match_against_pnid, keyed through the same normalize_tag logic
that load_pnid_normalized_asset_map uses to build its map."""

from __future__ import annotations

from p0.utils.asset_identity import normalize_tag
from p0.utils.pnid_match import match_against_pnid

_RAW_EQUIPMENT_IDS = [
    "PP-101A",
    "BOCPP PP-102B",
    "V-2210",
]


def _build_pnid_map(raw_ids: list[str]) -> dict[str, str]:
    """Mirrors load_pnid_normalized_asset_map's key-building logic."""
    pnid_map: dict[str, str] = {}
    for value in raw_ids:
        original = str(value or "").strip()
        if not original:
            continue
        key = normalize_tag(original)
        if key and key not in pnid_map:
            pnid_map[key] = original
    return pnid_map


def test_exact_match_after_normalization():
    pnid_map = _build_pnid_map(_RAW_EQUIPMENT_IDS)
    original = _RAW_EQUIPMENT_IDS[0]
    query = original.lower().replace("-", "--")

    matched, value, score = match_against_pnid(query, pnid_map)

    assert matched is True
    assert value == original
    assert score == 1.0


def test_platform_prefix_is_stripped_before_matching():
    pnid_map = _build_pnid_map(_RAW_EQUIPMENT_IDS)
    original = _RAW_EQUIPMENT_IDS[1]
    key = normalize_tag(original)
    prefix, tail = original.split(" ", 1)
    query = tail
    assert normalize_tag(query) == key

    matched, value, score = match_against_pnid(query, pnid_map)

    assert matched is True
    assert value == original
    assert score == 1.0


def test_tag_with_no_entry_does_not_match():
    pnid_map = _build_pnid_map(_RAW_EQUIPMENT_IDS)
    known_keys = {normalize_tag(v) for v in _RAW_EQUIPMENT_IDS}
    query = "ZZ-999Z"
    assert normalize_tag(query) not in known_keys

    matched, value, score = match_against_pnid(query, pnid_map)

    assert matched is False
    assert value == ""
    assert score != score  # NaN


def test_short_substring_of_a_known_key_does_not_false_positive():
    pnid_map = _build_pnid_map(_RAW_EQUIPMENT_IDS)
    original = _RAW_EQUIPMENT_IDS[2]
    key = normalize_tag(original)
    substring_query = key[-3:]
    assert substring_query != key
    assert substring_query not in pnid_map

    matched, value, score = match_against_pnid(substring_query, pnid_map)

    assert matched is False
    assert value == ""
    assert score != score  # NaN
