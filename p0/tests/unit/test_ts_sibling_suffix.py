"""Sibling instruments differing only by a trailing letter must not collapse to one key."""

from __future__ import annotations

import pandas as pd
import pytest

from p0.utils.ts_asset_identity import (
    _build_reference_assets,
    _extract_tag_asset_key,
    _pick_best_reference,
)

SIBLINGS = [
    "421-PDIA-105A",
    "421-PDIA-105B",
    "421-PDIA-105C",
    "131-PIA-113A",
    "131-PIA-113B",
    "K-2410A",
    "K-2410B",
]


@pytest.fixture()
def reference() -> pd.DataFrame:
    frame = pd.DataFrame({"normalized_asset": SIBLINGS, "equipment_id": SIBLINGS})
    return _build_reference_assets(frame)


@pytest.mark.parametrize(
    "tag,equipment",
    [
        ("421PDIA105A", "421-PDIA-105A"),
        ("421PDIA105B", "421-PDIA-105B"),
        ("421PDIA105C", "421-PDIA-105C"),
        ("131PIA113A", "131-PIA-113A"),
        ("131PIA113B", "131-PIA-113B"),
        ("K2410A.HHPC_Act_Flow", "K-2410A"),
        ("K2410B.HHPC_Act_Flow", "K-2410B"),
    ],
)
def test_each_tag_links_to_its_own_sibling(reference, tag, equipment):
    key = _extract_tag_asset_key(tag)
    assert _pick_best_reference(key, reference)[1] == equipment


def test_a_digit_leading_tag_keeps_its_trailing_letter():
    assert _extract_tag_asset_key("421PDIA105B") == "421PDIA105B"


def test_digit_leading_and_letter_leading_tags_behave_the_same():
    digit_keys = {_extract_tag_asset_key(t) for t in ("131PIA113A", "131PIA113B")}
    letter_keys = {_extract_tag_asset_key(t) for t in ("K2410A", "K2410B")}
    assert len(digit_keys) == 2
    assert len(letter_keys) == 2
