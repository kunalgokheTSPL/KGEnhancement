"""Column rename key matching."""

import pandas as pd
import yaml

from p0.utils.renamer import apply_column_rename

CFG = "p0/config/templates/_common/column_rename/pnid_column_rename.yaml"


def _cfg():
    return yaml.safe_load(open(CFG))


def test_underscore_spelling_matches_a_space_separated_key():
    out = apply_column_rename(
        pd.DataFrame({"Equipment_Tag": ["P-1"]}), _cfg(), "pid", verbose=False
    )
    assert "equipment_tag" in out.columns


def test_hyphen_and_dot_spellings_match_the_same_key():
    for header in ("EQUIPMENT-TAG", "Equipment.Tag", "equipment tag"):
        out = apply_column_rename(
            pd.DataFrame({header: ["P-1"]}), _cfg(), "pid", verbose=False
        )
        assert "equipment_tag" in out.columns, header


def test_an_undeclared_header_is_left_alone_not_truncated_onto_a_neighbour():
    out = apply_column_rename(
        pd.DataFrame({"equipment_weight_kg": ["120"]}), _cfg(), "pid", verbose=False
    )
    assert "equipment_weight_kg" in out.columns
    assert "equipment_id" not in out.columns


def test_a_multi_word_header_does_not_collapse_to_its_first_word():
    out = apply_column_rename(
        pd.DataFrame({"tag_colour_code": ["red"]}), _cfg(), "pid", verbose=False
    )
    assert "equipment_tag" not in out.columns
