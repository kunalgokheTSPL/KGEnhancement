"""A source with no natural key still gets a stable, lossless identity."""

from __future__ import annotations

import os

import pandas as pd

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.utils.row_identity import assign_stable_keys, content_digest


def _frame():
    """Two unique rows, two that share a base but differ, two byte-identical."""
    return pd.DataFrame([
        {"base": "A", "v": 1, "w": "x"},
        {"base": "B", "v": 2, "w": "y"},
        {"base": "C", "v": 3, "w": "p"},
        {"base": "C", "v": 4, "w": "q"},
        {"base": "D", "v": 5, "w": "z"},
        {"base": "D", "v": 5, "w": "z"},
    ])


def test_a_unique_base_is_used_untouched():
    out, _ = assign_stable_keys(_frame(), "base")
    assert out.loc[0, "tag_name"] == "A"
    assert out.loc[1, "tag_name"] == "B"


def test_rows_sharing_a_base_but_differing_get_different_keys():
    out, _ = assign_stable_keys(_frame(), "base")
    assert out.loc[2, "tag_name"] != out.loc[3, "tag_name"]
    assert out.loc[2, "tag_name"].startswith("C#")


def test_byte_identical_rows_collapse_onto_one_key():
    out, _ = assign_stable_keys(_frame(), "base")
    assert out.loc[4, "tag_name"] == out.loc[5, "tag_name"]


def test_nothing_that_differs_is_ever_lost():
    out, _ = assign_stable_keys(_frame(), "base")
    assert out["tag_name"].nunique() == 5


def test_the_report_says_exactly_what_happened():
    _, report = assign_stable_keys(_frame(), "base")
    assert report["rows"] == 6
    assert report["ambiguous_bases"] == 2
    assert report["rows_with_ambiguous_base"] == 4
    assert report["disambiguated"] == 3
    assert report["collapsed"] == 1


def test_the_same_input_always_produces_the_same_keys():
    first, _ = assign_stable_keys(_frame(), "base")
    second, _ = assign_stable_keys(_frame().sample(frac=1, random_state=7), "base")
    assert set(first["tag_name"]) == set(second["tag_name"])


def test_row_order_does_not_change_any_key():
    first, _ = assign_stable_keys(_frame(), "base")
    shuffled = _frame().iloc[::-1].reset_index(drop=True)
    second, _ = assign_stable_keys(shuffled, "base")
    assert first.loc[2, "tag_name"] in set(second["tag_name"])


def test_only_the_named_discriminators_decide_the_digest():
    df = pd.DataFrame([
        {"base": "C", "v": 1, "noise": "a"},
        {"base": "C", "v": 1, "noise": "b"},
    ])
    out, report = assign_stable_keys(df, "base", discriminators=["v"])
    assert out.loc[0, "tag_name"] == out.loc[1, "tag_name"]
    assert report["collapsed"] == 1


def test_a_digest_ignores_column_order():
    assert content_digest({"a": 1, "b": 2}, ["a", "b"]) == content_digest({"b": 2, "a": 1}, ["b", "a"])


def test_blank_and_missing_are_the_same_content():
    assert content_digest({"a": None}, ["a"]) == content_digest({}, ["a"])


def test_an_empty_frame_reports_nothing_rather_than_failing():
    _, report = assign_stable_keys(pd.DataFrame(), "base")
    assert report["rows"] == 0
