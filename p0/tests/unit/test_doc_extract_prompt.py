"""Extracted records are joined to equipment, so the prompt must protect identity."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.source_processing.documents import user_doc_extract as ude

COLS = ["Equipment Tag", "Manufacturer"]


def _prompt(descs=None):
    return ude._build_prompt(COLS, "Equipment Datasheets", descs)


def test_the_prompt_says_records_are_joined_to_other_data():
    assert "joined" in _prompt()


def test_the_prompt_forbids_prepending_the_equipment_noun():
    prompt = _prompt()

    assert "Pump P-101" in prompt
    assert "exactly as printed" in prompt


def test_the_prompt_forbids_keeping_a_combined_tag_range():
    assert "P-101A/B" in _prompt()


def test_the_prompt_requires_one_subject_per_entry():
    assert "One subject per entry" in _prompt()


def test_a_heading_covering_several_units_becomes_one_entry_each():
    assert "one entry per unit" in _prompt()


def test_the_prompt_tells_the_model_to_carry_heading_context_down():
    prompt = _prompt()

    assert "Carry down the context" in prompt
    assert "column header" in prompt


def test_the_prompt_still_demands_verbatim_values():
    assert "verbatim" in _prompt().lower()


def test_an_absent_field_is_still_left_empty_rather_than_guessed():
    prompt = _prompt()

    assert 'Leave a field ""' in prompt
    assert "guessed one is not" in prompt


def test_the_prompt_still_names_the_extraction_tool_once():
    assert _prompt().count(ude._EXTRACT_TOOL) == 1


def test_the_prompt_still_forbids_inventing_any_value_not_only_a_tag():
    prompt = _prompt().lower()

    assert "never infer" in prompt


def test_the_text_placeholder_survives_for_chunk_substitution():
    prompt = _prompt()

    assert "{text}" in prompt
    assert "excerpt body" in prompt.format(text="excerpt body")


def test_field_descriptions_still_render_beside_their_field():
    prompt = _prompt({"Equipment Tag": "the plant's own tag scheme"})

    assert "Equipment Tag — the plant's own tag scheme" in prompt
