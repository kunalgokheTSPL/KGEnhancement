"""Golden tests for the document extractor with a fake LLM."""

from __future__ import annotations

from p0.source_processing.documents.user_doc_extract import (
    _build_prompt,
    _entities_to_pipe,
    _fields_fingerprint,
    _make_doc_id,
    _make_record_id,
)


def test_build_prompt_lists_the_requested_fields_and_keeps_the_text_slot():
    prompt = _build_prompt(["Equipment", "Tag No."], "SOP")
    assert "Equipment" in prompt and "Tag No." in prompt
    assert "SOP" in prompt
    assert "{text}" in prompt


def test_doc_and_record_ids_are_stable_and_distinct():
    assert _make_doc_id("manual.pdf", "SOP") == _make_doc_id("manual.pdf", "SOP")
    assert _make_record_id("manual.pdf", "SOP", 0) != _make_record_id("manual.pdf", "SOP", 1)
    assert _make_doc_id("a.pdf", "SOP") != _make_doc_id("b.pdf", "SOP")


def test_fields_fingerprint_is_order_independent():
    assert _fields_fingerprint(["a", "b"]) == _fields_fingerprint(["b", "a"])
    assert _fields_fingerprint(["a"]) != _fields_fingerprint(["a", "b"])


def test_entities_to_pipe_skips_empty_and_nan_values():
    assert _entities_to_pipe({"A": "1", "B": "", "C": "nan", "D": "2"}) == "A: 1 | D: 2"


def test_the_row_key_and_the_type_key_survive_the_schema_projection(tmp_path):
    import pandas as pd
    from p0.source_processing.documents.user_doc_extract import process_user_documents

    type_dir = tmp_path / "ram_data"
    type_dir.mkdir()
    pd.DataFrame(
        {"Tag No.": ["K2410B", "P1310A"], "MTBF": ["4200", "3100"]}
    ).to_excel(type_dir / "ram.xlsx", index=False)

    df = process_user_documents(
        str(tmp_path),
        {"RAM Data": {"folder": "ram_data", "source_columns": ["Tag No.", "MTBF"]}},
    )

    assert "record_id" in df.columns
    assert "document_type_key" in df.columns
    assert df["record_id"].nunique() == len(df) == 2
    assert df["document_id"].nunique() == 1
    assert set(df["document_type_key"]) == {"RAM Data"}
