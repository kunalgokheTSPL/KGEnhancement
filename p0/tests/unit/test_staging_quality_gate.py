"""Unit tests for the pure checks in staging_quality_gate.py (787 LOC, previously 0%).

Covers the format validators and the tabular/schema checks with tiny in-memory
fixtures — no RustFS, no pipeline, no database. xlsx/docx fixtures are built by hand
as zip members so the tests do not depend on openpyxl / python-docx being present.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from p0.utils.staging_quality_gate import (
    BLOCK,
    PASS,
    _check_csv_parseable,
    _check_docx_valid,
    _check_encoding,
    _check_header_schema,
    _check_natural_keys,
    _check_pdf_valid,
    _check_type_conformance,
    _check_xlsx_valid,
    _find_file_by_stem,
    _load_tabular_file,
    _validate_file_format,
)


def _write(path: Path, data: bytes) -> Path:
    path.write_bytes(data)
    return path


def _zip_with(path: Path, member: str) -> Path:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(member, "<xml/>")
    return path


def test_encoding_utf8_matches_and_utf16_does_not(tmp_path):
    utf8 = _write(tmp_path / "a.csv", "plant,equipment\nP1,pump\n".encode("utf-8"))
    assert _check_encoding(utf8)["matches_expected"] is True

    utf16 = _write(tmp_path / "b.csv", "plant,equipment\nP1,pump\n".encode("utf-16"))
    assert _check_encoding(utf16)["matches_expected"] is False


def test_csv_parseable_counts_data_rows_not_header(tmp_path):
    csv = _write(tmp_path / "d.csv", b"a,b,c\n1,2,3\n4,5,6\n")
    res = _check_csv_parseable(csv)
    assert res["parseable"] is True
    assert res["headers"] == ["a", "b", "c"]
    assert res["row_count"] == 2


def test_csv_parseable_flags_empty_file(tmp_path):
    empty = _write(tmp_path / "e.csv", b"")
    res = _check_csv_parseable(empty)
    assert res["parseable"] is False
    assert res["row_count"] == 0


def test_pdf_valid_by_magic_and_size(tmp_path):
    good = _write(tmp_path / "g.pdf", b"%PDF-1.4\n" + b"x" * 200)
    assert _check_pdf_valid(good)["valid"] is True

    bad = _write(tmp_path / "b.pdf", b"not a pdf at all")
    assert _check_pdf_valid(bad)["valid"] is False


def test_xlsx_valid_requires_xl_member(tmp_path):
    good = _zip_with(tmp_path / "g.xlsx", "xl/workbook.xml")
    assert _check_xlsx_valid(good)["valid"] is True

    not_zip = _write(tmp_path / "b.xlsx", b"just text, not a zip")
    assert _check_xlsx_valid(not_zip)["valid"] is False


def test_docx_valid_requires_word_member(tmp_path):
    good = _zip_with(tmp_path / "g.docx", "word/document.xml")
    assert _check_docx_valid(good)["valid"] is True

    zip_without_word = _zip_with(tmp_path / "b.docx", "xl/workbook.xml")
    assert _check_docx_valid(zip_without_word)["valid"] is False


def test_validate_file_format_dispatches_on_extension(tmp_path):
    csv = _write(tmp_path / "x.csv", b"a,b\n1,2\n")
    assert _validate_file_format(csv)["parseable"] is True
    assert _validate_file_format(tmp_path / "x.unknown")["parseable"] is False


def test_load_tabular_file_reads_csv_as_strings(tmp_path):
    csv = _write(tmp_path / "t.csv", b"tag,value\nTI-1,10\n")
    df = _load_tabular_file(csv)
    assert list(df.columns) == ["tag", "value"]
    assert df.iloc[0]["value"] == "10"  # dtype=str


def test_find_file_by_stem_tries_extensions(tmp_path):
    (tmp_path / "equipment.xlsx").write_bytes(b"x")
    found = _find_file_by_stem(tmp_path, "equipment")
    assert found is not None and found.name == "equipment.xlsx"
    assert _find_file_by_stem(tmp_path, "missing") is None


def test_natural_keys_blocks_on_missing_column():
    df = pd.DataFrame({"other": [1, 2]})
    res = _check_natural_keys(df, ["equipment_id"], null_threshold=0.1)
    assert res["status"] == BLOCK
    assert res["missing_key_columns"] == ["equipment_id"]


def test_natural_keys_passes_clean_and_counts_duplicates():
    clean = pd.DataFrame({"k": ["a", "b", "c"]})
    assert _check_natural_keys(clean, ["k"], 0.1)["status"] == PASS

    dupes = pd.DataFrame({"k": ["a", "a", "b"]})
    res = _check_natural_keys(dupes, ["k"], 0.1)
    assert res["duplicate_count"] == 2


def test_natural_keys_blocks_when_nulls_exceed_threshold():
    df = pd.DataFrame({"k": [None, None, "x"]})
    res = _check_natural_keys(df, ["k"], null_threshold=0.5)
    assert res["status"] == BLOCK


def test_type_conformance_counts_numeric_and_date_violations():
    df = pd.DataFrame({"n": ["1", "2", "x"], "d": ["2020-01-01", "nope", None]})
    res = _check_type_conformance(df, {"n": "numeric", "d": "date"})
    assert res["n"]["violations"] == 1
    assert res["n"]["sample_violations"] == ["x"]
    assert res["d"]["violations"] == 1


def test_type_conformance_marks_missing_column():
    df = pd.DataFrame({"present": ["1"]})
    res = _check_type_conformance(df, {"absent": "numeric"})
    assert res["absent"]["status"] == "MISSING"


def test_header_schema_pass_and_block():
    ok = _check_header_schema(["a", "b", "c"], ["a", "b"])
    assert ok["status"] == PASS
    assert ok["missing_required"] == []
    assert "c" in ok["extra_columns"]

    bad = _check_header_schema(["a"], ["a", "z"])
    assert bad["status"] == BLOCK
    assert bad["missing_required"] == ["z"]
