"""The server inspects uploaded files, so the browser stops parsing PDFs and workbooks."""

from __future__ import annotations

import io
import pathlib

import pytest

from p0.api.services import file_metadata as fm

CONNECTORS = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
).read_text()


def test_content_type_from_filename():
    assert fm.content_type_for("a.pdf") == "application/pdf"
    assert "spreadsheet" in fm.content_type_for("a.xlsx")
    assert fm.content_type_for("mystery.zzz") == "application/octet-stream"


def test_empty_payload_still_returns_a_shape():
    meta = fm.inspect("a.pdf", b"")
    assert meta["kind"] == "unknown"
    assert meta["size_bytes"] == 0
    assert meta["extension"] == "pdf"


def test_unknown_extension_is_not_an_error():
    meta = fm.inspect("notes.zzz", b"hello")
    assert meta["kind"] == "unknown"
    assert meta["size_bytes"] == 5


def test_csv_header_and_row_count():
    payload = b"tag,unit,value\nA,degC,1\nB,barg,2\n"
    meta = fm.inspect("readings.csv", payload)
    assert meta["kind"] == "spreadsheet"
    sheet = meta["sheets"][0]
    assert sheet["columns"] == ["tag", "unit", "value"]
    assert sheet["row_count"] == 2
    assert sheet["column_count"] == 3


def test_tsv_uses_tab_delimiter():
    meta = fm.inspect("readings.tsv", b"tag\tunit\nA\tdegC\n")
    assert meta["sheets"][0]["columns"] == ["tag", "unit"]


def test_blank_lines_do_not_inflate_the_row_count():
    meta = fm.inspect("x.csv", b"a,b\n1,2\n\n\n3,4\n")
    assert meta["sheets"][0]["row_count"] == 2


def test_xlsx_sheet_names_and_headers():
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    workbook.active.title = "Tags"
    workbook.active.append(["tag_name", "unit"])
    workbook.active.append(["FI-101", "Nm3/hr"])
    workbook.create_sheet("Limits").append(["tag_name", "hh"])
    buffer = io.BytesIO()
    workbook.save(buffer)

    meta = fm.inspect("book.xlsx", buffer.getvalue())
    assert meta["kind"] == "spreadsheet"
    names = [s["name"] for s in meta["sheets"]]
    assert names == ["Tags", "Limits"]
    assert meta["sheets"][0]["columns"] == ["tag_name", "unit"]


def test_pdf_page_count_and_text_layer():
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Equipment list")
    doc.new_page()
    payload = doc.tobytes()
    doc.close()

    meta = fm.inspect("drawing.pdf", payload)
    assert meta["kind"] == "pdf"
    assert meta["page_count"] == 2
    assert meta["has_text_layer"] is True
    assert meta["pages"][0]["has_text"] is True
    assert meta["pages"][1]["has_text"] is False


def test_scanned_pdf_reports_no_text_layer():
    """This is the flag that decides OCR versus text extraction."""
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    doc.new_page()
    payload = doc.tobytes()
    doc.close()

    meta = fm.inspect("scan.pdf", payload)
    assert meta["has_text_layer"] is False


def test_corrupt_pdf_degrades_instead_of_raising():
    meta = fm.inspect("broken.pdf", b"%PDF-1.4 not really a pdf")
    assert meta["extension"] == "pdf"
    assert "page_count" not in meta or meta.get("page_count") is None


def test_flow_fields_extracts_only_stored_columns_for_pdf():
    fields = fm.flow_fields(
        {"kind": "pdf", "page_count": 12, "has_text_layer": True, "content_type": "application/pdf"}
    )
    assert fields == {
        "content_type": "application/pdf",
        "page_count": 12,
        "has_text_layer": True,
    }


def test_flow_fields_for_spreadsheet_carries_sheet_names_and_rows():
    fields = fm.flow_fields(
        {
            "kind": "spreadsheet",
            "content_type": "text/csv",
            "sheets": [{"name": "A", "row_count": 10}, {"name": "B", "row_count": 5}],
        }
    )
    assert fields["sheet_names"] == ["A", "B"]
    assert fields["rows_in"] == 15


def test_flow_fields_drops_nulls():
    assert fm.flow_fields({"kind": "unknown", "content_type": None}) == {}
    assert fm.flow_fields({}) == {}


def test_every_upload_endpoint_reports_a_duplicate_verdict():
    assert CONNECTORS.count("_flow.find_duplicates(") == 6


def test_uploads_record_who_uploaded_and_the_category():
    """Every flow_state upsert and object-metadata record carries the actor."""
    assert CONNECTORS.count("uploaded_by=get_actor()") >= 5
    assert CONNECTORS.count("category=") >= 5


def test_uploads_attach_server_extracted_metadata():
    assert CONNECTORS.count("_file_metadata.flow_fields(_meta)") == 4
    assert '"metadata"] = _meta' in CONNECTORS
