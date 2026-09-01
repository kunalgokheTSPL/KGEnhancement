"""A workbook is ingested sheet by sheet, and every row keeps its own identity."""

from __future__ import annotations

import logging
import os

import pandas as pd
import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.routers.connectors import ALLOWED_DOC_EXT
from p0.source_processing.documents import user_doc_extract as ude

COLS = ["Equipment Tag", "Description"]
LOG = logging.getLogger("test_doc_tabular_ingest")


@pytest.fixture
def workbook(tmp_path):
    def _make(name, sheets):
        path = tmp_path / name
        with pd.ExcelWriter(path) as writer:
            for sheet, rows in sheets.items():
                pd.DataFrame(rows).to_excel(
                    writer, sheet_name=sheet, index=False, header=False
                )
        return str(path)

    return _make


@pytest.fixture
def two_sheets(workbook):
    return workbook(
        "notifications.xlsx",
        {
            "Notification": [COLS, ["P-101", "seal leak"]],
            "Work Order": [COLS, ["K-201", "bearing noise"]],
        },
    )


def _extract(path):
    return ude._extract_from_tabular(
        path, COLS, "Alerts", os.path.basename(path), LOG, doc_type_key="alerts"
    )


def test_rows_from_every_sheet_are_ingested_not_just_one(two_sheets):
    tags = [r["equipment_tag"] for r in _extract(two_sheets)]

    assert tags == ["P-101", "K-201"]


def test_the_same_row_number_on_two_sheets_gets_two_record_ids(two_sheets):
    ids = [r["record_id"] for r in _extract(two_sheets)]

    assert len(set(ids)) == len(ids)


def test_a_row_names_the_sheet_it_came_from(two_sheets):
    sheets = [r["source_pages"] for r in _extract(two_sheets)]

    assert sheets == ["Notification", "Work Order"]


def test_a_row_carries_the_ordinal_of_its_sheet(two_sheets):
    ordinals = [r["chunk_index"] for r in _extract(two_sheets)]

    assert ordinals == [0, 1]


def test_all_rows_of_one_file_still_share_one_document_id(two_sheets):
    doc_ids = {r["document_id"] for r in _extract(two_sheets)}

    assert len(doc_ids) == 1


def test_a_sheet_holding_only_blank_rows_contributes_nothing(workbook):
    path = workbook(
        "notifications.xlsx",
        {
            "Notification": [COLS, ["P-101", "seal leak"]],
            "Cover": [["Cover"], [""], [""]],
        },
    )
    rows = _extract(path)

    assert [r["source_pages"] for r in rows] == ["Notification"]


def test_a_csv_ingests_with_no_sheet_label(tmp_path):
    path = tmp_path / "alerts.csv"
    path.write_text("Equipment Tag,Description\nP-101,seal leak\n")
    rows = _extract(str(path))

    assert len(rows) == 1
    assert rows[0]["source_pages"] == ""


def test_an_unreadable_file_yields_no_rows_rather_than_raising(tmp_path):
    path = tmp_path / "broken.xlsx"
    path.write_bytes(b"not a workbook")

    assert _extract(str(path)) == []


def test_binary_workbooks_are_read_as_tables_not_sent_to_the_llm():
    assert ".xlsb" in ude.SUPPORTED_EXTS
    assert ".xlsb" in ude.TABULAR_EXTS


def test_legacy_binary_word_files_are_not_accepted_anywhere():
    assert ".doc" not in ude.SUPPORTED_EXTS
    assert ".doc" not in ALLOWED_DOC_EXT


def test_the_upload_endpoint_accepts_every_format_the_extractor_reads():
    assert ude.SUPPORTED_EXTS <= ALLOWED_DOC_EXT
