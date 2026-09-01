"""Every column the document extractor emits must have a home in the CDM table."""

from __future__ import annotations

import os

import yaml

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.config import SCHEMA_FROZEN_FILE
from p0.source_processing.documents.user_doc_extract import process_user_documents

PROVENANCE = {"chunk_index", "page_start", "page_end", "source_pages"}

NOT_PERSISTED = {"document_type_key", "equipment_label", "description"}


def _table(name):
    """The declared column map for one CDM table, straight from the frozen schema."""
    with open(SCHEMA_FROZEN_FILE, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    tables = cfg.get("schema") or cfg.get("tables") or {}
    return (tables.get(name) or {}).get("columns") or {}


def _emitted(tmp_path):
    """The columns an extraction run produces, taken from an empty run's frame."""
    return set(process_user_documents(str(tmp_path), {}).columns)


def test_the_extractor_emits_the_page_provenance_it_worked_out(tmp_path):
    assert PROVENANCE <= _emitted(tmp_path)


def test_the_cdm_table_has_a_column_for_every_provenance_field():
    assert PROVENANCE <= set(_table("document_metadata"))


def test_the_provenance_columns_are_typed_for_the_values_they_hold():
    cols = _table("document_metadata")
    assert cols["chunk_index"] == "INTEGER"
    assert cols["page_start"] == "INTEGER"
    assert cols["page_end"] == "INTEGER"
    assert cols["source_pages"].startswith("VARCHAR")


def test_no_extracted_column_is_dropped_without_being_declared_so(tmp_path):
    homeless = _emitted(tmp_path) - set(_table("document_metadata"))
    assert homeless == NOT_PERSISTED, (
        "an extracted column has no CDM column and is not on the drop list: "
        f"{sorted(homeless - NOT_PERSISTED)}"
    )


REVIEW = {"row_version", "edited_by", "edited_at"}


def test_the_cdm_table_can_record_who_last_edited_a_row_and_when():
    assert REVIEW <= set(_table("document_metadata"))


def test_the_review_columns_are_typed_for_the_values_they_hold():
    cols = _table("document_metadata")
    assert cols["row_version"] == "INTEGER"
    assert cols["edited_by"].startswith("VARCHAR")
    assert cols["edited_at"] == "TIMESTAMP"


def test_review_bookkeeping_is_not_the_extractors_job(tmp_path):
    assert not (REVIEW & _emitted(tmp_path))


def test_a_row_is_still_identified_by_plant_site_and_record():
    with open(SCHEMA_FROZEN_FILE, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    tables = cfg.get("schema") or cfg.get("tables") or {}
    uniques = [
        c
        for c in (tables["document_metadata"].get("constraints") or [])
        if c.get("type") == "unique"
    ]

    assert [c["columns"] for c in uniques] == [["plant_code_id", "site", "record_id"]]
