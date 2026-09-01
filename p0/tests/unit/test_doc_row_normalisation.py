"""A reviewed document row is reduced to CDM columns without losing its identity."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.database import cdm_writer
from p0.api.routers.context import _DOC_CANONICAL, _normalise_doc_row

IDENTITY = ("plant_code_id", "site", "record_id")


def _schema_cols():
    pk, cols = cdm_writer._doc_metadata_schema()
    return {c for c in cols if c != pk}


def test_a_normalised_row_carries_every_column_of_its_own_identity():
    out = _normalise_doc_row({"record_id": "REC-1", "plant_code_id": "P1"})

    for col in IDENTITY:
        assert out.get(col) not in (None, "")


def test_a_row_that_names_no_site_takes_the_column_default():
    assert _normalise_doc_row({"record_id": "REC-1"})["site"] == "-"
    assert _normalise_doc_row({"record_id": "REC-1", "site": ""})["site"] == "-"
    assert _normalise_doc_row({"record_id": "REC-1", "site": None})["site"] == "-"


def test_a_site_the_reviewer_supplied_is_kept_and_trimmed():
    assert _normalise_doc_row({"site": "  PLANT-A  "})["site"] == "PLANT-A"


def test_the_short_field_names_the_review_screen_sends_are_expanded():
    out = _normalise_doc_row(
        {"id": "DOC-1", "type": "rca_reports", "equip": "P-101", "source": "a.pdf"}
    )

    assert out["document_id"] == "DOC-1"
    assert out["document_type"] == "rca_reports"
    assert out["equipment_tag"] == "P-101"
    assert out["source_file"] == "a.pdf"


def test_a_field_the_table_does_not_have_is_dropped_rather_than_bound():
    out = _normalise_doc_row({"record_id": "REC-1", "reviewer_note": "looks wrong"})

    assert "reviewer_note" not in out


def test_review_bookkeeping_never_arrives_from_the_client():
    out = _normalise_doc_row(
        {"record_id": "REC-1", "row_version": 9, "edited_by": "someone-else"}
    )

    assert "row_version" not in out
    assert "edited_by" not in out


def test_every_canonical_column_actually_exists_on_the_table():
    assert _DOC_CANONICAL <= _schema_cols()


def test_the_upsert_can_key_every_normalised_row():
    out = _normalise_doc_row({"record_id": "REC-1", "plant_code_id": "P1"})

    assert set(cdm_writer._DOC_CONFLICT_COLS) <= set(out)
