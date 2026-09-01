"""Tests for the review-patch re-transform helpers.

When a user edits a row on the review page, the edit must pass back through the
in-pipeline transform steps so the canonical fields are recomputed — otherwise a
hand-edit (e.g. changing a tag/equipment name) leaves derived columns stale. These
assert the key recomputations the fixes added:

  • TS: iotdb_tag_id re-derived from the (edited) tag_name; plant_code_id canonicalized.
  • Docs: plant_code_id canonicalized; canonical fields populated; record_id preserved.
  • P&ID: normalized_asset re-derived from the (edited) equipment_tag; plant_code_id
    canonicalized; connectivity rows get the canonical plant.

These need decrypted config (derived/identity YAML) so they ride the session
MASTER_KEY fixture; they do NOT touch the DB or RustFS.
"""

from __future__ import annotations

from p0.api.routers import context as C


def test_ts_retransform_rederives_iotdb_tag_id():
    edited = [
        {
            "tag_name": "1300FI-999.NEW",
            "iotdb_tag_id": "STALE_OLD",
            "unit": "C",
            "source_file": "tags.csv",
            "plant_code_id": "WRONG",
        }
    ]
    out = C._retransform_ts_rows("HYDRO", edited)
    r = out[0]
    assert r["iotdb_tag_id"] == "1300FI_999_NEW"
    assert r["plant_code_id"] == "HYDRO"


def test_ts_retransform_empty_is_noop():
    assert C._retransform_ts_rows("HYDRO", []) == []


def test_docs_retransform_canonicalizes_and_preserves_record_id():
    edited = [
        {
            "document_id": "DOC-1",
            "document_type": "sop",
            "title": "Edited title",
            "equipment_tag": "P-101",
            "source_file": "sop.pdf",
            "record_id": "REC-1",
            "plant_code_id": "WRONG",
        }
    ]
    out = C._retransform_docs_rows("HYDRO", edited)
    r = out[0]
    assert r["plant_code_id"] == "HYDRO"
    assert r["record_id"] == "REC-1"
    assert r["title"] == "Edited title"


def test_pnid_retransform_rederives_normalized_asset():
    ep = [
        {
            "pid_tag": "PT-1",
            "equipment_tag": "  p-101  ",
            "normalized_asset": "STALE",
            "plant_code_id": "WRONG",
            "source_file": "d.pdf",
        }
    ]
    ec = [
        {
            "from_equipment_ref": "P-101",
            "to_equipment_ref": "V-200",
            "plant_code_id": "WRONG",
            "source_file": "d.pdf",
        }
    ]
    out_ep, out_ec = C._retransform_pnid_rows("HYDRO", ep, ec)
    r = out_ep[0]
    assert r["equipment_tag"] == "P-101"
    assert r["normalized_asset"] == "P-101"
    assert r["plant_code_id"] == "HYDRO"
    assert out_ec[0]["plant_code_id"] == "HYDRO"
