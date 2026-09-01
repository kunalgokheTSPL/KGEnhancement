"""Golden tests for the deterministic SAP canonical builder (pure DataFrame in/out).

build_asset_cross_reference is the SAP-equipment ↔ P&ID-tag crosswalk — core CDM
linkage. These pin the exact output rows and match semantics (not just "it ran"), so a
regression in the matching logic writes visibly-wrong crosswalk data and a test fails.
"""

from __future__ import annotations

import pandas as pd

from p0.pipelines.canonical_builder_sap import build_asset_cross_reference


def test_exact_normalized_asset_match_pins_the_output_row():
    sap = pd.DataFrame(
        [
            {"equipment_id": "E1", "normalized_asset": "PUMP101"},
            {"equipment_id": "E2", "normalized_asset": "VALVE202"},
            {"equipment_id": "E3", "normalized_asset": "ORPHAN"},
        ]
    )
    pid = pd.DataFrame(
        [
            {"equipment_tag": "P-101", "normalized_asset": "PUMP101"},
            {"equipment_tag": "V-202", "normalized_asset": "VALVE202"},
        ]
    )
    out = build_asset_cross_reference(sap, pid, "plant_1")

    assert len(out) == 2  # E3 has no P&ID match -> not emitted
    by_sap = {r["sap_equipment_id"]: r for _, r in out.iterrows()}
    assert "E3" not in by_sap

    e1 = by_sap["E1"]
    assert e1["pid_tag"] == "P-101"
    assert e1["normalized_asset"] == "PUMP101"
    assert e1["match_method"] == "exact_normalized_asset"
    assert e1["confidence"] == 1.0
    assert e1["source_system"] == "SAP"
    assert e1["is_active"] == 1
    assert list(out["xref_uid"]) == [1, 2]  # sequential surrogate key


def test_contains_match_gets_lower_confidence():
    sap = pd.DataFrame(
        [{"equipment_id": "E1", "normalized_asset": "AREA5_PUMP101_SUFFIX"}]
    )
    pid = pd.DataFrame([{"equipment_tag": "P-101", "normalized_asset": "PUMP101"}])
    out = build_asset_cross_reference(sap, pid, "plant_1")

    assert len(out) == 1
    assert out.iloc[0]["match_method"] == "contains_pid_tag"
    assert out.iloc[0]["confidence"] == 0.8


def test_empty_inputs_return_empty():
    pid = pd.DataFrame([{"equipment_tag": "P-101", "normalized_asset": "PUMP101"}])
    assert build_asset_cross_reference(pd.DataFrame(), pid, "p").empty
    assert build_asset_cross_reference(pid, pd.DataFrame(), "p").empty


def test_no_match_returns_empty_frame_with_schema_columns():
    sap = pd.DataFrame([{"equipment_id": "E1", "normalized_asset": "AAA"}])
    pid = pd.DataFrame([{"equipment_tag": "P-1", "normalized_asset": "BBB"}])
    out = build_asset_cross_reference(sap, pid, "p")

    assert out.empty
    assert "sap_equipment_id" in out.columns  # typed-but-empty, not a bare frame


def test_duplicate_sap_rows_are_deduplicated():
    sap = pd.DataFrame(
        [
            {"equipment_id": "E1", "normalized_asset": "PUMP101"},
            {"equipment_id": "E1", "normalized_asset": "PUMP101"},
        ]
    )
    pid = pd.DataFrame([{"equipment_tag": "P-101", "normalized_asset": "PUMP101"}])
    out = build_asset_cross_reference(sap, pid, "plant_1")

    assert len(out) == 1
