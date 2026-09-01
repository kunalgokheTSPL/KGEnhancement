"""Equipment identity for findings sources: honest matches, no invented assets."""

from __future__ import annotations

import pandas as pd

from p0.utils import findings_identity as fi


def test_explicit_sap_equipment_wins_over_everything():
    result = fi.resolve(
        {"sap_equipment_id": "10883544", "equipment_ref": "BN-64L", "functional_location": "BNDB.PIN"},
        sap_equipment_columns=("sap_equipment_id",),
        equipment_columns=("equipment_ref",),
        floc_columns=("functional_location",),
    )
    assert result["normalized_asset"] == "10883544"
    assert result["asset_match_method"] == fi.SAP_EQUIPMENT
    assert result["asset_match_confidence"] == 1.0


def test_equipment_column_is_never_split_on_hyphen():
    """LEVL-LG-4110 is a whole tag; splitting it would yield 4110."""
    result = fi.resolve(
        {"equipment_ref": "LEVL-LG-4110"}, equipment_columns=("equipment_ref",)
    )
    assert result["normalized_asset"] == "LEVL-LG-4110"


def test_excel_float_ids_are_de_floated():
    result = fi.resolve({"equipment_ref": "10883544.0"}, equipment_columns=("equipment_ref",))
    assert result["normalized_asset"] == "10883544"


def test_floc_tail_is_taken_after_the_dot_then_the_hyphen():
    assert fi.from_floc("BKDA.CTW.HZD.V6410-V6410") == ("V6410", fi.FLOC_TAIL)
    assert fi.from_floc("BN14.CG001B-100PG0486101X") == ("100PG0486101X", fi.FLOC_TAIL)
    assert fi.from_floc("BKDA.STU.TOP-PRICELLAR") == ("PRICELLAR", fi.FLOC_TAIL)


def test_floc_without_a_hyphen_uses_the_dot_tail():
    assert fi.from_floc("BNDI.PIN.CG015ASB") == ("CG015ASB", fi.FLOC_SEGMENT)


def test_a_system_code_tail_stays_platform_qualified():
    """Bare PIN would merge every platform's piping into one asset."""
    assert fi.from_floc("BNDB.PIN") == ("BNDB.PIN", fi.FLOC_SEGMENT)
    assert fi.from_floc("BNDI.PIN") == ("BNDI.PIN", fi.FLOC_SEGMENT)
    assert fi.from_floc("BNDB.PIN")[0] != fi.from_floc("BNDI.PIN")[0]


def test_a_numeric_hyphen_fragment_is_rejected():
    value, method = fi.from_floc("BNGB.CG012E-1200")
    assert value == "CG012E-1200"
    assert method == fi.FLOC_SEGMENT


def test_tags_are_pulled_out_of_incident_titles():
    assert fi.from_text("BNP-A: COTP (P803) mechanical seal leak")[0] == "P803"
    assert fi.from_text("BNCPP-B: PSV-6040B of E-6040B fuel gas superheater crack")[0] == "PSV-6040B"
    assert fi.from_text("BNG-B: V-1600 PZT1602 nozzle leak")[0] == "V-1600"
    assert fi.from_text("BNG-B: 40-PG-0079-1143-P25 leak (CUI)")[0] == "40-PG-0079-1143-P25"
    assert fi.from_text("BNDP-I: BN67L flowline pinhole leak")[0] == "BN67L"


def test_a_well_tag_written_with_a_space_is_hyphenated():
    assert fi.from_text("BNJT-K: BN 78 choke valve leak")[0] == "BN-78"


def test_clock_times_in_narrative_text_are_not_assets():
    """'At 1003 hours' must not become asset AT-1003."""
    for narrative in (
        "At 1003 hours, an Operation technician reported a leak",
        "On 14 January 2020 at 1750 hrs, site team observed",
        "14/01/2020 at 1730 hrs, crew observed",
    ):
        assert fi.from_text(narrative) == ("", fi.UNMATCHED)


def test_classification_words_are_not_assets():
    assert fi.from_text("Tier 1 process safety event")[0] != "TIER-1"


def test_nothing_matched_is_reported_as_unmatched_not_blank_guessed():
    result = fi.resolve({"description": "Leak at Heating Medium Line"}, text_columns=("description",))
    assert result["normalized_asset"] == ""
    assert result["asset_match_method"] == fi.UNMATCHED
    assert result["asset_match_confidence"] == 0.0


def test_confidence_decreases_down_the_ladder():
    order = [fi.SAP_EQUIPMENT, fi.EQUIPMENT_TAG, fi.FLOC_TAIL, fi.FLOC_SEGMENT, fi.TEXT_EXTRACT, fi.UNMATCHED]
    scores = [fi.CONFIDENCE[m] for m in order]
    assert scores == sorted(scores, reverse=True)


def test_every_connector_declares_a_column_ladder():
    for connector in ("aif", "gloc", "lopc"):
        assert fi.spec_for(connector), f"{connector} has no column spec"


def test_aif_never_reads_its_equipment_column_as_a_tag():
    """That column carries an equipment CLASS — Piping Hydrocarbon, Vessel."""
    spec = fi.spec_for("aif")
    assert "equipment_ref" not in spec.get("equipment_columns", ())
    assert "equipment_class" not in spec.get("equipment_columns", ())
    assert spec["floc_columns"] == ("functional_location",)


def test_resolve_frame_adds_all_three_columns():
    df = pd.DataFrame([{"functional_location": "BNDI.PIN.CG015ASB"}, {"functional_location": None}])
    out = fi.resolve_frame(df, fi.spec_for("aif"))
    assert list(out["normalized_asset"]) == ["CG015ASB", ""]
    assert list(out["asset_match_confidence"]) == [0.80, 0.0]
    assert "asset_match_method" in out.columns


def test_resolve_frame_leaves_an_empty_frame_alone():
    assert fi.resolve_frame(pd.DataFrame(), fi.spec_for("aif")).empty
