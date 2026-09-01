"""Schema reading for AIF / GLOC / LOPC — template driven, junk tolerant."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from p0.api.services import findings


def test_all_connectors_are_declared():
    assert findings.CONNECTORS == ("aif", "gloc", "lopc", "upd_event", "trip_event")
    for connector in findings.CONNECTORS:
        assert findings.spec_for(connector), f"{connector} has no template"
        assert findings.target_table(connector)


def test_each_connector_targets_its_own_table():
    tables = {findings.target_table(c) for c in findings.CONNECTORS}
    assert tables == {
        "inspection_record_aif",
        "containment_event_gloc",
        "containment_event_lopc",
        "upd_event",
        "trip_event",
    }


def test_headers_match_regardless_of_spacing_case_or_punctuation():
    for header in ("Functional Location", "functional_location", "FUNC. LOCATION", "FLOC"):
        mapping = findings.column_map("aif", [header])
        assert mapping["mapped"][header] == "functional_location"


def test_unrecognised_headers_are_reported_not_dropped_silently():
    mapping = findings.column_map("aif", ["Notification", "Some Site Specific Column"])
    assert mapping["unmapped"] == ["Some Site Specific Column"]


def test_missing_required_columns_are_named():
    mapping = findings.column_map("aif", ["Description"])
    assert findings.missing_required("aif", mapping) == ["notification_no"]


def test_the_data_sheet_is_chosen_by_hint_not_position():
    assert findings.pick_sheet("aif", ["Cover", "Notes", "SKA open"]) == "SKA open"
    assert findings.pick_sheet("lopc", ["Summary", "Database"]) == "Database"
    assert findings.pick_sheet("gloc", ["Export"]) == "Export"


def test_a_file_with_no_hinted_sheet_falls_back_to_the_first():
    assert findings.pick_sheet("aif", ["Sheet1", "Sheet2"]) == "Sheet1"


def test_no_sheets_is_not_an_error():
    assert findings.pick_sheet("aif", []) is None


def test_classification_spelling_variants_fold_together():
    assert findings.canonical_value("incident_classification", "Slight Release") == "slight_release"
    assert findings.canonical_value("incident_classification", "Slight release") == "slight_release"


def test_an_unknown_vocabulary_value_is_passed_through_untouched():
    assert findings.canonical_value("severity", "Catastrophic") == "Catastrophic"


def test_dates_parse_across_the_formats_real_exports_use():
    for text in ("2020-01-02", "2020-01-02 00:00:00", "02/01/2020", "2 January 2020"):
        assert isinstance(findings.coerce_date(text), datetime)


def test_an_unparseable_date_is_null_not_an_exception():
    assert findings.coerce_date("sometime last year") is None
    assert findings.coerce_date("") is None


def test_a_numeric_column_holding_words_yields_null():
    """The leak-amount column really does contain 'Valve' and 'Gas'."""
    assert findings.coerce_number("Valve") is None
    assert findings.coerce_number("14.02") == 14.02


def test_the_raw_cell_is_preserved_when_the_number_is_junk():
    row = findings.normalise_row("lopc", {"leak_amount_kg": "Valve"})
    assert row["leak_amount_kg"] is None
    assert row["leak_amount_raw"] == "Valve"


def test_booleans_accept_yes_and_no():
    assert findings.coerce_boolean("Yes") is True
    assert findings.coerce_boolean("No") is False
    assert findings.coerce_boolean("maybe") is None


def test_aif_identity_comes_from_the_functional_location():
    result = findings.resolve_identity({"functional_location": "BNDI.PIN.CG015ASB"}, "aif")
    assert result["normalized_asset"] == "CG015ASB"


def test_aif_equipment_class_never_becomes_an_asset():
    result = findings.resolve_identity(
        {"equipment_class": "Piping Hydrocarbon", "functional_location": ""}, "aif"
    )
    assert result["normalized_asset"] == ""


def test_gloc_prefers_the_sap_column_when_it_is_populated():
    result = findings.resolve_identity(
        {"sap_equipment_id": "10883544", "equipment_ref": "BN-64L"}, "gloc"
    )
    assert result["normalized_asset"] == "10883544"


def test_record_ids_are_unique_even_when_the_natural_key_repeats():
    rows = [
        {"notification_no": None, "report_date": "2020-03-11", "equipment_ref": "BN-64L"},
        {"notification_no": None, "report_date": "2020-03-11", "equipment_ref": "BN-64L"},
    ]
    ids = findings.assign_record_ids("gloc", rows)
    assert len(set(ids)) == 2
    assert ids[1].endswith("#2")


def test_record_ids_are_stable_across_runs():
    rows = [{"notification_no": "1"}, {"notification_no": "1"}, {"notification_no": "2"}]
    assert findings.assign_record_ids("aif", rows) == findings.assign_record_ids("aif", rows)


def test_a_row_with_no_key_at_all_still_gets_one():
    ids = findings.assign_record_ids("gloc", [{}])
    assert ids == ["row-1"]


def test_excel_float_ids_do_not_reach_the_key():
    assert findings.key_part("23810041.0") == "23810041"
    assert findings.key_part("nan") == ""
    assert findings.key_part(pd.NaT) == ""


def test_read_frame_renames_coerces_and_resolves_in_one_pass():
    df = pd.DataFrame(
        [{"Notification": 24447021, "Functional Location": "BNDI.PIN.CG015ASB",
          "AIF Priority": "1", "Creation Date": "2023-02-20"}]
    )
    out, mapping = findings.read_frame("aif", df)
    assert out.loc[0, "notification_no"] == 24447021
    assert out.loc[0, "priority"] == 1
    assert out.loc[0, "normalized_asset"] == "CG015ASB"
    assert isinstance(out.loc[0, "creation_date"], datetime)
    assert mapping["unmapped"] == []


def test_read_frame_leaves_an_empty_frame_alone():
    out, mapping = findings.read_frame("aif", pd.DataFrame())
    assert out.empty
    assert mapping["missing"] == []


def test_the_catalogue_describes_all_connectors():
    entries = {entry["key"] for entry in findings.catalogue()}
    assert entries == {"aif", "gloc", "lopc", "upd_event", "trip_event"}
    for entry in findings.catalogue():
        assert entry["label"]
        assert ".xlsx" in entry["extensions"]


def test_no_upload_reports_success_when_the_flow_write_failed():
    """A swallowed DB error returned HTTP 200 with the file marked 'uploaded'."""
    import pathlib

    router = (
        pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
    ).read_text()
    assert router.count("flow_state write failed for") == 5, (
        "every upload path must surface a failed flow_state write"
    )
    assert '_log.warning("[pnid/upload] flow_state write failed' not in router
    assert router.count('file_status["status"] = "rejected"') >= 5


def test_a_none_from_upsert_is_treated_as_a_failure():
    """flow.upsert_file catches its own errors and returns None."""
    import pathlib

    router = (
        pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
    ).read_text()
    assert router.count("could not be recorded in flow_state") == 6, (
        "every upload must fail loudly when flow_state did not take the row"
    )
