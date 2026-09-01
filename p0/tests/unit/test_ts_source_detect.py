"""Detection must name the right timeseries source, or none at all."""

from __future__ import annotations

import os

import pandas as pd
import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.source_processing.ts.source_detect import (
    AmbiguousSourceError,
    detect_ts_source,
    load_signatures,
)

PI_HEADERS = ["Selected(x)", "Parent", "Name", "ObjectType", "Categories",
              "AttributeDataReference", "AttributeConfigString"]

ALARM_HEADERS = ["Tag", "Alarm", "Count", "Percent", "Accum %", "Priority",
                 "Point Description", "Unit", "Limit", "Plant_Hierarchy",
                 "Upper_Equipment"]

EXCURSION_HEADERS = ["Tag", "Boundary", "Point Description", "Priority", "Count",
                     "Percent", "Accum %", "Group", "Boundary Type",
                     "Boundary Value", "Critical Operating Parameter",
                     "Equipments", "Description", "EQUIPMENT"]

PROTEAN_HEADERS = ["Parent", "Name", "ObjectType", "Description",
                   "ProteanLowerLimit", "OEMLowerLimit", "OEMUpperLimit",
                   "ProteanUpperLimit", "|OperatingValue", "Error", None]

GENERIC_TS_HEADERS = ["Tag Name", "Description", "Unit", "Data Type",
                      "Equipment Id", "Op Limit L", "Op Limit H"]


def _book(tmp_path, sheets, name="upload.xlsx", lead_blank_rows=0):
    """A workbook on disk whose sheets carry the given headers."""
    path = tmp_path / name
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, headers in sheets.items():
            frame = pd.DataFrame([["v"] * len(headers)], columns=headers)
            frame.to_excel(
                writer, sheet_name=sheet_name, index=False,
                startrow=lead_blank_rows,
            )
    return str(path)


def test_the_pi_export_is_recognised(tmp_path):
    assert detect_ts_source(_book(tmp_path, {"Final": PI_HEADERS}))["source"] == "osi_pi"


def test_the_alarms_export_is_recognised(tmp_path):
    got = detect_ts_source(_book(tmp_path, {"Sheet1": ALARM_HEADERS}))
    assert got["source"] == "alerts_prime_alarms"


def test_the_excursions_export_is_recognised(tmp_path):
    got = detect_ts_source(_book(tmp_path, {"Exported Alarm Analysis Data": EXCURSION_HEADERS}))
    assert got["source"] == "alerts_prime_excursions"


def test_the_protean_export_is_recognised(tmp_path):
    got = detect_ts_source(_book(tmp_path, {"A": PROTEAN_HEADERS}))
    assert got["source"] == "alerts_protean"


def test_protean_is_found_on_every_sheet_that_carries_it(tmp_path):
    got = detect_ts_source(
        _book(tmp_path, {"A": PROTEAN_HEADERS, "B": PROTEAN_HEADERS, "C": PROTEAN_HEADERS})
    )
    assert len(got["sheets"]) == 3


def test_an_ordinary_timeseries_file_is_left_to_the_existing_path(tmp_path):
    assert detect_ts_source(_book(tmp_path, {"Sheet1": GENERIC_TS_HEADERS})) is None


def test_detection_does_not_depend_on_the_sheet_name(tmp_path):
    got = detect_ts_source(_book(tmp_path, {"anything at all": ALARM_HEADERS}))
    assert got["source"] == "alerts_prime_alarms"


def test_a_header_below_the_first_row_is_still_found(tmp_path):
    got = detect_ts_source(
        _book(tmp_path, {"S": EXCURSION_HEADERS}, lead_blank_rows=4)
    )
    assert got["source"] == "alerts_prime_excursions"
    assert got["sheets"][0]["header_row"] == 4


def test_a_non_excel_upload_is_not_claimed(tmp_path):
    path = tmp_path / "tags.csv"
    path.write_text("Tag,Alarm,Plant_Hierarchy,Upper_Equipment\n1,2,3,4\n")
    assert detect_ts_source(str(path)) is None


def test_two_sources_claiming_one_workbook_raises_rather_than_guessing(tmp_path):
    sigs = {
        "one": {"required_headers": ["Tag", "Alarm"]},
        "two": {"required_headers": ["Tag", "Limit"]},
    }
    book = _book(tmp_path, {"S": ["Tag", "Alarm", "Limit"]})
    with pytest.raises(AmbiguousSourceError):
        detect_ts_source(book, signatures=sigs)


def test_a_signature_with_no_required_headers_never_matches(tmp_path):
    sigs = {"empty": {"required_headers": []}}
    assert detect_ts_source(_book(tmp_path, {"S": ALARM_HEADERS}), signatures=sigs) is None


def test_every_declared_signature_names_headers_to_match_on():
    sigs = load_signatures()
    assert sigs
    for key, sig in sigs.items():
        assert sig.get("required_headers"), f"{key} would match nothing"


def test_no_signature_is_a_subset_of_another():
    sigs = load_signatures()
    sets = {k: {h.lower().replace(" ", "") for h in v["required_headers"]} for k, v in sigs.items()}
    for key, tokens in sets.items():
        for other, other_tokens in sets.items():
            if key != other:
                assert not tokens <= other_tokens, f"{key} is a subset of {other}"
