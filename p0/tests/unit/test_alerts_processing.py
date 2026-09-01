from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from p0.source_processing.alerts.alerts_processing import (
    _description_candidates,
    _source_record_id,
    process_alerts_workbook,
    resolve_equipment_id,
)


def test_source_record_id_is_stable_across_upload_batches():
    first = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    second = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    assert first == second
    assert first.startswith("alerts:")


def test_source_record_id_changes_for_different_logical_alert():
    first = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    second = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=2,
        tag_number="P-102",
        date_open="2026-01-02",
    )

    assert first != second

def test_normalized_asset_uses_canonical_pid_equipment_id():
    result = resolve_equipment_id(
        tag_number="G7510",
        description=None,
        pnid_equipment_ids=["G-7510"],
    )

    assert result["equipment_id"] == "G-7510"
    assert result["normalized_asset"] == "G-7510"
    assert result["asset_match_method"] == "tag_number"
    assert result["asset_match_confidence"] == 1.0
    
    
def test_same_index_with_different_tag_number_gets_different_id():
    first = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    second = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-102",
        date_open="2026-01-01",
    )

    assert first != second

def test_description_match_keeps_canonical_normalized_asset():
    result = resolve_equipment_id(
        tag_number=None,
        description="Issue observed near G7510",
        pnid_equipment_ids=["G-7510"],
    )

    assert result["equipment_id"] == "G-7510"
    assert result["normalized_asset"] == "G-7510"
    assert result["asset_match_method"] == "description"
    assert result["asset_match_confidence"] == 0.8
    
def test_same_index_and_tag_with_different_date_open_gets_different_id():
    first = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    second = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-02",
    )

    assert first != second

def test_tag_number_without_hyphen_returns_canonical_pid_equipment_id():
    result = resolve_equipment_id(
        tag_number="G7510",
        description=None,
        pnid_equipment_ids=["G-7510"],
    )

    assert result["equipment_id"] == "G-7510"
    assert result["asset_match_method"] == "tag_number"
    assert result["asset_match_confidence"] == 1.0


def test_alphanumeric_tag_returns_canonical_pid_equipment_id():
    result = resolve_equipment_id(
        tag_number="K2410A",
        description=None,
        pnid_equipment_ids=["K-2410A"],
    )

    assert result["equipment_id"] == "K-2410A"
    assert result["asset_match_method"] == "tag_number"
    assert result["asset_match_confidence"] == 1.0


def test_tag_number_source_value_is_not_used_as_equipment_id_when_pid_has_canonical_format():
    result = resolve_equipment_id(
        tag_number="G7510",
        description="Generator alert",
        pnid_equipment_ids=["G-7510"],
    )

    assert result["equipment_id"] == "G-7510"
    assert result["equipment_id"] != "G7510"
    
def test_source_record_id_stays_same_when_excel_row_changes():
    """
    Excel row number must not be part of the logical alert identity.
    """
    before_row_insert = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    after_row_insert = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    assert before_row_insert == after_row_insert


def test_blank_source_index_can_use_remaining_business_key():
    record_id = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=None,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    assert record_id.startswith("alerts:")


def test_completely_blank_business_key_is_rejected():
    with pytest.raises(
        ValueError,
        match="no usable business identity",
    ):
        _source_record_id(
            plant_code_id="PLANT_A",
            source_index=None,
            tag_number=None,
            date_open=None,
        )


def test_source_record_id_is_different_for_different_plants():
    first = _source_record_id(
        plant_code_id="PLANT_A",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    second = _source_record_id(
        plant_code_id="PLANT_B",
        source_index=1,
        tag_number="P-101",
        date_open="2026-01-01",
    )

    assert first != second


def test_tag_number_is_preserved_when_matching_pid():
    result = resolve_equipment_id(
        tag_number=" p-101 ",
        description="Anything",
        pnid_equipment_ids=["P-101"],
    )

    assert result["equipment_id"] == "P-101"
    assert result["asset_match_method"] == "tag_number"
    assert result["asset_match_confidence"] == 1.0


def test_description_match_is_only_accepted_if_it_exists_in_pid():
    result = resolve_equipment_id(
        tag_number=None,
        description="Issue observed near SDV-2930",
        pnid_equipment_ids=["SDV-2930"],
    )

    assert result["equipment_id"] == "SDV-2930"
    assert result["asset_match_method"] == "description"
    assert result["asset_match_confidence"] == 0.8


def test_description_candidate_is_rejected_when_not_in_pid():
    result = resolve_equipment_id(
        tag_number=None,
        description="Issue observed near SDV-2930",
        pnid_equipment_ids=["P-101"],
    )

    assert result["equipment_id"] is None
    assert result["asset_match_method"] == "description_unmatched"
    assert result["asset_match_confidence"] == 0.0


def test_description_candidate_extraction_is_conservative():
    candidates = _description_candidates(
        "Pump issue at P-101 and valve SDV-2930."
    )

    assert "P-101" in candidates
    assert "SDV-2930" in candidates


def _write_alerts_workbook(
    path: Path,
) -> None:
    open_rows = pd.DataFrame(
        [
            {
                "Index": 1,
                "Region": "R1",
                "Field": "F1",
                "OEM": "OEM",
                "Unit Type": "Pump",
                "Tag Number": "P-101",
                "Priority": "High",
                "Status": "Open",
                "Title": "Open alert",
                "Description": "Pump P-101 issue",
                "Tag": "T1",
                "Current Action": "Inspect",
                "Action Taken": "",
                "Date Open": "2026-01-01",
                "Date closed": None,
                "Equipment Class": "Pump",
                "Subunit": "SU1",
                "Maintainable Item": "MI1",
                "Fault Category": "FC1",
            }
        ]
    )

    closed_rows = pd.DataFrame(
        [
            {
                "Index": 2,
                "Region": "R1",
                "Field": "F1",
                "OEM": "OEM",
                "Unit Type": "Valve",
                "Tag Number": None,
                "Priority": "Low",
                "Status": "Closed",
                "Title": "Closed alert",
                "Description": "Resolved at SDV-2930",
                "Tag": "T2",
                "Current Action": "",
                "Action Taken": "Closed",
                "Date Open": "2026-01-02",
                "Date closed": "2026-01-03",
                "Equipment Class": "Valve",
                "Subunit": "SU2",
                "Maintainable Item": "MI2",
                "Fault Category": "FC2",
            }
        ]
    )

    with pd.ExcelWriter(
        path,
        engine="openpyxl",
    ) as writer:
        open_rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )

        closed_rows.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )


def test_process_alerts_workbook_combines_open_and_closed_rows(
    tmp_path,
):
    workbook = tmp_path / "ALERTS.xlsx"
    _write_alerts_workbook(workbook)

    result = process_alerts_workbook(
        workbook,
        plant_code_id="PLANT_A",
        upload_batch_id="batch-a",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=[
            "P-101",
            "SDV-2930",
        ],
    )

    assert len(result) == 2

    assert set(
        result["alert_status"]
    ) == {
        "open",
        "closed",
    }

    assert result[
        "source_record_id"
    ].notna().all()

    assert result[
        "source_record_id"
    ].is_unique

    assert (
        result["source_system"]
        == "alerts"
    ).all()


def test_same_workbook_different_batch_keeps_same_source_record_ids(
    tmp_path,
):
    workbook = tmp_path / "ALERTS.xlsx"
    _write_alerts_workbook(workbook)

    first = process_alerts_workbook(
        workbook,
        plant_code_id="PLANT_A",
        upload_batch_id="batch-a",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=[
            "P-101",
            "SDV-2930",
        ],
    )

    second = process_alerts_workbook(
        workbook,
        plant_code_id="PLANT_A",
        upload_batch_id="batch-b",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=[
            "P-101",
            "SDV-2930",
        ],
    )

    assert (
        first["upload_batch_id"]
        != second["upload_batch_id"]
    ).all()

    assert first[
        "source_record_id"
    ].tolist() == second[
        "source_record_id"
    ].tolist()


def test_row_insertion_does_not_change_existing_source_record_ids(
    tmp_path,
):
    """
    Regression test for the PR review comment.

    Adding a new row above existing alerts must not change the logical IDs
    of existing alerts.
    """
    original_workbook = tmp_path / "ALERTS_original.xlsx"

    original_rows = pd.DataFrame(
        [
            {
                "Index": 101,
                "Tag Number": "P-101",
                "Date Open": "2026-01-01",
                "Title": "Alert A",
                "Description": "Issue on P-101",
            },
            {
                "Index": 102,
                "Tag Number": "P-102",
                "Date Open": "2026-01-02",
                "Title": "Alert B",
                "Description": "Issue on P-102",
            },
        ]
    )

    empty_closed = pd.DataFrame(
        columns=original_rows.columns
    )

    with pd.ExcelWriter(
        original_workbook,
        engine="openpyxl",
    ) as writer:
        original_rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )

        empty_closed.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )

    original_result = process_alerts_workbook(
        original_workbook,
        plant_code_id="PLANT_A",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=[
            "P-101",
            "P-102",
            "P-999",
        ],
    )

    updated_workbook = tmp_path / "ALERTS_updated.xlsx"

    updated_rows = pd.DataFrame(
        [
            {
                "Index": 999,
                "Tag Number": "P-999",
                "Date Open": "2026-01-03",
                "Title": "New Alert",
                "Description": "New issue",
            },
            {
                "Index": 101,
                "Tag Number": "P-101",
                "Date Open": "2026-01-01",
                "Title": "Alert A",
                "Description": "Issue on P-101",
            },
            {
                "Index": 102,
                "Tag Number": "P-102",
                "Date Open": "2026-01-02",
                "Title": "Alert B",
                "Description": "Issue on P-102",
            },
        ]
    )

    with pd.ExcelWriter(
        updated_workbook,
        engine="openpyxl",
    ) as writer:
        updated_rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )

        empty_closed.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )

    updated_result = process_alerts_workbook(
        updated_workbook,
        plant_code_id="PLANT_A",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=[
            "P-101",
            "P-102",
            "P-999",
        ],
    )

    original_by_index = {
        str(row["source_index"]): row["source_record_id"]
        for _, row in original_result.iterrows()
    }

    updated_by_index = {
        str(row["source_index"]): row["source_record_id"]
        for _, row in updated_result.iterrows()
    }

    assert (
        original_by_index["101"]
        == updated_by_index["101"]
    )

    assert (
        original_by_index["102"]
        == updated_by_index["102"]
    )


def test_duplicate_source_index_is_allowed_when_composite_key_differs(
    tmp_path,
):
    """
    The real ALERTS workbook reuses Index values. Repeated Index values are
    valid when Tag Number or Date Open makes the composite identity unique.
    """
    workbook = tmp_path / "ALERTS_repeated_index.xlsx"

    rows = pd.DataFrame(
        [
            {
                "Index": 1,
                "Tag Number": "P-101",
                "Date Open": "2026-01-01",
                "Title": "Alert A",
                "Description": "Issue A",
            },
            {
                "Index": 1,
                "Tag Number": "P-102",
                "Date Open": "2026-01-01",
                "Title": "Alert B",
                "Description": "Issue B",
            },
        ]
    )

    empty_closed = pd.DataFrame(
        columns=rows.columns
    )

    with pd.ExcelWriter(
        workbook,
        engine="openpyxl",
    ) as writer:
        rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )

        empty_closed.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )

    result = process_alerts_workbook(
        workbook,
        plant_code_id="PLANT_A",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=[
            "P-101",
            "P-102",
        ],
    )

    assert len(result) == 2
    assert result["source_record_id"].is_unique


def test_blank_source_index_is_allowed_when_composite_key_is_usable(
    tmp_path,
):
    workbook = tmp_path / "ALERTS_blank_index.xlsx"

    open_rows = pd.DataFrame(
        columns=[
            "Index",
            "Tag Number",
            "Date Open",
            "Title",
            "Description",
        ]
    )

    closed_rows = pd.DataFrame(
        [
            {
                "Index": None,
                "Tag Number": "SDV-2930",
                "Date Open": "2026-01-03",
                "Title": "Closed alert",
                "Description": "Resolved alert",
            }
        ]
    )

    with pd.ExcelWriter(
        workbook,
        engine="openpyxl",
    ) as writer:
        open_rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )

        closed_rows.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )

    result = process_alerts_workbook(
        workbook,
        plant_code_id="PLANT_A",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=["SDV-2930"],
    )

    assert len(result) == 1
    assert pd.isna(result.iloc[0]["source_index"])
    assert result.iloc[0]["source_record_id"].startswith("alerts:")


def test_duplicate_composite_business_key_is_disambiguated(tmp_path):
    workbook = tmp_path / "alerts_duplicates.xlsx"

    open_rows = pd.DataFrame(
        [
            {
                "Index": "1001",
                "Tag No.": "G7510",
                "Date Open": "2026-01-01",
                "Title": "Duplicate Alert",
                "Description": "Same alert description",
            },
            {
                "Index": "1001",
                "Tag No.": "G7510",
                "Date Open": "2026-01-01",
                "Title": "Duplicate Alert",
                "Description": "Same alert description",
            },
        ]
    )

    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        open_rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )

    result = process_alerts_workbook(
        str(workbook),
        plant_code_id="TEST_PLANT",
        source_file="alerts_duplicates.xlsx",
        pnid_equipment_ids=["G7510"],
    )

    assert len(result) == 2

    assert result["source_record_id"].notna().all()

    assert result["source_record_id"].nunique() == 2
    
    
def test_open_to_closed_keeps_same_source_record_id(
    tmp_path,
):
    open_workbook = tmp_path / "ALERTS_open.xlsx"
    closed_workbook = tmp_path / "ALERTS_closed.xlsx"

    row = {
        "Index": 10,
        "Tag Number": "P-101",
        "Date Open": "2026-01-01",
        "Title": "High vibration",
        "Description": "Pump P-101 issue",
    }

    open_rows = pd.DataFrame([row])
    closed_rows = pd.DataFrame([row])
    empty = pd.DataFrame(columns=open_rows.columns)

    with pd.ExcelWriter(
        open_workbook,
        engine="openpyxl",
    ) as writer:
        open_rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )
        empty.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )

    with pd.ExcelWriter(
        closed_workbook,
        engine="openpyxl",
    ) as writer:
        empty.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )
        closed_rows.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )

    open_result = process_alerts_workbook(
        open_workbook,
        plant_code_id="PLANT_A",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=["P-101"],
    )

    closed_result = process_alerts_workbook(
        closed_workbook,
        plant_code_id="PLANT_A",
        source_file="ALERTS.xlsx",
        pnid_equipment_ids=["P-101"],
    )

    assert open_result.iloc[0]["alert_status"] == "open"
    assert closed_result.iloc[0]["alert_status"] == "closed"
    assert (
        open_result.iloc[0]["source_record_id"]
        == closed_result.iloc[0]["source_record_id"]
    )


def test_empty_formatting_rows_are_ignored(
    tmp_path,
):
    workbook = tmp_path / "ALERTS.xlsx"

    rows = pd.DataFrame(
        [
            {
                "Index": 1,
                "Tag Number": "P-101",
                "Title": "Real alert",
                "Description": "Issue",
            },
            {
                "Index": None,
                "Tag Number": "   ",
                "Title": None,
                "Description": None,
            },
        ]
    )

    empty_closed = pd.DataFrame(
        columns=rows.columns
    )

    with pd.ExcelWriter(
        workbook,
        engine="openpyxl",
    ) as writer:
        rows.to_excel(
            writer,
            sheet_name="Open Alerts",
            index=False,
        )

        empty_closed.to_excel(
            writer,
            sheet_name="Closed Alerts",
            index=False,
        )

    result = process_alerts_workbook(
        workbook,
        plant_code_id="PLANT_A",
        source_file="ALERTS.xlsx",
    )

    assert len(result) == 1