"""A table is read from whichever sheet and header row actually hold its data."""

import pandas as pd
import pytest

from p0.utils.tabular import read_table, read_tables


def _write(path, sheets):
    with pd.ExcelWriter(path) as writer:
        for name, rows in sheets.items():
            pd.DataFrame(rows).to_excel(writer, sheet_name=name, index=False, header=False)


@pytest.fixture
def book(tmp_path):
    def _make(name, sheets):
        path = tmp_path / name
        _write(path, sheets)
        return str(path)

    return _make


def test_an_empty_first_sheet_does_not_hide_the_real_one(book):
    path = book(
        "excursions.xlsx",
        {
            "Sheet1": [[]],
            "Data": [["Tag No.", "Description"], ["K2410B", "gas compressor"]],
        },
    )
    df, sheet, header = read_table(path, ["Tag No."])
    assert sheet == "Data"
    assert header == 0
    assert list(df["Tag No."]) == ["K2410B"]


def test_a_title_block_above_the_header_is_skipped(book):
    path = book(
        "fmea.xlsx",
        {
            "FMEA": [
                ["Failure Mode and Effects Analysis"],
                ["Rev 3", "2026-01-01"],
                ["Component", "Failure Mode", "Effect"],
                ["seal", "leak", "loss of containment"],
            ]
        },
    )
    df, _, header = read_table(path, ["Component", "Failure Mode", "Effect"])
    assert header == 2
    assert list(df.columns) == ["Component", "Failure Mode", "Effect"]
    assert df.iloc[0]["Failure Mode"] == "leak"


def test_the_sheet_naming_the_declared_columns_wins_over_a_bigger_one(book):
    path = book(
        "workorders.xlsx",
        {
            "Notification": [["Notification", "Text"]] + [[f"N{i}", "x"] for i in range(50)],
            "Order": [["Order", "Equipment"], ["4501", "PUMP-0986-SK"]],
        },
    )
    df, sheet, _ = read_table(path, ["Order", "Equipment"])
    assert sheet == "Order"
    assert list(df["Equipment"]) == ["PUMP-0986-SK"]


def test_with_nothing_declared_the_sheet_holding_the_most_rows_wins(book):
    path = book(
        "split.xlsx",
        {
            "Sheet1": [["A", "B"], ["1", "2"]],
            "Sheet1 (2)": [["A", "B"]] + [[str(i), "x"] for i in range(20)],
        },
    )
    _, sheet, _ = read_table(path, [])
    assert sheet == "Sheet1 (2)"


def test_a_single_sheet_file_reads_exactly_as_before(book):
    path = book("plain.xlsx", {"Sheet1": [["Tag", "Value"], ["K2410B", "7"]]})
    df, sheet, header = read_table(path, ["Tag"])
    assert (sheet, header) == ("Sheet1", 0)
    assert df.to_dict("records") == [{"Tag": "K2410B", "Value": "7"}]


def test_a_csv_is_read_whole_and_reports_no_sheet(tmp_path):
    path = tmp_path / "ram.csv"
    path.write_text("Tag No.,MTBF\nP-1310A,4200\n")
    df, sheet, header = read_table(str(path), ["Tag No."])
    assert (sheet, header) == ("", 0)
    assert df.iloc[0]["MTBF"] == "4200"


def test_blank_values_come_back_as_empty_strings_not_nan(book):
    path = book("gaps.xlsx", {"S": [["Tag", "Note"], ["K2410B", None]]})
    df, _, _ = read_table(path, ["Tag"])
    assert df.iloc[0]["Note"] == ""


def test_padding_columns_excel_invents_are_dropped(book):
    path = book("padded.xlsx", {"S": [["Tag", "Note", None], ["K2410B", "ok", None]]})
    df, _, _ = read_table(path, ["Tag"])
    assert list(df.columns) == ["Tag", "Note"]


def test_every_sheet_that_holds_data_is_returned(book):
    path = book(
        "workorders.xlsx",
        {
            "Notification": [["Notification", "Equipment"], ["10001", "P-101"]],
            "Work Order": [["Order", "Equipment"], ["20001", "K-201"]],
        },
    )
    tables = read_tables(path, ["Equipment"])

    assert [sheet for _, sheet, _ in tables] == ["Notification", "Work Order"]


def test_a_sheet_behind_the_first_one_is_no_longer_lost(book):
    path = book(
        "workorders.xlsx",
        {
            "Notification": [["Notification", "Equipment"], ["10001", "P-101"]],
            "Work Order": [["Order", "Equipment"], ["20001", "K-201"]],
        },
    )
    single = read_table(path, ["Equipment"])[1]
    every = {sheet for _, sheet, _ in read_tables(path, ["Equipment"])}

    assert single in every
    assert every - {single}


def test_a_placeholder_sheet_with_no_rows_is_skipped(book):
    path = book(
        "ers.xlsx",
        {
            "Proposed Task List": [["Component", "Task"], ["seal", "inspect"]],
            "U-IMAGe": [[]],
        },
    )
    assert [sheet for _, sheet, _ in read_tables(path, ["Component"])] == [
        "Proposed Task List"
    ]


def test_a_sheet_padded_out_with_blank_rows_is_skipped(book):
    path = book(
        "ers.xlsx",
        {
            "Proposed Task List": [["Component", "Task"], ["seal", "inspect"]],
            "U-Image": [["U-Image"], [""], [""], [""]],
        },
    )
    assert [sheet for _, sheet, _ in read_tables(path, ["Component"])] == [
        "Proposed Task List"
    ]


def test_each_sheet_keeps_its_own_header_row(book):
    path = book(
        "mixed.xlsx",
        {
            "Flat": [["Tag", "Value"], ["P-101", "5"]],
            "Titled": [
                ["Monthly Report"],
                ["Rev 2"],
                ["Tag", "Value"],
                ["K-201", "9"],
            ],
        },
    )
    headers = {sheet: header for _, sheet, header in read_tables(path, ["Tag"])}

    assert headers == {"Flat": 0, "Titled": 2}


def test_a_csv_still_comes_back_as_a_single_table(tmp_path):
    path = tmp_path / "rows.csv"
    path.write_text("Tag,Value\nP-101,5\n")
    tables = read_tables(str(path), ["Tag"])

    assert len(tables) == 1
    assert list(tables[0][0]["Tag"]) == ["P-101"]


def test_reading_one_table_still_picks_a_single_best_sheet(book):
    path = book(
        "workorders.xlsx",
        {
            "Notification": [["Notification", "Equipment"], ["10001", "P-101"]],
            "Work Order": [["Order", "Equipment"], ["20001", "K-201"]],
        },
    )
    df, sheet, header = read_table(path, ["Order", "Equipment"])

    assert sheet == "Work Order"
    assert header == 0
    assert list(df["Order"]) == ["20001"]
