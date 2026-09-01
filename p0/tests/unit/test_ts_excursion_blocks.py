"""A block-structured excursion export yields summary rows, not event rows."""

from __future__ import annotations

import os

import pytest
from openpyxl import Workbook

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.source_processing.ts.excursion_blocks import (
    InconsistentBlockEquipmentError,
    read_excursion_blocks,
)

SUMMARY_HEADER = ["Tag", "Boundary", "Point Description", "Priority", "Count",
                  "Percent", "Accum %", "Group", "Boundary Type", "Boundary Value",
                  "Critical Operating Parameter", "Equipments", "Description",
                  "EQUIPMENT"]

SUB_HEADER = ["Timestamp", "State", "Tag", "Boundary", "Unit", "Priority",
              "Comment", "Category", "Point Description",
              "Critical Operating Parameter", "Equipments", "Description",
              "EQUIPMENT", "Group", "Boundary Type", "Boundary Value",
              "PV Value", "Equipment"]


def _summary(tag, boundary, value):
    """One 14-column summary row."""
    return [tag, boundary, "desc", "High", 5, 1.0, 1.0, "G", "Safe Operating Limit",
            value, "Yes", "EQ-PLURAL", "d", "NO EQUIPMENT NAME/TAG"]


def _event(equipment):
    """One nested event row, offset one column to the right."""
    row = [None] * (len(SUB_HEADER) + 1)
    row[len(SUB_HEADER)] = equipment
    row[11] = "EQ-PLURAL-IN-EVENT"
    row[13] = "EQ-CAPS-IN-EVENT"
    return row


def _book(tmp_path, blocks, name="exc.xlsx"):
    """A workbook shaped like the real export: summary, blank, sub-header, events."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Exported Alarm Analysis Data"
    ws.append(SUMMARY_HEADER)
    for summary, equipments in blocks:
        ws.append(summary)
        ws.append([])
        ws.append([None] + SUB_HEADER)
        for equipment in equipments:
            ws.append(_event(equipment))
        ws.append([])
    path = tmp_path / name
    wb.save(path)
    return str(path)


def _two_blocks(tmp_path):
    return _book(tmp_path, [
        (_summary("T1", "T1.SOL-L", 3.0), ["EQ-1", "EQ-1", "EQ-1"]),
        (_summary("T2", "T2.IPF-HH", 90.0), ["EQ-2", "EQ-2"]),
    ])


def test_one_row_per_block(tmp_path):
    out = read_excursion_blocks(_two_blocks(tmp_path))
    assert len(out) == 2


def test_the_event_rows_are_discarded(tmp_path):
    out = read_excursion_blocks(_two_blocks(tmp_path))
    assert out["Tag"].tolist() == ["T1", "T2"]


def test_the_summary_header_names_the_columns(tmp_path):
    out = read_excursion_blocks(_two_blocks(tmp_path))
    for column in SUMMARY_HEADER:
        assert column in out.columns


def test_each_block_carries_its_own_equipment(tmp_path):
    out = read_excursion_blocks(_two_blocks(tmp_path)).set_index("Tag")
    assert out.loc["T1", "alerts_prime_equipment"] == "EQ-1"
    assert out.loc["T2", "alerts_prime_equipment"] == "EQ-2"


def test_the_lifted_column_is_the_singular_one_not_the_plural_or_the_caps(tmp_path):
    out = read_excursion_blocks(_two_blocks(tmp_path)).set_index("Tag")
    assert out.loc["T1", "alerts_prime_equipment"] == "EQ-1"
    assert out.loc["T1", "Equipments"] == "EQ-PLURAL"
    assert out.loc["T1", "EQUIPMENT"] == "NO EQUIPMENT NAME/TAG"


def test_a_block_whose_events_disagree_raises(tmp_path):
    path = _book(tmp_path, [(_summary("T1", "T1.SOL-L", 3.0), ["EQ-1", "EQ-OTHER"])])
    with pytest.raises(InconsistentBlockEquipmentError):
        read_excursion_blocks(path)


def test_a_block_with_no_events_has_no_equipment(tmp_path):
    path = _book(tmp_path, [(_summary("T1", "T1.SOL-L", 3.0), [])])
    out = read_excursion_blocks(path)
    assert out["alerts_prime_equipment"].tolist() == [None]


def test_the_boundary_value_survives_the_parse(tmp_path):
    out = read_excursion_blocks(_two_blocks(tmp_path)).set_index("Tag")
    assert out.loc["T1", "Boundary Value"] == 3.0
    assert out.loc["T2", "Boundary Value"] == 90.0


def test_an_empty_sheet_yields_nothing(tmp_path):
    wb = Workbook()
    wb.active.title = "Exported Alarm Analysis Data"
    path = tmp_path / "empty.xlsx"
    wb.save(path)
    assert read_excursion_blocks(str(path)).empty
