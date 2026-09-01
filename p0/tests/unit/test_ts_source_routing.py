"""The TS entry point routes a workbook to its own source, or leaves it alone."""

from __future__ import annotations

import os

import pandas as pd
import pytest
from openpyxl import Workbook

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.source_processing.ts.ts_processing import run_ts_source_processing

PI = ["Selected(x)", "Parent", "Name", "ObjectType", "Categories",
      "AttributeDataReference", "AttributeConfigString"]

ALARMS = ["Tag", "Alarm", "Count", "Percent", "Accum %", "Priority",
          "Point Description", "Unit", "Limit", "Plant_Hierarchy", "Upper_Equipment"]

PROTEAN = ["Parent", "Name", "ObjectType", "Description", "ProteanLowerLimit",
           "OEMLowerLimit", "OEMUpperLimit", "ProteanUpperLimit",
           "|OperatingValue", "Error"]

GENERIC = ["Tag Name", "Description", "Unit", "Data Type", "Equipment Id"]

DEEP = "\\".join(["S", "P", "F", "U", "SYS", "SUB", "EQ-1"])


def _rows(headers, rows, tmp_path, name="upload.xlsx"):
    """A single-sheet workbook on disk."""
    frame = pd.DataFrame(rows, columns=headers)
    staging = tmp_path / "in"
    staging.mkdir(exist_ok=True)
    frame.to_excel(staging / name, index=False)
    return str(staging)


def _run(staging, tmp_path, name="upload.xlsx"):
    """The entry point, against a throwaway work dir."""
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    return run_ts_source_processing(ts_in=staging, work_dir=str(work), target_file=name)


def test_a_pi_export_is_routed_to_the_pi_source(tmp_path):
    staging = _rows(PI, [["x", DEEP, "N", "Attribute", "c", "PI Point", "srv.TAG-1.PV"]], tmp_path)
    out = _run(staging, tmp_path)
    assert list(out) == ["osi_pi"]


def test_a_routed_pi_frame_already_carries_its_derived_identity(tmp_path):
    staging = _rows(PI, [["x", DEEP, "N", "Attribute", "c", "PI Point", "srv.TAG-1.PV"]], tmp_path)
    frame = _run(staging, tmp_path)["osi_pi"]
    assert frame.loc[0, "pi_tag_id"] == "TAG-1"
    assert frame.loc[0, "pi_state"] == "PV"
    assert frame.loc[0, "equipment_id"] == "EQ-1"
    assert frame.loc[0, "tag_name"] == "TAG-1.PV"


def test_an_alarms_export_is_routed_to_the_alarms_source(tmp_path):
    staging = _rows(ALARMS, [["T1", "HI", 1, 0.1, 0.1, "HIGH", "d", "U", 5, "a.b", "%X"]], tmp_path)
    assert list(_run(staging, tmp_path)) == ["alerts_prime_alarms"]


def test_a_protean_export_is_routed_and_its_analyses_dropped(tmp_path):
    staging = _rows(PROTEAN, [
        [DEEP, "Vibration", "Element", "d", 1, "a", "b", 2, "=ref", None],
        [DEEP, "Calc", "Analysis", "d", 1, "a", "b", 2, "=ref", None],
    ], tmp_path)
    frame = _run(staging, tmp_path)["alerts_protean"]
    assert len(frame) == 1
    assert frame.loc[0, "equipment_id"] == "SYS"


def test_every_routed_frame_records_where_it_came_from(tmp_path):
    staging = _rows(ALARMS, [["T1", "HI", 1, 0.1, 0.1, "HIGH", "d", "U", 5, "a.b", "%X"]], tmp_path)
    frame = _run(staging, tmp_path)["alerts_prime_alarms"]
    assert frame.loc[0, "source_system"] == "alerts_prime_alarms"
    assert frame.loc[0, "source_file"] == "upload.xlsx"


def test_an_ordinary_export_still_takes_the_original_path(tmp_path):
    staging = _rows(GENERIC, [["T1", "d", "U", "FLOAT", "EQ-1"]], tmp_path)
    out = _run(staging, tmp_path)
    assert list(out) == ["timeseries"]
    assert "source_system" not in out["timeseries"].columns


def test_a_file_that_is_not_tag_metadata_at_all_is_still_refused(tmp_path):
    staging = _rows(["Colour", "Shape"], [["red", "round"]], tmp_path)
    with pytest.raises(ValueError, match="does not look like a timeseries"):
        _run(staging, tmp_path)
