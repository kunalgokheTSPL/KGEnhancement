"""PI and PROTEAN rows get an identity and an equipment without losing rows."""

from __future__ import annotations

import io
import os

import pandas as pd
import yaml

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.config import COMMON_DIR
from p0.source_processing.ts.source_parsers import (
    equipment_at_depth,
    equipment_at_index,
    parse_protean,
    parse_pi_tags,
    split_config_string,
    split_hierarchy,
)

RENAME_FILE = COMMON_DIR / "column_rename" / "timeseries_column_rename.yaml"


def _spec(source):
    """The parse rules a source declares."""
    with io.open(RENAME_FILE, encoding="utf-8") as fh:
        return yaml.safe_load(fh)["sources"][source]["parse"]


def test_a_hierarchy_splits_on_its_separator():
    assert split_hierarchy("A\\B\\C", "\\") == ["A", "B", "C"]


def test_a_blank_hierarchy_has_no_segments():
    assert split_hierarchy(None, "\\") == []


def test_a_deep_path_ends_at_its_equipment():
    path = "\\".join(["S", "P", "F", "U", "SYS", "SUB", "EQ-1001"])
    assert equipment_at_depth(path, "\\", 7) == "EQ-1001"


def test_a_shallow_path_yields_no_equipment_rather_than_an_area_name():
    path = "\\".join(["S", "P", "F", "U", "SYS", "GAS DISTRIBUTION"])
    assert equipment_at_depth(path, "\\", 7) is None


def test_equipment_can_sit_at_a_fixed_depth_instead_of_the_end():
    path = "\\".join(["S", "P", "F", "U", "TURBINE-1", "BEARING", "PROBE"])
    assert equipment_at_index(path, "\\", 4) == "TURBINE-1"


def test_a_point_reference_splits_into_a_tag_and_an_attribute():
    assert split_config_string("srv.path.TAG-1.PV", ".") == ("TAG-1", "PV")


def test_a_bare_point_reference_is_all_tag_and_no_attribute():
    assert split_config_string("TAG-ONLY", ".") == ("TAG-ONLY", None)


def test_a_blank_point_reference_yields_nothing():
    assert split_config_string(None, ".") == (None, None)


def _pi_frame():
    deep = "\\".join(["S", "P", "F", "U", "SYS", "SUB", "EQ-1"])
    shallow = "\\".join(["S", "P", "F", "U", "SYS", "AREA"])
    return pd.DataFrame([
        {"Parent": deep, "AttributeConfigString": "srv.TAG-1.PV"},
        {"Parent": shallow, "AttributeConfigString": "srv.TAG-1.AVG"},
        {"Parent": shallow, "AttributeConfigString": "BARE-TAG"},
        {"Parent": deep, "AttributeConfigString": "srv.TAG-1.PV"},
    ])


def test_pi_rows_keep_their_tag_and_attribute():
    out, _ = parse_pi_tags(_pi_frame(), _spec("osi_pi"))
    assert out.loc[0, "pi_tag_id"] == "TAG-1"
    assert out.loc[0, "pi_state"] == "PV"


def test_pi_equipment_resolves_only_on_deep_paths():
    out, report = parse_pi_tags(_pi_frame(), _spec("osi_pi"))
    assert report["equipment_resolved"] == 2
    assert pd.isna(out.loc[1, "equipment_id"])


def test_no_pi_row_is_dropped_even_when_tags_repeat():
    frame = _pi_frame()
    out, _ = parse_pi_tags(frame, _spec("osi_pi"))
    assert len(out) == len(frame)


def test_pi_rows_that_differ_keep_separate_identities():
    out, _ = parse_pi_tags(_pi_frame(), _spec("osi_pi"))
    assert out.loc[0, "tag_name"] != out.loc[1, "tag_name"]


def _protean_frames():
    base = "\\".join(["S", "P", "F", "U", "TURBINE-1", "SUB"])
    return [pd.DataFrame([
        {"Parent": base, "Name": "Vibration", "ObjectType": "Element", "|OperatingValue": "1"},
        {"Parent": base, "Name": "Vibration", "ObjectType": "Element", "|OperatingValue": "2"},
        {"Parent": base, "Name": "Vibration", "ObjectType": "Element", "|OperatingValue": "2"},
        {"Parent": base, "Name": "Calc", "ObjectType": "Analysis", "|OperatingValue": "9"},
    ])]


def test_protean_calculations_are_not_metadata_rows():
    out, report = parse_protean(_protean_frames(), _spec("alerts_protean"))
    assert report["analysis_rows_dropped"] == 1
    assert "Analysis" not in set(out["ObjectType"])


def test_protean_equipment_comes_from_its_fixed_depth():
    out, _ = parse_protean(_protean_frames(), _spec("alerts_protean"))
    assert set(out["equipment_id"]) == {"TURBINE-1"}


def test_protean_rows_that_differ_survive_a_shared_name():
    out, _ = parse_protean(_protean_frames(), _spec("alerts_protean"))
    assert out.loc[0, "tag_name"] != out.loc[1, "tag_name"]


def test_identical_protean_rows_collapse_onto_one_identity():
    out, report = parse_protean(_protean_frames(), _spec("alerts_protean"))
    assert report["collapsed"] == 1
    assert out["tag_name"].nunique() == 2


def test_every_sheet_contributes_its_rows():
    frames = _protean_frames() + _protean_frames()
    out, _ = parse_protean(frames, _spec("alerts_protean"))
    assert len(out) == 6


def test_no_sheets_at_all_is_not_an_error():
    out, report = parse_protean([], _spec("alerts_protean"))
    assert out.empty and report["rows"] == 0


def test_blank_trailer_rows_are_not_metadata():
    base = "\\".join(["S", "P", "F", "U", "TURBINE-1", "SUB"])
    frames = [pd.DataFrame([
        {"Parent": base, "Name": "Vibration", "ObjectType": "Element"},
        {"Parent": None, "Name": None, "ObjectType": None},
    ])]
    out, report = parse_protean(frames, _spec("alerts_protean"))
    assert report["blank_rows_dropped"] == 1
    assert len(out) == 1


def test_a_protean_identity_names_the_equipment_it_belongs_to():
    base = "\\".join(["S", "P", "F", "U", "TURBINE-1", "SUB"])
    frames = [pd.DataFrame([
        {"Parent": base, "Name": "Vibration", "ObjectType": "Element"},
    ])]
    out, _ = parse_protean(frames, _spec("alerts_protean"))
    assert out.loc[0, "tag_name"] == "TURBINE-1.Vibration"


def test_the_same_parameter_on_two_machines_stays_two_tags():
    one = "\\".join(["S", "P", "F", "U", "TURBINE-1", "SUB"])
    two = "\\".join(["S", "P", "F", "U", "TURBINE-2", "SUB"])
    frames = [pd.DataFrame([
        {"Parent": one, "Name": "Vibration", "ObjectType": "Element"},
        {"Parent": two, "Name": "Vibration", "ObjectType": "Element"},
    ])]
    out, _ = parse_protean(frames, _spec("alerts_protean"))
    assert out["tag_name"].nunique() == 2
