"""Pins template-driven hierarchy levels and the level a tag links at."""

from __future__ import annotations

import pathlib

import pandas as pd
import yaml

import p0
from p0.source_processing.ts import source_parsers as sp

_RENAME = (
    pathlib.Path(p0.__file__).parent
    / "config"
    / "templates"
    / "_common"
    / "column_rename"
    / "timeseries_column_rename.yaml"
)

_FACILITY = "SARAWAK\\PCSB (SKO)\\FIELD\\PLAT-A\\FACILITIES\\COMPRESSOR\\K-2410A"
_WELL = "SARAWAK\\PCSB (SKO)\\FIELD\\PLAT-B\\WELLS\\WELL 101-LS"


def _pi_spec() -> dict:
    return yaml.safe_load(_RENAME.read_text())["sources"]["osi_pi"]["parse"]


def test_the_shipped_template_declares_the_level_map():
    """Without levels the parser keeps the old one-segment behaviour."""
    spec = _pi_spec()
    assert len(spec["levels"]) == 7
    assert spec["link_preference"][0] == "equipment"
    assert spec["level_confidence"]["equipment"] > spec["level_confidence"]["process_unit"]


def test_every_declared_level_is_named_from_the_path():
    named = sp.hierarchy_levels(_FACILITY, "\\", _pi_spec()["levels"])
    assert named["site"] == "FIELD"
    assert named["area"] == "PLAT-A"
    assert named["asset_class"] == "FACILITIES"
    assert named["equipment"] == "K-2410A"


def test_a_shallower_path_names_fewer_levels_without_inventing_equipment():
    named = sp.hierarchy_levels(_WELL, "\\", _pi_spec()["levels"])
    assert named["process_unit"] == "WELL 101-LS"
    assert "equipment" not in named


def test_the_deepest_available_level_is_chosen():
    spec = _pi_spec()
    pref = spec["link_preference"]
    assert sp.deepest_link(sp.hierarchy_levels(_FACILITY, "\\", spec["levels"]), pref) == "equipment"
    assert sp.deepest_link(sp.hierarchy_levels(_WELL, "\\", spec["levels"]), pref) == "process_unit"


def test_a_well_level_row_links_instead_of_being_dropped(monkeypatch):
    """1,813 rows used to resolve to nothing because they have no equipment segment."""
    spec = _pi_spec()
    frame = pd.DataFrame(
        [
            {"Parent": _FACILITY, "AttributeConfigString": "A.B.241ASV143.PV", "Name": "x"},
            {"Parent": _WELL, "AttributeConfigString": "A.B.101FI104.PV", "Name": "y"},
        ]
    )
    out, report = sp.parse_pi_tags(frame, spec)
    assert list(out["asset_link_level"]) == ["equipment", "process_unit"]
    assert out.at[0, "asset_link_confidence"] > out.at[1, "asset_link_confidence"]
    assert report["link_levels"] == {"equipment": 1, "process_unit": 1}


def test_a_point_without_a_config_string_takes_identity_from_name():
    """55 real PI points carry no AttributeConfigString; they used to lose identity."""
    spec = _pi_spec()
    frame = pd.DataFrame(
        [{"Parent": _WELL, "AttributeConfigString": None, "Name": "Gas Injection|Average|Flowrate A"}]
    )
    out, report = sp.parse_pi_tags(frame, spec)
    assert report["identity_from_name_fallback"] == 1
    assert str(out.at[0, "tag_name"]).startswith("Gas Injection")


def test_a_spec_without_levels_is_left_alone():
    """Back-compat: the level columns only appear when the template asks for them."""
    frame = pd.DataFrame([{"Parent": _FACILITY, "AttributeConfigString": "A.B.C.PV"}])
    out, _ = sp.parse_pi_tags(frame, {"equipment_at_depth": 7})
    assert "asset_link_level" not in out.columns


_SCHEMA = (
    pathlib.Path(p0.__file__).parent / "config" / "templates" / "_common" / "schema.yaml"
)
_ENTITIES = (
    pathlib.Path(p0.__file__).parent / "config" / "templates" / "_common" / "entities.yaml"
)


def _ts_schema_columns() -> dict:
    return yaml.safe_load(_SCHEMA.read_text())["schema"]["timeseries_metadata"]["columns"]


def _ts_entity_attributes() -> dict:
    return yaml.safe_load(_ENTITIES.read_text())["entities"]["timeseries_metadata"]["attributes"]


def test_a_column_needs_both_the_schema_and_the_entity_to_reach_the_cdm():
    """apply_schema drops what the schema lacks; the entity builder drops what it does not declare."""
    columns, attributes = _ts_schema_columns(), _ts_entity_attributes()
    for field in ("asset_link_level", "asset_link_confidence",
                  "asset_match_method", "asset_match_confidence"):
        assert field in columns, f"{field} would be dropped by apply_schema"
        assert field in attributes, f"{field} would be dropped by the entity builder"


def test_the_hierarchy_fields_reach_the_cdm():
    columns, attributes = _ts_schema_columns(), _ts_entity_attributes()
    for field in ("site", "area", "process_unit"):
        assert field in columns
        assert field in attributes


def test_the_hierarchy_is_mapped_onto_the_canonical_columns():
    """The parser emits hier_*; only a rename mapping puts it on the canonical column."""
    mappings = yaml.safe_load(_RENAME.read_text())["sources"]["osi_pi"]["mappings"]
    assert mappings["hier_site"] == "site"
    assert mappings["hier_area"] == "area"
    assert mappings["hier_process_unit"] == "process_unit"


def test_the_downstream_contract_columns_are_untouched():
    """p1 and p2 read these; the hierarchy work is additive and must not move them."""
    columns = _ts_schema_columns()
    for field in ("ts_uid", "plant_code_id", "tag_name", "iotdb_tag_id", "unit",
                  "data_type", "equipment_id", "normalized_asset", "is_active"):
        assert field in columns
