"""Pins the final CDM to the schema layout the templates actually ship."""

from __future__ import annotations

import pathlib

import yaml

import p0
from p0.pipelines.build_final_cdm import _build_schema_map

_SCHEMA = (
    pathlib.Path(p0.__file__).parent / "config" / "templates" / "_common" / "schema.yaml"
)


def _schema() -> dict:
    return yaml.safe_load(_SCHEMA.read_text())


def test_the_shipped_schema_nests_under_schema_not_tables():
    """A bare get('tables') sees nothing here, which empties every dedupe key."""
    doc = _schema()
    assert "schema" in doc
    assert "tables" not in doc


def test_the_schema_map_reads_the_shipped_layout():
    columns, uid, dedupe = _build_schema_map(_schema())
    assert uid["equipment"] == "equipment_uid"
    assert dedupe["equipment"] == ["plant_code_id", "site", "normalized_asset"]
    assert "normalized_asset" in columns["equipment"]


def test_equipment_dedupe_keys_are_never_empty():
    """_merge_equipment groups by these unfiltered, so an empty list is a ValueError."""
    _, _, dedupe = _build_schema_map(_schema())
    assert dedupe["equipment"]
    assert dedupe["timeseries_metadata"]


def _cdm() -> dict:
    path = _SCHEMA.parent / "cdm_config.yaml"
    return yaml.safe_load(path.read_text())["consolidation"]


def test_the_graph_edge_table_has_a_source_mapping():
    """It was a declared target with no source, so the merged CDM held zero edges."""
    cons = _cdm()
    assert "asset_relationship" in cons["target_tables"]
    mapping = cons["source_files"]["asset_relationship"]
    assert set(mapping) == {"PID", "TS"}
    assert set(mapping) <= set(cons["source_dirs"])


def test_the_declared_edge_paths_are_the_files_the_stages_write():
    mapping = _cdm()["source_files"]["asset_relationship"]
    assert mapping["TS"] == "relationships/ts_relationship.csv"
    assert mapping["PID"] == "relationships/asset_relationship.csv"


def test_every_mapped_source_key_is_a_known_source_dir():
    cons = _cdm()
    known = set(cons["source_dirs"])
    for table, mapping in cons["source_files"].items():
        assert set(mapping) <= known, table
