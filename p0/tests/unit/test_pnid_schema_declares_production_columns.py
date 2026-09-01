"""The pnid tables declare the columns production actually writes, not leaving them to the fallback."""

from __future__ import annotations

import pathlib

import yaml

SCHEMA = yaml.safe_load(
    (
        pathlib.Path(__file__).resolve().parents[2]
        / "config"
        / "templates"
        / "_common"
        / "schema.yaml"
    ).read_text()
)["schema"]

PNID_TABLES = ("equipment_pid", "equipment_connectivity")


def test_the_pnid_tables_declare_their_upload_batch():
    for table in PNID_TABLES:
        assert "upload_batch_id" in SCHEMA[table]["columns"], table


def test_the_upload_batch_matches_the_width_every_other_table_uses():
    widths = {
        spec["columns"]["upload_batch_id"]
        for spec in SCHEMA.values()
        if "upload_batch_id" in spec.get("columns", {})
    }
    assert widths == {"VARCHAR(64)"}


def test_the_upload_batch_is_wide_enough_for_a_uuid():
    declared = SCHEMA["equipment_pid"]["columns"]["upload_batch_id"]
    assert int(declared[len("VARCHAR(") : -1]) >= 36


def test_connectivity_keeps_both_of_its_identity_columns():
    columns = SCHEMA["equipment_connectivity"]["columns"]
    assert "conn_uid" in columns
    assert "connectivity_uid" in columns
