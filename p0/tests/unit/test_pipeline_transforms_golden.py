"""Golden tests for the shared deterministic pipeline transforms.

apply_column_rename / apply_schema / apply_derived_fields / apply_identity_rules are the
config-driven DataFrame steps that EVERY stage (sap/pnid/ts/docs) runs. Pure pandas
in/out — pinning the exact reshaping here catches a regression in the shared transform
layer that would otherwise only surface as wrong data deep in a pipeline run.
"""

from __future__ import annotations

import pandas as pd

from p0.utils.derived import apply_derived_fields
from p0.utils.identity import apply_identity_rules
from p0.utils.renamer import apply_column_rename
from p0.utils.schema_apply import apply_schema

_SCHEMA = {
    "schema": {
        "equipment": {
            "columns": {"equipment_id": "text", "name": "text", "created_at": "TIMESTAMP"}
        }
    }
}


def test_apply_column_rename_maps_configured_columns_and_keeps_the_rest():
    cfg = {"sources": {"sap": {"mappings": {"EQUNR": "equipment_id", "EQKTX": "description"}}}}
    df = pd.DataFrame({"EQUNR": ["E1"], "EQKTX": ["Pump"], "OTHER": ["x"]})
    out = apply_column_rename(df, cfg, "sap", verbose=False)
    assert "equipment_id" in out.columns
    assert "description" in out.columns
    assert "OTHER" in out.columns  # unmapped columns preserved
    assert out.iloc[0]["equipment_id"] == "E1"


def test_apply_column_rename_is_a_noop_on_empty():
    assert apply_column_rename(pd.DataFrame(), {}, "sap", verbose=False).empty


def test_apply_schema_reorders_adds_missing_and_drops_extra():
    df = pd.DataFrame({"name": ["Pump"], "equipment_id": ["E1"], "extra": ["drop me"]})
    out = apply_schema(df, _SCHEMA, "equipment")
    assert list(out.columns) == ["equipment_id", "name", "created_at"]  # schema order
    assert "extra" not in out.columns
    assert out.iloc[0]["equipment_id"] == "E1"
    assert pd.isna(out.iloc[0]["created_at"])  # missing col added; TIMESTAMP coerced to NaT


def test_apply_schema_returns_input_for_an_unknown_table():
    df = pd.DataFrame({"a": [1]})
    assert list(apply_schema(df, _SCHEMA, "nope").columns) == ["a"]


def test_apply_derived_fields_adds_plant_and_audit_timestamps():
    out = apply_derived_fields(
        pd.DataFrame({"equipment_id": ["E1"]}), {}, "equipment", plant_code_id="plant_1"
    )
    assert out.iloc[0]["plant_code_id"] == "plant_1"
    assert "ingested_at" in out.columns
    assert "updated_at" in out.columns


def test_apply_derived_fields_backfills_plant_from_a_sap_werks_column(monkeypatch):
    monkeypatch.delenv("PLANT_CODE", raising=False)
    out = apply_derived_fields(
        pd.DataFrame({"equipment_id": ["E1"], "WERKS": ["1000"]}),
        {},
        "equipment",
        plant_code_id=None,
    )
    assert out.iloc[0]["plant_code_id"] == "1000"


def test_apply_identity_rules_derives_floc_and_normalized_asset():
    df = pd.DataFrame({"functional_location": ["PLANT-A-01"], "equipment_id": ["E1"]})
    out = apply_identity_rules(df, {}, "equipment")
    assert "floc" in out.columns and out.iloc[0]["floc"]
    assert "normalized_asset" in out.columns and out.iloc[0]["normalized_asset"]
