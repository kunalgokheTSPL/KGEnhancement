"""Choosing the equipment id column on the P&ID reference."""

import pandas as pd
import yaml

from p0.utils.ts_asset_identity import enrich_timeseries_asset_identity


def _reference(tmp_path, equipment_id_values):
    path = str(tmp_path / "equipment.parquet")
    pd.DataFrame(
        {
            "normalized_asset": ["K-2410A", "P-1001"],
            "equipment_tag": ["K2410A", "P1001"],
            "equipment_id": equipment_id_values,
        }
    ).to_parquet(path)
    return path


def _tags():
    return pd.DataFrame(
        {"tag_name": ["K-2410A.PV", "P-1001.PV"], "normalized_asset": ["", ""]}
    )


def test_an_empty_equipment_id_column_does_not_blank_the_match(tmp_path):
    path = _reference(tmp_path, ["", ""])
    out = enrich_timeseries_asset_identity(_tags(), reference_candidates=[path])
    ids = out["equipment_id"].astype("string").fillna("").str.strip()
    assert ids.ne("").any()


def test_a_populated_equipment_id_column_is_still_preferred(tmp_path):
    path = _reference(tmp_path, ["EQ-1", "EQ-2"])
    out = enrich_timeseries_asset_identity(_tags(), reference_candidates=[path])
    ids = set(out["equipment_id"].astype("string").fillna("").str.strip())
    assert ids & {"EQ-1", "EQ-2"}


def test_a_pid_only_plant_can_still_reach_an_equipment_id():
    entities = yaml.safe_load(open("p0/config/templates/_common/entities.yaml"))
    ents = entities.get("entities") or entities
    refs = ents["equipment"]["attributes"]["equipment_id"]["from"]
    assert "pid.equipment_tag" in refs
    assert refs.index("sap_assets.equipment_id") < refs.index("pid.equipment_tag")
