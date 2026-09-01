"""Regression tests for the timeseries column-rename target_priority.

Guards two data-quality fixes triggered by real HMU exports:
  - op_limit_l/h: an explicit "L Limit"/"H Limit" must beat the generic
    "Minimum"/"Maximum" (which previously won by column order, so op_limit_l
    wrongly took Minimum=0 and the real limit was dropped).
  - equipment_id: "Asset ID" beats "Equipment Type".

Pure (no DB / IoTDB) — runs the real renamer against the real config.
"""

from __future__ import annotations

import os

import pandas as pd
import yaml

from p0.utils.renamer import apply_column_rename

_CFG_PATH = os.path.join(
    os.path.dirname(__file__),
    "../../config/templates/_common/column_rename/timeseries_column_rename.yaml",
)


def _cfg():
    with open(_CFG_PATH) as f:
        return yaml.safe_load(f)


def _rename(df):
    return apply_column_rename(df, _cfg(), source_name="timeseries", verbose=False)


def test_explicit_l_limit_beats_minimum():
    df = pd.DataFrame(
        {
            "PI Tag": ["X.PV"],
            "Minimum": ["0"],
            "LL Limit": ["660"],
            "L Limit": ["9238"],
            "H Limit": ["17345"],
            "HH Limit": ["23312"],
            "Maximum": ["23312"],
        }
    )
    out = _rename(df)
    assert out["op_limit_l"].iloc[0] == "9238"
    assert out["op_limit_h"].iloc[0] == "17345"
    assert out["op_limit_ll"].iloc[0] == "660"
    assert out["op_limit_hh"].iloc[0] == "23312"


def test_minimum_maximum_fill_when_alone():
    df = pd.DataFrame({"PI Tag": ["X.PV"], "Minimum": ["5"], "Maximum": ["95"]})
    out = _rename(df)
    assert out["op_limit_l"].iloc[0] == "5"
    assert out["op_limit_h"].iloc[0] == "95"


def test_asset_id_beats_equipment_type():
    df = pd.DataFrame(
        {
            "PI Tag": ["X.PV"],
            "Equipment Type": ["U-2300"],
            "Asset ID": ["21-P-1001"],
        }
    )
    out = _rename(df)
    assert out["equipment_id"].iloc[0] == "21-P-1001"
