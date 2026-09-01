"""_check_sap_column_mapping_gate is Stage 8 of the SAP pipeline flow: it decides
whether a staged file's columns are trustworthy enough to process, or whether the
pipeline must stop and ask the user to map them via PUT /context/saveSapJoins.

detect_sap_table_at_path is monkeypatched here rather than exercised for real —
these tests pin the gate's own branching (filename match passes, unrecognized
always blocks, and an unrecognized file is not rescued by an unrelated plant
override), independent of the real column-detection heuristics.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import p0.utils.sap_table_detect as sap_table_detect
from p0.pipelines.run_sap_end_to_end import (
    SapColumnMappingRequired,
    _check_sap_column_mapping_gate,
)


def _stage_file(tmp_path: Path, name: str) -> Path:
    p = tmp_path / name
    p.write_text("COL_A,COL_B\n1,2\n", encoding="utf-8")
    return p


def test_unrecognized_file_blocks_the_pipeline(tmp_path, monkeypatch):
    _stage_file(tmp_path, "mystery_export.csv")

    monkeypatch.setattr(
        sap_table_detect,
        "detect_sap_table_at_path",
        lambda path: {"matched_by": "unrecognized", "detected_table": None, "match_score": 0.0},
    )
    monkeypatch.setattr(sap_table_detect, "load_sap_table_to_datasets", lambda: {})

    with pytest.raises(SapColumnMappingRequired):
        _check_sap_column_mapping_gate(str(tmp_path), {}, "plant_1_testcase")


def test_unrecognized_file_blocks_even_with_unrelated_plant_overrides(tmp_path, monkeypatch):
    """Regression: an "unrecognized" file must stop the pipeline unconditionally —
    a join_override for some other dataset on the plant must not vouch for a file
    detection couldn't identify at all."""
    _stage_file(tmp_path, "mystery_export.csv")

    monkeypatch.setattr(
        sap_table_detect,
        "detect_sap_table_at_path",
        lambda path: {"matched_by": "unrecognized", "detected_table": None, "match_score": 0.0},
    )
    monkeypatch.setattr(sap_table_detect, "load_sap_table_to_datasets", lambda: {})

    user_cfg = {
        "sap_processing": {
            "join_overrides": {"plant_1_testcase": {"workorder": {"AUFNR": "AUFNR"}}}
        }
    }

    with pytest.raises(SapColumnMappingRequired):
        _check_sap_column_mapping_gate(str(tmp_path), user_cfg, "plant_1_testcase")


def test_standard_filename_matched_file_passes_without_mapping(tmp_path, monkeypatch):
    _stage_file(tmp_path, "EQUI.csv")

    monkeypatch.setattr(
        sap_table_detect,
        "detect_sap_table_at_path",
        lambda path: {"matched_by": "filename", "detected_table": "EQUI", "match_score": 1.0},
    )
    monkeypatch.setattr(sap_table_detect, "load_sap_table_to_datasets", lambda: {})

    _check_sap_column_mapping_gate(str(tmp_path), {}, "plant_1_testcase")
